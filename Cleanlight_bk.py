print("App started — hardened with read-first + pagination + full read + append", flush=True)
from flask import Flask, request, jsonify
import requests
import os
import unicodedata
import zstandard as zstd
import base64
import logging
import time
import json

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Accept": "application/json"
}

# --- Global read-first state ---
READ_CONTEXT = {"loaded": False, "timestamp": 0}
READ_TIMEOUT = 600

# --- Allowed tables and fields ---
ALLOWED_FIELDS = {
    "cleanlight_canvas": ["cognition", "mir", "insight", "codex", "images"],
    "cleanlight_map": ["cognition", "mir", "insight", "codex", "images", "pointer_net", "macro_group"]
}
ALLOWED_TABLES = set(ALLOWED_FIELDS.keys())

# ------------------ Logging & Merge ------------------
@app.before_request
def log_and_merge():
    app.logger.info(f"{request.method} {request.path} args={dict(request.args)} body={request.get_data(as_text=True)}")
    if request.method in ['POST', 'PATCH']:
        try:
            body = request.get_json(force=True, silent=True) or {}
            merged = {**request.args.to_dict(), **body}
            request.merged_json = merged
        except Exception:
            request.merged_json = request.args.to_dict()

def extract_table(request):
    table = request.args.get('table')
    if not table:
        try:
            body = request.get_json(silent=True) or {}
            table = body.get('table')
        except Exception:
            table = None
    return table

# ------------------ Base Alphabets ------------------
def get_base_alphabet(n):
    safe = []
    for codepoint in range(0x20, 0x2FFFF):
        ch = chr(codepoint)
        name = unicodedata.name(ch, "")
        if (
            0xD800 <= codepoint <= 0xDFFF or
            0xFDD0 <= codepoint <= 0xFDEF or
            codepoint & 0xFFFE == 0xFFFE or
            "CONTROL" in name or
            "PRIVATE USE" in name or
            "COMBINING" in name or
            "FORMAT" in name or
            name == ""
        ):
            continue
        safe.append(ch)
        if len(safe) == n:
            break
    return ''.join(safe)

BASE1K = get_base_alphabet(1000)
BASE10K = get_base_alphabet(10000)

# ------------------ Encoding/Decoding Helpers ------------------
def int_to_baseN(num, alphabet):
    if num == 0:
        return alphabet[0]
    base = len(alphabet)
    digits = []
    while num:
        digits.append(alphabet[num % base])
        num //= base
    return ''.join(reversed(digits))

def baseN_to_int(s, alphabet):
    base = len(alphabet)
    alpha_map = {ch: i for i, ch in enumerate(alphabet)}
    num = 0
    for ch in s:
        num = num * base + alpha_map[ch]
    return num

def encode_std1k(plaintext: str) -> str:
    cctx = zstd.ZstdCompressor()
    compressed = cctx.compress(plaintext.encode('utf-8'))
    as_int = int.from_bytes(compressed, 'big')
    return int_to_baseN(as_int, BASE1K)

def decode_std1k(std1k_str: str) -> str:
    as_int = baseN_to_int(std1k_str, BASE1K)
    num_bytes = (as_int.bit_length() + 7) // 8
    compressed = as_int.to_bytes(num_bytes, 'big')
    dctx = zstd.ZstdDecompressor()
    return dctx.decompress(compressed).decode('utf-8')

def encode_std10k(image_bytes: bytes) -> str:
    cctx = zstd.ZstdCompressor()
    compressed = cctx.compress(image_bytes)
    as_int = int.from_bytes(compressed, 'big')
    return int_to_baseN(as_int, BASE10K)

def decode_std10k(std10k_str: str) -> bytes:
    as_int = baseN_to_int(std10k_str, BASE10K)
    num_bytes = (as_int.bit_length() + 7) // 8
    compressed = as_int.to_bytes(num_bytes, 'big')
    dctx = zstd.ZstdDecompressor()
    return dctx.decompress(compressed)

# ------------------ Field Processing ------------------
def process_fields(data, encode=True, table=None):
    processed = {}
    if table not in ALLOWED_TABLES:
        raise ValueError("Table not allowed")

    for key, val in data.items():
        if key not in ALLOWED_FIELDS[table]:
            raise ValueError(f"Field {key} not allowed in table {table}")
        if key == "cognition":
            processed[key] = val
        elif key == "images" and val is not None:
            if encode:
                if isinstance(val, str):
                    image_bytes = base64.b64decode(val)
                else:
                    image_bytes = val
                processed[key] = encode_std10k(image_bytes)
            else:
                processed[key] = val
        elif key in ("mir", "codex", "insight", "pointer_net", "macro_group") and val is not None:
            if encode and isinstance(val, str):
                processed[key] = encode_std1k(val)
            else:
                processed[key] = val
        else:
            processed[key] = val
    return processed

def decode_row_for_api(row):
    for key in row:
        if key == "cognition":
            continue
        elif key == "images" and row[key]:
            image_bytes = decode_std10k(row[key])
            row[key] = base64.b64encode(image_bytes).decode('ascii')
        elif key in ("mir", "codex", "insight", "pointer_net", "macro_group") and row[key]:
            try:
                row[key] = decode_std1k(row[key])
            except Exception:
                pass
    return row

def enforce_read_first():
    if not READ_CONTEXT["loaded"] or (time.time() - READ_CONTEXT["timestamp"] > READ_TIMEOUT):
        return jsonify({"error": "Must read cleanlight_canvas and cleanlight_map before modifying data."}), 403

# ------------------ CRUD ------------------
@app.route('/supa/select_full_table', methods=['GET'])
def supa_select_full_table():
    table = request.args.get('table')
    if table not in ALLOWED_TABLES:
        return jsonify({"error": "Table not allowed"}), 400

    all_rows = []
    limit = 1000
    offset = 0

    while True:
        url = f"{SUPABASE_URL}/rest/v1/{table}?limit={limit}&offset={offset}"
        try:
            r = requests.get(url, headers=HEADERS, timeout=60)
            r.raise_for_status()
            chunk = r.json()
        except Exception as e:
            return jsonify({"error": f"Failed to fetch Supabase: {str(e)}"}), 500

        if not isinstance(chunk, list):
            return jsonify({"error": "Supabase did not return a list"}), 400

        if not chunk:
            break

        chunk = [decode_row_for_api(row) for row in chunk]
        all_rows.extend(chunk)

        if len(chunk) < limit:
            break
        offset += limit

    READ_CONTEXT["loaded"] = True
    READ_CONTEXT["timestamp"] = time.time()
    return jsonify({"data": all_rows}), 200

@app.route('/supa/append', methods=['PATCH'])
def supa_append():
    table = extract_table(request)
    if table not in ALLOWED_TABLES:
        return jsonify({"error": "Table not allowed"}), 400
    err = enforce_read_first()
    if err:
        return err

    col = request.args.get('col')
    val = request.args.get('val')
    if not (col and val):
        return jsonify({"error": "Missing params"}), 400

    # Fetch existing row
    try:
        r = requests.get(f"{SUPABASE_URL}/rest/v1/{table}?{col}=eq.{val}", headers=HEADERS, timeout=30)
        existing_rows = r.json()
    except Exception as e:
        return jsonify({"error": f"Failed to fetch existing row: {str(e)}"}), 500

    if not existing_rows:
        return jsonify({"error": "Row not found"}), 404

    existing = decode_row_for_api(existing_rows[0])

    # Merge logic
    raw = getattr(request, "merged_json", request.get_json(force=True) or {})
    new_data = raw.get("fields", raw)

    for k, v in new_data.items():
        if k in existing and existing[k] and v:
            try:
                if k == "pointer_net":
                    old_list = json.loads(existing[k]) if isinstance(existing[k], str) else existing[k]
                    new_list = json.loads(v) if isinstance(v, str) else v
                    merged = list(dict.fromkeys(old_list + new_list))  # dedupe
                    existing[k] = json.dumps(merged)
                elif isinstance(existing[k], str):
                    existing[k] += "\n" + v
                else:
                    existing[k] = v
            except Exception:
                existing[k] = v
        else:
            existing[k] = v

    encoded = process_fields(existing, encode=True, table=table)

    # Write back
    url = f"{SUPABASE_URL}/rest/v1/{table}?{col}=eq.{val}"
    r = requests.patch(url, headers=HEADERS, json=encoded)
    return (r.text, r.status_code, r.headers.items())

@app.route('/')
def index():
    return "Cleanlight API with append support is live.", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
