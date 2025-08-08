print("App started — hardened with read-first + pagination + full read", flush=True)
from flask import Flask, request, jsonify
import requests
import os
import unicodedata
import io
import zstandard as zstd
import base64
import logging
import time

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
READ_TIMEOUT = 600  # seconds until read context expires

# --- Whitelisted tables & fields ---
ALLOWED_FIELDS = {
    "cleanlight_canvas": ["cognition", "mir", "insight", "codex", "images"],
    "cleanlight_map": ["cognition", "mir", "insight", "codex", "images", "pointer_net", "macro_group"]
}
ALLOWED_TABLES = set(ALLOWED_FIELDS.keys())

# ------------------ LOGGING & MERGE ------------------
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

# ------------------ BASE ALPHABETS ------------------
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

# ------------------ ENCODING/DECODING HELPERS ------------------
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

# ------------------ FIELD PROCESSING ------------------
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
        elif key in ("mir", "codex", "insight") and val is not None:
            if encode:
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
        elif key in ("mir", "codex", "insight") and row[key]:
            row[key] = decode_std1k(row[key])
    return row

def enforce_read_first():
    if not READ_CONTEXT["loaded"] or (time.time() - READ_CONTEXT["timestamp"] > READ_TIMEOUT):
        raise PermissionError("Must read cleanlight_canvas and cleanlight_map before modifying data.")

# ------------------ SUPABASE CRUD ------------------
@app.route('/supa/select_full_table', methods=['GET'])
def supa_select_full_table():
    table = request.args.get('table')
    if table not in ALLOWED_TABLES:
        return jsonify({"error": "Table not allowed"}), 400

    all_rows = []
    limit = 1000  # max chunk size per request
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
            break  # no more rows

        # Decode each row from STD1K/STD10K for API output
        chunk = [decode_row_for_api(row) for row in chunk]
        all_rows.extend(chunk)

        if len(chunk) < limit:
            break  # last page
        offset += limit

    READ_CONTEXT["loaded"] = True
    READ_CONTEXT["timestamp"] = time.time()

    return jsonify({"data": all_rows}), 200


@app.route('/supa/select', methods=['GET'])
def supa_select():
    table = request.args.get('table')
    if table not in ALLOWED_TABLES:
        return jsonify({"error": "Table not allowed"}), 400

    try:
        limit = min(int(request.args.get('limit', 100)), 100)
    except ValueError:
        limit = 100
    offset = int(request.args.get('offset', 0))

    url = f"{SUPABASE_URL}/rest/v1/{table}?limit={limit}&offset={offset}"
    r = requests.get(url, headers=HEADERS)

    try:
        data = r.json()
        if isinstance(data, dict) and data.get('message'):
            return jsonify({"error": data['message']}), 400
    except Exception as e:
        return jsonify({"error": f"Failed to parse Supabase response: {str(e)}"}), 400

    if not isinstance(data, list):
        return jsonify({"error": "Supabase did not return a list"}), 400

    data = [decode_row_for_api(row) for row in data]

    # mark context loaded if reading both tables
    if table in ("cleanlight_canvas", "cleanlight_map"):
        READ_CONTEXT["loaded"] = True
        READ_CONTEXT["timestamp"] = time.time()

    return jsonify({"data": data}), 200

@app.route('/supa/select_full', methods=['GET'])
def supa_select_full():
    table = request.args.get('table')
    full_flag = request.args.get('full_read', 'false').lower() == 'true'

    # Only allow specific tables
    ALLOWED_TABLES = {"cleanlight_canvas", "cleanlight_map"}
    if table not in ALLOWED_TABLES:
        return jsonify({"error": "Table not allowed"}), 400

    # Require explicit flag to avoid accidental heavy reads
    if not full_flag:
        return jsonify({"error": "Must set full_read=true to use this endpoint"}), 400

    # Build URL with full_read param to force backend allowance
    url = f"{SUPABASE_URL}/rest/v1/{table}?full_read=true"

    try:
        r = requests.get(url, headers=HEADERS)
    except requests.RequestException as e:
        return jsonify({"error": f"Failed to connect to Supabase: {str(e)}"}), 500

    try:
        data = r.json()
        if isinstance(data, dict) and data.get('message'):
            return jsonify({"error": data['message']}), 400
    except Exception as e:
        return jsonify({"error": f"Failed to parse Supabase response: {str(e)}"}), 400

    if not isinstance(data, list):
        return jsonify({"error": "Supabase did not return a list"}), 400

    # Decode all STD1K/STD10K fields for readability
    data = [decode_row_for_api(row) for row in data]

    # Track context (optional)
    import time
    READ_CONTEXT["loaded"] = True
    READ_CONTEXT["timestamp"] = time.time()

    return jsonify({"data": data}), 200

@app.route('/supa/insert', methods=['POST'])
def supa_insert():
    table = extract_table(request)
    if table not in ALLOWED_TABLES:
        return jsonify({"error": "Table not allowed"}), 400
    enforce_read_first()

    raw = getattr(request, "merged_json", request.get_json(force=True) or {})
    if isinstance(raw, dict):
        raw.pop("table", None)
    if "fields" in raw and isinstance(raw["fields"], dict):
        raw = raw["fields"]

    try:
        encoded_row = process_fields(raw, encode=True, table=table)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    url = f"{SUPABASE_URL}/rest/v1/{table}"
    r = requests.post(url, headers=HEADERS, json=encoded_row)
    return (r.text, r.status_code, r.headers.items())

@app.route('/supa/update', methods=['PATCH'])
def supa_update():
    table = extract_table(request)
    if table not in ALLOWED_TABLES:
        return jsonify({"error": "Table not allowed"}), 400
    enforce_read_first()

    col = request.args.get('col')
    val = request.args.get('val')
    if not (col and val):
        return jsonify({"error": "Missing params"}), 400

    raw = getattr(request, "merged_json", request.get_json(force=True) or {})
    update_data = raw.get("fields", raw) if raw else {}
    if isinstance(update_data, dict):
        update_data.pop("table", None)

    try:
        encoded_data = process_fields(update_data, encode=True, table=table)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    url = f"{SUPABASE_URL}/rest/v1/{table}?{col}=eq.{val}"
    r = requests.patch(url, headers=HEADERS, json=encoded_data)
    return (r.text, r.status_code, r.headers.items())

@app.route('/supa/delete', methods=['DELETE'])
def supa_delete():
    table = request.args.get('table')
    if table not in ALLOWED_TABLES:
        return jsonify({"error": "Table not allowed"}), 400
    enforce_read_first()

    col = request.args.get('col')
    val = request.args.get('val')
    if not (col and val):
        return jsonify({"error": "Missing params"}), 400

    url = f"{SUPABASE_URL}/rest/v1/{table}?{col}=eq.{val}"
    r = requests.delete(url, headers=HEADERS)
    return (r.text, r.status_code, r.headers.items())

# ------------------ DECODERS ------------------
@app.route('/decode1k', methods=['POST'])
def decode1k():
    req = request.get_json(force=True)
    value = req.get('encoded')
    if not value:
        return jsonify({"error": "Missing encoded value"}), 400
    try:
        decoded = decode_std1k(value)
        return jsonify({"decoded": decoded}), 200
    except Exception as e:
        return jsonify({"error": f"Failed to decode: {str(e)}"}), 400

@app.route('/decode10k', methods=['POST'])
def decode10k():
    req = request.get_json(force=True)
    value = req.get('encoded')
    if not value:
        return jsonify({"error": "Missing encoded value"}), 400
    try:
        decoded = decode_std10k(value)
        return jsonify({"decoded": base64.b64encode(decoded).decode('ascii')}), 200
    except Exception as e:
        return jsonify({"error": f"Failed to decode: {str(e)}"}), 400

@app.route('/')
def index():
    return "Cleanlight 2.1 hardened API with read-first + pagination + full-read is live.", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)




