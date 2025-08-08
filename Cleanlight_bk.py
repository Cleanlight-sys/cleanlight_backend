print("App started — hardened version", flush=True)
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

# ------------------ GLOBAL STATE ------------------
# Simple in-memory context read enforcement
_context_reads = {"canvas": False, "map": False, "timestamp": None}
_CONTEXT_TIMEOUT = 600  # seconds

def _reset_context_reads():
    _context_reads["canvas"] = False
    _context_reads["map"] = False
    _context_reads["timestamp"] = None

def _context_is_ready():
    if not (_context_reads["canvas"] and _context_reads["map"]):
        return False
    if _context_reads["timestamp"] and (time.time() - _context_reads["timestamp"] > _CONTEXT_TIMEOUT):
        _reset_context_reads()
        return False
    return True

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
_ALLOWED_FIELDS = {"cognition", "mir", "insight", "codex", "images"}
_ALLOWED_TABLES = {"cleanlight_canvas", "cleanlight_map"}

def process_fields(data, encode=True):
    processed = {}
    for key, val in data.items():
        if key not in _ALLOWED_FIELDS:
            continue  # skip unexpected fields
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

# ------------------ SUPABASE CRUD ------------------
@app.route('/supa/select', methods=['GET'])
def supa_select():
    table = request.args.get('table')
    if table not in _ALLOWED_TABLES:
        return jsonify({"error": "Invalid table"}), 400
    limit = min(int(request.args.get('limit', 10)), 100)
    url = f"{SUPABASE_URL}/rest/v1/{table}?limit={limit}"
    r = requests.get(url, headers=HEADERS)
    data = r.json()
    if not isinstance(data, list):
        return jsonify({"error": "Supabase did not return a list"}), 400
    data = [decode_row_for_api(row) for row in data]
    # mark read state
    if table == "cleanlight_canvas":
        _context_reads["canvas"] = True
    elif table == "cleanlight_map":
        _context_reads["map"] = True
    if _context_reads["canvas"] and _context_reads["map"]:
        _context_reads["timestamp"] = time.time()
    return jsonify({"data": data}), 200

def _reject_if_no_context():
    if not _context_is_ready():
        return jsonify({"error": "Must read cleanlight_canvas and cleanlight_map before modifying data."}), 400
    return None

@app.route('/supa/insert', methods=['POST'])
def supa_insert():
    if (err := _reject_if_no_context()):
        return err
    table = extract_table(request)
    if table not in _ALLOWED_TABLES:
        return jsonify({"error": "Invalid table"}), 400
    raw = getattr(request, "merged_json", request.get_json(force=True) or {})
    if isinstance(raw, dict):
        raw.pop("table", None)
    if "fields" in raw and isinstance(raw["fields"], dict):
        raw = raw["fields"]
    encoded_row = process_fields(raw)
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    r = requests.post(url, headers=HEADERS, json=encoded_row)
    return (r.text, r.status_code, r.headers.items())

@app.route('/supa/update', methods=['PATCH'])
def supa_update():
    if (err := _reject_if_no_context()):
        return err
    table = extract_table(request)
    if table not in _ALLOWED_TABLES:
        return jsonify({"error": "Invalid table"}), 400
    col = request.args.get('col')
    val = request.args.get('val')
    if not (table and col and val):
        return jsonify({"error": "Missing params"}), 400
    raw = getattr(request, "merged_json", request.get_json(force=True) or {})
    update_data = raw.get("fields", raw) if raw else {}
    if isinstance(update_data, dict):
        update_data.pop("table", None)
    encoded_data = process_fields(update_data)
    url = f"{SUPABASE_URL}/rest/v1/{table}?{col}=eq.{val}"
    r = requests.patch(url, headers=HEADERS, json=encoded_data)
    return (r.text, r.status_code, r.headers.items())

@app.route('/supa/delete', methods=['DELETE'])
def supa_delete():
    if (err := _reject_if_no_context()):
        return err
    table = request.args.get('table')
    if table not in _ALLOWED_TABLES:
        return jsonify({"error": "Invalid table"}), 400
    col = request.args.get('col')
    val = request.args.get('val')
    if not (table and col and val):
        return jsonify({"error": "Missing params"}), 400
    url = f"{SUPABASE_URL}/rest/v1/{table}?{col}=eq.{val}"
    r = requests.delete(url, headers=HEADERS)
    return (r.text, r.status_code, r.headers.items())

# ------------------ HUMAN-READABLE DECODERS ------------------
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
    return "Cleanlight 2.1 — hardened, read-first enforced.", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
