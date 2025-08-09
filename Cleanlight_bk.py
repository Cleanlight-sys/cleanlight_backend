print("App started — Cleanlight API (read-first + pagination + clanker proxy)", flush=True)

import traceback
from urllib.parse import urlencode
from flask import Flask, request, jsonify, Response, stream_with_context
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

READ_CONTEXT = {"loaded": False, "timestamp": 0}
READ_TIMEOUT = 600

ALLOWED_FIELDS = {
    "cleanlight_canvas": ["id", "cognition", "mir", "insight", "codex", "images"],
    "cleanlight_map": ["id", "cognition", "mir", "insight", "codex", "images", "pointer_net", "macro_group"]
}
ALLOWED_TABLES = set(ALLOWED_FIELDS.keys())

# ------------------ ALPHABETS ------------------
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

# ------------------ ENCODING HELPERS ------------------
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
    compressed = as_int.to_bytes((as_int.bit_length() + 7) // 8, 'big')
    return zstd.ZstdDecompressor().decompress(compressed).decode('utf-8')

def encode_std10k(image_bytes: bytes) -> str:
    cctx = zstd.ZstdCompressor()
    compressed = cctx.compress(image_bytes)
    as_int = int.from_bytes(compressed, 'big')
    return int_to_baseN(as_int, BASE10K)

def decode_std10k(std10k_str: str) -> bytes:
    as_int = baseN_to_int(std10k_str, BASE10K)
    compressed = as_int.to_bytes((as_int.bit_length() + 7) // 8, 'big')
    return zstd.ZstdDecompressor().decompress(compressed)

# ------------------ FIELD PROCESSING ------------------
def process_fields(data, encode=True, table=None):
    processed = {}
    if table not in ALLOWED_TABLES:
        raise ValueError("Table not allowed")

    for key, val in data.items():
        if key not in ALLOWED_FIELDS[table]:
            raise ValueError(f"Field {key} not allowed in table {table}")

        if key in ("id", "cognition") or val is None:
            processed[key] = val
        elif key == "images":
            if encode:
                img_bytes = base64.b64decode(val) if isinstance(val, str) else val
                processed[key] = encode_std10k(img_bytes)
            else:
                processed[key] = val
        elif key in ("mir", "codex", "insight", "pointer_net"):
            if encode:
                processed[key] = encode_std1k(val if isinstance(val, str) else json.dumps(val))
            else:
                processed[key] = val
        else:
            processed[key] = val
    return processed

def decode_row_for_api(row):
    for key in row:
        if key in ("id", "cognition") or not row[key]:
            continue
        if key == "images":
            row[key] = base64.b64encode(decode_std10k(row[key])).decode('ascii')
        elif key in ("mir", "codex", "insight", "pointer_net"):
            row[key] = decode_std1k(row[key])
    return row

def enforce_read_first():
    if not READ_CONTEXT["loaded"] or (time.time() - READ_CONTEXT["timestamp"] > READ_TIMEOUT):
        raise PermissionError("Must read cleanlight_canvas and cleanlight_map before modifying data.")

def safe_request(method, url, **kwargs):
    try:
        r = requests.request(method, url, timeout=60, **kwargs)
        r.raise_for_status()
        return r
    except requests.RequestException as e:
        return jsonify({"error": str(e)}), 500

# ------------------ ROUTES ------------------
@app.before_request
def log_and_merge():
    app.logger.info(f"{request.method} {request.path} args={dict(request.args)}")
    if request.method in ['POST', 'PATCH']:
        try:
            body = request.get_json(force=True, silent=True) or {}
            request.merged_json = {**request.args.to_dict(), **body}
        except Exception:
            request.merged_json = request.args.to_dict()

@app.route('/flask/select_full_table', methods=['GET'])
@app.route('/supa/select_full_table', methods=['GET'])
def select_full_table():
    table = request.args.get('table')
    if table not in ALLOWED_TABLES:
        return jsonify({"error": "Table not allowed"}), 400

    limit = 500
    offset = 0
    READ_CONTEXT["loaded"] = True
    READ_CONTEXT["timestamp"] = time.time()

    @stream_with_context
    def generate():
        yield "["
        first = True
        while True:
            r = safe_request("GET", f"{SUPABASE_URL}/rest/v1/{table}?limit={limit}&offset={offset}", headers=HEADERS)
            if isinstance(r, tuple):
                yield "]"
                return
            chunk = r.json()
            if not chunk:
                break
            for row in chunk:
                if not first:
                    yield ","
                yield json.dumps(decode_row_for_api(row))
                first = False
            if len(chunk) < limit:
                break
            offset += limit
        yield "]"

    return Response(generate(), mimetype='application/json')

@app.route('/flask/insert', methods=['POST'])
@app.route('/supa/insert', methods=['POST'])
def insert_row():
    table = request.args.get('table') or request.merged_json.get('table')
    enforce_read_first()
    row = request.merged_json.get("fields", request.merged_json)
    row.pop("table", None)
    encoded = process_fields(row, encode=True, table=table)
    return safe_request("POST", f"{SUPABASE_URL}/rest/v1/{table}", headers=HEADERS, json=encoded).text, 200

@app.route('/flask/update', methods=['PATCH'])
@app.route('/supa/update', methods=['PATCH'])
def update_row():
    table = request.args.get('table') or request.merged_json.get('table')
    enforce_read_first()
    col, val = request.args.get('col'), request.args.get('val')
    data = request.merged_json.get("fields", request.merged_json)
    data.pop("table", None)
    encoded = process_fields(data, encode=True, table=table)
    return safe_request("PATCH", f"{SUPABASE_URL}/rest/v1/{table}?{col}=eq.{val}", headers=HEADERS, json=encoded).text, 200

@app.route('/flask/append', methods=['PATCH'])
@app.route('/supa/append', methods=['PATCH'])
def append_row():
    table = request.args.get('table') or request.merged_json.get('table')
    enforce_read_first()
    col, val = request.args.get('col'), request.args.get('val')
    existing_r = safe_request("GET", f"{SUPABASE_URL}/rest/v1/{table}?{col}=eq.{val}", headers=HEADERS)
    if isinstance(existing_r, tuple):
        return existing_r
    rows = existing_r.json()
    if not rows:
        return jsonify({"error": "Row not found"}), 404
    existing = decode_row_for_api(rows[0])
    append_data = request.merged_json.get("fields", request.merged_json)
    append_data.pop("table", None)
    for k, v in append_data.items():
        if isinstance(existing.get(k), dict) and isinstance(v, dict):
            existing[k].update(v)
        else:
            existing[k] = v
    encoded = process_fields(existing, encode=True, table=table)
    return safe_request("PATCH", f"{SUPABASE_URL}/rest/v1/{table}?{col}=eq.{val}", headers=HEADERS, json=encoded).text, 200

# ------------------ CLANKER ------------------
@app.route('/clanker', methods=['POST'])
def clanker():
    try:
        data = request.get_json(force=True)
        method, path = data.get("method", "").upper(), data.get("path", "")
        params, json_body = data.get("params", {}), data.get("json", {})
        if not method or not path:
            return jsonify({"error": "Missing method or path"}), 400
        if not path.startswith("/"):
            path = "/" + path
        if path.startswith("/flask/"):
            target_url = request.host_url.rstrip("/") + path
            headers = {}
        elif path.startswith("/supa/"):
            supa_path = path.replace("/supa/", "")
            target_url = f"{SUPABASE_URL}/rest/v1/{supa_path}"
            headers = HEADERS
        else:
            return jsonify({"error": "Invalid path namespace"}), 400
        r = requests.request(method, target_url, headers=headers, params=params, json=json_body if method in ["POST", "PATCH", "DELETE"] else None, timeout=60)
        try:
            resp_data = r.json()
        except Exception:
            resp_data = r.text
        return jsonify({"status_code": r.status_code, "response": resp_data}), 200
    except Exception as e:
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
