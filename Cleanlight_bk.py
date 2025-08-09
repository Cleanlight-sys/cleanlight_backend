print("App started — Cleanlight 2.1 law-compliant, read-first + pointer_net sync", flush=True)

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

# ---------------- State + Config ----------------
READ_CONTEXT = {"loaded": False, "timestamp": 0}
READ_TIMEOUT = 600
ALLOWED_FIELDS = {
    "cleanlight_canvas": ["id", "cognition", "mir", "insight", "codex", "images", "checksums", "timestamps"],
    "cleanlight_map": ["id", "cognition", "mir", "insight", "codex", "images", "pointer_net", "macro_group"]
}
ALLOWED_TABLES = set(ALLOWED_FIELDS.keys())

# ---------------- Utilities ----------------
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

def process_fields(data, encode=True, table=None):
    processed = {}
    if table not in ALLOWED_TABLES:
        raise ValueError("Table not allowed")
    for key, val in data.items():
        if key not in ALLOWED_FIELDS[table]:
            raise ValueError(f"Field {key} not allowed")
        if key == "id" or key == "cognition" or key == "pointer_net":
            processed[key] = val
        elif key == "images" and val is not None:
            processed[key] = encode_std10k(base64.b64decode(val)) if encode else val
        elif key in ("mir", "codex", "insight") and val is not None:
            processed[key] = encode_std1k(json.dumps(val) if isinstance(val, (dict, list)) else val) if encode else val
        else:
            processed[key] = val
    return processed

def decode_row_for_api(row):
    for key in row:
        if key == "images" and row[key]:
            row[key] = base64.b64encode(decode_std10k(row[key])).decode('ascii')
        elif key in ("mir", "codex", "insight") and row[key]:
            row[key] = json.loads(decode_std1k(row[key]))
    return row

def safe_request(method, url, **kwargs):
    try:
        r = requests.request(method, url, timeout=60, **kwargs)
        r.raise_for_status()
        return r
    except requests.RequestException as e:
        return jsonify({"error": str(e)}), 500

def enforce_read_first():
    if not READ_CONTEXT["loaded"] or (time.time() - READ_CONTEXT["timestamp"] > READ_TIMEOUT):
        raise PermissionError("Must read tables first")

def update_pointer_net_from_canvas():
    # Fetch latest codex/mir from canvas
    r = safe_request("GET", f"{SUPABASE_URL}/rest/v1/cleanlight_canvas", headers=HEADERS)
    if isinstance(r, tuple): return
    canvas_data = r.json()
    pointer_net_data = {"nodes": [c.get("id") for c in canvas_data]}
    # Update map pointer_net
    safe_request("PATCH", f"{SUPABASE_URL}/rest/v1/cleanlight_map?id=eq.1", headers=HEADERS, json={"pointer_net": pointer_net_data})

# ---------------- Routes ----------------
@app.route('/flask/select_full_table', methods=['GET'])
def select_full_table():
    table = request.args.get('table')
    if table not in ALLOWED_TABLES:
        return jsonify({"error": "Table not allowed"}), 400
    limit = 500
    offset = 0
    @stream_with_context
    def generate():
        first_chunk = True
        READ_CONTEXT.update({"loaded": True, "timestamp": time.time()})
        yield "["
        while True:
            r = safe_request("GET", f"{SUPABASE_URL}/rest/v1/{table}?limit={limit}&offset={offset}", headers=HEADERS)
            if isinstance(r, tuple): yield "]"; return
            chunk = r.json()
            if not chunk: break
            for row in chunk:
                row = decode_row_for_api(row)
                if not first_chunk: yield ","
                yield json.dumps(row)
                first_chunk = False
            if len(chunk) < limit: break
            offset += limit
        yield "]"
    return Response(generate(), mimetype='application/json')

@app.route('/flask/insert', methods=['POST'])
def insert():
    table = request.args.get('table')
    enforce_read_first()
    raw = request.json.get("fields", request.json)
    encoded = process_fields(raw, encode=True, table=table)
    r = safe_request("POST", f"{SUPABASE_URL}/rest/v1/{table}", headers=HEADERS, json=encoded)
    if table == "cleanlight_canvas": update_pointer_net_from_canvas()
    return r.text, r.status_code

@app.route('/flask/update', methods=['PATCH'])
def update():
    table = request.args.get('table')
    enforce_read_first()
    col, val = request.args.get('col'), request.args.get('val')
    raw = request.json.get("fields", request.json)
    encoded = process_fields(raw, encode=True, table=table)
    r = safe_request("PATCH", f"{SUPABASE_URL}/rest/v1/{table}?{col}=eq.{val}", headers=HEADERS, json=encoded)
    if table == "cleanlight_canvas": update_pointer_net_from_canvas()
    return r.text, r.status_code

@app.route('/flask/append', methods=['PATCH'])
def append():
    table = request.args.get('table')
    enforce_read_first()
    col, val = request.args.get('col'), request.args.get('val')
    # Fetch existing
    r = safe_request("GET", f"{SUPABASE_URL}/rest/v1/{table}?{col}=eq.{val}", headers=HEADERS)
    if isinstance(r, tuple): return r
    existing = decode_row_for_api(r.json()[0])
    incoming = request.json.get("fields", request.json)
    for k, v in incoming.items():
        if isinstance(existing.get(k), dict) and isinstance(v, dict):
            existing[k].update(v)
        else:
            existing[k] = v
    encoded = process_fields(existing, encode=True, table=table)
    r2 = safe_request("PATCH", f"{SUPABASE_URL}/rest/v1/{table}?{col}=eq.{val}", headers=HEADERS, json=encoded)
    if table == "cleanlight_canvas": update_pointer_net_from_canvas()
    return r2.text, r2.status_code

@app.route('/clanker', methods=['POST'])
def clanker():
    try:
        data = request.json
        method, path, params, json_body = data["method"].upper(), data["path"], data.get("params", {}), data.get("json", {})
        if not path.startswith("/"): path = "/" + path
        if path.startswith("/flask/"):
            target_url = request.host_url.rstrip("/") + path.replace("/flask", "")
            headers = {}
        elif path.startswith("/supa/"):
            target_url = f"{SUPABASE_URL}/rest/v1/" + path.replace("/supa/", "")
            headers = HEADERS
        else:
            return jsonify({"error": "Invalid namespace"}), 400
        r = requests.request(method, target_url, headers=headers, params=params, json=json_body if method in ["POST", "PATCH"] else None, timeout=60)
        try: resp_data = r.json()
        except: resp_data = r.text
        return jsonify({"status_code": r.status_code, "response": resp_data}), 200
    except Exception as e:
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
