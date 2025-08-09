print("Cleanlight 2.1 — Law-Compliant API + Auto Map Updates (Aug 2025)", flush=True)

import traceback
import requests
import os
import unicodedata
import zstandard as zstd
import base64
import logging
import time
import json

from flask import Flask, request, jsonify, Response, stream_with_context

# -------------------- CONFIG --------------------
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
READ_TIMEOUT = 600  # seconds

ALLOWED_FIELDS = {
    "cleanlight_canvas": ["id", "cognition", "mir", "insight", "codex", "images"],
    "cleanlight_map": ["id", "cognition", "mir", "insight", "codex", "images", "pointer_net", "macro_group"]
}
ALLOWED_TABLES = set(ALLOWED_FIELDS.keys())

# -------------------- UTILITIES --------------------
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
    if num == 0: return alphabet[0]
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
    return zstd.ZstdDecompressor().decompress(compressed).decode('utf-8')

def encode_std10k(image_bytes: bytes) -> str:
    compressed = zstd.ZstdCompressor().compress(image_bytes)
    as_int = int.from_bytes(compressed, 'big')
    return int_to_baseN(as_int, BASE10K)

def decode_std10k(std10k_str: str) -> bytes:
    as_int = baseN_to_int(std10k_str, BASE10K)
    num_bytes = (as_int.bit_length() + 7) // 8
    compressed = as_int.to_bytes(num_bytes, 'big')
    return zstd.ZstdDecompressor().decompress(compressed)

def safe_request(method, url, **kwargs):
    try:
        r = requests.request(method, url, timeout=60, **kwargs)
        r.raise_for_status()
        return r
    except requests.RequestException as e:
        return jsonify({"error": f"Request to Supabase failed: {str(e)}"}), 500

# -------------------- FIELD ENCODING --------------------
def process_fields(data, encode=True, table=None, merge_jsonb=False):
    if table not in ALLOWED_TABLES:
        raise ValueError("Table not allowed")
    processed = {}
    for key, val in data.items():
        if key not in ALLOWED_FIELDS[table]:
            raise ValueError(f"Field {key} not allowed in table {table}")

        if key in ("id", "cognition", "pointer_net"):
            processed[key] = val
        elif key == "images" and val is not None:
            processed[key] = encode_std10k(base64.b64decode(val)) if encode else val
        elif key in ("mir", "codex", "insight") and val is not None:
            if merge_jsonb and isinstance(val, dict):
                val = json.dumps(val)
            processed[key] = encode_std1k(val if isinstance(val, str) else json.dumps(val)) if encode else val
        else:
            processed[key] = val
    return processed

def decode_row_for_api(row):
    for key in row:
        if key in ("id", "cognition", "pointer_net"):
            continue
        elif key == "images" and row[key]:
            row[key] = base64.b64encode(decode_std10k(row[key])).decode('ascii')
        elif key in ("mir", "codex", "insight") and row[key]:
            row[key] = decode_std1k(row[key])
    return row

def enforce_read_first():
    if not READ_CONTEXT["loaded"] or (time.time() - READ_CONTEXT["timestamp"] > READ_TIMEOUT):
        raise PermissionError("Must read cleanlight_canvas and cleanlight_map before modifying data.")

# -------------------- ROUTES --------------------
@app.before_request
def log_and_merge():
    app.logger.info(f"{request.method} {request.path} args={dict(request.args)} body={request.get_data(as_text=True)}")
    if request.method in ['POST', 'PATCH']:
        try:
            body = request.get_json(force=True, silent=True) or {}
            request.merged_json = {**request.args.to_dict(), **body}
        except:
            request.merged_json = request.args.to_dict()

@app.route('/flask/select_full_table', methods=['GET'])
def select_full_table():
    table = request.args.get('table')
    filter_nonblank_cog = request.args.get('filter') == 'cognition_nonblank'
    if table not in ALLOWED_TABLES:
        return jsonify({"error": "Table not allowed"}), 400

    limit, offset = 500, 0
    READ_CONTEXT.update({"loaded": True, "timestamp": time.time()})

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
            if not chunk: break
            for row in chunk:
                row = decode_row_for_api(row)
                if filter_nonblank_cog and not row.get("cognition"): continue
                if not first: yield ","
                yield json.dumps(row)
                first = False
            if len(chunk) < limit: break
            offset += limit
        yield "]"
    return Response(generate(), mimetype='application/json')

@app.route('/flask/insert', methods=['POST'])
def insert_row():
    table = request.args.get('table') or request.merged_json.get('table')
    enforce_read_first()
    raw = request.merged_json.get("fields", request.merged_json)
    raw.pop("table", None)
    encoded = process_fields(raw, encode=True, table=table)
    res = safe_request("POST", f"{SUPABASE_URL}/rest/v1/{table}", headers=HEADERS, json=encoded)
    return res.text if not isinstance(res, tuple) else res

@app.route('/flask/update', methods=['PATCH'])
def update_row():
    table = request.args.get('table') or request.merged_json.get('table')
    enforce_read_first()
    col, val = request.args.get('col'), request.args.get('val')
    raw = request.merged_json.get("fields", request.merged_json)
    raw.pop("table", None)
    encoded = process_fields(raw, encode=True, table=table)
    res = safe_request("PATCH", f"{SUPABASE_URL}/rest/v1/{table}?{col}=eq.{val}", headers=HEADERS, json=encoded)
    if table == "cleanlight_canvas" and any(k in raw for k in ("mir", "codex")):
        auto_update_map()
    return res.text if not isinstance(res, tuple) else res

@app.route('/flask/append', methods=['PATCH'])
def append_row():
    table = request.args.get('table') or request.merged_json.get('table')
    enforce_read_first()
    col, val = request.args.get('col'), request.args.get('val')
    r = safe_request("GET", f"{SUPABASE_URL}/rest/v1/{table}?{col}=eq.{val}", headers=HEADERS)
    if isinstance(r, tuple): return r
    rows = r.json()
    if not rows: return jsonify({"error": "Row not found"}), 404
    existing = decode_row_for_api(rows[0])
    append_data = request.merged_json.get("fields", request.merged_json)
    append_data.pop("table", None)
    for k, v in append_data.items():
        if isinstance(existing.get(k), dict) and isinstance(v, dict):
            existing[k].update(v)
        else:
            existing[k] = v
    encoded = process_fields(existing, encode=True, table=table)
    res = safe_request("PATCH", f"{SUPABASE_URL}/rest/v1/{table}?{col}=eq.{val}", headers=HEADERS, json=encoded)
    if table == "cleanlight_canvas" and any(k in append_data for k in ("mir", "codex")):
        auto_update_map()
    return res.text if not isinstance(res, tuple) else res

def auto_update_map():
    """Auto-sync cleanlight_map.pointer_net when codices change."""
    # Fetch latest map, apply updates (placeholder for actual graph logic)
    app.logger.info("Auto-updating pointer_net in cleanlight_map...")
    # Implement actual sync logic here

@app.route("/clanker", methods=["POST"])
def clanker():
    try:
        data = request.get_json(force=True)
        method, path = data.get("method", "").upper(), data.get("path", "")
        params, json_body = data.get("params", {}), data.get("json", {})
        if not path.startswith("/"): path = "/" + path
        if path.startswith("/flask/"):
            target_url = request.host_url.rstrip("/") + path.replace("/flask", "")
            headers = {}
        elif path.startswith("/supa/"):
            supa_path = path.replace("/supa/", "")
            target_url = f"{SUPABASE_URL}/rest/v1/{supa_path}"
            headers = HEADERS
        else:
            return jsonify({"error": "Invalid path namespace"}), 400
        r = requests.request(method=method, url=target_url, headers=headers, params=params, json=json_body if method in ["POST", "PATCH", "DELETE"] else None, timeout=60)
        try: resp_data = r.json()
        except: resp_data = r.text
        return jsonify({"status_code": r.status_code, "response": resp_data}), 200
    except Exception as e:
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
