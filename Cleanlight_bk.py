print("App started — hardened with read-first + pagination + full read + clanker proxy", flush=True)

import traceback
from urllib.parse import urlencode
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
READ_TIMEOUT = 600  # seconds

# --- Whitelisted tables & fields ---
ALLOWED_FIELDS = {
    "cleanlight_canvas": ["id", "cognition", "mir", "insight", "codex", "images"],
    "cleanlight_map": ["id", "cognition", "mir", "insight", "codex", "images", "pointer_net", "macro_group"]
}
ALLOWED_TABLES = set(ALLOWED_FIELDS.keys())

# ------------------ LOGGING & MERGE ------------------
@app.route('/echo_test', methods=['POST'])
def echo_test():
    """
    Simple echo endpoint to test connectivity and payload handling.
    Returns exactly what you sent, plus server timestamp.
    """
    try:
        incoming = request.get_json(force=True, silent=False)
    except Exception as e:
        return jsonify({"error": f"Invalid JSON: {str(e)}"}), 400

    return jsonify({
        "status": "ok",
        "received": incoming,
        "server_time": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    }), 200

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

def extract_table(req):
    table = req.args.get('table')
    if not table:
        try:
            body = req.get_json(silent=True) or {}
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
def process_fields(data, encode=True, table=None, merge_jsonb=False):
    processed = {}
    if table not in ALLOWED_TABLES:
        raise ValueError("Table not allowed")

    for key, val in data.items():
        if key not in ALLOWED_FIELDS[table]:
            raise ValueError(f"Field {key} not allowed in table {table}")
        if key == "id":  # Pass ID through untouched
            processed[key] = val
            continue
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
        elif key in ("mir", "codex", "insight", "pointer_net") and val is not None:
            if merge_jsonb and isinstance(val, dict):
                try:
                    existing_json = json.loads(data.get(key, "{}")) if isinstance(data.get(key), str) else {}
                except Exception:
                    existing_json = {}
                merged_json = {**existing_json, **val}
                val = json.dumps(merged_json)
            if encode:
                processed[key] = encode_std1k(val if isinstance(val, str) else json.dumps(val))
            else:
                processed[key] = val
        else:
            processed[key] = val
    return processed

def decode_row_for_api(row):
    for key in row:
        if key == "cognition" or key == "id":
            continue
        elif key == "images" and row[key]:
            image_bytes = decode_std10k(row[key])
            row[key] = base64.b64encode(image_bytes).decode('ascii')
        elif key in ("mir", "codex", "insight", "pointer_net") and row[key]:
            row[key] = decode_std1k(row[key])
    return row

def enforce_read_first():
    if not READ_CONTEXT["loaded"] or (time.time() - READ_CONTEXT["timestamp"] > READ_TIMEOUT):
        raise PermissionError("Must read cleanlight_canvas and cleanlight_map before modifying data.")

# ------------------ CRUD ENDPOINTS ------------------
def safe_request(method, url, **kwargs):
    try:
        r = requests.request(method, url, timeout=60, **kwargs)
        r.raise_for_status()
        return r
    except requests.RequestException as e:
        return jsonify({"error": f"Request to Supabase failed: {str(e)}"}), 500

@app.route('/supa/select_full_table', methods=['GET'])
def supa_select_full_table():
    table = request.args.get('table')
    if table not in ALLOWED_TABLES:
        return jsonify({"error": "Table not allowed"}), 400

    all_rows = []
    limit = 500
    offset = 0

    while True:
        r = safe_request("GET", f"{SUPABASE_URL}/rest/v1/{table}?limit={limit}&offset={offset}", headers=HEADERS)
        if isinstance(r, tuple):
            return r
        chunk = r.json()
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

@app.route('/supa/insert', methods=['POST'])
def supa_insert():
    table = extract_table(request)
    if table not in ALLOWED_TABLES:
        return jsonify({"error": "Table not allowed"}), 400
    enforce_read_first()

    raw = getattr(request, "merged_json", request.get_json(force=True) or {})
    if isinstance(raw, dict):
        raw.pop("table", None)
    if "fields" in raw:
        raw = raw["fields"]

    encoded_row = process_fields(raw, encode=True, table=table)
    return safe_request("POST", f"{SUPABASE_URL}/rest/v1/{table}", headers=HEADERS, json=encoded_row).text, 200

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
    update_data = raw.get("fields", raw)
    update_data.pop("table", None)

    encoded_data = process_fields(update_data, encode=True, table=table)
    return safe_request("PATCH", f"{SUPABASE_URL}/rest/v1/{table}?{col}=eq.{val}", headers=HEADERS, json=encoded_data).text, 200

@app.route('/supa/append', methods=['PATCH'])
def supa_append():
    table = extract_table(request)
    if table not in ALLOWED_TABLES:
        return jsonify({"error": "Table not allowed"}), 400
    enforce_read_first()

    col = request.args.get('col')
    val = request.args.get('val')
    if not (col and val):
        return jsonify({"error": "Missing params"}), 400

    # Get existing row
    r = safe_request("GET", f"{SUPABASE_URL}/rest/v1/{table}?{col}=eq.{val}", headers=HEADERS)
    if isinstance(r, tuple):
        return r
    rows = r.json()
    if not rows:
        return jsonify({"error": "Row not found"}), 404
    existing = decode_row_for_api(rows[0])

    # Merge fields
    raw = getattr(request, "merged_json", request.get_json(force=True) or {})
    append_data = raw.get("fields", raw)
    append_data.pop("table", None)

    for k, v in append_data.items():
        if isinstance(existing.get(k), dict) and isinstance(v, dict):
            existing[k].update(v)
        elif isinstance(existing.get(k), str) and isinstance(v, dict):
            try:
                old_json = json.loads(existing[k])
                if isinstance(old_json, dict):
                    old_json.update(v)
                    existing[k] = old_json
            except Exception:
                existing[k] = v
        else:
            existing[k] = v

    encoded = process_fields(existing, encode=True, table=table)
    return safe_request("PATCH", f"{SUPABASE_URL}/rest/v1/{table}?{col}=eq.{val}", headers=HEADERS, json=encoded).text, 200

# ------------------ CLANKER UNIVERSAL ENDPOINT ------------------
@app.route("/clanker", methods=["POST"])
def clanker():
    try:
        data = request.get_json(force=True)
        method = data.get("method")
        path = data.get("path")
        params = data.get("params", {})
        json_body = data.get("json", {})

        if not method or not path:
            return jsonify({"error": "method and path are required"}), 400

        # Merge query params into path if provided
        if params:
            query_string = urlencode(params)
            path = f"{path}?{query_string}"

        # Forward the request internally
        url = request.host_url.rstrip("/") + path
        resp = requests.request(method, url, json=json_body, headers=request.headers)

        return jsonify({
            "status": resp.status_code,
            "data": resp.json() if resp.headers.get("Content-Type", "").startswith("application/json") else resp.text
        }), resp.status_code

    except Exception as e:
        return jsonify({"error": str(e)}), 500
        
@app.route('/')
def index():
    return "Cleanlight 2.1 — Hardened API + Clanker Universal Proxy is live.", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)






