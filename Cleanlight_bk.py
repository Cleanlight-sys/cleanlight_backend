# cleanlight_backend.py
# Cleanlight 2.1 – Procedural Cognition Engine Backend
# August 2025 – Complete rewrite: universal CRUD, no self-calls, stable streaming

import os
import time
import json
import base64
import logging
import unicodedata
import traceback
import requests
import zstandard as zstd
from flask import Flask, request, jsonify, Response, stream_with_context

# ------------------ Flask Setup ------------------
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

# ------------------ State ------------------
READ_CONTEXT = {"loaded": False, "timestamp": 0}
READ_TIMEOUT = 600  # seconds

# ------------------ Allowed Tables/Fields ------------------
ALLOWED_FIELDS = {
    "cleanlight_canvas": ["id", "cognition", "mir", "insight", "codex", "images"],
    "cleanlight_map": ["id", "cognition", "mir", "insight", "codex", "images", "pointer_net", "macro_group"]
}
ALLOWED_TABLES = set(ALLOWED_FIELDS.keys())

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

# ------------------ Encoding/Decoding ------------------
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

def encode_std1k(text):
    cctx = zstd.ZstdCompressor()
    compressed = cctx.compress(text.encode('utf-8'))
    as_int = int.from_bytes(compressed, 'big')
    return int_to_baseN(as_int, BASE1K)

def decode_std1k(encoded):
    as_int = baseN_to_int(encoded, BASE1K)
    compressed = as_int.to_bytes((as_int.bit_length() + 7) // 8, 'big')
    dctx = zstd.ZstdDecompressor()
    return dctx.decompress(compressed).decode('utf-8')

def encode_std10k(image_bytes):
    cctx = zstd.ZstdCompressor()
    compressed = cctx.compress(image_bytes)
    as_int = int.from_bytes(compressed, 'big')
    return int_to_baseN(as_int, BASE10K)

def decode_std10k(encoded):
    as_int = baseN_to_int(encoded, BASE10K)
    compressed = as_int.to_bytes((as_int.bit_length() + 7) // 8, 'big')
    dctx = zstd.ZstdDecompressor()
    return dctx.decompress(compressed)

# ------------------ Field Processing ------------------
def encode_row_for_storage(data, table):
    out = {}
    for k, v in data.items():
        if k not in ALLOWED_FIELDS[table]:
            raise ValueError(f"Field {k} not allowed for {table}")
        if k in ("mir", "insight", "codex", "pointer_net") and v is not None:
            out[k] = encode_std1k(json.dumps(v) if not isinstance(v, str) else v)
        elif k == "images" and v is not None:
            img_bytes = base64.b64decode(v) if isinstance(v, str) else v
            out[k] = encode_std10k(img_bytes)
        else:
            out[k] = v
    return out

def decode_row_from_storage(data):
    for k, v in list(data.items()):
        if k in ("mir", "insight", "codex", "pointer_net") and v:
            try:
                data[k] = json.loads(decode_std1k(v))
            except:
                data[k] = decode_std1k(v)
        elif k == "images" and v:
            data[k] = base64.b64encode(decode_std10k(v)).decode('ascii')
    return data

# ------------------ Enforcement ------------------
def enforce_read_first():
    if not READ_CONTEXT["loaded"] or (time.time() - READ_CONTEXT["timestamp"] > READ_TIMEOUT):
        raise PermissionError("Must read canvas/map first.")

# ------------------ Supabase Call Helper ------------------
def supa_request(method, table, params=None, json_data=None):
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    r = requests.request(method, url, headers=HEADERS, params=params, json=json_data, timeout=30)
    r.raise_for_status()
    return r.json()

# ------------------ CRUD Logic ------------------
def select_full_table_logic(params):
    table = params.get("table")
    if table not in ALLOWED_TABLES:
        return {"error": "Table not allowed"}

    limit, offset = 500, 0
    READ_CONTEXT["loaded"] = True
    READ_CONTEXT["timestamp"] = time.time()

    def generator():
        first = True
        yield "["
        while True:
            chunk = supa_request("GET", table, params={"limit": limit, "offset": offset})
            if not chunk:
                break
            for row in chunk:
                row = decode_row_from_storage(row)
                if not first:
                    yield ","
                yield json.dumps(row)
                first = False
            if len(chunk) < limit:
                break
            offset += limit
        yield "]"

    return Response(stream_with_context(generator()), mimetype='application/json')

def insert_logic(params, fields):
    table = params.get("table")
    if table not in ALLOWED_TABLES:
        return {"error": "Table not allowed"}
    enforce_read_first()
    data = encode_row_for_storage(fields, table)
    return supa_request("POST", table, json_data=data)

def update_logic(params, fields):
    table = params.get("table")
    col = params.get("col")
    val = params.get("val")
    if table not in ALLOWED_TABLES or not col or not val:
        return {"error": "Invalid params"}
    enforce_read_first()
    data = encode_row_for_storage(fields, table)
    return supa_request("PATCH", table, params={col: f"eq.{val}"}, json_data=data)

def append_logic(params, fields):
    table = params.get("table")
    col = params.get("col")
    val = params.get("val")
    if table not in ALLOWED_TABLES or not col or not val:
        return {"error": "Invalid params"}
    enforce_read_first()

    rows = supa_request("GET", table, params={col: f"eq.{val}"})
    if not rows:
        return {"error": "Row not found"}
    existing = decode_row_from_storage(rows[0])

    for k, v in fields.items():
        if isinstance(existing.get(k), dict) and isinstance(v, dict):
            existing[k].update(v)
        else:
            existing[k] = v

    data = encode_row_for_storage(existing, table)
    return supa_request("PATCH", table, params={col: f"eq.{val}"}, json_data=data)

# ------------------ Universal Handler ------------------
@app.route("/", methods=["GET"])
def index():
    """
    Root wake-up endpoint for Render.
    Shows service name and confirms it's running.
    """
    return jsonify({
        "status": "ok",
        "service": "cleanlight_backend",
        "message": "Service is live and awaiting API calls."
    }), 200


@app.route("/health", methods=["GET"])
def health():
    """
    Lightweight healthcheck endpoint.
    Used for pinging/waking the Render instance before real work.
    """
    return jsonify({
        "status": "healthy",
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
        "read_context_loaded": READ_CONTEXT["loaded"],
        "tables_allowed": list(ALLOWED_TABLES)
    }), 200
@app.route("/clanker", methods=["POST"])

def clanker():
    try:
        payload = request.get_json(force=True)
        method = payload.get("method", "").upper()
        scope = payload.get("scope", "flask")
        endpoint = payload.get("endpoint", "")
        params = payload.get("params", {}) or {}
        fields = payload.get("fields", {}) or {}

        if scope == "flask":
            func_map = {
                "select_full_table": lambda p, f=None: select_full_table_logic(p),
                "insert": insert_logic,
                "update": update_logic,
                "append": append_logic
            }
            if endpoint not in func_map:
                return jsonify({"error": "Invalid endpoint"}), 400
            result = func_map[endpoint](params, fields) if method in ("POST", "PATCH") else func_map[endpoint](params)
            return result if isinstance(result, Response) else jsonify(result)

        elif scope == "supa":
            table = params.pop("table", None)
            if not table:
                return jsonify({"error": "Missing table"}), 400
            result = supa_request(method, table, params=params, json_data=fields)
            return jsonify(result)

        else:
            return jsonify({"error": "Invalid scope"}), 400

    except PermissionError as e:
        return jsonify({"error": str(e)}), 403
    except Exception as e:
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500

# ------------------ Debug/Echo ------------------
@app.route("/echo_test", methods=["POST"])
def echo_test():
    return jsonify({
        "status": "ok",
        "received": request.get_json(force=True),
        "time": time.time()
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

