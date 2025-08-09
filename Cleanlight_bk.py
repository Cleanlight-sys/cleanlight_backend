# cleanlight_backend.py
from flask import Flask, request, jsonify, Response, stream_with_context
import requests, json, os, time, base64, unicodedata, zstandard as zstd
from datetime import datetime

app = Flask(__name__)

# --- Config ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Accept": "application/json"
}

ALLOWED_FIELDS = {
    "cleanlight_canvas": ["id", "cognition", "mir", "insight", "codex", "images"],
    "cleanlight_map": ["id", "cognition", "mir", "insight", "codex", "images", "pointer_net", "macro_group"]
}
ALLOWED_TABLES = set(ALLOWED_FIELDS.keys())

READ_CONTEXT = {"loaded": False, "timestamp": 0}
READ_TIMEOUT = 600


# --- Encoding Helpers ---
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

def encode_std1k(plaintext):
    compressed = zstd.ZstdCompressor().compress(plaintext.encode("utf-8"))
    return int_to_baseN(int.from_bytes(compressed, "big"), BASE1K)

def decode_std1k(encoded):
    as_int = baseN_to_int(encoded, BASE1K)
    compressed = as_int.to_bytes((as_int.bit_length() + 7) // 8, "big")
    return zstd.ZstdDecompressor().decompress(compressed).decode("utf-8")

def encode_std10k(image_bytes):
    compressed = zstd.ZstdCompressor().compress(image_bytes)
    return int_to_baseN(int.from_bytes(compressed, "big"), BASE10K)

def decode_std10k(encoded):
    as_int = baseN_to_int(encoded, BASE10K)
    compressed = as_int.to_bytes((as_int.bit_length() + 7) // 8, "big")
    return zstd.ZstdDecompressor().decompress(compressed)


# --- Field Processing ---
def process_fields(data, table, encode=True):
    processed = {}
    for key, val in data.items():
        if key not in ALLOWED_FIELDS[table]:
            raise ValueError(f"Field {key} not allowed for table {table}")
        if key in ("id", "cognition", "pointer_net"):
            processed[key] = val
        elif key == "images" and val is not None:
            if encode:
                processed[key] = encode_std10k(base64.b64decode(val))
            else:
                processed[key] = val
        elif key in ("mir", "codex", "insight") and val is not None:
            if encode:
                if not isinstance(val, str):
                    val = json.dumps(val)
                processed[key] = encode_std1k(val)
            else:
                processed[key] = val
        else:
            processed[key] = val
    return processed

def decode_row(row):
    for k, v in list(row.items()):
        if k == "images" and v:
            row[k] = base64.b64encode(decode_std10k(v)).decode("ascii")
        elif k in ("mir", "codex", "insight") and v:
            row[k] = decode_std1k(v)
    return row

def enforce_read_first():
    if not READ_CONTEXT["loaded"] or (time.time() - READ_CONTEXT["timestamp"] > READ_TIMEOUT):
        raise PermissionError("Must read tables first")


# --- Routes ---
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "time": datetime.utcnow().isoformat()})


@app.route("/flask/select_full_table", methods=["GET"])
def select_full_table():
    table = request.args.get("table")
    if table not in ALLOWED_TABLES:
        return jsonify({"error": "Table not allowed"}), 400

    limit, offset = 500, 0
    first_chunk = True
    rows_fetched = 0

    @stream_with_context
    def generate():
        nonlocal offset, first_chunk, rows_fetched
        yield "["
        while True:
            r = requests.get(f"{SUPABASE_URL}/rest/v1/{table}?limit={limit}&offset={offset}", headers=HEADERS)
            if r.status_code != 200:
                yield json.dumps({"error": r.text})
                break
            chunk = r.json()
            if not chunk:
                break
            for row in chunk:
                if not first_chunk:
                    yield ","
                yield json.dumps(decode_row(row))
                first_chunk = False
                rows_fetched += 1
            if len(chunk) < limit:
                break
            offset += limit
        yield "]"
        if rows_fetched > 0:
            READ_CONTEXT.update({"loaded": True, "timestamp": time.time()})

    return Response(generate(), mimetype="application/json")


@app.route("/flask/insert", methods=["POST"])
def insert():
    table = request.args.get("table")
    if table not in ALLOWED_TABLES:
        return jsonify({"error": "Table not allowed"}), 400
    try:
        enforce_read_first()
    except PermissionError as e:
        return jsonify({"error": str(e)}), 403

    body = request.get_json(force=True).get("fields", {})
    encoded = process_fields(body, table, encode=True)
    r = requests.post(f"{SUPABASE_URL}/rest/v1/{table}", headers=HEADERS, json=encoded)
    return jsonify(r.json()), r.status_code


@app.route("/flask/update", methods=["PATCH"])
def update():
    table = request.args.get("table")
    col = request.args.get("col")
    val = request.args.get("val")
    if table not in ALLOWED_TABLES or not col or not val:
        return jsonify({"error": "Invalid params"}), 400
    try:
        enforce_read_first()
    except PermissionError as e:
        return jsonify({"error": str(e)}), 403

    body = request.get_json(force=True).get("fields", {})
    encoded = process_fields(body, table, encode=True)
    r = requests.patch(f"{SUPABASE_URL}/rest/v1/{table}?{col}=eq.{val}", headers=HEADERS, json=encoded)
    return jsonify(r.json()), r.status_code


@app.route("/flask/append", methods=["PATCH"])
def append():
    table = request.args.get("table")
    col = request.args.get("col")
    val = request.args.get("val")
    if table not in ALLOWED_TABLES or not col or not val:
        return jsonify({"error": "Invalid params"}), 400
    try:
        enforce_read_first()
    except PermissionError as e:
        return jsonify({"error": str(e)}), 403

    existing = requests.get(f"{SUPABASE_URL}/rest/v1/{table}?{col}=eq.{val}", headers=HEADERS)
    if existing.status_code != 200:
        return jsonify({"error": existing.text}), existing.status_code
    data = existing.json()
    if not data:
        return jsonify({"error": "Row not found"}), 404

    decoded = decode_row(data[0])
    updates = request.get_json(force=True).get("fields", {})
    for k, v in updates.items():
        if isinstance(decoded.get(k), dict) and isinstance(v, dict):
            decoded[k].update(v)
        else:
            decoded[k] = v

    encoded = process_fields(decoded, table, encode=True)
    r = requests.patch(f"{SUPABASE_URL}/rest/v1/{table}?{col}=eq.{val}", headers=HEADERS, json=encoded)
    return jsonify(r.json()), r.status_code


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
