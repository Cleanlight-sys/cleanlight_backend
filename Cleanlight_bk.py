from flask import Flask, request, jsonify
import requests, json, os, base64, zstandard as zstd
from datetime import datetime

app = Flask(__name__)

# ---- Config ----
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Accept": "application/json"
}

ALLOWED_FIELDS = {
    "cleanlight_canvas": ["id", "cognition", "mir", "insight", "codex", "images", "checksums", "timestamps"],
    "cleanlight_map": ["id", "cognition", "mir", "insight", "codex", "images", "pointer_net", "macro_group"]
}
ALLOWED_TABLES = set(ALLOWED_FIELDS.keys())

# ---- Encoding helpers ----
def is_base1k_string(s):
    """Quick check: True if all characters are in BASE1K alphabet."""
    if not isinstance(s, str):
        return False
    return all(ch in BASE1K for ch in s)

def decode_row(row):
    for k in list(row.keys()):
        if k == "images" and row[k]:
            try:
                row[k] = base64.b64encode(decode_std10k(row[k])).decode('ascii')
            except Exception:
                pass  # leave as-is if bad data
        elif k in ("mir", "codex", "insight") and row[k]:
            if is_base1k_string(row[k]):
                try:
                    row[k] = decode_std1k(row[k])
                except Exception:
                    pass
            # else: already human readable, leave as-is
        # cognition is already plain text — leave untouched
    return row

def get_base_alphabet(n):
    safe = []
    for codepoint in range(0x20, 0x2FFFF):
        ch = chr(codepoint)
        name = ch.encode("unicode_escape").decode()
        if (
            0xD800 <= codepoint <= 0xDFFF or
            0xFDD0 <= codepoint <= 0xFDEF or
            codepoint & 0xFFFE == 0xFFFE
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
    return int_to_baseN(int.from_bytes(compressed, 'big'), BASE1K)

def decode_std1k(std1k_str: str) -> str:
    as_int = baseN_to_int(std1k_str, BASE1K)
    compressed = as_int.to_bytes((as_int.bit_length() + 7) // 8, 'big')
    return zstd.ZstdDecompressor().decompress(compressed).decode('utf-8')

def encode_std10k(image_bytes: bytes) -> str:
    cctx = zstd.ZstdCompressor()
    compressed = cctx.compress(image_bytes)
    return int_to_baseN(int.from_bytes(compressed, 'big'), BASE10K)

def decode_std10k(std10k_str: str) -> bytes:
    as_int = baseN_to_int(std10k_str, BASE10K)
    compressed = as_int.to_bytes((as_int.bit_length() + 7) // 8, 'big')
    return zstd.ZstdDecompressor().decompress(compressed)

# ---- Field processing ----
def process_fields(data, table):
    processed = {}
    for key, val in data.items():
        if key not in ALLOWED_FIELDS[table]:
            raise ValueError(f"Field {key} not allowed for table {table}")
        if key in ("id", "cognition", "pointer_net"):
            processed[key] = val
        elif key == "images" and val is not None:
            processed[key] = encode_std10k(base64.b64decode(val))
        elif key in ("mir", "codex", "insight") and val is not None:
            processed[key] = encode_std1k(val if isinstance(val, str) else json.dumps(val))
        else:
            processed[key] = val
    return processed

def decode_row(row):
    for k in list(row.keys()):
        if k == "images" and row[k]:
            row[k] = base64.b64encode(decode_std10k(row[k])).decode('ascii')
        elif k in ("mir", "codex", "insight") and row[k]:
            row[k] = decode_std1k(row[k])
    return row

# ---- CRUD endpoint ----
@app.route("/flask/command", methods=["POST"])
def command():
    payload = request.get_json(force=True) or {}
    action = payload.get("action")
    table = payload.get("table")
    where = payload.get("where")
    fields = payload.get("fields")

    # Basic validation
    if action not in ["read_table", "read_row", "insert", "update", "append"]:
        return jsonify({"error": "Invalid action"}), 400
    if table not in ALLOWED_TABLES:
        return jsonify({"error": "Invalid table"}), 400

    # Execute action
    if action == "read_table":
        r = requests.get(f"{SUPABASE_URL}/rest/v1/{table}", headers=HEADERS)
        return jsonify([decode_row(row) for row in r.json()])

    if action == "read_row":
        if not where:
            return jsonify({"error": "Missing 'where'"}), 400
        r = requests.get(f"{SUPABASE_URL}/rest/v1/{table}?{where['col']}=eq.{where['val']}", headers=HEADERS)
        return jsonify([decode_row(row) for row in r.json()])

    if action == "insert":
        if not fields:
            return jsonify({"error": "Missing 'fields'"}), 400
        encoded = process_fields(fields, table)
        r = requests.post(f"{SUPABASE_URL}/rest/v1/{table}", headers=HEADERS, json=encoded)
        return jsonify(r.json()), r.status_code

    if action == "update":
        if not where or not fields:
            return jsonify({"error": "Missing 'where' or 'fields'"}), 400
        encoded = process_fields(fields, table)
        r = requests.patch(f"{SUPABASE_URL}/rest/v1/{table}?{where['col']}=eq.{where['val']}", headers=HEADERS, json=encoded)
        return jsonify(r.json()), r.status_code

    if action == "append":
        if not where or not fields:
            return jsonify({"error": "Missing 'where' or 'fields'"}), 400
        existing = requests.get(f"{SUPABASE_URL}/rest/v1/{table}?{where['col']}=eq.{where['val']}", headers=HEADERS).json()
        if not existing:
            return jsonify({"error": "Row not found"}), 404
        decoded = decode_row(existing[0])
        for k, v in fields.items():
            if isinstance(decoded.get(k), dict) and isinstance(v, dict):
                decoded[k].update(v)
            else:
                decoded[k] = v
        encoded = process_fields(decoded, table)
        r = requests.patch(f"{SUPABASE_URL}/rest/v1/{table}?{where['col']}=eq.{where['val']}", headers=HEADERS, json=encoded)
        return jsonify(r.json()), r.status_code

    return jsonify({"error": "Unknown error"}), 500

# ---- Health check ----
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "time": datetime.utcnow().isoformat()})

