from flask import Flask, request, jsonify
import requests, json, os, time, base64, unicodedata, zstandard as zstd
from datetime import datetime
from uuid import uuid4

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

# ---- State store for interactive ops ----
STATE = {}
STATE_TIMEOUT = 300  # seconds

def prune_state():
    now = time.time()
    expired = [k for k,v in STATE.items() if now - v["timestamp"] > STATE_TIMEOUT]
    for k in expired:
        del STATE[k]

# ---- Encoding helpers ----
def get_base_alphabet(n):
    safe = []
    for codepoint in range(0x20, 0x2FFFF):
        ch = chr(codepoint)
        name = unicodedata.name(ch, "")
        if (
            0xD800 <= codepoint <= 0xDFFF or
            0xFDD0 <= codepoint <= 0xFDEF or
            codepoint & 0xFFFE == 0xFFFE or
            "CONTROL" in name or "PRIVATE USE" in name or
            "COMBINING" in name or "FORMAT" in name or
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
def process_fields(data, encode=True, table=None):
    processed = {}
    for key, val in data.items():
        if key not in ALLOWED_FIELDS[table]:
            raise ValueError(f"Field {key} not allowed")
        if key in ("id", "cognition", "pointer_net"):
            processed[key] = val
        elif key == "images" and val is not None:
            processed[key] = encode_std10k(base64.b64decode(val)) if encode else val
        elif key in ("mir", "codex", "insight") and val is not None:
            processed[key] = encode_std1k(val if isinstance(val, str) else json.dumps(val)) if encode else val
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

# ---- Command endpoint ----
@app.route("/flask/command", methods=["POST"])
def command():
    prune_state()
    payload = request.get_json(force=True) or {}
    action = payload.get("action")
    table = payload.get("table")
    where = payload.get("where", {})
    fields = payload.get("fields")
    token = payload.get("state_token")

    # Resume from state if token provided
    if token and token in STATE:
        state = STATE[token]
        action = action or state.get("action")
        table = table or state.get("table")
        where = where or state.get("where", {})
        fields = fields or state.get("fields")
        STATE[token].update({"action": action, "table": table, "where": where, "fields": fields})
    else:
        # New operation, validate basics
        if not action:
            t = str(uuid4())
            STATE[t] = {"timestamp": time.time(), "table": table, "where": where, "fields": fields, "action": None}
            return jsonify({"prompt": "What action do you want? (read_table, read_row, insert, update, append)", "state_token": t})
        if not table:
            t = str(uuid4())
            STATE[t] = {"timestamp": time.time(), "action": action, "where": where, "fields": fields, "table": None}
            return jsonify({"prompt": "Which table? (cleanlight_canvas, cleanlight_map)", "state_token": t})

    # Now check for missing pieces
    if action in ("insert", "update", "append") and not fields:
        t = token or str(uuid4())
        STATE[t] = {"timestamp": time.time(), "action": action, "table": table, "where": where, "fields": None}
        return jsonify({"prompt": "Please provide fields to write.", "state_token": t})
    if action in ("update", "append", "read_row") and not where:
        t = token or str(uuid4())
        STATE[t] = {"timestamp": time.time(), "action": action, "table": table, "fields": fields, "where": None}
        return jsonify({"prompt": "Which row? Provide {\"col\": ..., \"val\": ...}", "state_token": t})

    # Execute action
    if action == "read_table":
        r = requests.get(f"{SUPABASE_URL}/rest/v1/{table}", headers=HEADERS)
        return jsonify([decode_row(row) for row in r.json()])

    if action == "read_row":
        r = requests.get(f"{SUPABASE_URL}/rest/v1/{table}?{where['col']}=eq.{where['val']}", headers=HEADERS)
        return jsonify([decode_row(row) for row in r.json()])

    if action == "insert":
        encoded = process_fields(fields, encode=True, table=table)
        r = requests.post(f"{SUPABASE_URL}/rest/v1/{table}", headers=HEADERS, json=encoded)
        return jsonify(r.json()), r.status_code

    if action == "update":
        encoded = process_fields(fields, encode=True, table=table)
        r = requests.patch(f"{SUPABASE_URL}/rest/v1/{table}?{where['col']}=eq.{where['val']}", headers=HEADERS, json=encoded)
        return jsonify(r.json()), r.status_code

    if action == "append":
        existing = requests.get(f"{SUPABASE_URL}/rest/v1/{table}?{where['col']}=eq.{where['val']}", headers=HEADERS).json()
        if not existing:
            return jsonify({"error": "Row not found"}), 404
        decoded = decode_row(existing[0])
        for k,v in fields.items():
            if isinstance(decoded.get(k), dict) and isinstance(v, dict):
                decoded[k].update(v)
            else:
                decoded[k] = v
        encoded = process_fields(decoded, encode=True, table=table)
        r = requests.patch(f"{SUPABASE_URL}/rest/v1/{table}?{where['col']}=eq.{where['val']}", headers=HEADERS, json=encoded)
        return jsonify(r.json()), r.status_code

    return jsonify({"error": "Unknown action"}), 400

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "time": datetime.utcnow().isoformat()})
