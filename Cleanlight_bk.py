print("App started — top of file", flush=True)
from flask import Flask, request, jsonify
import requests
import os
import unicodedata
import io
import zstandard as zstd
import base64
import logging

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

def process_fields(data, encode=True):
    processed = {}
    for key, val in data.items():
        if key == "cognition":
            processed[key] = val  # Always plain text
        elif key == "images" and val is not None:
            if encode:
                # Accepts base64 for images, store as STD10K
                if isinstance(val, str):
                    image_bytes = base64.b64decode(val)
                else:
                    image_bytes = val
                processed[key] = encode_std10k(image_bytes)
            else:
                processed[key] = val
        elif key in ("mir", "codex", "insight") and val is not None:
            if encode:
                # Accept plain text for all, store as STD1K
                processed[key] = encode_std1k(val)
            else:
                processed[key] = val
        else:
            processed[key] = val
    return processed

def decode_row_for_api(row):
    # For API output, convert STD1K/STD10K fields to base64/plaintext
    for key in row:
        if key == "cognition":
            continue  # Always plain text
        elif key == "images" and row[key]:
            # Decode STD10K to bytes, encode as base64 (for display)
            image_bytes = decode_std10k(row[key])
            row[key] = base64.b64encode(image_bytes).decode('ascii')
        elif key in ("mir", "codex", "insight") and row[key]:
            # Decode STD1K to plaintext
            row[key] = decode_std1k(row[key])
    return row

# ------------------ SUPABASE CRUD ------------------

@app.route('/supa/select', methods=['GET'])
def supa_select():
    table = request.args.get('table')
    decode_flag = request.args.get('decode', 'false').lower() == 'true'
    limit = request.args.get('limit')
    if not table:
        return jsonify({"error": "Missing table"}), 400

    url = f"{SUPABASE_URL}/rest/v1/{table}"
    if limit:
        url += f"?limit={limit}"

    r = requests.get(url, headers=HEADERS)

    try:
        data = r.json()
        # If Supabase returns error dict (e.g., {'message': 'error here'})
        if isinstance(data, dict) and data.get('message'):
            return jsonify({"error": data['message']}), 400
    except Exception as e:
        return jsonify({"error": f"Failed to parse Supabase response: {str(e)}"}), 400

    if not isinstance(data, list):
        return jsonify({"error": "Supabase did not return a list"}), 400

    # Always decode for now; you could honor decode_flag if you like
    data = [decode_row_for_api(row) for row in data]

    return jsonify({"data": data}), 200
    
@app.route('/supa/insert', methods=['POST'])
def supa_insert():
    table = request.args.get('table')
    if not table:
        return jsonify({"error": "Missing table"}), 400
    raw = getattr(request, "merged_json", request.json)
    encoded_row = process_fields(raw)
    url = f"{SUPABASE_URL}/rest/v1/{table}"   # <--- This is perfect.
    print(f"Posting to: {url}", flush=True)
    r = requests.post(url, headers=HEADERS, json=encoded_row)
    app.logger.info(f"Supabase response: {r.status_code} {r.text}")
    return (r.text, r.status_code, r.headers.items())
    
@app.route('/supa/update', methods=['PATCH'])
def supa_update():
    table = request.args.get('table')
    col = request.args.get('col')
    val = request.args.get('val')
    if not (table and col and val):
        return jsonify({"error": "Missing params"}), 400
    raw = getattr(request, "merged_json", request.json)
    update_data = raw.get("fields", {}) if raw else {}
    encoded_data = process_fields(update_data)
    url = f"{SUPABASE_URL}/rest/v1/{table}?{col}=eq.{val}"
    r = requests.patch(url, headers=HEADERS, json=encoded_data)
    return (r.text, r.status_code, r.headers.items())

@app.route('/supa/delete', methods=['DELETE'])
def supa_delete():
    table = request.args.get('table')
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
        # Output as base64 for easy handling
        return jsonify({"decoded": base64.b64encode(decoded).decode('ascii')}), 200
    except Exception as e:
        return jsonify({"error": f"Failed to decode: {str(e)}"}), 400

@app.route('/')
def index():
    return "Cleanlight 2.0 STDxK (no Smart64 for text) API is live.", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)





