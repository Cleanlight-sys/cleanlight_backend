from flask import Flask, request, jsonify
import requests
import os
import unicodedata
import io
from arithmeticcoding import ArithmeticEncoder, ArithmeticDecoder, SimpleFrequencyTable
import base64
import logging

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# --- Load Supabase credentials from environment ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Accept": "application/json"
}

@app.before_request
def log_and_merge():
    """Log every request and merge query/body for resilience."""
    app.logger.info(f"{request.method} {request.path} args={dict(request.args)} body={request.get_data(as_text=True)}")
    if request.method in ['POST', 'PATCH']:
        try:
            body = request.get_json(force=True, silent=True) or {}
            merged = {**request.args.to_dict(), **body}
            request.merged_json = merged
        except Exception:
            request.merged_json = request.args.to_dict()

# --- SUPABASE CRUD ENDPOINTS ---
@app.route('/supa/select', methods=['GET'])
def supa_select():
    table = request.args.get('table')
    if not table:
        return jsonify({"error": "Missing table"}), 400
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    r = requests.get(url, headers=HEADERS)
    try:
        data = r.json()
        return jsonify({"data": data or []}), r.status_code
    except Exception as e:
        return jsonify({"error": "Bad JSON", "raw": r.text}), 500

@app.route('/supa/insert', methods=['POST'])
def supa_insert():
    table = request.args.get('table')
    if not table:
        return jsonify({"error": "Missing table"}), 400
    row = getattr(request, "merged_json", request.json)
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    r = requests.post(url, headers=HEADERS, json=row)
    return (r.text, r.status_code, r.headers.items())

@app.route('/supa/update', methods=['PATCH'])
def supa_update():
    table = request.args.get('table')
    match_column = request.args.get('col')
    match_value = request.args.get('val')
    if not (table and match_column and match_value):
        return jsonify({"error": "Missing params"}), 400
    raw = getattr(request, "merged_json", request.json)
    # The incoming JSON will be {"fields": { ... }}
    update_data = raw.get("fields", {}) if raw else {}
    url = f"{SUPABASE_URL}/rest/v1/{table}?{match_column}=eq.{match_value}"
    r = requests.patch(url, headers=HEADERS, json=update_data)
    return (r.text, r.status_code, r.headers.items())

@app.route('/supa/update_body', methods=['POST'])
def supa_update_body():
    table = request.args.get('table') or request.merged_json.get('table')
    match_column = request.args.get('col') or request.merged_json.get('col')
    match_value = request.args.get('val') or request.merged_json.get('val')
    if not (table and match_column and match_value):
        return jsonify({"error": "Missing params"}), 400
    update_data = request.merged_json
    url = f"{SUPABASE_URL}/rest/v1/{table}?{match_column}=eq.{match_value}"
    r = requests.patch(url, headers=HEADERS, json=update_data)
    try:
        return jsonify({"success": True, "updated_row": r.json()}), r.status_code
    except Exception:
        return jsonify({"success": True, "raw": r.text}), r.status_code

@app.route('/supa/delete', methods=['DELETE'])
def supa_delete():
    table = request.args.get('table')
    match_column = request.args.get('col')
    match_value = request.args.get('val')
    if not (table and match_column and match_value):
        return jsonify({"error": "Missing params"}), 400
    url = f"{SUPABASE_URL}/rest/v1/{table}?{match_column}=eq.{match_value}"
    r = requests.delete(url, headers=HEADERS)
    return (r.text, r.status_code, r.headers.items())

# --- Cleanlight base1k/base10k alphabet ---
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

def compress_arithmetic(data):
    freq = SimpleFrequencyTable([1]*257)
    out = io.BytesIO()
    enc = ArithmeticEncoder(32, out)
    for b in data:
        enc.write(freq, b)
        freq.increment(b)
    enc.write(freq, 256)
    enc.finish()
    return out.getvalue()

def decompress_arithmetic(data):
    freq = SimpleFrequencyTable([1]*257)
    inp = io.BytesIO(data)
    dec = ArithmeticDecoder(32, inp)
    out_bytes = []
    while True:
        sym = dec.read(freq)
        if sym == 256:
            break
        out_bytes.append(sym)
        freq.increment(sym)
    return bytes(out_bytes)

@app.route('/encode1k', methods=['POST'])
def encode1k():
    content = request.get_json()
    if not content or 'data' not in content:
        return jsonify({'error': 'Missing data'}), 400
    raw = content['data']
    if isinstance(raw, str):
        raw = raw.encode('utf-8')
    compressed = compress_arithmetic(raw)
    as_int = int.from_bytes(compressed, 'big')
    alphabet = get_base_alphabet(1000)
    encoded = int_to_baseN(as_int, alphabet)
    return jsonify({'encoded': encoded})

@app.route('/decode1k', methods=['POST'])
def decode1k():
    content = request.get_json()
    if not content or 'encoded' not in content:
        return jsonify({'error': 'Missing encoded'}), 400
    encoded = content['encoded']
    alphabet = get_base_alphabet(1000)
    as_int = baseN_to_int(encoded, alphabet)
    num_bytes = (as_int.bit_length() + 7) // 8
    compressed = as_int.to_bytes(num_bytes, 'big')
    try:
        raw = decompress_arithmetic(compressed)
        try:
            text = raw.decode('utf-8')
        except Exception:
            text = str(raw)
        return jsonify({'data': text})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/encode10k', methods=['POST'])
def encode10k():
    content = request.get_json()
    if not content or 'data' not in content:
        return jsonify({'error': 'Missing data'}), 400
    raw = content['data']
    if isinstance(raw, str):
        try:
            raw = base64.b64decode(raw)
        except Exception:
            raw = raw.encode('utf-8')
    compressed = compress_arithmetic(raw)
    as_int = int.from_bytes(compressed, 'big')
    alphabet = get_base_alphabet(10000)
    encoded = int_to_baseN(as_int, alphabet)
    return jsonify({'encoded': encoded})

@app.route('/decode10k', methods=['POST'])
def decode10k():
    content = request.get_json()
    if not content or 'encoded' not in content:
        return jsonify({'error': 'Missing encoded'}), 400
    encoded = content['encoded']
    alphabet = get_base_alphabet(10000)
    as_int = baseN_to_int(encoded, alphabet)
    num_bytes = (as_int.bit_length() + 7) // 8
    compressed = as_int.to_bytes(num_bytes, 'big')
    try:
        raw = decompress_arithmetic(compressed)
        b64 = base64.b64encode(raw).decode('utf-8')
        return jsonify({'data': b64})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/')
def index():
    return "Cleanlight Key Master API is live.", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

