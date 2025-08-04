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
    app.logger.info(f"{request.method} {request.path} args={dict(request.args)} body={request.get_data(as_text=True)}")
    if request.method in ['POST', 'PATCH']:
        try:
            body = request.get_json(force=True, silent=True) or {}
            merged = {**request.args.to_dict(), **body}
            request.merged_json = merged
        except Exception:
            request.merged_json = request.args.to_dict()

# ------------------- SMART 1K HELPERS -------------------
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

def encode_smart_1k(value):
    if not isinstance(value, str):
        value = str(value)
    compressed = compress_arithmetic(value.encode('utf-8'))
    as_int = int.from_bytes(compressed, 'big')
    return int_to_baseN(as_int, get_base_alphabet(1000))

def decode_smart_1k(value):
    try:
        as_int = baseN_to_int(value, get_base_alphabet(1000))
        num_bytes = (as_int.bit_length() + 7) // 8
        compressed = as_int.to_bytes(num_bytes, 'big')
        return decompress_arithmetic(compressed).decode('utf-8')
    except Exception:
        return value

def process_fields(data, encode=True):
    processed = {}
    for key, val in data.items():
        if key == "cognition":
            processed[key] = val  # leave cognition uncompressed
        else:
            processed[key] = encode_smart_1k(val) if encode else val
    return processed

# ------------------- SUPABASE CRUD -------------------
@app.route('/supa/select', methods=['GET'])
def supa_select():
    table = request.args.get('table')
    decode_flag = request.args.get('decode', 'false').lower() == 'true'
    if not table:
        return jsonify({"error": "Missing table"}), 400
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    r = requests.get(url, headers=HEADERS)
    data = r.json()

    if decode_flag:
        for row in data:
            for key, val in row.items():
                if isinstance(val, str) and key != "cognition":
                    row[key] = decode_smart_1k(val)

    return jsonify({"data": data or []}), r.status_code

@app.route('/supa/insert', methods=['POST'])
def supa_insert():
    table = request.args.get('table')
    if not table:
        return jsonify({"error": "Missing table"}), 400
    raw = getattr(request, "merged_json", request.json)
    encoded_row = process_fields(raw)
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    r = requests.post(url, headers=HEADERS, json=encoded_row)
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

@app.route('/')
def index():
    return "Cleanlight Key Master API with SMART_1K compression is live.", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
