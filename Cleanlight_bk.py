from flask import Flask, request, jsonify
import requests, json, os, base64
import zstandard as zstd
from datetime import datetime

app = Flask(__name__)

# ---- Config ----
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY")

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Prefer": "return=representation",
}

ALLOWED_FIELDS = {
    "cleanlight_canvas": ["id", "cognition", "mir", "insight", "codex", "images", "checksums", "timestamps"],
    "cleanlight_map":    ["id", "cognition", "mir", "insight", "codex", "images", "pointer_net", "macro_group"],
}

# ---- Encoding helpers ----
def get_base_alphabet(n):
    safe = []
    for codepoint in range(0x21, 0x2FFFF):
        ch = chr(codepoint)
        if (0xD800 <= codepoint <= 0xDFFF) or (0xFDD0 <= codepoint <= 0xFDEF) or (codepoint & 0xFFFE == 0xFFFE):
            continue
        if ch in {'"', "'", "\\"}:
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

# ---- Utilities ----
def _json_or_text(resp):
    try:
        if resp.content and resp.headers.get("Content-Type","").startswith("application/json"):
            return resp.json()
    except Exception:
        pass
    return {"status": resp.status_code, "body": resp.text}

def decode_row(row):
    row = dict(row)
    if "images" in row and row["images"]:
        try:
            row["images"] = base64.b64encode(decode_std10k(row["images"])).decode('ascii')
        except Exception:
            pass
    for k in ("mir", "codex", "insight"):
        if k in row and row[k]:
            try:
                row[k] = decode_std1k(row[k])
            except Exception:
                pass
    return row

def process_single_field(table, field, value):
    if field not in ALLOWED_FIELDS[table]:
        raise ValueError(f"Field {field} not allowed for table {table}")
    if field == "images" and value is not None:
        return encode_std10k(base64.b64decode(value))
    if field in ("mir", "codex", "insight") and value is not None:
        return encode_std1k(value if isinstance(value, str) else json.dumps(value))
    return value

def process_fields(table, data):
    out = {}
    for k, v in (data or {}).items():
        if k not in ALLOWED_FIELDS[table]:
            raise ValueError(f"Field {k} not allowed for table {table}")
        out[k] = process_single_field(table, k, v)
    return out

def sb_list(table, limit=50, offset=0, select=None):
    params = {"limit": limit, "offset": offset}
    if select:
        params["select"] = select
    r = requests.get(f"{SUPABASE_URL}/rest/v1/{table}", headers=HEADERS, params=params)
    data = _json_or_text(r)
    if isinstance(data, list):
        if not select or "id" in select or "," not in select or "*" in select:
            return [decode_row(x) for x in data], r.status_code
        # column-only rows need per-cell decode if field is encoded
        return data, r.status_code
    return data, r.status_code

def sb_get_by_id(table, row_id, select="*"):
    r = requests.get(f"{SUPABASE_URL}/rest/v1/{table}?id=eq.{row_id}&select={select}", headers=HEADERS)
    data = _json_or_text(r)
    if isinstance(data, list):
        if not data:
            return None, 404
        return decode_row(data[0]) if select == "*" else data[0], 200
    return data, r.status_code

def sb_insert(table, fields):
    encoded = process_fields(table, fields)
    r = requests.post(f"{SUPABASE_URL}/rest/v1/{table}", headers=HEADERS, json=encoded)
    data = _json_or_text(r)
    if isinstance(data, list) and data:
        return decode_row(data[0]), r.status_code
    if isinstance(data, dict):
        return decode_row(data), r.status_code
    return data, r.status_code

def sb_update_by_id(table, row_id, fields):
    encoded = process_fields(table, fields)
    r = requests.patch(f"{SUPABASE_URL}/rest/v1/{table}?id=eq.{row_id}", headers=HEADERS, json=encoded)
    data = _json_or_text(r)
    if isinstance(data, list) and data:
        return decode_row(data[0]), r.status_code
    if isinstance(data, dict):
        return decode_row(data), r.status_code
    return data, r.status_code

def sb_merge_cell(table, row_id, field, value):
    # fetch current value
    existing, code = sb_get_by_id(table, row_id)
    if code == 404 or existing is None:
        return {"error": "Not found"}, 404
    cur = existing.get(field)
    # merge semantics
    if isinstance(cur, dict) and isinstance(value, dict):
        new_val = {**cur, **value}
    elif isinstance(cur, list) and isinstance(value, list):
        new_val = cur + value
    else:
        # scalars or mismatched types → overwrite
        new_val = value
    # store (with encoding)
    stored = process_single_field(table, field, new_val)
    r = requests.patch(f"{SUPABASE_URL}/rest/v1/{table}?id=eq.{row_id}", headers=HEADERS, json={field: stored})
    data = _json_or_text(r)
    if isinstance(data, list) and data:
        return decode_row(data[0]), r.status_code
    if isinstance(data, dict):
        return decode_row(data), r.status_code
    return data, r.status_code

def decode_cell_value(table, field, raw_value):
    # inverse of process_single_field
    if field == "images" and raw_value:
        try:
            return base64.b64encode(decode_std10k(raw_value)).decode('ascii')
        except Exception:
            return raw_value
    if field in ("mir", "codex", "insight") and raw_value:
        try:
            return decode_std1k(raw_value)
        except Exception:
            return raw_value
    return raw_value

# ---- Health ----
@app.get("/health")
def health():
    return jsonify({"status": "ok", "time": datetime.utcnow().isoformat()})

# ---------- CANVAS ----------
@app.get("/canvas")
def list_canvas():
    limit = int(request.args.get("limit", 50))
    offset = int(request.args.get("offset", 0))
    data, code = sb_list("cleanlight_canvas", limit, offset)
    return jsonify(data), code

@app.get("/canvas/all")
def list_canvas_all():
    cap = min(int(request.args.get("limit", 10000)), 10000)
    rows = []
    offset = 0
    page = 1000  # chunk to avoid PostgREST caps
    while len(rows) < cap:
        batch, code = sb_list("cleanlight_canvas", min(page, cap - len(rows)), offset)
        if not isinstance(batch, list) or not batch:
            break
        rows.extend(batch)
        offset += len(batch)
        if len(batch) < page:
            break
    return jsonify(rows[:cap]), 200

@app.post("/canvas")
def create_canvas():
    body = request.get_json(force=True) or {}
    if isinstance(body.get("images"), str) and len(body["images"]) > 7_000_000:
        return jsonify({"error": "images too large"}), 413
    data, code = sb_insert("cleanlight_canvas", body)
    return jsonify(data), code

@app.get("/canvas/<int:row_id>")
def get_canvas(row_id):
    data, code = sb_get_by_id("cleanlight_canvas", row_id)
    if code == 404:
        return jsonify({"error": "Not found"}), 404
    return jsonify(data), code

@app.patch("/canvas/<int:row_id>")
def update_canvas(row_id):
    body = request.get_json(force=True) or {}
    data, code = sb_update_by_id("cleanlight_canvas", row_id, body)
    if code == 404:
        return jsonify({"error": "Not found"}), 404
    return jsonify(data), code

@app.get("/canvas/<int:row_id>/cell/<field>")
def get_canvas_cell(row_id, field):
    if field not in ALLOWED_FIELDS["cleanlight_canvas"]:
        return jsonify({"error": "Invalid field"}), 400
    rec, code = sb_get_by_id("cleanlight_canvas", row_id, select=f"id,{field}")
    if code == 404 or rec is None:
        return jsonify({"error": "Not found"}), 404
    value = decode_cell_value("cleanlight_canvas", field, rec.get(field))
    return jsonify({"id": row_id, "field": field, "value": value}), 200

@app.post("/canvas/<int:row_id>/cell/<field>")
def append_canvas_cell(row_id, field):
    if field not in ALLOWED_FIELDS["cleanlight_canvas"]:
        return jsonify({"error": "Invalid field"}), 400
    body = request.get_json(force=True) or {}
    if "value" not in body:
        return jsonify({"error": "Missing 'value'"}), 400
    data, code = sb_merge_cell("cleanlight_canvas", row_id, field, body["value"])
    return jsonify(data), code

@app.get("/canvas/column/<field>")
def get_canvas_column(field):
    if field not in ALLOWED_FIELDS["cleanlight_canvas"]:
        return jsonify({"error": "Invalid field"}), 400
    limit = int(request.args.get("limit", 50))
    offset = int(request.args.get("offset", 0))
    raw, code = sb_list("cleanlight_canvas", limit, offset, select=f"id,{field}")
    if not isinstance(raw, list):
        return jsonify(raw), code
    out = []
    for r in raw:
        val = decode_cell_value("cleanlight_canvas", field, r.get(field))
        out.append({"id": r.get("id"), "value": val})
    return jsonify(out), 200

# ---------- MAP ----------
@app.get("/map")
def list_map():
    limit = int(request.args.get("limit", 50))
    offset = int(request.args.get("offset", 0))
    data, code = sb_list("cleanlight_map", limit, offset)
    return jsonify(data), code

@app.get("/map/all")
def list_map_all():
    cap = min(int(request.args.get("limit", 10000)), 10000)
    rows = []
    offset = 0
    page = 1000
    while len(rows) < cap:
        batch, code = sb_list("cleanlight_map", min(page, cap - len(rows)), offset)
        if not isinstance(batch, list) or not batch:
            break
        rows.extend(batch)
        offset += len(batch)
        if len(batch) < page:
            break
    return jsonify(rows[:cap]), 200

@app.post("/map")
def create_map():
    body = request.get_json(force=True) or {}
    if isinstance(body.get("images"), str) and len(body["images"]) > 7_000_000:
        return jsonify({"error": "images too large"}), 413
    data, code = sb_insert("cleanlight_map", body)
    return jsonify(data), code

@app.get("/map/<int:row_id>")
def get_map(row_id):
    data, code = sb_get_by_id("cleanlight_map", row_id)
    if code == 404:
        return jsonify({"error": "Not found"}), 404
    return jsonify(data), code

@app.patch("/map/<int:row_id>")
def update_map(row_id):
    body = request.get_json(force=True) or {}
    data, code = sb_update_by_id("cleanlight_map", row_id, body)
    if code == 404:
        return jsonify({"error": "Not found"}), 404
    return jsonify(data), code

@app.get("/map/<int:row_id>/cell/<field>")
def get_map_cell(row_id, field):
    if field not in ALLOWED_FIELDS["cleanlight_map"]:
        return jsonify({"error": "Invalid field"}), 400
    rec, code = sb_get_by_id("cleanlight_map", row_id, select=f"id,{field}")
    if code == 404 or rec is None:
        return jsonify({"error": "Not found"}), 404
    value = decode_cell_value("cleanlight_map", field, rec.get(field))
    return jsonify({"id": row_id, "field": field, "value": value}), 200

@app.post("/map/<int:row_id>/cell/<field>")
def append_map_cell(row_id, field):
    if field not in ALLOWED_FIELDS["cleanlight_map"]:
        return jsonify({"error": "Invalid field"}), 400
    body = request.get_json(force=True) or {}
    if "value" not in body:
        return jsonify({"error": "Missing 'value'"}), 400
    data, code = sb_merge_cell("cleanlight_map", row_id, field, body["value"])
    return jsonify(data), code

@app.get("/map/column/<field>")
def get_map_column(field):
    if field not in ALLOWED_FIELDS["cleanlight_map"]:
        return jsonify({"error": "Invalid field"}), 400
    limit = int(request.args.get("limit", 50))
    offset = int(request.args.get("offset", 0))
    raw, code = sb_list("cleanlight_map", limit, offset, select=f"id,{field}")
    if not isinstance(raw, list):
        return jsonify(raw), code
    out = []
    for r in raw:
        val = decode_cell_value("cleanlight_map", field, r.get(field))
        out.append({"id": r.get("id"), "value": val})
    return jsonify(out), 200
