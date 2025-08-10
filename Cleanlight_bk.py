# Cleanlight_bk.py — unified backend (rid OR where, auto-paging, std1k/std10k)
# Start with: gunicorn Cleanlight_bk:app

from flask import Flask, request, jsonify
import os, json, base64, requests
import zstandard as zstd
from datetime import datetime
from urllib.parse import quote_plus

# -------------------- 1) App --------------------
app = Flask(__name__)

# -------------------- 2) Config --------------------
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

# Allowed fields (must match your DB schema)
ALLOWED_FIELDS = {
    "cleanlight_canvas": [
        "id", "cognition", "mir", "insight", "codex", "images", "checksums", "timestamps"
    ],
    "cleanlight_map": [
        # if your PK is map_id, you may add it here; ALLOWED_FIELDS is for updatable cols, not filters
        "id", "cognition", "mir", "insight", "codex", "images", "pointer_net", "macro_group"
    ],
}

# -------------------- 3) Encoding helpers --------------------
def get_base_alphabet(n: int) -> str:
    safe = []
    for codepoint in range(0x21, 0x2FFFF):  # skip space; avoid control/surrogates/nonchars
        if (0xD800 <= codepoint <= 0xDFFF) or (0xFDD0 <= codepoint <= 0xFDEF) or (codepoint & 0xFFFE == 0xFFFE):
            continue
        ch = chr(codepoint)
        if ch in {'"', "'", "\\"}:
            continue
        safe.append(ch)
        if len(safe) == n:
            break
    return ''.join(safe)

BASE1K = get_base_alphabet(1000)
BASE10K = get_base_alphabet(10000)

def int_to_baseN(num: int, alphabet: str) -> str:
    if num == 0:
        return alphabet[0]
    base = len(alphabet)
    digits = []
    while num:
        digits.append(alphabet[num % base])
        num //= base
    return ''.join(reversed(digits))

def baseN_to_int(s: str, alphabet: str) -> int:
    base = len(alphabet)
    alpha_map = {ch: i for i, ch in enumerate(alphabet)}
    num = 0
    for ch in s:
        num = num * base + alpha_map[ch]
    return num

def encode_std1k(plaintext: str) -> str:
    cctx = zstd.ZstdCompressor()
    compressed = cctx.compress(plaintext.encode("utf-8"))
    return int_to_baseN(int.from_bytes(compressed, "big"), BASE1K)

def decode_std1k(std1k_str: str) -> str:
    as_int = baseN_to_int(std1k_str, BASE1K)
    compressed = as_int.to_bytes((as_int.bit_length() + 7) // 8, "big")
    return zstd.ZstdDecompressor().decompress(compressed).decode("utf-8")

def encode_std10k(image_bytes: bytes) -> str:
    cctx = zstd.ZstdCompressor()
    compressed = cctx.compress(image_bytes)
    return int_to_baseN(int.from_bytes(compressed, "big"), BASE10K)

def decode_std10k(std10k_str: str) -> bytes:
    as_int = baseN_to_int(std10k_str, BASE10K)
    compressed = as_int.to_bytes((as_int.bit_length() + 7) // 8, "big")
    return zstd.ZstdDecompressor().decompress(compressed)

# -------------------- 4) Row/field processing --------------------
def process_single_field(table: str, field: str, value):
    if field not in ALLOWED_FIELDS[table]:
        raise ValueError(f"Field {field} not allowed for table {table}")
    if field == "images" and value is not None:
        return encode_std10k(base64.b64decode(value))  # base64 in → std10k stored
    if field in ("mir", "codex", "insight") and value is not None:
        return encode_std1k(value if isinstance(value, str) else json.dumps(value))
    return value

def process_fields(table: str, data: dict) -> dict:
    out = {}
    for k, v in (data or {}).items():
        if k not in ALLOWED_FIELDS[table]:
            raise ValueError(f"Field {k} not allowed for table {table}")
        out[k] = process_single_field(table, k, v)
    return out

def decode_row(row: dict) -> dict:
    row = dict(row)
    if "images" in row and row["images"]:
        try:
            row["images"] = base64.b64encode(decode_std10k(row["images"])).decode("ascii")
        except Exception:
            pass
    for k in ("mir", "codex", "insight"):
        if k in row and row[k]:
            try:
                row[k] = decode_std1k(row[k])
            except Exception:
                pass
    return row

def decode_cell_value(table: str, field: str, raw_value):
    if field == "images" and raw_value:
        try:
            return base64.b64encode(decode_std10k(raw_value)).decode("ascii")
        except Exception:
            return raw_value
    if field in ("mir", "codex", "insight") and raw_value:
        try:
            return decode_std1k(raw_value)
        except Exception:
            return raw_value
    return raw_value

# -------------------- 5) Supabase helpers (id OR where) --------------------
def _json_or_text(resp: requests.Response):
    try:
        if resp.content and resp.headers.get("Content-Type", "").startswith("application/json"):
            return resp.json()
    except Exception:
        pass
    return {"status": resp.status_code, "body": resp.text}

def sb_list(table: str, limit=50, offset=0, select: str | None = None):
    params = {"limit": limit, "offset": offset}
    if select:
        params["select"] = select
    r = requests.get(f"{SUPABASE_URL}/rest/v1/{table}", headers=HEADERS, params=params)
    data = _json_or_text(r)
    if isinstance(data, list) and (not select or select == "*" or "id" in select):
        return [decode_row(x) for x in data], r.status_code
    return data, r.status_code

def sb_get_by_id(table: str, row_id: int, select: str = "*"):
    r = requests.get(f"{SUPABASE_URL}/rest/v1/{table}?id=eq.{row_id}&select={select}", headers=HEADERS)
    data = _json_or_text(r)
    if isinstance(data, list):
        if not data:
            return None, 404
        return (decode_row(data[0]) if select == "*" else data[0]), 200
    return data, r.status_code

def sb_get_where(table: str, col: str, val, select: str = "*"):
    qv = quote_plus(str(val))
    r = requests.get(f"{SUPABASE_URL}/rest/v1/{table}?{col}=eq.{qv}&select={select}", headers=HEADERS)
    data = _json_or_text(r)
    if isinstance(data, list):
        if not data:
            return None, 404
        return (decode_row(data[0]) if select == "*" else data[0]), 200
    return data, r.status_code

def sb_insert(table: str, fields: dict):
    encoded = process_fields(table, fields)
    r = requests.post(f"{SUPABASE_URL}/rest/v1/{table}", headers=HEADERS, json=encoded)
    data = _json_or_text(r)
    if isinstance(data, list) and data:
        return decode_row(data[0]), r.status_code
    if isinstance(data, dict):
        return decode_row(data), r.status_code
    return data, r.status_code

def sb_update_by_id(table: str, row_id: int, fields: dict):
    encoded = process_fields(table, fields)
    r = requests.patch(f"{SUPABASE_URL}/rest/v1/{table}?id=eq.{row_id}", headers=HEADERS, json=encoded)
    data = _json_or_text(r)
    if isinstance(data, list) and data:
        return decode_row(data[0]), r.status_code
    if isinstance(data, dict):
        return decode_row(data), r.status_code
    return data, r.status_code

def sb_update_where(table: str, col: str, val, fields: dict):
    encoded = process_fields(table, fields)
    qv = quote_plus(str(val))
    r = requests.patch(f"{SUPABASE_URL}/rest/v1/{table}?{col}=eq.{qv}", headers=HEADERS, json=encoded)
    data = _json_or_text(r)
    if isinstance(data, list) and data:
        return decode_row(data[0]), r.status_code
    if isinstance(data, dict):
        return decode_row(data), r.status_code
    return data, r.status_code

def sb_delete_where(table: str, col: str, val):
    qv = quote_plus(str(val))
    r = requests.delete(f"{SUPABASE_URL}/rest/v1/{table}?{col}=eq.{qv}", headers=HEADERS)
    return _json_or_text(r), r.status_code

def sb_merge_cell_by_where(table: str, col: str, val, field: str, value):
    rec, code = sb_get_where(table, col, val, select="*")
    if code == 404 or rec is None:
        return {"error": "Not found"}, 404
    cur = rec.get(field)
    if isinstance(cur, dict) and isinstance(value, dict):
        new_val = {**cur, **value}
    elif isinstance(cur, list) and isinstance(value, list):
        new_val = cur + value
    else:
        new_val = value
    stored = process_single_field(table, field, new_val)
    qv = quote_plus(str(val))
    r = requests.patch(f"{SUPABASE_URL}/rest/v1/{table}?{col}=eq.{qv}", headers=HEADERS, json={field: stored})
    res = _json_or_text(r)
    if isinstance(res, list) and res:
        return decode_row(res[0]), r.status_code
    if isinstance(res, dict):
        return decode_row(res), r.status_code
    return res, r.status_code

def _read_table_autopage(table: str, limit_total=10000, page_size=1000, start_offset=0):
    rows = []
    offset = int(start_offset)
    cap = int(limit_total)
    step = max(1, min(int(page_size), 1000))
    while len(rows) < cap:
        batch, code = sb_list(table, min(step, cap - len(rows)), offset)
        if code >= 300 or not isinstance(batch, list) or not batch:
            break
        rows.extend(batch)
        offset += len(batch)
        if len(batch) < step:
            break
    return rows[:cap]

# -------------------- 6) Normalizers --------------------
def _norm_action(a):
    if not a:
        return None
    a = str(a).lower().strip()
    aliases = {
        "create": "write", "insert": "write", "add": "write",
        "patch": "update", "modify": "update",
        "appendcell": "append_cell", "append_field": "append_cell",
        "writecell": "write_cell", "set_cell": "write_cell",
        "get": "read_row", "read": "read_row", "fetch": "read_row",
        "column": "read_column", "all": "read_all", "list": "read_table",
        "remove": "delete", "del": "delete",
    }
    return aliases.get(a, a)

def _pick(dct, *keys):
    for k in keys:
        if k in dct and dct[k] is not None:
            return dct[k]
    return None

def _norm_table(payload):
    t = _pick(payload, "target", "table", "Target")
    if t:
        return str(t)
    tables = payload.get("tables")
    if isinstance(tables, list) and tables:
        return str(tables[0])
    return None

def _norm_rid(payload):
    rid = _pick(payload, "rid", "id", "TargetRID")
    if rid is not None:
        return int(rid)
    where = payload.get("where")
    # prefer explicit rid only when where.col is literally 'id'
    if isinstance(where, dict) and str(where.get("col")).lower() == "id":
        return int(where.get("val"))
    return None

def _norm_field(payload):
    return _pick(payload, "field", "TargetField")

def _norm_value(payload):
    if "value" in payload:
        return payload["value"]
    if "payload" in payload:
        return payload["payload"]
    if "fields" in payload:
        return payload["fields"]
    return None

# -------------------- 7) Routes --------------------
@app.get("/health")
def health():
    return jsonify({"status": "ok", "time": datetime.utcnow().isoformat()})

@app.post("/command")
def unified_command():
    body = request.get_json(force=True) or {}

    # Legacy shim: {action, table, where, fields}
    if "action" in body and "table" in body and ("fields" in body or body.get("action") in ("read_table", "read_row")):
        body = dict(body)
        body.setdefault("target", body.get("table"))
        # If where.id present, surface as rid; otherwise keep where for generic ops
        w = body.get("where") or {}
        if body.get("action") == "read_row" and "rid" not in body and str(w.get("col")).lower() == "id":
            body["rid"] = w.get("val")

    action = _norm_action(body.get("action"))
    table = _norm_table(body)
    rid = _norm_rid(body)
    field = _norm_field(body)
    value = _norm_value(body)
    where = body.get("where") if isinstance(body.get("where"), dict) else None
    where_col = str(where.get("col")) if where and "col" in where else None
    where_val = where.get("val") if where and "val" in where else None

    if action is None or table is None:
        return jsonify({"error": "Missing action or target table"}), 400
    if table not in ("cleanlight_canvas", "cleanlight_map"):
        return jsonify({"error": "Invalid table"}), 400

    autopage = bool(body.get("autopage", action in ("read_table", "read_all")))
    limit_total = int(body.get("limit_total", 10000))
    limit = int(body.get("limit", 1000))
    offset = int(body.get("offset", 0))

    try:
        # ---- READS ----
        if action == "read_table":
            if autopage:
                rows = _read_table_autopage(table, limit_total=limit_total, page_size=limit, start_offset=offset)
                return jsonify(rows), 200
            data, code = sb_list(table, limit, offset)
            return jsonify(data), code

        if action == "read_all":
            rows = _read_table_autopage(table, limit_total=limit_total, page_size=limit, start_offset=offset)
            return jsonify(rows), 200

        if action == "read_row":
            if rid is not None:
                data, code = sb_get_by_id(table, rid)
            elif where_col and where_val is not None:
                data, code = sb_get_where(table, where_col, where_val, select="*")
            else:
                return jsonify({"error": "Missing rid or where {col,val}"}), 400
            if code == 404:
                return jsonify({"error": "Not found"}), 404
            return jsonify(data), code

        if action == "read_cell":
            if not field:
                return jsonify({"error": "Missing field"}), 400
            if field not in ALLOWED_FIELDS[table]:
                return jsonify({"error": "Invalid field"}), 400
            if rid is not None:
                rec, code = sb_get_by_id(table, rid, select=f"id,{field}")
            elif where_col and where_val is not None:
                rec, code = sb_get_where(table, where_col, where_val, select=f"id,{field}")
            else:
                return jsonify({"error": "Missing rid or where {col,val}"}), 400
            if code == 404 or rec is None:
                return jsonify({"error": "Not found"}), 404
            v = decode_cell_value(table, field, rec.get(field))
            return jsonify({"id": rec.get("id"), "field": field, "value": v}), 200

        if action == "read_column":
            if not field:
                return jsonify({"error": "Missing field"}), 400
            if field not in ALLOWED_FIELDS[table]:
                return jsonify({"error": "Invalid field"}), 400
            raw, code = sb_list(table, limit, offset, select=f"id,{field}")
            if not isinstance(raw, list):
                return jsonify(raw), code
            out = [{"id": r.get("id"), "value": decode_cell_value(table, field, r.get(field))} for r in raw]
            return jsonify(out), 200

        # ---- WRITES ----
        if action in ("write", "create", "insert", "add"):
            if not isinstance(value, dict):
                return jsonify({"error": "payload/fields must be an object"}), 400
            if isinstance(value.get("images"), str) and len(value["images"]) > 7_000_000:
                return jsonify({"error": "images too large"}), 413
            data, code = sb_insert(table, value)
            return jsonify(data), code

        if action == "update":
            if not isinstance(value, dict):
                return jsonify({"error": "payload/fields must be an object"}), 400
            if rid is not None:
                data, code = sb_update_by_id(table, rid, value)
            elif where_col and where_val is not None:
                data, code = sb_update_where(table, where_col, where_val, value)
            else:
                return jsonify({"error": "Missing rid or where {col,val}"}), 400
            if isinstance(data, dict) and data.get("error") == "Not found":
                return jsonify(data), 404
            return jsonify(data), code

        if action == "append":
            if not isinstance(value, dict):
                return jsonify({"error": "payload/fields must be an object"}), 400
            # read existing, merge, write back (by rid OR where)
            if rid is not None:
                existing, code = sb_get_by_id(table, rid)
                if code == 404 or existing is None:
                    return jsonify({"error": "Not found"}), 404
                merged = dict(existing)
                for k, v in value.items():
                    cur = merged.get(k)
                    if isinstance(cur, dict) and isinstance(v, dict):
                        cur = {**cur, **v}
                    elif isinstance(cur, list) and isinstance(v, list):
                        cur = cur + v
                    else:
                        cur = v
                    merged[k] = cur
                merged.pop("id", None)
                data, code = sb_update_by_id(table, rid, merged)
                return jsonify(data), code
            elif where_col and where_val is not None:
                existing, code = sb_get_where(table, where_col, where_val, select="*")
                if code == 404 or existing is None:
                    return jsonify({"error": "Not found"}), 404
                merged = dict(existing)
                for k, v in value.items():
                    cur = merged.get(k)
                    if isinstance(cur, dict) and isinstance(v, dict):
                        cur = {**cur, **v}
                    elif isinstance(cur, list) and isinstance(v, list):
                        cur = cur + v
                    else:
                        cur = v
                    merged[k] = cur
                merged.pop("id", None)
                data, code = sb_update_where(table, where_col, where_val, merged)
                return jsonify(data), code
            else:
                return jsonify({"error": "Missing rid or where {col,val}"}), 400

        if action in ("write_cell", "append_cell"):
            if not field:
                return jsonify({"error": "Missing field"}), 400
            if field not in ALLOWED_FIELDS[table]:
                return jsonify({"error": "Invalid field"}), 400
            if action == "append_cell":
                if rid is not None:
                    # reuse by-id merge via read+patch
                    rec, code = sb_get_by_id(table, rid)
                    if code == 404 or rec is None:
                        return jsonify({"error": "Not found"}), 404
                    cur = rec.get(field)
                    if isinstance(cur, dict) and isinstance(value, dict):
                        new_val = {**cur, **value}
                    elif isinstance(cur, list) and isinstance(value, list):
                        new_val = cur + value
                    else:
                        new_val = value
                    stored = process_single_field(table, field, new_val)
                    r = requests.patch(f"{SUPABASE_URL}/rest/v1/{table}?id=eq.{rid}", headers=HEADERS, json={field: stored})
                    res = _json_or_text(r)
                    if isinstance(res, list) and res:
                        return jsonify(decode_row(res[0])), r.status_code
                    if isinstance(res, dict):
                        return jsonify(decode_row(res)), r.status_code
                    return jsonify(res), r.status_code
                elif where_col and where_val is not None:
                    data, code = sb_merge_cell_by_where(table, where_col, where_val, field, value)
                    return jsonify(data), code
                else:
                    return jsonify({"error": "Missing rid or where {col,val}"}), 400
            else:
                # write_cell: overwrite single field
                stored = process_single_field(table, field, value)
                if rid is not None:
                    r = requests.patch(f"{SUPABASE_URL}/rest/v1/{table}?id=eq.{rid}", headers=HEADERS, json={field: stored})
                elif where_col and where_val is not None:
                    qv = quote_plus(str(where_val))
                    r = requests.patch(f"{SUPABASE_URL}/rest/v1/{table}?{where_col}=eq.{qv}", headers=HEADERS, json={field: stored})
                else:
                    return jsonify({"error": "Missing rid or where {col,val}"}), 400
                res = _json_or_text(r)
                if isinstance(res, list) and res:
                    return jsonify(decode_row(res[0])), r.status_code
                if isinstance(res, dict):
                    return jsonify(decode_row(res)), r.status_code
                return jsonify(res), r.status_code

        if action == "delete":
            if rid is not None:
                # prefer where to avoid assuming 'id'
                data, code = sb_delete_where(table, "id", rid)
                return jsonify(data), code
            elif where_col and where_val is not None:
                data, code = sb_delete_where(table, where_col, where_val)
                return jsonify(data), code
            else:
                return jsonify({"error": "Missing rid or where {col,val}"}), 400

        return jsonify({"error": f"Unknown action '{action}'"}), 400

    except ValueError as ve:
        return jsonify({"error": "bad_request", "details": str(ve)}), 400
    except Exception as e:
        return jsonify({"error": "server_error", "details": str(e)}), 500
