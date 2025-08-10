# Cleanlight_bk.py — Unified /command backend (decode-on-read, always)
# Run: gunicorn Cleanlight_bk:app

from flask import Flask, request, jsonify
import os, json, base64, requests
import zstandard as zstd
from datetime import datetime
from urllib.parse import quote_plus

# -------------------- App --------------------
app = Flask(__name__)

# -------------------- Config --------------------
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

# Writable fields only (reads are unrestricted)
WRITEABLE_FIELDS = {
    "cleanlight_canvas": [
        "id", "cognition", "mir", "insight", "codex", "images", "checksums", "timestamps"
    ],
    "cleanlight_map": [
        "map_id", "pointer_net", "checksum"
    ],
}

MAX_IDS_PER_CALL = 25
DEFAULT_AUTOPAGE_LIMIT = 10000

# -------------------- Alphabets (stable + legacy) --------------------
def _is_nonchar(cp): return (0xFDD0 <= cp <= 0xFDEF) or (cp & 0xFFFE) == 0xFFFE
def _is_surrogate(cp): return 0xD800 <= cp <= 0xDFFF

# v1 legacy (older stored data)
def get_base_alphabet_v1(n: int) -> str:
    out = []
    for cp in range(0x20, 0x2FFFF):
        if _is_surrogate(cp) or _is_nonchar(cp): continue
        out.append(chr(cp))
        if len(out) == n: break
    return ''.join(out)

# v2 current (safer)
def get_base_alphabet_v2(n: int) -> str:
    out = []
    for cp in range(0x21, 0x2FFFF):
        if _is_surrogate(cp) or _is_nonchar(cp): continue
        ch = chr(cp)
        if ch in {'"', "'", "\\"}: continue
        out.append(ch)
        if len(out) == n: break
    return ''.join(out)

BASE1K         = get_base_alphabet_v2(1000)
BASE10K        = get_base_alphabet_v2(10000)
LEGACY_BASE1K  = get_base_alphabet_v1(1000)
LEGACY_BASE10K = get_base_alphabet_v1(10000)

def int_to_baseN(num: int, alphabet: str) -> str:
    if num == 0: return alphabet[0]
    base = len(alphabet); digits = []
    while num:
        digits.append(alphabet[num % base]); num //= base
    return ''.join(reversed(digits))

def baseN_to_int(s: str, alphabet: str) -> int:
    amap = {ch:i for i,ch in enumerate(alphabet)}
    base = len(alphabet); num = 0
    for ch in s:
        num = num * base + amap[ch]  # KeyError if unknown char
    return num

def _try_decode_std(encoded: str, alph: str) -> bytes:
    as_int = baseN_to_int(encoded, alph)
    return as_int.to_bytes((as_int.bit_length() + 7)//8, "big")

def encode_std1k(plaintext: str) -> str:
    cctx = zstd.ZstdCompressor()
    return int_to_baseN(int.from_bytes(cctx.compress(plaintext.encode("utf-8")), "big"), BASE1K)

def decode_std1k(std1k_str: str) -> str:
    for alph in (BASE1K, LEGACY_BASE1K):
        try:
            comp = _try_decode_std(std1k_str, alph)
            return zstd.ZstdDecompressor().decompress(comp).decode("utf-8")
        except Exception:
            continue
    return std1k_str  # give up: return original string

def encode_std10k(image_bytes: bytes) -> str:
    cctx = zstd.ZstdCompressor()
    return int_to_baseN(int.from_bytes(cctx.compress(image_bytes), "big"), BASE10K)

def decode_std10k(std10k_str: str) -> bytes:
    for alph in (BASE10K, LEGACY_BASE10K):
        try:
            comp = _try_decode_std(std10k_str, alph)
            return zstd.ZstdDecompressor().decompress(comp)
        except Exception:
            continue
    return std10k_str.encode("utf-8", errors="ignore")

# -------------------- Heuristic decode helpers --------------------
def _looks_like_baseN(s: str) -> bool:
    """Heuristic: treat as std1k only if every char is in either alphabet and length >= 16."""
    if not isinstance(s, str) or len(s) < 16:
        return False
    # Fast check: all chars must live in current OR legacy alphabet
    ok_cur = all((ch in BASE1K) for ch in s)
    ok_old = all((ch in LEGACY_BASE1K) for ch in s)
    return ok_cur or ok_old

def maybe_decode_string(field: str, val: str):
    """Decode std1k/std10k when appropriate; otherwise return original."""
    if val is None or not isinstance(val, str):
        return val
    if field == "images":
        try:
            # If it is already base64, keep it; else it's likely std10k
            # Try std10k → base64
            dec = decode_std10k(val)
            return base64.b64encode(dec).decode("ascii")
        except Exception:
            return val
    # std1k fields or any string that looks like std1k
    if field in ("mir","codex","insight") or _looks_like_baseN(val):
        txt = decode_std1k(val)
        return txt
    return val

def deep_decode_obj(obj):
    """Recursively decode strings that look encoded; special handling for known fields."""
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            if isinstance(v, str):
                out[k] = maybe_decode_string(k, v)
            elif isinstance(v, (dict, list)):
                out[k] = deep_decode_obj(v)
            else:
                out[k] = v
        return out
    if isinstance(obj, list):
        return [deep_decode_obj(x) for x in obj]
    if isinstance(obj, str):
        return maybe_decode_string("", obj)
    return obj

# -------------------- Write processing --------------------
def process_single_field(table: str, field: str, value):
    if field not in WRITEABLE_FIELDS[table]:
        raise ValueError(f"Field {field} not allowed for table {table}")
    if field == "images" and value is not None:
        return encode_std10k(base64.b64decode(value))  # base64 in → std10k stored
    if field in ("mir", "codex", "insight") and value is not None:
        return encode_std1k(value if isinstance(value, str) else json.dumps(value))
    return value

def process_fields(table: str, data: dict) -> dict:
    out = {}
    for k, v in (data or {}).items():
        if k not in WRITEABLE_FIELDS[table]:
            raise ValueError(f"Field {k} not allowed for table {table}")
        out[k] = process_single_field(table, k, v)
    return out

# -------------------- Supabase helpers --------------------
def _json_or_text(resp: requests.Response):
    try:
        if resp.content and resp.headers.get("Content-Type", "").startswith("application/json"):
            return resp.json()
    except Exception:
        pass
    return {"status": resp.status_code, "body": resp.text}

def sb_list(table: str, limit=50, offset=0, select: str | None = None, *, raw=False):
    params = {"limit": limit, "offset": offset}
    if select:
        params["select"] = select
    r = requests.get(f"{SUPABASE_URL}/rest/v1/{table}", headers=HEADERS, params=params)
    data = _json_or_text(r)
    if isinstance(data, list):
        return (data if raw else [deep_decode_obj(x) for x in data]), r.status_code
    return data, r.status_code

def sb_get_by_key(table: str, key_col: str, key_val, select: str = "*", *, raw=False):
    qv = quote_plus(str(key_val))
    r = requests.get(f"{SUPABASE_URL}/rest/v1/{table}?{key_col}=eq.{qv}&select={select}", headers=HEADERS)
    data = _json_or_text(r)
    if isinstance(data, list):
        if not data:
            return None, 404
        return (data[0] if raw else deep_decode_obj(data[0])), 200
    return data, r.status_code

def sb_insert(table: str, fields: dict):
    encoded = process_fields(table, fields)
    r = requests.post(f"{SUPABASE_URL}/rest/v1/{table}", headers=HEADERS, json=encoded)
    data = _json_or_text(r)
    if isinstance(data, list) and data:
        return deep_decode_obj(data[0]), r.status_code
    if isinstance(data, dict):
        return deep_decode_obj(data), r.status_code
    return data, r.status_code

def sb_update_where(table: str, col: str, val, fields: dict):
    encoded = process_fields(table, fields)
    qv = quote_plus(str(val))
    r = requests.patch(f"{SUPABASE_URL}/rest/v1/{table}?{col}=eq.{qv}", headers=HEADERS, json=encoded)
    data = _json_or_text(r)
    if isinstance(data, list) and data:
        return deep_decode_obj(data[0]), r.status_code
    if isinstance(data, dict):
        return deep_decode_obj(data), r.status_code
    return data, r.status_code

def sb_delete_where(table: str, col: str, val):
    qv = quote_plus(str(val))
    r = requests.delete(f"{SUPABASE_URL}/rest/v1/{table}?{col}=eq.{qv}", headers=HEADERS)
    return _json_or_text(r), r.status_code

# -------------------- Read-before-write (silent) --------------------
def _satisfy_read_before_write(table: str, key_col=None, where_col=None, where_val=None, rid=None):
    try:
        if rid is not None and key_col:
            _ = sb_get_by_key(table, key_col, rid, select=key_col, raw=True)
        elif where_col and where_val is not None:
            _ = sb_get_by_key(table, where_col, where_val, select=where_col, raw=True)
        else:
            _ = sb_list(table, limit=1, offset=0, raw=True)
    except Exception:
        pass

# -------------------- Normalizers --------------------
def _norm_action(a):
    if not a: return None
    a = str(a).lower().strip()
    aliases = {
        "create":"write","insert":"write","add":"write",
        "patch":"update","modify":"update",
        "appendcell":"append_cell","append_field":"append_cell",
        "writecell":"write_cell","set_cell":"write_cell",
        "get":"read_row","read":"read_row","fetch":"read_row",
        "column":"read_column","all":"read_all","list":"read_table",
        "ids":"read_table_ids","rows":"read_rows",
        "remove":"delete","del":"delete",
    }
    return aliases.get(a, a)

def _pick(dct, *keys):
    for k in keys:
        if k in dct and dct[k] is not None:
            return dct[k]
    return None

def _norm_table(payload):
    t = _pick(payload, "target","table","Target")
    if t: return str(t)
    tables = payload.get("tables")
    if isinstance(tables, list) and tables:
        return str(tables[0])
    return None

def _norm_rid(payload):
    rid = _pick(payload, "rid","id","TargetRID")
    if rid is not None:
        try: return int(rid)
        except Exception: return None
    where = payload.get("where")
    if isinstance(where, dict) and "val" in where:
        try: return int(where.get("val"))
        except Exception: return None
    return None

def _norm_field(payload): return _pick(payload, "field","TargetField")

def _norm_value(payload):
    if "value" in payload: return payload["value"]
    if "payload" in payload: return payload["payload"]
    if "fields" in payload: return payload["fields"]
    return None

# -------------------- Routes --------------------
@app.get("/health")
def health():
    return jsonify({"status":"ok","time":datetime.utcnow().isoformat()})

@app.post("/command")
def unified_command():
    body = request.get_json(force=True) or {}

    # Legacy shim for older callers
    if "action" in body and "table" in body and ("fields" in body or body.get("action") in ("read_table","read_row")):
        body = dict(body)
        body.setdefault("target", body.get("table"))
        w = body.get("where") or {}
        if body.get("action") == "read_row" and "rid" not in body and "val" in w:
            try: body["rid"] = int(w.get("val"))
            except Exception: pass

    action   = _norm_action(body.get("action"))
    table    = _norm_table(body)
    rid      = _norm_rid(body)
    field    = _norm_field(body)
    value    = _norm_value(body)
    where    = body.get("where") if isinstance(body.get("where"), dict) else None
    key_col  = body.get("key_col") or "id"   # IMPORTANT: set key_col:"map_id" for cleanlight_map
    ids      = body.get("ids") if isinstance(body.get("ids"), list) else None
    select   = body.get("select") or "*"

    where_col = str(where.get("col")) if where and "col" in where else None
    where_val = where.get("val") if where and "val" in where else None

    if action is None or table is None:
        return jsonify({"error":"Missing action or target table"}), 400
    if table not in WRITEABLE_FIELDS:
        return jsonify({"error":"Invalid table"}), 400

    limit_total = int(body.get("limit_total", DEFAULT_AUTOPAGE_LIMIT))
    limit       = int(body.get("limit", 1000))
    offset      = int(body.get("offset", 0))

    try:
        # ---------- READS (force-decode everywhere) ----------
        if action == "read_table":
            rows, code = sb_list(table, min(limit, 1000), offset, select=select, raw=False)
            return jsonify(rows), code

        if action == "read_all":
            rows_accum = []
            off = offset
            while len(rows_accum) < limit_total:
                batch, code = sb_list(table, min(limit, 1000), off, select=select, raw=False)
                if code >= 300 or not isinstance(batch, list) or not batch: break
                rows_accum.extend(batch)
                off += len(batch)
                if len(batch) < min(limit, 1000): break
            return jsonify(rows_accum[:limit_total]), 200

        if action == "read_table_ids":
            raw_list, code = sb_list(table, min(limit, 1000), offset, select=key_col, raw=True)
            if code >= 300 or not isinstance(raw_list, list):
                return jsonify(raw_list), code
            out_ids = []
            for r in raw_list:
                v = r.get(key_col) if isinstance(r, dict) else None
                if v is not None:
                    try: out_ids.append(int(v))
                    except: pass
            next_off = offset + len(out_ids) if len(raw_list) == min(limit,1000) else None
            return jsonify({"target":table, "key_col":key_col, "ids": out_ids, "next_offset": next_off}), 200

        if action == "read_rows":
            if not ids:
                return jsonify({"error":"Missing ids array"}), 400
            if len(ids) > MAX_IDS_PER_CALL:
                ids = ids[:MAX_IDS_PER_CALL]
            rows = []
            for v in ids:
                rec, code = sb_get_by_key(table, key_col, v, select=select, raw=True)  # fetch raw
                if code == 404 or rec is None: continue
                rows.append(deep_decode_obj(rec))  # decode
            return jsonify(rows), 200

        if action == "read_row":
            if rid is not None:
                rec, code = sb_get_by_key(table, key_col, rid, select=select, raw=True)  # raw
            elif where_col and where_val is not None:
                rec, code = sb_get_by_key(table, where_col, where_val, select=select, raw=True)
            else:
                return jsonify({"error":"Missing rid or where {col,val}"}), 400
            if code == 404 or rec is None:
                return jsonify({"error":"Not found"}), 404
            return jsonify(deep_decode_obj(rec)), 200  # decode

        if action == "read_cell":
            if not field:
                return jsonify({"error":"Missing field"}), 400
            # always raw fetch for a precise slice
            sel = f"{key_col},{field}"
            if rid is not None:
                rec, code = sb_get_by_key(table, key_col, rid, select=sel, raw=True)
            elif where_col and where_val is not None:
                rec, code = sb_get_by_key(table, where_col, where_val, select=sel, raw=True)
            else:
                return jsonify({"error":"Missing rid or where {col,val}"}), 400
            if code == 404 or rec is None:
                return jsonify({"error":"Not found"}), 404
            val = maybe_decode_string(field, rec.get(field))
            return jsonify({key_col: rec.get(key_col), "field": field, "value": val}), 200

        if action == "read_column":
            if not field:
                return jsonify({"error":"Missing field"}), 400
            raw_list, code = sb_list(table, min(limit, 1000), offset, select=f"{key_col},{field}", raw=True)
            if not isinstance(raw_list, list):
                return jsonify(raw_list), code
            out = [{key_col: r.get(key_col), "value": maybe_decode_string(field, r.get(field))} for r in raw_list]
            return jsonify(out), 200

        # ---------- WRITES ----------
        if action in ("write","create","insert","add"):
            if not isinstance(value, dict):
                return jsonify({"error":"payload/fields must be an object"}), 400
            if isinstance(value.get("images"), str) and len(value["images"]) > 7_000_000:
                return jsonify({"error":"images too large"}), 413
            _satisfy_read_before_write(table, key_col=key_col, where_col=where_col, where_val=where_val, rid=rid)
            data, code = sb_insert(table, value)
            return jsonify(data), code

        if action == "update":
            if not isinstance(value, dict):
                return jsonify({"error":"payload/fields must be an object"}), 400
            _satisfy_read_before_write(table, key_col=key_col, where_col=where_col, where_val=where_val, rid=rid)
            if rid is not None:
                data, code = sb_update_where(table, key_col, rid, value)
            elif where_col and where_val is not None:
                data, code = sb_update_where(table, where_col, where_val, value)
            else:
                return jsonify({"error":"Missing rid or where {col,val}"}), 400
            if isinstance(data, dict) and data.get("error") == "Not found":
                return jsonify(data), 404
            return jsonify(data), code

        if action == "append":
            if not isinstance(value, dict):
                return jsonify({"error":"payload/fields must be an object"}), 400
            if rid is not None:
                base_rec, code = sb_get_by_key(table, key_col, rid, select="*", raw=True)
            elif where_col and where_val is not None:
                base_rec, code = sb_get_by_key(table, where_col, where_val, select="*", raw=True)
            else:
                return jsonify({"error":"Missing rid or where {col,val}"}), 400
            if code == 404 or base_rec is None:
                return jsonify({"error":"Not found"}), 404
            merged = dict(base_rec)
            for k, v in value.items():
                cur = merged.get(k)
                if isinstance(cur, dict) and isinstance(v, dict): cur = {**cur, **v}
                elif isinstance(cur, list) and isinstance(v, list): cur = cur + v
                else: cur = v
                merged[k] = cur
            merged.pop("id", None); merged.pop("map_id", None)
            _satisfy_read_before_write(table, key_col=key_col, where_col=where_col, where_val=where_val, rid=rid)
            if rid is not None:
                data, code = sb_update_where(table, key_col, rid, merged)
            else:
                data, code = sb_update_where(table, where_col, where_val, merged)
            return jsonify(data), code

        if action in ("write_cell","append_cell"):
            if not field:
                return jsonify({"error":"Missing field"}), 400
            _satisfy_read_before_write(table, key_col=key_col, where_col=where_col, where_val=where_val, rid=rid)
            if action == "append_cell":
                if rid is not None:
                    rec, code = sb_get_by_key(table, key_col, rid, select="*", raw=True)
                elif where_col and where_val is not None:
                    rec, code = sb_get_by_key(table, where_col, where_val, select="*", raw=True)
                else:
                    return jsonify({"error":"Missing rid or where {col,val}"}), 400
                if code == 404 or rec is None:
                    return jsonify({"error":"Not found"}), 404
                cur = rec.get(field)
                if isinstance(cur, dict) and isinstance(value, dict): new_val = {**cur, **value}
                elif isinstance(cur, list) and isinstance(value, list): new_val = cur + value
                else: new_val = value
                stored = process_single_field(table, field, new_val)
                if rid is not None:
                    r = requests.patch(f"{SUPABASE_URL}/rest/v1/{table}?{key_col}=eq.{quote_plus(str(rid))}", headers=HEADERS, json={field: stored})
                else:
                    r = requests.patch(f"{SUPABASE_URL}/rest/v1/{table}?{where_col}=eq.{quote_plus(str(where_val))}", headers=HEADERS, json={field: stored})
                res = _json_or_text(r)
                if isinstance(res, list) and res:
                    return jsonify(deep_decode_obj(res[0])), r.status_code
                if isinstance(res, dict):
                    return jsonify(deep_decode_obj(res)), r.status_code
                return jsonify(res), r.status_code
            else:
                stored = process_single_field(table, field, value)
                if rid is not None:
                    r = requests.patch(f"{SUPABASE_URL}/rest/v1/{table}?{key_col}=eq.{quote_plus(str(rid))}", headers=HEADERS, json={field: stored})
                elif where_col and where_val is not None:
                    r = requests.patch(f"{SUPABASE_URL}/rest/v1/{table}?{where_col}=eq.{quote_plus(str(where_val))}", headers=HEADERS, json={field: stored})
                else:
                    return jsonify({"error":"Missing rid or where {col,val}"}), 400
                res = _json_or_text(r)
                if isinstance(res, list) and res:
                    return jsonify(deep_decode_obj(res[0])), r.status_code
                if isinstance(res, dict):
                    return jsonify(deep_decode_obj(res)), r.status_code
                return jsonify(res), r.status_code

        if action == "delete":
            if rid is not None:
                data, code = sb_delete_where(table, key_col, rid)
            elif where_col and where_val is not None:
                data, code = sb_delete_where(table, where_col, where_val)
            else:
                return jsonify({"error":"Missing rid or where {col,val}"}), 400
            return jsonify(data), code

        return jsonify({"error": f"Unknown action '{action}'"}), 400

    except ValueError as ve:
        return jsonify({"error":"bad_request","details":str(ve)}), 400
    except Exception as e:
        return jsonify({"error":"server_error","details":str(e)}), 500
