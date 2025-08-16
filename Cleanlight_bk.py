# Cleanlight_bk.py — Unified backend with hint + echo support
# Run: gunicorn -w 2 -b 0.0.0.0:8000 Cleanlight_bk:app

from flask import Flask, request, jsonify, Response, stream_with_context
from datetime import datetime, timezone
import db
import laws
import codec
from laws import CleanlightLawError
import json 
import time, sys, json as _json


app = Flask(__name__)

def _now_z():
    return datetime.now(timezone.utc).isoformat()

@app.before_request
def _log_start():
    request._t0 = time.time()

# -------- no-store on every response to avoid “phantom” reads --------
@app.after_request
def _log_request(resp):
    try:
        dur_ms = int((time.time() - getattr(request, "_t0", time.time())) * 1000)
        rec = {
            "path": request.path,
            "method": request.method,
            "status": resp.status_code,
            "dur_ms": dur_ms,
        }
        # include common body fields when JSON
        try:
            body = request.get_json(silent=True) or {}
            for k in ("action","table","rid"):
                if k in body: rec[k] = body[k]
        except Exception:
            pass
        print(_json.dumps(rec), file=sys.stdout, flush=True)
    except Exception:
        pass
    # keep existing no-store headers
    return resp

def add_no_store(resp):
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    return resp

def scan_rows(table: str, start_id: int = 0, limit: int = 100, select: str = "*", skip_archived: bool = True):
    base = f"{SUPABASE_URL}/rest/v1/{table}?id=gt.{start_id}&select={select}&order=id.asc&limit={limit}"
    if skip_archived:
        base += "&archived_at=is.null"
    r = requests.get(base, headers=HEADERS)
    r.raise_for_status()
    return r.json()

def _decode_record(record):
    if isinstance(record, list):
        return [{k: codec.decode_field(k, v) for k, v in r.items()} for r in record]
    return {k: codec.decode_field(k, v) for k, v in record.items()}

def _mir_snapshot(limit=5):
    try:
        rows = db.read_table("cleanlight_canvas", select="id,mir", limit=limit, order="id.desc")
        return [{"id": r.get("id"), "mir": codec.decode_field("mir", r.get("mir"))} for r in rows]
    except Exception:
        return []

def _wrap(data, echo=None, hint=None, error=None):
    out = {"data": data, "mir": _mir_snapshot()}
    if echo is not None:
        out["echo"] = echo
    if hint is not None:
        out["hint"] = hint
    if error is not None:
        out["error"] = error
    return out

def _err(msg, code=400, echo=None, hint=None, error=None):
    return jsonify(_wrap(None, echo=echo, hint=hint or msg, error=error)), code

@app.post("/command/delete")
def command_delete():
    """
    Consequential delete-only alias.
    Body: { "table": "...", "rid": <int>, "key_col": "id" }
    """
    body = request.get_json(force=True) or {}
    table   = body.get("table")
    rid     = body.get("rid")
    key_col = body.get("key_col")

    # Reuse the same table aliasing/guards as /command
    TABLE_ALIASES = {
        "codex": "cleanlight_canvas",
        "graph_bundle": "cleanlight_canvas",
        "graph.bundle": "cleanlight_canvas",
    }
    DEFAULT_KEYS = {
        "cleanlight_canvas": "id",
        "cleanlight_tags": "tag"
    }

    echo = body.get("echo")
    if not table:
        return _err("Missing table.", echo=echo, error={"code":"TABLE_REQUIRED"})
    table = TABLE_ALIASES.get(table, table)
    if table not in DEFAULT_KEYS:
        return _err("Unknown or unsupported table.",
                    echo=echo,
                    hint=f"Use one of: {', '.join(DEFAULT_KEYS.keys())}",
                    error={"law":"Router","field":"table","code":"UNKNOWN_TABLE","table":table})

    if rid is None:
        return _err("Missing rid.", echo=echo, error={"code":"RID_REQUIRED"})
    key_col = key_col or DEFAULT_KEYS.get(table, "id")

    try:
        db.delete_row(table, key_col, rid)
    except RuntimeError as e:
        return _err("Delete failed", 500, echo=echo, hint=str(e), error={"code":"DELETE_FAIL"})
    return jsonify(_wrap({"status": "deleted", "table": table, "rid": rid}, echo=echo))

@app.get("/health")
def health():
    try:
        last = db.read_table("cleanlight_canvas", select="id,updated_at", limit=1, order="id.desc")
        last_row = last[0] if last else {}
        return jsonify({
            "status": "ok",
            "time": _now_z(),
            "last_id": last_row.get("id"),
            "last_updated_at": last_row.get("updated_at"),
        })
    except Exception as e:
        return jsonify({"status": "degraded", "time": _now_z(), "error": str(e)}), 200
        
DEFAULT_SELECTS = {
    "cleanlight_canvas": "*",
    "cleanlight_tags": "tag,description,created_by,created_at"
}
DEFAULT_KEYS = {
    "cleanlight_canvas": "id",
    "cleanlight_tags": "tag"
}
TABLE_ALIASES = {
    "codex": "cleanlight_canvas",
    "graph_bundle": "cleanlight_canvas",
    "graph.bundle": "cleanlight_canvas",
}

@app.post("/command")
def command():
    body = request.get_json(force=True) or {}
    action  = body.get("action")
    table   = body.get("table")
    field   = body.get("field")
    rid     = body.get("rid")
    ids     = body.get("ids")
    value   = body.get("value") or body.get("payload") or body.get("fields") or {}
    select  = body.get("select")
    key_col = body.get("key_col")
    echo    = body.get("echo")

    if not action or not table:
        return _err("Missing action or table.", echo=echo)

    # Canonicalize table names
    table = TABLE_ALIASES.get(table, table)
    if table not in DEFAULT_KEYS:
        # Explicitly tell the agent which tables are valid
        return _err("Unknown or unsupported table.",
                    echo=echo,
                    hint=f"Use one of: {', '.join(DEFAULT_KEYS.keys())}",
                    error={"law":"Router", "field":"table", "code":"UNKNOWN_TABLE", "table": table})

    select  = select  or DEFAULT_SELECTS.get(table, "*")
    key_col = key_col or DEFAULT_KEYS.get(table, "id")

    # ---------- READS ----------
    if action == "read_table":
        rows = db.read_table(table, select=select, order="id.asc")
        return jsonify(_wrap([_decode_record(r) for r in rows], echo=echo))

    if action == "read_all":
        rows = db.read_all_rows(table, select=select)
        return jsonify(_wrap([_decode_record(r) for r in rows], echo=echo))

    if action == "read_row":
        rec = db.read_row(table, key_col, rid, select=select)
        if not rec: return _err("Not found", 404, echo=echo, error={"code":"NOT_FOUND","field":key_col,"id":rid})
        return jsonify(_wrap(_decode_record(rec), echo=echo))

    if action == "read_rows":
        if not ids: return _err("Missing ids", echo=echo, error={"code":"IDS_REQUIRED"})
        rows = db.read_rows(table, key_col, ids, select=select)
        return jsonify(_wrap([_decode_record(r) for r in rows], echo=echo))

    if action == "read_cell":
        if not field: return _err("Missing field", echo=echo, error={"code":"FIELD_REQUIRED"})
        rec = db.read_row(table, key_col, rid, select=f"{key_col},{field}")
        if not rec: return _err("Not found", 404, echo=echo, error={"code":"NOT_FOUND","field":key_col,"id":rid})
        out = {key_col: rec[key_col], "field": field, "value": codec.decode_field(field, rec.get(field))}
        return jsonify(_wrap(out, echo=echo))

    if action == "read_column":
        if not field: return _err("Missing field", echo=echo, error={"code":"FIELD_REQUIRED"})
        rows = db.read_table(table, select=f"{key_col},{field}", order=f"{key_col}.asc")
        out = [{key_col: r[key_col], "value": codec.decode_field(field, r.get(field))} for r in rows]
        return jsonify(_wrap(out, echo=echo))

    # ---------- WRITES ----------
    def _normalize_images(val: dict):
        if isinstance(val, dict) and "images" in val and isinstance(val["images"], str):
            val["images"] = [val["images"]]
        return val

    if action in ("write", "insert", "create"):
        value = _normalize_images(value)
        try:
            if table == "cleanlight_canvas":
                now = datetime.utcnow().isoformat()
                value.setdefault("created_at", now)
                value.setdefault("updated_at", now)
                laws.enforce_canvas_laws(value, system_delta=body.get("system_delta", False), mode="insert")
            elif table == "cleanlight_tags":
                if "created_at" not in value:
                    value["created_at"] = datetime.utcnow().isoformat()
                laws.enforce_tag_laws(value, action="insert")
        except CleanlightLawError as e:
            return _err(str(e), 400, echo=echo, hint=e.hint, error={"law":e.law,"field":e.field,"code":e.code})
            
        if table == "cleanlight_canvas" and isinstance(value.get("codex"), (dict, str)):
            try:
                obj = value["codex"]
                if isinstance(obj, str):
                    import json as _j
                    obj = _j.loads(obj)  # best-effort; if it fails we still accept raw string
                ok, hints = codec.validate_graph_bundle(obj)
                if hints:
                    # push a MIR hint without blocking
                    stamp = datetime.utcnow().isoformat()+"Z"
                    pre = (value.get("mir") or "").strip()
                    hint_txt = f"[{stamp}] codex.validate: " + "; ".join(hints)
                    value["mir"] = (pre + "\n" + hint_txt).strip() if pre else hint_txt
            except Exception:
                pass  # never block

        encoded = {k: codec.encode_field(k, v) for k, v in value.items()}
        
        try:
            inserted = db.insert_row(table, encoded)
        except RuntimeError as e:
            return _err("Insert failed", 500, echo=echo, hint=str(e), error={"code":"INSERT_FAIL"})
        def generate():
            yield json.dumps(_wrap(_decode_record(inserted), echo=echo))
        return Response(stream_with_context(generate()), mimetype='application/json')

    if action in ("update", "patch"):
        if not rid:
            return _err("Missing rid", echo=echo, error={"code":"RID_REQUIRED"})
        value = _normalize_images(value)
        
        # Guard: append-only fields must not be overwritten via update
        if any(k in value for k in ("mir","insight")):
            return _err("Append-only field.",
                        echo=echo,
                        hint="Use action='append_fields' for mir/insight.",
                        error={"code":"USE_APPEND_FIELDS","field":"mir|insight"})

        # archival controls
        if body.get("archive") is True:
            value["archived_at"] = datetime.utcnow().isoformat()
        elif body.get("unarchive") is True:
            value["archived_at"] = None
        elif "archived_at" in value:
            del value["archived_at"]
            
        if table == "cleanlight_canvas":
            value["updated_at"] = datetime.utcnow().isoformat()  # or _now_z()    

        try:
            if table == "cleanlight_canvas":
                laws.enforce_canvas_laws(value, system_delta=body.get("system_delta", False), mode="update")
            elif table == "cleanlight_tags":
                laws.enforce_tag_laws(value, action="update")
        except CleanlightLawError as e:
            return _err(str(e), 400, echo=echo, hint=e.hint, error={"law":e.law,"field":e.field,"code":e.code})

        if table == "cleanlight_canvas" and isinstance(value.get("codex"), (dict, str)):
            try:
                obj = value["codex"]
                if isinstance(obj, str):
                    import json as _j
                    obj = _j.loads(obj)
                ok, hints = codec.validate_graph_bundle(obj)
                if hints:
                    stamp = datetime.utcnow().isoformat()+"Z"
                    pre = (value.get("mir") or "").strip()
                    hint_txt = f"[{stamp}] codex.validate: " + "; ".join(hints)
                    value["mir"] = (pre + "\n" + hint_txt).strip() if pre else hint_txt
            except Exception:
                pass
                
        encoded = {k: codec.encode_field(k, v) for k, v in value.items()}
        
        try:
            updated = db.update_row(table, key_col, rid, encoded)
        except RuntimeError as e:
            return _err("Update failed", 500, echo=echo, hint=str(e), error={"code":"UPDATE_FAIL"})
        decoded = _decode_record(updated)
        return jsonify(_wrap(decoded, echo=echo))

    # ---------- APPEND MODE ----------
    if action == "append_fields":
        if not rid:
            return _err("Missing rid", echo=echo, error={"code":"RID_REQUIRED"})
        original_raw = db.read_row(table, key_col, rid)
        if not original_raw:
            return _err("Record not found", 404, echo=echo, error={"code":"NOT_FOUND","id":rid})
        original = _decode_record(original_raw)

        updated = original.copy()
        value = _normalize_images(value)
        timestamp = datetime.now(timezone.utc).isoformat()

        for field, new_val in value.items():
            old_val = original.get(field)
            if field == "tags":
                old_tags = old_val if isinstance(old_val, list) else []
                new_tags = new_val if isinstance(new_val, list) else [new_val]
                updated[field] = list(dict.fromkeys(old_tags + new_tags))  # preserve order
            elif field == "mir":
                old = (old_val or "").strip()
                pre = f"[{timestamp}] "
                updated[field] = f"{old}\n{pre}{(new_val or '').strip()}" if old else f"{pre}{(new_val or '').strip()}"
            elif field == "insight":
                old = (old_val or "").strip()
                sep = "\n---\n" if old else ""
                updated[field] = f"{old}{sep}{(new_val or '').strip()}"
            elif field == "cognition":
                old = (old_val or "").strip()
                header = f"## APPENDED {timestamp}\n"
                updated[field] = f"{old}\n{header}{(new_val or '').strip()}" if old else f"{header}{(new_val or '').strip()}"
            else:
                updated[field] = new_val

        if body.get("archive") is True:
            updated["archived_at"] = timestamp
        elif body.get("unarchive") is True:
            updated["archived_at"] = None
        elif "archived_at" in value:
            updated.pop("archived_at", None)

        if table == "cleanlight_canvas":
            updated["updated_at"] = timestamp

        # Re-validate only the pieces we touched
        try:
            laws.enforce_canvas_laws({k: updated.get(k) for k in value.keys()},
                                     system_delta=body.get("system_delta", False),
                                     mode="append")
        except CleanlightLawError as e:
            return _err(str(e), 400, echo=echo, hint=e.hint, error={"law":e.law,"field":e.field,"code":e.code})

        encoded = {k: codec.encode_field(k, v) for k, v in updated.items()}
        try:
            updated_row = db.update_row(table, key_col, rid, encoded)
        except RuntimeError as e:
            return _err("Append failed", 500, echo=echo, hint=str(e), error={"code":"APPEND_FAIL"})
        return jsonify(_wrap(_decode_record(updated_row), echo=echo))

    if action == "delete":
        return _err(
            "Delete not allowed via /command.",
            echo=echo,
            hint="Use POST /command/delete { table, rid }",
            error={"code":"DELETE_REDIRECT","law":"Router"}
        )

    return _err("Unknown action", echo=echo, error={"code":"UNKNOWN_ACTION"})

@app.post("/command/scan")
def command_scan():
    body = request.get_json(force=True) or {}
    table   = body.get("table") or "cleanlight_canvas"
    start_id= int(body.get("start_id") or 0)
    limit   = int(body.get("limit") or 100)
    select  = body.get("select") or "id,tags,updated_at"
    echo    = body.get("echo")

    # reuse aliasing checks
    table = TABLE_ALIASES.get(table, table)
    if table not in DEFAULT_KEYS:
        return _err("Unknown or unsupported table.",
                    echo=echo,
                    hint=f"Use one of: {', '.join(DEFAULT_KEYS.keys())}",
                    error={"law":"Router","field":"table","code":"UNKNOWN_TABLE","table":table})

    try:
        rows = db.scan_rows(table, start_id=start_id, limit=limit, select=select, skip_archived=True)
        next_id = rows[-1]["id"] if rows else start_id
        return jsonify(_wrap({"rows":[_decode_record(r) for r in rows], "next_start_id": next_id}, echo=echo))
    except RuntimeError as e:
        return _err("Scan failed", 500, echo=echo, hint=str(e), error={"code":"SCAN_FAIL"})

# ----- Admin migration (robust) -----
@app.post("/admin/migrate_encoded_tags")
def migrate_encoded_tags():
    """
    Fix legacy rows where tags are encoded or stored incorrectly.
    Handles:
      - tags as encoded smart1k STRING of a JSON array
      - tags as JSON string of array
      - tags as comma/space separated STRING
      - tags as LIST with each item encoded or plain
    Args (JSON):
      dry_run: bool (default True)
      limit: int (optional)
    """
    body = request.get_json(silent=True) or {}
    dry_run = body.get("dry_run", True)
    limit = body.get("limit")

    import re, json as pyjson
    TAG_RE = re.compile(r"^[a-z0-9_]+$")

    def is_canonical_tag(s: str) -> bool:
        return isinstance(s, str) and bool(TAG_RE.fullmatch(s))

    def try_json_array(s: str):
        try:
            val = pyjson.loads(s)
            if isinstance(val, list):
                return val
        except Exception:
            pass
        return None

    def split_plain_list(s: str):
        parts = re.split(r"[,\s]+", s.strip())
        parts = [p for p in parts if p]
        return parts if parts else None

    def normalize_tags_maybe_decode(value):
        decoded_count = 0

        if isinstance(value, list):
            out = []
            changed = False
            for item in value:
                if isinstance(item, str):
                    dec = None
                    try:
                        dec_try = codec.decode_smart1k(item)
                        if is_canonical_tag(dec_try):
                            dec = dec_try
                    except Exception:
                        dec = None
                    if dec is not None:
                        out.append(dec); decoded_count += 1; changed = True
                    else:
                        out.append(item)
                else:
                    out.append(item)
            seen = set(); norm = []
            for t in out:
                if t not in seen:
                    seen.add(t); norm.append(t)
            return (norm if changed else None, decoded_count)

        if isinstance(value, str):
            s = value
            try:
                s_dec = codec.decode_smart1k(s)
                arr = try_json_array(s_dec)
                if arr is not None:
                    arr2 = [str(t) for t in arr if is_canonical_tag(str(t))]
                    if arr2:
                        seen = set(); norm = []
                        for t in arr2:
                            if t not in seen:
                                seen.add(t); norm.append(t)
                        return (norm, len(arr2))
                else:
                    parts = split_plain_list(s_dec) or []
                    parts2 = [p for p in parts if is_canonical_tag(p)]
                    if parts2:
                        seen = set(); norm = []
                        for t in parts2:
                            if t not in seen:
                                seen.add(t); norm.append(t)
                        return (norm, len(parts2))
            except Exception:
                pass

            if s.strip().startswith("[") and s.strip().endswith("]"):
                arr = try_json_array(s)
                if arr is not None:
                    arr2 = [str(t) for t in arr if is_canonical_tag(str(t))]
                    if arr2:
                        seen = set(); norm = []
                        for t in arr2:
                            if t not in seen:
                                seen.add(t); norm.append(t)
                        return (norm, 0)

            parts = split_plain_list(s) or []
            parts2 = [p for p in parts if is_canonical_tag(p)]
            if parts2:
                seen = set(); norm = []
                for t in parts2:
                    if t not in seen:
                        seen.add(t); norm.append(t)
                return (norm, 0)

        return (None, 0)

    changed = []
    examined = 0
    created_tags = set()
    decoded_counts = {"rows_changed": 0, "items_decoded": 0}

    rows = db.read_all_rows("cleanlight_canvas", select="id,tags")
    if isinstance(limit, int) and limit > 0:
        rows = rows[:limit]

    try:
        allowed_rows = db.read_table("cleanlight_tags", select="tag")
        allowed = {r["tag"] for r in allowed_rows}
    except Exception:
        allowed = set()

    for r in rows:
        examined += 1
        rid = r.get("id")
        tags_val = r.get("tags")
        new_tags, dec_items = normalize_tags_maybe_decode(tags_val)
        if new_tags is None:
            continue

        for t in new_tags:
            if is_canonical_tag(t) and t not in allowed:
                if not dry_run:
                    try:
                        db.insert_row("cleanlight_tags", {
                            "tag": t,
                            "description": f"Auto-created during tag migration for canvas row {rid}.",
                            "created_by": "migration"
                        })
                        allowed.add(t)
                        created_tags.add(t)
                    except Exception:
                        allowed.add(t)
                else:
                    created_tags.add(t)

        if not dry_run:
            payload = {"tags": new_tags}
            encoded = {k: codec.encode_field(k, v) for k, v in payload.items()}
            db.update_row("cleanlight_canvas", "id", rid, encoded)

        decoded_counts["rows_changed"] += 1
        decoded_counts["items_decoded"] += dec_items
        changed.append({"id": rid, "from": tags_val, "to": new_tags})

    return jsonify({
        "dry_run": dry_run,
        "examined_rows": examined,
        "rows_changed": decoded_counts["rows_changed"],
        "decoded_items": decoded_counts["items_decoded"],
        "new_tags_created": sorted(created_tags),
        "samples": changed[:10]
    })





