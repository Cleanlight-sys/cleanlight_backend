# Cleanlight_bk.py — Unified backend with hint + echo support
# Run: gunicorn -w 2 -b 0.0.0.0:8000 Cleanlight_bk:app

from flask import Flask, request, jsonify, Response, stream_with_context
from datetime import datetime
import db
import laws
import codec
from laws import CleanlightLawError
import json
import time

app = Flask(__name__)

def _decode_record(record):
    if isinstance(record, list):
        return [{k: codec.decode_field(k, v) for k, v in r.items()} for r in record]
    return {k: codec.decode_field(k, v) for k, v in record.items()}

def _mir_snapshot(limit=5):
    try:
        rows = db.read_table("cleanlight_canvas", select="id,mir", limit=limit)
        return [{"id": r.get("id"), "mir": codec.decode_field("mir", r.get("mir"))} for r in rows]
    except Exception:
        return []

def _wrap(data, echo=None, hint=None):
    out = {"data": data, "mir": _mir_snapshot()}
    if echo is not None:
        out["echo"] = echo
    if hint is not None:
        out["hint"] = hint
    return out

def _err(msg, code=400, echo=None, hint=None):
    return jsonify(_wrap(None, echo=echo, hint=hint or msg)), code

@app.get("/health")
def health():
    return jsonify({"status": "ok", "time": datetime.utcnow().isoformat()})
    
DEFAULT_SELECTS = {
    "cleanlight_canvas": "*",
    "cleanlight_tags": "tag,description,created_by,created_at"
}

DEFAULT_KEYS = {
    "cleanlight_canvas": "id",
    "cleanlight_tags": "tag"
}

@app.post("/command")
def command():
    body = request.get_json(force=True) or {}
    action  = body.get("action")
    table   = body.get("table")
    field   = body.get("field")
    rid     = body.get("rid")
    ids     = body.get("ids")
    value   = body.get("value") or body.get("payload") or body.get("fields")
    where   = body.get("where", {})
    select  = body.get("select") or DEFAULT_SELECTS.get(table, "*")
    key_col = body.get("key_col") or DEFAULT_KEYS.get(table, "id")
    echo    = body.get("echo")

    if not action or not table:
        return _err("Missing action or table.", echo=echo)

    # ---------- READS ----------
    if action == "read_table":
        rows = db.read_table(table, select=select)
        return jsonify(_wrap([_decode_record(r) for r in rows], echo=echo))

    if action == "read_all":
        rows = db.read_all_rows(table, select=select)
        return jsonify(_wrap([_decode_record(r) for r in rows], echo=echo))

    if action == "read_row":
        rec = db.read_row(table, key_col, rid, select=select)
        if not rec: return _err("Not found", 404, echo=echo)
        return jsonify(_wrap(_decode_record(rec), echo=echo))

    if action == "read_rows":
        if not ids: return _err("Missing ids", echo=echo)
        rows = db.read_rows(table, key_col, ids, select=select)
        return jsonify(_wrap([_decode_record(r) for r in rows], echo=echo))

    if action == "read_cell":
        if not field: return _err("Missing field", echo=echo)
        rec = db.read_row(table, key_col, rid, select=f"{key_col},{field}")
        if not rec: return _err("Not found", 404, echo=echo)
        out = {key_col: rec[key_col], "field": field, "value": codec.decode_field(field, rec.get(field))}
        return jsonify(_wrap(out, echo=echo))

    if action == "read_column":
        if not field: return _err("Missing field", echo=echo)
        rows = db.read_table(table, select=f"{key_col},{field}")
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
                laws.enforce_canvas_laws(value, system_delta=body.get("system_delta", False))
            elif table == "cleanlight_tags":
                if "created_at" not in value:
                    value["created_at"] = datetime.utcnow().isoformat()
                laws.enforce_tag_laws(value, action="insert")
        except CleanlightLawError as e:
            return _err(str(e), 400, echo=echo, hint=getattr(e, "hint", str(e)))

        encoded = {k: codec.encode_field(k, v) for k, v in value.items()}
        try:
            inserted = db.insert_row(table, encoded)
        except RuntimeError as e:
            return _err("Insert failed", 500, echo=echo, hint=str(e))

        def generate():
            yield json.dumps(_wrap(_decode_record(inserted), echo=echo))
        return Response(stream_with_context(generate()), mimetype='application/json')

    if action in ("update", "patch"):
        if not rid:
            return _err("Missing rid", echo=echo)

        value = _normalize_images(value)

        # ========== ARCHIVAL CONTROL ==========
        if body.get("archive") is True:
            value["archived_at"] = datetime.utcnow().isoformat()
        elif body.get("unarchive") is True:
            value["archived_at"] = None
        elif "archived_at" in value:
            del value["archived_at"]  # Prevent raw overwrite

        try:
            if table == "cleanlight_canvas":
                laws.enforce_canvas_laws(value, system_delta=body.get("system_delta", False))
            elif table == "cleanlight_tags":
                laws.enforce_tag_laws(value, action="update")
        except CleanlightLawError as e:
            return _err(str(e), 400, echo=echo, hint=getattr(e, "hint", str(e)))

        encoded = {k: codec.encode_field(k, v) for k, v in value.items()}
        try:
            updated = db.update_row(table, key_col, rid, encoded)
        except RuntimeError as e:
            return _err("Update failed", 500, echo=echo, hint=str(e))

        decoded = _decode_record(updated)
        return jsonify(_wrap(decoded, echo=echo))

   # ---------- APPEND MODE ----------
    if action == "append_fields":
        if not rid:
            return _err("Missing rid", echo=echo)

        original = db.read_row(table, key_col, rid)
        if not original:
            return _err("Record not found", 404, echo=echo)

        updated = original.copy()
        value = _normalize_images(value)

        timestamp = datetime.utcnow().isoformat()

        for field, new_val in value.items():
            old_val = original.get(field)

            if field == "tags":
                old_tags = old_val if isinstance(old_val, list) else []
                new_tags = new_val if isinstance(new_val, list) else [new_val]
                updated[field] = list(sorted(set(old_tags + new_tags)))

            elif field == "mir":
                prefix = f"[{timestamp}] "
                updated[field] = f"{old_val.strip()}\n{prefix}{new_val.strip()}"

            elif field == "insight":
                updated[field] = f"{old_val.strip()}\n---\n{new_val.strip()}"

            elif field == "cognition":
                updated[field] = f"{old_val.strip()}\n## APPENDED {timestamp}\n{new_val.strip()}"

            else:
                updated[field] = new_val  # fallback overwrite

        # ========== ARCHIVAL CONTROL ==========
        if body.get("archive") is True:
            updated["archived_at"] = timestamp
        elif body.get("unarchive") is True:
            updated["archived_at"] = None
        elif "archived_at" in value:
            del updated["archived_at"]

        encoded = {k: codec.encode_field(k, v) for k, v in updated.items()}
        try:
            updated_row = db.update_row(table, key_col, rid, encoded)
        except RuntimeError as e:
            return _err("Append failed", 500, echo=echo, hint=str(e))

        return jsonify(_wrap(_decode_record(updated_row), echo=echo))
    
    if action == "delete":
        if not rid:
            return _err("Missing rid", echo=echo)
        try:
            db.delete_row(table, key_col, rid)
        except RuntimeError as e:
            return _err("Delete failed", 500, echo=echo, hint=str(e))

        return jsonify(_wrap({"status": "deleted"}, echo=echo))

    return _err("Unknown action", echo=echo)

