# Cleanlight_bk.py — Unified backend
# Run: gunicorn -w 2 -b 0.0.0.0:8000 Cleanlight_bk:app

from flask import Flask, request, jsonify
from datetime import datetime
import db
import laws
import codec

app = Flask(__name__)

def _decode_record(record: dict):
    return {k: codec.decode_field(k, v) for k, v in record.items()}

def _mir_snapshot():
    try:
        rows = db.read_all_rows("cleanlight_canvas", select="id,mir")
        return [{ "id": r.get("id"), "mir": codec.decode_field("mir", r.get("mir")) } for r in rows]
    except Exception:
        return []

def _wrap(data):
    return {"data": data, "mir": _mir_snapshot()}

def _err(msg, code=400):
    return jsonify({"error": msg, "mir": _mir_snapshot()}), code

@app.get("/health")
def health():
    return jsonify({"status": "ok", "time": datetime.utcnow().isoformat()})

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
    select  = body.get("select") or "*"
    key_col = body.get("key_col") or "id"

    if not action or not table:
        return _err("Missing action or table.")

    # ---------- READS ----------
    if action == "read_table":
        rows = db.read_table(table, select=select)
        return jsonify(_wrap([_decode_record(r) for r in rows]))

    if action == "read_all":
        rows = db.read_all_rows(table, select=select)
        return jsonify(_wrap([_decode_record(r) for r in rows]))

    if action == "read_row":
        rec = db.read_row(table, key_col, rid, select=select)
        if not rec: return _err("Not found", 404)
        return jsonify(_wrap(_decode_record(rec)))

    if action == "read_rows":
        if not ids: return _err("Missing ids")
        rows = db.read_rows(table, key_col, ids, select=select)
        return jsonify(_wrap([_decode_record(r) for r in rows]))

    if action == "read_cell":
        if not field: return _err("Missing field")
        rec = db.read_row(table, key_col, rid, select=f"{key_col},{field}")
        if not rec: return _err("Not found", 404)
        out = {key_col: rec[key_col], "field": field, "value": codec.decode_field(field, rec.get(field))}
        return jsonify(_wrap(out))

    if action == "read_column":
        if not field: return _err("Missing field")
        rows = db.read_table(table, select=f"{key_col},{field}")
        out = [{key_col: r[key_col], "value": codec.decode_field(field, r.get(field))} for r in rows]
        return jsonify(_wrap(out))

    # ---------- WRITES ----------
    # Optional normalization: if single string image → list
    def _normalize_images(val: dict):
        if isinstance(val, dict) and "images" in val and isinstance(val["images"], str):
            val["images"] = [val["images"]]
        return val

    if action in ("write", "insert", "create"):
        value = _normalize_images(value)
        if table == "cleanlight_canvas":
            laws.enforce_canvas_laws(value, system_delta=body.get("system_delta", False))
        elif table == "cleanlight_tags":
            laws.enforce_tag_laws(value, action="insert")
        encoded = {k: codec.encode_field(k, v) for k, v in value.items()}
        rec = db.insert_row(table, encoded)
        return jsonify(_wrap(_decode_record(rec)))

    if action == "update":
        value = _normalize_images(value)
        if table == "cleanlight_canvas":
            laws.enforce_canvas_laws(value, system_delta=body.get("system_delta", False))
        elif table == "cleanlight_tags":
            laws.enforce_tag_laws(value, action="update")
        encoded = {k: codec.encode_field(k, v) for k, v in value.items()}
        rec = db.update_row(table, key_col, rid, encoded)
        return jsonify(_wrap(_decode_record(rec)))

    if action == "append":
        rec = db.read_row(table, key_col, rid)
        if not rec: return _err("Not found", 404)
        merged = dict(rec)
        # decode first so merges happen on logical (decoded) shapes
        merged = _decode_record(merged)
        value  = _normalize_images(value)
        for k, v in value.items():
            if isinstance(merged.get(k), list) and isinstance(v, list):
                merged[k] = merged[k] + v
            elif isinstance(merged.get(k), dict) and isinstance(v, dict):
                merged[k] = {**merged[k], **v}
            else:
                merged[k] = v
        if table == "cleanlight_canvas":
            laws.enforce_canvas_laws(merged, system_delta=body.get("system_delta", False))
        elif table == "cleanlight_tags":
            laws.enforce_tag_laws(merged, action="update")
        encoded = {k: codec.encode_field(k, v) for k, v in merged.items()}
        rec = db.update_row(table, key_col, rid, encoded)
        return jsonify(_wrap(_decode_record(rec)))

    if action in ("write_cell", "append_cell"):
        if not field: return _err("Missing field")
        rec = db.read_row(table, key_col, rid)
        if not rec: return _err("Not found", 404)
        logical = _decode_record(rec)
        if action == "append_cell":
            cur = logical.get(field)
            if isinstance(cur, list) and isinstance(value, list):
                logical[field] = cur + value
            elif isinstance(cur, dict) and isinstance(value, dict):
                logical[field] = {**cur, **value}
            else:
                logical[field] = value
        else:
            logical[field] = value
        if table == "cleanlight_canvas":
            laws.enforce_canvas_laws(logical, system_delta=body.get("system_delta", False))
        elif table == "cleanlight_tags":
            laws.enforce_tag_laws(logical, action="update")
        encoded_val = codec.encode_field(field, logical[field])
        updated = db.update_row(table, key_col, rid, {field: encoded_val})
        return jsonify(_wrap(_decode_record(updated)))

    if action == "delete":
        if table == "cleanlight_tags":
            laws.enforce_tag_laws(value or {}, action="delete", allow_delete=body.get("allow_delete", False))
        db.delete_row(table, key_col, rid)
        return jsonify(_wrap({"status": "deleted"}))

    return _err("Invalid action", 400)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
