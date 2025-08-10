# --- ADD to your Flask file (reuse your existing imports/helpers/HEADERS/etc.) ---

# ---------- Normalization helpers (accept many synonyms) ----------
def _norm_action(a):
    if not a: return None
    a = str(a).lower().strip()
    aliases = {
        "create":"write", "insert":"write", "add":"write",
        "patch":"update", "modify":"update",
        "appendcell":"append_cell", "append_field":"append_cell",
        "writecell":"write_cell", "set_cell":"write_cell",
        "get":"read_row", "read":"read_row", "fetch":"read_row",
        "column":"read_column", "all":"read_all", "list":"read_table",
        "remove":"delete", "del":"delete",
    }
    return aliases.get(a, a)

def _pick(dct, *keys):
    for k in keys:
        if k in dct and dct[k] is not None:
            return dct[k]
    return None

def _norm_table(payload):
    # prefer explicit single-target
    t = _pick(payload, "target", "table", "Target")
    if t: return str(t)
    # fallback: array
    tables = payload.get("tables")
    if isinstance(tables, list) and tables:
        return str(tables[0])
    return None

def _norm_rid(payload):
    rid = _pick(payload, "rid", "id", "TargetRID")
    if rid is not None: return int(rid)
    where = payload.get("where")
    if isinstance(where, dict) and str(where.get("col")).lower() == "id":
        return int(where.get("val"))
    return None

def _norm_field(payload):
    return _pick(payload, "field", "TargetField")

def _norm_value(payload):
    # cell ops may send {value:...}; row ops use {payload:{...}} or {fields:{...}}
    if "value" in payload:
        return payload["value"]
    if "payload" in payload:
        return payload["payload"]
    if "fields" in payload:
        return payload["fields"]
    return None

def _is_canvas(table): return table == "cleanlight_canvas"
def _is_map(table):    return table == "cleanlight_map"

# ---------- Auto-paging read (no confirmations) ----------
def _read_table_autopage(table, limit_total=10000, page_size=1000, start_offset=0):
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

# ---------- Unified /command ----------
@app.post("/command")
def unified_command():
    body = request.get_json(force=True) or {}

    # Back-compat: accept legacy shape directly
    if "action" in body and "table" in body and ("fields" in body or body.get("action") in ("read_table","read_row")):
        # Normalize legacy into unified keys
        body = dict(body)  # copy
        body.setdefault("target", body.get("table"))
        if body.get("action") == "read_row" and "rid" not in body:
            w = body.get("where") or {}
            if str(w.get("col")) == "id":
                body["rid"] = w.get("val")

    action = _norm_action(body.get("action"))
    table  = _norm_table(body)
    rid    = _norm_rid(body)
    field  = _norm_field(body)
    value  = _norm_value(body)

    if action is None or table is None:
        return jsonify({"error":"Missing action or target table"}), 400
    if table not in ("cleanlight_canvas", "cleanlight_map"):
        return jsonify({"error":"Invalid table"}), 400

    # Defaults for reads
    autopage    = bool(body.get("autopage", action in ("read_table","read_all")))
    limit_total = int(body.get("limit_total", 10000))
    limit       = int(body.get("limit", 1000))
    offset      = int(body.get("offset", 0))

    # ---- ACTIONS ----
    try:
        if action == "read_table":
            if autopage:
                rows = _read_table_autopage(table, limit_total=limit_total, page_size=limit, start_offset=offset)
                return jsonify(rows), 200
            # manual page
            data, code = sb_list(table, limit, offset)
            return jsonify(data), code

        if action == "read_all":
            rows = _read_table_autopage(table, limit_total=limit_total, page_size=limit, start_offset=offset)
            return jsonify(rows), 200

        if action == "read_row":
            if rid is None: return jsonify({"error":"Missing rid"}), 400
            data, code = sb_get_by_id(table, rid)
            if code == 404: return jsonify({"error":"Not found"}), 404
            return jsonify(data), code

        if action == "read_cell":
            if rid is None or not field: return jsonify({"error":"Missing rid or field"}), 400
            if field not in ALLOWED_FIELDS[table]: return jsonify({"error":"Invalid field"}), 400
            rec, code = sb_get_by_id(table, rid, select=f"id,{field}")
            if code == 404 or rec is None: return jsonify({"error":"Not found"}), 404
            v = decode_cell_value(table, field, rec.get(field))
            return jsonify({"id": rid, "field": field, "value": v}), 200

        if action in ("write","create","insert","add"):
            if not isinstance(value, dict): return jsonify({"error":"payload/fields must be an object"}), 400
            if isinstance(value.get("images"), str) and len(value["images"]) > 7_000_000:
                return jsonify({"error": "images too large"}), 413
            data, code = sb_insert(table, value)
            return jsonify(data), code

        if action == "update":
            if rid is None: return jsonify({"error":"Missing rid"}), 400
            if not isinstance(value, dict): return jsonify({"error":"payload/fields must be an object"}), 400
            data, code = sb_update_by_id(table, rid, value)
            if code == 404: return jsonify({"error":"Not found"}), 404
            return jsonify(data), code

        if action == "append":
            if rid is None: return jsonify({"error":"Missing rid"}), 400
            if not isinstance(value, dict): return jsonify({"error":"payload/fields must be an object"}), 400
            # append = merge whole-record fields (dicts merge; lists extend; scalars overwrite)
            existing, code = sb_get_by_id(table, rid)
            if code == 404 or existing is None: return jsonify({"error":"Not found"}), 404
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

        if action in ("write_cell","append_cell"):
            if rid is None or not field: return jsonify({"error":"Missing rid or field"}), 400
            if field not in ALLOWED_FIELDS[table]: return jsonify({"error":"Invalid field"}), 400
            # append_cell merges; write_cell overwrites
            if action == "append_cell":
                data, code = sb_merge_cell(table, rid, field, value)
                return jsonify(data), code
            else:
                # write_cell = overwrite single field
                stored = process_single_field(table, field, value)
                r = requests.patch(f"{SUPABASE_URL}/rest/v1/{table}?id=eq.{rid}", headers=HEADERS, json={field: stored})
                res = _json_or_text(r)
                if isinstance(res, list) and res:
                    return jsonify(decode_row(res[0])), r.status_code
                if isinstance(res, dict):
                    return jsonify(decode_row(res)), r.status_code
                return jsonify(res), r.status_code

        if action == "read_column":
            if not field: return jsonify({"error":"Missing field"}), 400
            if field not in ALLOWED_FIELDS[table]: return jsonify({"error":"Invalid field"}), 400
            raw, code = sb_list(table, limit, offset, select=f"id,{field}")
            if not isinstance(raw, list): return jsonify(raw), code
            out = [{"id": r.get("id"), "value": decode_cell_value(table, field, r.get(field))} for r in raw]
            return jsonify(out), 200

        if action == "delete":
            if rid is None: return jsonify({"error":"Missing rid"}), 400
            r = requests.delete(f"{SUPABASE_URL}/rest/v1/{table}?id=eq.{rid}", headers=HEADERS)
            return jsonify(_json_or_text(r)), r.status_code

        return jsonify({"error": f"Unknown action '{action}'"}), 400

    except ValueError as ve:
        return jsonify({"error":"bad_request","details":str(ve)}), 400
    except Exception as e:
        return jsonify({"error":"server_error","details":str(e)}), 500
