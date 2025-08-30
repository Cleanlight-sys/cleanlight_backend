import os, requests
from flask import jsonify
from Cleanlight_bk import wrap, SUPABASE_URL, HEADERS, TABLE_KEYS

def handle(table, body):
    rid = body.get("rid")
    payload = body.get("payload", {})
    if not rid:
        return jsonify(wrap(None, body, "Add 'rid': <id>", {"code":"RID_REQUIRED","field":"rid"})), 400
    key_col = TABLE_KEYS.get(table, "id")
    url = f"{SUPABASE_URL}/rest/v1/{table}?{key_col}=eq.{rid}"
    r = requests.patch(url, headers={**HEADERS,"Content-Type":"application/json"}, json=payload)
    if r.status_code != 200:
        return jsonify(wrap(None, body, "Update failed", {"code":"UPDATE_FAIL","detail":r.text})), 500
    return jsonify(wrap(r.json(), body))
