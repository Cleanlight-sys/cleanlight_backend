import os, requests
from flask import jsonify
from config import wrap, SUPABASE_URL, HEADERS, TABLE_KEYS

def handle(table, body):
    rid = body.get("rid")
    if not rid:
        return jsonify(wrap(None, body, "Add 'rid': <id>", {"code":"RID_REQUIRED","field":"rid"})), 400
    key_col = TABLE_KEYS.get(table, "id")
    url = f"{SUPABASE_URL}/rest/v1/{table}?{key_col}=eq.{rid}"
    r = requests.delete(url, headers=HEADERS)
    if r.status_code != 204:
        return jsonify(wrap(None, body, "Delete failed", {"code":"DELETE_FAIL","detail":r.text})), 500
    return jsonify(wrap({"status":"deleted","rid":rid}, body))
