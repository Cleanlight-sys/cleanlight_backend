import os, requests
from flask import jsonify
from Cleanlight_bk import wrap

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
HEADERS = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
TABLE_KEYS = { "docs": "doc_id", "chunks": "id", "graph": "id", "edges": "id" }

def handle(table, body):
    rid = body.get("rid")
    select = body.get("select", "*")
    if not rid:
        return jsonify(wrap(None, body, "Add 'rid': <id>", {"code":"RID_REQUIRED","field":"rid"})), 400
    key_col = TABLE_KEYS.get(table, "id")
    url = f"{SUPABASE_URL}/rest/v1/{table}?{key_col}=eq.{rid}&select={select}"
    r = requests.get(url, headers=HEADERS)
    rows = r.json()
    if not rows:
        return jsonify(wrap(None, body, "Not found", {"code":"NOT_FOUND","id":rid})), 404
    return jsonify(wrap(rows[0], body))
