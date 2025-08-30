import os, requests
from flask import jsonify
from Cleanlight_bk import wrap

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
HEADERS = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}

def handle(table, body):
    payload = body.get("payload", {})
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    r = requests.post(url, headers={**HEADERS,"Content-Type":"application/json"}, json=payload)
    if r.status_code not in (200,201):
        return jsonify(wrap(None, body, "Insert failed", {"code":"WRITE_FAIL","detail":r.text})), 500
    return jsonify(wrap(r.json(), body))
