import os, requests
from flask import jsonify
from Cleanlight_bk import wrap, SUPABASE_URL, HEADERS, TABLE_KEYS

def handle(table, body):
    payload = body.get("payload", {})
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    r = requests.post(url, headers={**HEADERS,"Content-Type":"application/json"}, json=payload)
    if r.status_code not in (200,201):
        return jsonify(wrap(None, body, "Insert failed", {"code":"WRITE_FAIL","detail":r.text})), 500
    return jsonify(wrap(r.json(), body))
