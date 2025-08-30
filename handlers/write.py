import requests
from config import SUPABASE_URL, HEADERS

def handle(table, body):
    payload = body.get("payload", {})
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    r = requests.post(url, headers={**HEADERS,"Content-Type":"application/json"}, json=payload)

    if r.status_code not in (200, 201):
        return None, "Insert failed", {"code":"WRITE_FAIL", "detail": r.text}

    return r.json(), None, None
