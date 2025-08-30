import requests
from config import SUPABASE_URL, HEADERS, TABLE_KEYS

def handle(table, body):
    rid = body.get("rid")
    payload = body.get("payload", {})
    if not rid:
        return None, "Add 'rid': <id>", {"code":"RID_REQUIRED", "field":"rid"}

    key_col = TABLE_KEYS.get(table, "id")
    url = f"{SUPABASE_URL}/rest/v1/{table}?{key_col}=eq.{rid}"
    r = requests.patch(url, headers={**HEADERS,"Content-Type":"application/json"}, json=payload)

    if r.status_code != 200:
        return None, "Update failed", {"code":"UPDATE_FAIL", "detail": r.text}

    return r.json(), None, None
