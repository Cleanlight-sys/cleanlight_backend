import requests
from config import SUPABASE_URL, HEADERS, TABLE_KEYS

def handle(table, body):
    rid = body.get("rid")
    select = body.get("select", "*")
    if not rid:
        return None, "Add 'rid': <id>", {"code":"RID_REQUIRED", "field":"rid"}

    key_col = TABLE_KEYS.get(table, "id")
    url = f"{SUPABASE_URL}/rest/v1/{table}?{key_col}=eq.{rid}&select={select}"

    r = requests.get(url, headers=HEADERS)
    if r.status_code != 200:
        return None, "Supabase error", {"code": "READ_FAIL", "detail": r.text}

    rows = r.json()
    if not rows:
        return None, "Not found", {"code": "NOT_FOUND", "id": rid}

    return rows[0], None, None
