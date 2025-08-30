import requests
from config import SUPABASE_URL, HEADERS, TABLE_KEYS

def handle(table, body):
    rid = body.get("rid")
    if not rid:
        return None, "Add 'rid': <id>", {"code":"RID_REQUIRED", "field":"rid"}

    key_col = TABLE_KEYS.get(table, "id")
    url = f"{SUPABASE_URL}/rest/v1/{table}?{key_col}=eq.{rid}"
    r = requests.delete(url, headers=HEADERS)

    if r.status_code != 204:
        return None, "Delete failed", {"code":"DELETE_FAIL", "detail": r.text}

    return {"status": "deleted", "rid": rid, "table": table}, None, None
