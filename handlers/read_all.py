import requests
from config import SUPABASE_URL, HEADERS

def handle(table, body):
    select = body.get("select", "*")
    filters = body.get("filters") or {}
    stream  = body.get("stream", False)
    limit   = int(body.get("limit", 100))

    # Build query string
    qs = []
    for k, v in filters.items():
        qs.append(f"{k}={v}")
    if not stream:
        qs.append(f"limit={limit}")
    filter_qs = "&" + "&".join(qs) if qs else ""
    url = f"{SUPABASE_URL}/rest/v1/{table}?select={select}{filter_qs}"

    r = requests.get(url, headers=HEADERS)
    if r.status_code != 200:
        return None, "Supabase error", {"code": "READ_FAIL", "detail": r.text}

    return r.json(), None, None
