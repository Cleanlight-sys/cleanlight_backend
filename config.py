# config.py
import os

# Supabase credentials
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
HEADERS = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}

# Table key map
TABLE_KEYS = {
    "docs": "doc_id",
    "chunks": "id",
    "graph": "id",
    "edges": "id",
}
def wrap(data=None, body=None, hint=None, error=None):
    echo = {"original_body": body}
    out = {"data": data, "echo": echo}
    if hint is not None: out["hint"] = hint
    if error is not None: out["error"] = error
    return out
