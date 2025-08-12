# db.py — Supabase access layer + tag cache

import os
import time
import requests
from urllib.parse import quote_plus

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY")

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Prefer": "return=representation",
}

# ---- tag cache ----
_allowed_tags = {"tags": [], "ts": 0}
TAG_TTL = 60

def refresh_allowed_tags(force=False):
    now = time.time()
    if force or (now - _allowed_tags["ts"] > TAG_TTL):
        r = requests.get(f"{SUPABASE_URL}/rest/v1/cleanlight_tags?select=tag", headers=HEADERS)
        r.raise_for_status()
        _allowed_tags["tags"] = [row["tag"] for row in r.json()]
        _allowed_tags["ts"] = now
    return _allowed_tags["tags"]

def get_allowed_tags():
    return refresh_allowed_tags(False)

# ---- helpers ----
def _json_or_text(resp: requests.Response):
    try:
        if resp.content and resp.headers.get("Content-Type","").startswith("application/json"):
            return resp.json()
    except Exception:
        pass
    return {"status": resp.status_code, "body": resp.text}

# ---- CRUD ----
def read_table(table: str, select="*", limit=50, offset=0):
    params = {"select": select, "limit": limit, "offset": offset}
    r = requests.get(f"{SUPABASE_URL}/rest/v1/{table}", headers=HEADERS, params=params)
    r.raise_for_status()
    return r.json()

def read_all_rows(table: str, select="*"):
    # You can page if needed; for simplicity: big limit
    params = {"select": select, "limit": 10000, "offset": 0}
    r = requests.get(f"{SUPABASE_URL}/rest/v1/{table}", headers=HEADERS, params=params)
    r.raise_for_status()
    return r.json()

def read_row(table: str, key_col: str, rid, select="*"):
    qv = quote_plus(str(rid))
    r = requests.get(f"{SUPABASE_URL}/rest/v1/{table}?{key_col}=eq.{qv}&select={select}", headers=HEADERS)
    r.raise_for_status()
    data = r.json()
    return data[0] if isinstance(data, list) and data else None

def read_rows(table: str, key_col: str, ids, select="*"):
    # in= supports comma-separated or array; we’ll OR them via multiple eq calls for safety
    results = []
    for rid in ids:
        row = read_row(table, key_col, rid, select)
        if row: results.append(row)
    return results

def insert_row(table: str, payload: dict):
    r = requests.post(f"{SUPABASE_URL}/rest/v1/{table}", headers=HEADERS, json=payload)
    r.raise_for_status()
    data = r.json()
    return data[0] if isinstance(data, list) and data else data

def update_row(table: str, key_col: str, rid, payload: dict):
    qv = quote_plus(str(rid))
    r = requests.patch(f"{SUPABASE_URL}/rest/v1/{table}?{key_col}=eq.{qv}", headers=HEADERS, json=payload)
    r.raise_for_status()
    data = r.json()
    return data[0] if isinstance(data, list) and data else data

def delete_row(table: str, key_col: str, rid):
    qv = quote_plus(str(rid))
    r = requests.delete(f"{SUPABASE_URL}/rest/v1/{table}?{key_col}=eq.{qv}", headers=HEADERS)
    r.raise_for_status()
    return {"status": "deleted"}
