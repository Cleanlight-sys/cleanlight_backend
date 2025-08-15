# db.py — Supabase interaction layer for Cleanlight

import os
import requests
from datetime import datetime

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY")

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=representation"
}

# ---------- BASIC GETTERS ----------
def read_table(table: str, select="*", limit=1000):
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/{table}?select={select}&limit={limit}",
        headers=HEADERS
    )
    r.raise_for_status()
    return r.json()

def read_all_rows(table: str, select="*"):
    return read_table(table, select=select, limit=10000)

def read_row(table: str, key_col: str, rid, select="*"):
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/{table}?{key_col}=eq.{rid}&select={select}",
        headers=HEADERS
    )
    r.raise_for_status()
    data = r.json()
    return data[0] if data else None

def read_rows(table: str, key_col: str, ids: list, select="*"):
    if not ids:
        return []
    id_str = ",".join(map(str, ids))
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/{table}?{key_col}=in.({id_str})&select={select}",
        headers=HEADERS
    )
    r.raise_for_status()
    return r.json()

def read_cell(table: str, key_col: str, rid, field: str):
    row = read_row(table, key_col, rid, select=f"{key_col},{field}")
    return row.get(field) if row else None

def read_column(table: str, key_col: str, field: str):
    rows = read_table(table, select=f"{key_col},{field}")
    return [{key_col: r[key_col], "value": r[field]} for r in rows if field in r]

# ---------- WRITES ----------
def insert_row(table: str, payload: dict):
    r = requests.post(f"{SUPABASE_URL}/rest/v1/{table}", headers=HEADERS, json=payload)
    try:
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        raise RuntimeError(f"Supabase insert failed: {r.status_code} → {r.text}") from e
    data = r.json()
    return data[0] if isinstance(data, list) and data else data

def update_row(table: str, key_col: str, rid, payload: dict):
    # ✅ Inject updated_at just before the mutation
    payload["updated_at"] = datetime.utcnow().isoformat()

    r = requests.patch(
        f"{SUPABASE_URL}/rest/v1/{table}?{key_col}=eq.{rid}",
        headers=HEADERS,
        json=payload
    )

    try:
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        raise RuntimeError(f"Supabase update failed: {r.status_code} → {r.text}") from e

    data = r.json()
    return data[0] if isinstance(data, list) and data else data
def delete_row(table: str, key_col: str, rid):
    r = requests.delete(f"{SUPABASE_URL}/rest/v1/{table}?{key_col}=eq.{rid}", headers=HEADERS)
    try:
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        raise RuntimeError(f"Supabase delete failed: {r.status_code} → {r.text}") from e
    return True


