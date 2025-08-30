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

