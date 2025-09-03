# handlers/query.py
"""
Central Instant-SME query handler with:
- Early limiting at the DB boundary (prevents ResponseTooLargeError)
- Pass-through of `filters` and `filters_str`
- Optional chunk text truncation via `chunk_text_max`
- Light special-casing for graph label lookups (q="seam")
"""
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Tuple
import os
import re

# Get Supabase/PostgREST client
try:
    from utils.schema import get_supabase  # project helper
except Exception:  # pragma: no cover
    def get_supabase():
        from supabase import create_client
        url = os.environ["SUPABASE_URL"]
        key = os.environ["SUPABASE_KEY"]
        return create_client(url, key)


ALLOWED_TABLES = {"docs", "chunks", "graph", "edges", "images", "kcs", "bundle"}


def _apply_filter_pair(q, col: str, op: str, val: str):
    op = op.lower()
    if op in {"eq", "neq", "gt", "gte", "lt", "lte", "like", "ilike", "is", "cs", "cd", "ov", "fts", "plfts", "phfts", "wfts"}:
        return q.filter(col, op, val)
    # fallback: equality
    return q.eq(col, val)


def _parse_filters_str(filters_str: str) -> List[Tuple[str, str, str]]:
    # e.g., label=ilike.%seam%&doc_id=eq.f0d296...
    out: List[Tuple[str, str, str]] = []
    if not filters_str:
        return out
    for pair in filters_str.split("&"):
        if not pair or "=" not in pair:
            continue
        col, rhs = pair.split("=", 1)
        if "." in rhs:
            op, val = rhs.split(".", 1)
        else:
            op, val = "eq", rhs
        out.append((col, op, val))
    return out


def _shorten_chunks(rows: List[Dict[str, Any]], max_len: int) -> None:
    if not rows or not max_len:
        return
    for r in rows:
        t = r.get("text")
        if isinstance(t, str) and len(t) > max_len:
            r["text"] = t[:max_len] + "…"


def handle(table: str, body: Dict[str, Any], **kwargs) -> Tuple[List[Dict[str, Any]], Optional[str], Dict[str, Any]]:
    if table not in ALLOWED_TABLES:
        return [], f"Table not allowed: {table}", {}

    q_text: Optional[str] = body.get("q")
    limit: int = max(1, int(body.get("limit") or 50))
    filters: Dict[str, Any] = body.get("filters") or {}
    filters_str: Optional[str] = body.get("filters_str")
    chunk_text_max: int = int(body.get("chunk_text_max") or 0)

    db = get_supabase()

    # --- special-case: graph label lookup like q="seam" ---------------------
    if table == "graph" and q_text and not filters and not filters_str:
        query = db.table("graph").select("id,label,doc_id,page,ntype").ilike("label", f"%{q_text}%").limit(limit)
        res = query.execute()
        data = getattr(res, "data", None) or res.get("data")  # supabase v2 compat
        return data or [], None, {"limited": True, "count": len(data or [])}

    # --- general path --------------------------------------------------------
    query = db.table(table).select("*").limit(limit)

    # FTS via q_text shortcuts when present
    if q_text:
        # Prefer weighted FTS on common columns
        if table == "chunks":
            query = query.filter("text", "wfts", q_text)  # websearch_to_tsquery
        elif table == "kcs":
            # Search in q + a_ref text cast
            query = query.or_(f"q.wfts.{q_text},a_ref.wfts.{q_text}")
        elif table == "docs":
            query = query.filter("title", "ilike", f"%{q_text}%")
        elif table == "graph":
            query = query.filter("label", "ilike", f"%{q_text}%")

    # Structured filters dict
    if filters:
        for col, spec in filters.items():
            if isinstance(spec, str) and "." in spec:
                op, val = spec.split(".", 1)
                query = _apply_filter_pair(query, col, op, val)
            else:
                query = query.eq(col, spec)

    # Raw filters string
    for col, op, val in _parse_filters_str(filters_str or ""):
        query = _apply_filter_pair(query, col, op, val)

    res = query.execute()
    rows = getattr(res, "data", None) or res.get("data") or []

    if table == "chunks" and chunk_text_max:
        _shorten_chunks(rows, chunk_text_max)

    return rows, None, {"limited": True, "count": len(rows)}
