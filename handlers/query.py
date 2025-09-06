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

def _rows_from_res(res):
    # Supabase v2: APIResponse has .data (may be []), never dict-like .get
    try:
        if hasattr(res, "data"):
            return res.data or []          # preserve [], return []
        if isinstance(res, dict):
            return res.get("data") or []
    except Exception:
        pass
    return []

# --- 1) Rename your current implementation to a private helper ----------------
def _handle_impl(table: str, body: Dict[str, Any], **kwargs) -> Tuple[List[Dict[str, Any]], Optional[str], Dict[str, Any]]:
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
        query = (
            db.table("graph")
              .select("id,label,doc_id,page,ntype")
              .ilike("label", f"%{q_text}%")
              .limit(limit)
        )
        res = query.execute()
        data = _rows_from_res(res)
        return data, None, {"limited": True, "count": len(data)}

        # ---- semantic bundle (graph of graphs) ----
    if table == "bundle":
        # We keep using 'q' here **internally** for the topic string.
        topic = (body.get("q") or "").strip()
        from smesvc.bundle import build as build_bundle  # local import to keep handler thin
        result = build_bundle(topic, limits={"l0": 8, "l1": 5, "l2": 25, "l3": 20, "chunk_text_max": 300})
        return result, None, {"limited": True}
    elif table == "bundle":
         return _bundle.build(body.get("q"), limits)
    elif table == "ask":
        from smesvc.ask import run as ask_run
        return ask_run(body.get("q"), {
            "strategy": body.get("strategy", "bundle_then_chunks_v1"),
            "max_steps": body.get("max_steps", 6),
            "beam": body.get("beam", 2),
            "return_trace": body.get("return_trace", True),
            "citations_max": body.get("citations_max", 6),
            "chunk_text_max": body.get("chunk_text_max", 800),
        })    

    # --- general path --------------------------------------------------------
    query = db.table(table).select("*").limit(limit)

    # FTS via q_text shortcuts when present
    if q_text:
        if table == "chunks":
            query = query.filter("text", "wfts", q_text)  # websearch_to_tsquery
        elif table == "kcs":
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
    rows = _rows_from_res(res)

    if table == "chunks" and chunk_text_max:
        _shorten_chunks(rows, chunk_text_max)

    return rows, None, {"limited": True, "count": len(rows)}


# --- 2) Public entry that supports both calling conventions ------------------
def handle(body_or_table, maybe_body: Optional[Dict[str, Any]] = None, **kwargs) -> Tuple[List[Dict[str, Any]], Optional[str], Dict[str, Any]]:
    """
    Back-compat shim:
      - New style (what Cleanlight_bk.py does): handle(body)
      - Old style: handle(table, body)

    Always return (data, error, meta); never raise.
    """
    try:
        # New style: single dict argument
        if isinstance(body_or_table, dict) and maybe_body is None:
            body = body_or_table or {}
            table = body.get("table")
            if not table:
                return [], "Missing required field: 'table'", {}
            return _handle_impl(table, body, **kwargs)

        # Old style: (table, body)
        table = body_or_table
        body = maybe_body or {}
        return _handle_impl(table, body, **kwargs)

    except Exception as e:
        # Fail soft: return JSON-friendly error instead of 500
        return [], f"/query failed: {e.__class__.__name__}: {e}", {}
