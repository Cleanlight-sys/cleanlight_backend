# smesvc/hints.py
"""Build the agent hints envelope (capabilities, coverage, recommend, prototypes, map tiles).
Why: self-aware responses reduce retries and floods.
"""
from __future__ import annotations
from typing import Any, Dict, List, Optional
import os

def _sb():
    # Lazy import so module import doesn't crash during deploy if deps/env not ready yet
    try:
        from supabase import create_client, Client  # type: ignore
    except Exception as e:
        raise RuntimeError("Supabase client not available. Ensure 'supabase' is in requirements.txt and deployed.") from e
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    if not url or not key:
        raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY in environment.")
    return create_client(url, key)

def capabilities() -> Dict[str, Any]:
    sb = _sb()
    def cnt(t):
        # count='exact' ensures we can read .count reliably
        return (getattr(sb.table(t).select("count", count='exact').limit(1).execute(), "count", None) or 0)
    return {
        "docs":   cnt("docs"),
        "chunks": cnt("chunks"),
        "graph":  cnt("graph"),
        "edges":  cnt("edges"),
        "images": cnt("images"),
        "kcs":    cnt("kcs"),
    }

def coverage() -> Dict[str, Any]:
    try:
        sb = _sb()
        # Top docs: a small sample (tune select/limit as you like)
        top_docs = (
            sb.table("docs")
              .select("doc_id,title,meta")
              .limit(8)
              .execute()
              .data or []
        )
        # “Recent” fallback when we don’t have created_at:
        # Use doc_id descending as a crude proxy (or drop ordering entirely if you prefer).
        recent_docs = (
            sb.table("docs")
              .select("doc_id,title,meta")
              .order("doc_id", desc=True)  # safe: doc_id exists; adjust if you prefer alphabetical
              .limit(8)
              .execute()
              .data or []
        )
        return {"top_docs": top_docs, "recent_docs": recent_docs}
    except Exception as e:
        # Fail soft so /hint never 500s
        return {"top_docs": [], "recent_docs": [], "_warn": f"coverage degraded: {e.__class__.__name__}"}

def recommend(question: Optional[str] = None, doc: Optional[str] = None) -> List[Dict[str, Any]]:
    calls: List[Dict[str, Any]] = []
    # keep your existing recs; below are safe examples you already hinted at
    calls.append({
        "title": "Browse graph by label",
        "call": {
            "path": "/query",
            "body": {
                "action": "query",
                "table": "graph",
                "select": "id,doc_id,label,ntype,page",
                "filters": {"label": "ilike.%seam%"},
                "limit": 25
            }
        }
    })
    if doc:
        calls.append({
            "title": "Focus on doc pattern",
            "call": {"path": "/query", "body": {
                "action": "query", "table": "docs",
                "select": "doc_id,title,meta",
                "filters": {"title": f"ilike.{doc}"},
                "limit": 8
            }}
        })
    return calls

def build_hints(question: Optional[str] = None, doc: Optional[str] = None) -> Dict[str, Any]:
    return {
        "capabilities": capabilities(),
        "coverage": coverage(),
        "limits": {"default_top_k": 8, "max_rows": 1000},
        "recommend": recommend(question, doc),
        # MiniLM prototypes and map tiles can be added in Phase 2b
    }
