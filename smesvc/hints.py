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

# smesvc/hints.py — recommend(): emit only shapes that the deployed /query accepts
def recommend(question: Optional[str] = None, doc: Optional[str] = None) -> List[Dict[str, Any]]:
    recs: List[Dict[str, Any]] = []

    # Subject/Topic bundle (graph-of-graphs)
    recs.append({
        "title": "Subject bundle (flat seams / beaver felt)",
        "call": {
            "path": "/query",
            "body": {
                "action": "query",
                "table": "bundle",
                "q": question or "flat seams 80/20 beaver felt",
                "limit": 1  # ignored by bundle, kept for gateway parity
            }
        }
    })

    # Minimal inventory
    recs.append({
        "title": "List docs",
        "call": {"path": "/query", "body": {"action": "query", "table": "docs", "limit": 5}}
    })

    # Doc-scoped samples remain (optional)
    if doc:
        recs.append({
            "title": "Chunks by doc pattern",
            "call": {"path": "/query", "body": {
                "action":"query","table":"chunks",
                "filters_str": f"doc_id=ilike.{doc}", "chunk_text_max": 400, "limit": 50
            }}
        })
        recs.append({
            "title": "Edges by doc pattern",
            "call": {"path": "/query", "body": {
                "action":"query","table":"edges","filters_str": f"doc_id=ilike.{doc}","limit": 200
            }}
        })
    return recs

def build_hints(question: Optional[str] = None, doc: Optional[str] = None) -> Dict[str, Any]:
    h: Dict[str, Any] = {
        "capabilities": capabilities(),
        "coverage": coverage(),
        "limits": {"default_top_k": 8, "max_rows": 1000},
        "recommend": recommend(question, doc),
    }
    # Teach the agent the standard flow without breaking existing consumers.
    h["agent_default_flow"] = (
        "Use bundle → targeted chunks. If you need surrounding context, pull a same-doc page window (±1 page)."
    )
    h["strategies"] = [
        {
            "name": "bundle_then_chunks",
            "when": "General knowledge questions requiring SME synthesis",
            "steps": [
                {"table": "bundle", "q": (question or "<seed_phrase>"), "limit": 1, "chunk_text_max": 800},
                {"table": "chunks", "q": "<precision_terms 3–6>", "limit": 10, "chunk_text_max": 800}
            ],
            "notes": [
                "Derive precision terms from bundle.l2 labels and l3 chunk n-grams.",
                "Prefer chunks that include all precision terms; exclude decorative-only hits when the task is structural."
            ],
        },
        {
            "name": "widen_context_window",
            "when": "Follow-ups/variations after you have a hit chunk (e.g., wider ribbon)",
            "steps": [
                {"table": "chunks", "filters_str": "id=eq.<hit_id>", "chunk_text_max": 400},
                {"table": "chunks", "filters_str": "doc_id=eq.<doc>&page_from=gte.<pf-1>&page_to=lte.<pt+1>", "limit": 20, "chunk_text_max": 800}
            ],
            "notes": [
                "Clamp page_from ≥ 1; avoid id=in.(…); prefer same-doc page window to fetch neighbors."
            ],
        },
    ]
    h["examples"] = [
        {
            "seed_phrase": question or "how should I stitch a crown ribbon to a hat?",
            "precision_terms": "crown tip felled seam quarter inch gather back"
        }
    ]
    return h
