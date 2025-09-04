# smesvc/hints.py
"""Build the agent hints envelope (capabilities, coverage, recommend, prototypes, map tiles).
Why: self-aware responses reduce retries and floods.
"""
from __future__ import annotations
from typing import Any, Dict, List, Optional
import os
from supabase import create_client, Client

def _sb() -> Client:
    return create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

def capabilities() -> Dict[str, Any]:
    sb = _sb()
    def cnt(t):
        return (getattr(sb.table(t).select("count", count='exact').limit(1).execute(), "count", None) or 0)
    return {
        "tables": ["docs","chunks","graph","edges","images","kcs","prototypes"],
        "has_embeddings": True,
        "doc_count": cnt("docs"),
        "chunk_count": cnt("chunks"),
        "topic_count": cnt("graph"),
    }

def coverage(limit: int = 25) -> Dict[str, Any]:
    sb = _sb()
    docs = (sb.table("docs").select("doc_id,title").limit(limit).execute().data) or []
    # top topics by DF (size in prototypes topic:*)
    topics = (sb.table("prototypes").select("prototype_id,topic,size").ilike("prototype_id","topic:%").order("size", desc=True).limit(20).execute().data) or []
    return {"docs": docs, "top_topics": topics}

def recommend(question: Optional[str] = None, doc: Optional[str] = None) -> List[Dict[str, Any]]:
    calls = []
    if doc:
        calls.append({"title": "Broaden search", "call": {"path": "/ask", "body": {"question": question or "", "top_k": 8}}})
    else:
        calls.append({"title": "Focus on millinery", "call": {"path": "/ask", "body": {"question": question or "", "doc": "%millinery%", "top_k": 8}}})
    calls.append({"title": "Narrow topic: seam", "call": {"path": "/query", "body": {"action":"query","table":"graph","filters":{"label":"ilike.%seam%"}, "limit": 25}}})
    return calls

def build_hints(question: Optional[str] = None, doc: Optional[str] = None) -> Dict[str, Any]:
    return {
        "capabilities": capabilities(),
        "coverage": coverage(),
        "limits": {"default_top_k": 8, "max_rows": 1000},
        "recommend": recommend(question, doc),
        # MiniLM prototypes and map tiles can be added in Phase 2b
    }

