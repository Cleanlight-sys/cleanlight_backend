# smesvc/bundle.py
from __future__ import annotations
from typing import Any, Dict, List, Tuple, Optional
import os

from .emb import embed_texts, cosine, lexical_score

def _sb():
    from supabase import create_client  # lazy import
    url = os.environ["SUPABASE_URL"]; key = os.environ["SUPABASE_KEY"]
    return create_client(url, key)

def _rows(res) -> List[Dict[str, Any]]:
    try:
        if hasattr(res, "data"):
            return res.data or []
        if isinstance(res, dict):
            return res.get("data") or []
    except Exception:
        pass
    return []

def _topk_scored(pairs: List[Tuple[float, Dict[str, Any]]], k: int) -> List[Dict[str, Any]]:
    return [row for _, row in sorted(pairs, key=lambda kv: kv[0], reverse=True)[:k]]

def _score_by_texts(query: str, texts: List[str]) -> Optional[List[float]]:
    embs = embed_texts([query] + texts)
    if embs is None:
        # lexical fallback
        return [lexical_score(query, t) for t in texts]
    qv, tvs = embs[0], embs[1:]
    return [cosine(qv, tv) for tv in tvs]

def build(topic: str, limits: Optional[Dict[str, int]] = None) -> Dict[str, Any]:
    """
    Level 0: subjects (kcs) that match the topic
    Level 1: docs related to the topic
    Level 2: graph nodes related to topic
    Level 3: chunks (shortened) related to topic
    """
    topic = (topic or "").strip()
    if not topic:
        return {"topic": topic, "l0": [], "l1": [], "l2": [], "l3": [], "meta": {"note": "empty topic"}}

    lim = {
        "l0": 8,   # subjects
        "l1": 5,   # docs
        "l2": 25,  # graph nodes
        "l3": 20,  # chunks
        "chunk_text_max": 300
    }
    if limits:
        lim.update({k: int(v) for k, v in limits.items() if k in lim})

    sb = _sb()

   
    # --- L0: subjects (kcs.q) ---
    kcs = _rows(sb.table("kcs").select("id,q,a_ref").limit(200).execute())
    kcs_scores = _score_by_texts(topic, [k.get("q","") for k in kcs]) or []
    l0 = _topk_scored(list(zip(kcs_scores, kcs)), lim["l0"])

    # --- L1: docs ---
    docs = _rows(sb.table("docs").select("doc_id,title,meta").limit(50).execute())
    docs_texts = [f'{d.get("title","")} {str((d.get("meta") or {}).get("author",""))}' for d in docs]
    docs_scores = _score_by_texts(topic, docs_texts) or []
    l1 = _topk_scored(list(zip(docs_scores, docs)), lim["l1"])

    # --- L2: graph nodes ---
    graph = _rows(sb.table("graph").select("id,doc_id,label,ntype,page").limit(300).execute())
    graph_texts = [g.get("label","") for g in graph]
    graph_scores = _score_by_texts(topic, graph_texts) or []
    l2 = _topk_scored(list(zip(graph_scores, graph)), lim["l2"])

    # --- L3: chunks (short text) ---
    chunks = _rows(sb.table("chunks").select("id,doc_id,page_from,page_to,text").limit(300).execute())
    chunk_texts = [c.get("text","") for c in chunks]
    chunk_scores = _score_by_texts(topic, chunk_texts) or []
    l3 = _topk_scored(list(zip(chunk_scores, chunks)), lim["l3"])

    # truncate chunk text
    mx = lim["chunk_text_max"]
    for c in l3:
        t = c.get("text", "")
        if isinstance(t, str) and len(t) > mx:
            c["text"] = t[:mx-1] + "…"

    return {
        "topic": topic,
        "l0": l0,
        "l1": l1,
        "l2": l2,
        "l3": l3,
        "meta": {
            "limits": lim,
            "notes": ["semantic" if embed_texts(["ok"]) else "lexical_fallback"]
        }
    }
