# smesvc/scoring.py
"""Blend FTS rank with MiniLM cosine + small boosts for topics/KCs.
Why: deterministic, transparent ranking for agent answers.
"""
from __future__ import annotations


from typing import Dict, List, Any
import math


try:
    from jobs.embed_chunks import embed_texts  # reuse provider setup
except Exception:
    def embed_texts(texts, provider="minilm"):
        raise RuntimeError("Install Phase 2 embedding job or supply embeddings externally")




def _cos(a: List[float], b: List[float]) -> float:
    s = sum(x*y for x, y in zip(a, b))
    na = math.sqrt(sum(x*x for x in a)) or 1.0
    nb = math.sqrt(sum(x*x for x in b)) or 1.0
    return s/(na*nb)




def score_and_rerank(question: str, rows: List[Dict[str, Any]], provider: str = "minilm") -> List[Dict[str, Any]]:
    """Add `score` and return rows sorted desc.
    Why: agents get a clear ordering; no hidden magic.
    """
    if not rows:
        return rows
    qv = embed_texts([question], provider=provider)[0]


    def fts_norm(r):
        return float(r.get("rank") or 0.0)  # assume already 0..1; if not, pre-normalize upstream


    out = []
    for r in rows:
        cv = r.get("embedding_384") or r.get("centroid_384") or []
        cos = _cos(qv, cv) if cv else 0.0
        topic_bonus = 0.1 if r.get("topic") else 0.0
        kc_bonus = 0.05 if r.get("near_kc") else 0.0
        score = 0.55*fts_norm(r) + 0.30*cos + topic_bonus + kc_bonus
        r2 = dict(r); r2["score"] = round(score, 4)
        out.append(r2)
    out.sort(key=lambda x: x["score"], reverse=True)
    return out
