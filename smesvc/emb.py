# smesvc/emb.py
from __future__ import annotations
from typing import List, Optional
import math

_model = None

def _load_model():
    global _model
    if _model is not None:
        return _model
    try:
        # Optional dependency; present when runtime is Python 3.11 with torch wheels
        from sentence_transformers import SentenceTransformer  # type: ignore
        _model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
    except Exception:
        _model = None
    return _model

def embed_texts(texts: List[str]) -> Optional[List[List[float]]]:
    m = _load_model()
    if m is None:
        return None
    # small batches; model is light
    return [vec.tolist() for vec in m.encode(texts, normalize_embeddings=True)]

def cosine(a: List[float], b: List[float]) -> float:
    s = sum(x*y for x, y in zip(a, b))
    # embeddings are normalized when MiniLM is used → s in [-1,1]
    return float(s)

def lexical_score(q: str, text: str) -> float:
    # ultra-light fallback: token overlap score in [0,1]
    if not q or not text:
        return 0.0
    qs = {t for t in q.lower().split() if len(t) > 2}
    if not qs:
        return 0.0
    ts = set(text.lower().split())
    inter = len(qs & ts)
    return inter / max(1, len(qs))
