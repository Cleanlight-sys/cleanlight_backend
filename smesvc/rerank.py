## Step 1 — Add reranker head (standalone)
#**File:** `smesvc/rerank.py`
#```python
from typing import List, Dict

def rerank(question: str, candidates: List[Dict], top_k: int = 10) -> List[Dict]:
    """
    Deterministic fallback reranker (replace internals later with a cross‑encoder).
    Scores by token coverage + brevity; enforces light diversity by length band.
    """
    q_tokens = {t for t in (question or "").lower().split() if t.isalpha()}
    def score(ch):
        txt = (ch.get("text") or "").lower()
        toks = set(t for t in txt.split() if t.isalpha())
        cov = len(q_tokens & toks) / (len(q_tokens) or 1)
        brev = 1.0 / max(len(txt), 80) * 80
        return 0.65*cov + 0.35*brev
    ranked = sorted(candidates or [], key=score, reverse=True)
    seen_bands, out = set(), []
    for ch in ranked:
        band = len((ch.get("text") or "")) // 200
        if band in seen_bands:  # avoid near‑dupes
            continue
        seen_bands.add(band)
        out.append(ch)
        if len(out) >= max(1, top_k):
            break
    return out
