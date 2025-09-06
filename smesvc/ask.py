## Step 5 — Create orchestrator (minimal)
#**File:** `smesvc/ask.py`
#> Minimal version that leans on existing `smesvc.bundle.build` for retrieval, then applies rerank + calibration + answer modes. (We avoid inventing DB helpers.)
#```python

from typing import TypedDict, List, Dict
from .rerank import rerank
from .nli import consistency_score
from .calibrate import calibrate
from .answer_modes import assemble, choose_mode
from . import bundle as _bundle

class AnswerPack(TypedDict):
    answer: str
    citations: List[Dict]
    meta: Dict
    trace: List[Dict]

def run(question: str, opts: dict) -> Dict:
    limits = {
        "l0": 8, "l1": 6, "l2": 12, "l3": 40,  # safe defaults; bundle uses these if present
        "chunk_text_max": int(opts.get("chunk_text_max", 800)),
    }
    # STEP 1: bundle (SME lens)
    b = _bundle.build(question, limits)
    data = b or {}
    l3 = (data.get("l3") or [])  # candidate chunks
    trace = [{"step": 1, "call": {"table": "bundle", "q": question},
              "result_summary": {"l1": len(data.get("l1") or []), "l2": len(data.get("l2") or []), "l3": len(l3)}}]

    # STEP 2: rerank + select
    ranked = rerank(question, l3, top_k=min(10, len(l3) or 10))
    cons, contradictions = consistency_score(ranked[:6])
    feats = {
        "coverage": 0.6,                 # placeholder until precision‑term coverage is added
        "consistency": cons,
        "diversity": 0.7,
        "lexical_fallback": bool(data.get("meta", {}).get("lexical_fallback")),
    }
    cal = calibrate(feats)

    # STEP 3: assemble answer
    mode = choose_mode(question)
    answer = assemble(ranked, mode=mode)

    out: AnswerPack = {
        "answer": answer,
        "citations": [{k: v for k, v in ch.items() if k in ("id","doc_id","offset")} for ch in ranked][: opts.get("citations_max", 6)],
        "meta": {
            "lexical_fallback": feats["lexical_fallback"],
            "confidence": cal["confidence"],
            "answer_mode": mode,
            "contradictions": contradictions[:3],
        },
        "trace": trace,
    }
    return {"data": out}
