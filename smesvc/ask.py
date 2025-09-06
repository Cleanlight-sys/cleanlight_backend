# smesvc/ask.py
from __future__ import annotations
from typing import TypedDict, List, Dict, Tuple, Any
from . import bundle as _bundle


# -------- helpers (inlined to keep stack small) --------

def _rerank(question: str, candidates: List[Dict], top_k: int = 10) -> List[Dict]:
    q_tokens = {t for t in (question or "").lower().split() if t.isalpha()}
    def score(ch: Dict) -> float:
        txt = (ch.get("text") or "").lower()
        toks = set(t for t in txt.split() if t.isalpha())
        cov = len(q_tokens & toks) / (len(q_tokens) or 1)
        brev = 1.0 / max(len(txt), 80) * 80
        return 0.65 * cov + 0.35 * brev
    ranked = sorted(candidates or [], key=score, reverse=True)
    seen_bands: set[int] = set(); out: List[Dict] = []
    for ch in ranked:
        band = len((ch.get("text") or "")) // 200
        if band in seen_bands: continue
        seen_bands.add(band); out.append(ch)
        if len(out) >= max(1, top_k): break
    return out

def _consistency_score(chunks: List[Dict]) -> Tuple[float, List[str]]:
    return 0.98, []  # stub; swap with real NLI later

def _calibrate(coverage: float, consistency: float, diversity: float, lexical_fallback: bool) -> float:
    import math
    z = (1.8*coverage + 1.6*consistency + 1.2*diversity) - (0.8 if lexical_fallback else 0.0) - 1.2
    return round(1.0 / (1.0 + math.exp(-z)), 3)

def _choose_mode(question: str) -> str:
    q = (question or "").lower()
    if q.startswith(("how ", "how do", "how to")) or "steps" in q: return "procedure"
    if " compare " in q or " vs " in q or "difference" in q: return "comparison"
    if q.startswith(("what is", "define", "definition")): return "definition"
    return "sme"

def _assemble(chunks: List[Dict], mode: str) -> str:
    texts = [c.get("text","").strip() for c in (chunks or []) if c.get("text")]
    if not texts:
        return "I don’t have enough evidence to answer confidently."
    if mode == "procedure":
        steps: List[str] = []
        for t in texts[:5]:
            for s in t.split("."):
                s = s.strip()
                if s and any(k in s.lower() for k in ("sew","stitch","turn","gather","press","join","attach","bind")):
                    steps.append(s)
        steps = steps[:7] or [texts[0]]
        return "Steps:\n- " + "\n- ".join(steps)
    if mode == "comparison":
        a = texts[0][:240]; b = texts[1][:240] if len(texts) > 1 else texts[0][:240]
        return f"Summary (A vs B):\n- Evidence A: {a}\n- Evidence B: {b}"
    if mode == "definition":
        return texts[0][:600]
    return " ".join(texts)[:1200]

# -------- orchestrator --------

class AnswerPack(TypedDict):
    answer: str
    citations: List[Dict[str, Any]]
    meta: Dict[str, Any]
    trace: List[Dict[str, Any]]

def run(question: str, opts: dict) -> Dict[str, Any]:
    limits = {"l0": 8, "l1": 6, "l2": 12, "l3": 40, "chunk_text_max": int(opts.get("chunk_text_max", 800))}
    b = _bundle.build(question, limits)
    data = b or {}
    l3 = (data.get("l3") or [])
    trace = [{
        "step": 1, "call": {"table": "bundle", "q": question},
        "result_summary": {"l1": len(data.get("l1") or []), "l2": len(data.get("l2") or []), "l3": len(l3)}
    }]

    ranked = _rerank(question, l3, top_k=min(10, len(l3) or 10))
    cons, contradictions = _consistency_score(ranked[:6])
    docs = [c.get("doc_id") for c in ranked]
    diversity = len({d for d in docs if d is not None}) / max(1, len(docs))
    notes = (data.get("meta") or {}).get("notes", [])
    lexical_fallback = bool(notes and "lexical_fallback" in notes)
    coverage = 0.6
    confidence = _calibrate(coverage, cons, diversity, lexical_fallback)

    mode = _choose_mode(question)
    answer = _assemble(ranked, mode=mode)

    out: AnswerPack = {
        "answer": answer,
        "citations": [{k: v for k, v in ch.items() if k in ("id","doc_id","offset")} for ch in ranked][: int(opts.get("citations_max", 6))],
        "meta": {
            "lexical_fallback": lexical_fallback,
            "confidence": confidence,
            "answer_mode": mode,
            "contradictions": contradictions[:3],
        },
        "trace": trace if bool(opts.get("return_trace", True)) else [],
    }
    return {"data": out}

# public aliases for tests/compat (module-level)
rerank = _rerank
assemble = _assemble
consistency_score = _consistency_score
calibrate = _calibrate
__all__ = ["run", "rerank", "assemble", "consistency_score", "calibrate"]

