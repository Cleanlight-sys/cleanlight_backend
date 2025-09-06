## Step 4 — Add answer modes (standalone)
#**File:** `smesvc/answer_modes.py`
#```python

from typing import List, Dict

def choose_mode(question: str) -> str:
    q = (question or "").lower()
    if q.startswith(("how ", "how do", "how to")) or "steps" in q:
        return "procedure"
    if any(k in q for k in (" compare ", " vs ", "difference")):
        return "comparison"
    if q.startswith(("what is", "define", "definition")):
        return "definition"
    return "sme"

def assemble(chunks: List[Dict], mode: str = "sme") -> str:
    texts = [c.get("text", "").strip() for c in (chunks or []) if c.get("text")]
    if not texts:
        return "I don’t have enough evidence to answer confidently."
    if mode == "procedure":
        steps = []
        for t in texts[:5]:
            for s in t.split("."):
                s = s.strip()
                if s and any(k in s.lower() for k in ("sew","stitch","turn","gather","press","join")):
                    steps.append(s)
        steps = steps[:7] or [texts[0]]
        return "Steps:\n- " + "\n- ".join(steps)
    if mode == "comparison":
        a = texts[0][:240]
        b = texts[1][:240] if len(texts) > 1 else texts[0][:240]
        return f"Summary (A vs B):\n- Evidence A: {a}\n- Evidence B: {b}"
    if mode == "definition":
        return texts[0][:600]
    return " ".join(texts)[:1200]
