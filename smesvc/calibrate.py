## Step 2 — Add confidence calibrator (standalone)
#**File:** `smesvc/calibrate.py`
#```python
from math import exp

def _sigmoid(x: float) -> float:
    return 1.0 / (1.0 + exp(-x))

def calibrate(conf_feats: dict) -> dict:
    """Combine simple features into a calibrated confidence ∈ [0,1]."""
    cov = float(conf_feats.get("coverage", 0.6))
    con = float(conf_feats.get("consistency", 0.9))
    div = float(conf_feats.get("diversity", 0.7))
    fall = bool(conf_feats.get("lexical_fallback", False))
    z = (1.8*cov + 1.6*con + 1.2*div) - (0.8 if fall else 0.0) - 1.2
    return {"confidence": round(_sigmoid(z), 3), **conf_feats}
