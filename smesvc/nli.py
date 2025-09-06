# Step 3 — Add NLI/consistency stub (standalone)
# **File:** `smesvc/nli.py`
# ```python

from typing import List, Dict, Tuple

def consistency_score(chunks: List[Dict]) -> Tuple[float, list]:
    '''
    Stub that always returns high consistency; swap with real NLI later.
    Returns: (consistency∈[0,1], contradictions:list)
    '''
    return 0.98, []
