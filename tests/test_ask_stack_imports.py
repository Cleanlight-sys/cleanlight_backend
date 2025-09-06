#**File:** `tests/test_ask_stack_imports.py`
#```python
import importlib

def test_symbols_exist():
    assert hasattr(importlib.import_module('smesvc.ask'), 'run')
    assert hasattr(importlib.import_module('smesvc.rerank'), 'rerank')
    assert hasattr(importlib.import_module('smesvc.answer_modes'), 'assemble')
    assert hasattr(importlib.import_module('smesvc.nli'), 'consistency_score')
    assert hasattr(importlib.import_module('smesvc.calibrate'), 'calibrate')
