import importlib

def test_symbols_exist():
    m = importlib.import_module("smesvc.ask")
    for name in ("run", "rerank", "assemble", "consistency_score", "calibrate"):
        assert hasattr(m, name), f"missing {name}"
