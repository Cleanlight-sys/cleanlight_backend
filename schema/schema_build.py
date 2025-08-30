# schema/build.py
from .base import base
from .paths_query import query
from .paths_hint import hint

def build_spec():
    spec = base.copy()
    spec["paths"] = {}
    spec["paths"].update(query)
    spec["paths"].update(hint)
    return spec
