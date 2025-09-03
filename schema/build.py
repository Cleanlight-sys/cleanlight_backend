# schema/build.py
from .base import base
from .paths_query import get as query_paths
from .paths_hint import get as hint_paths
from .paths_schema_get import get as schema_get_paths

def build_spec():
    spec = base.copy()
    spec["paths"] = {}
    spec["paths"].update(query_paths())
    spec["paths"].update(hint_paths())
    spec["paths"].update(schema_get_paths())
    return spec
