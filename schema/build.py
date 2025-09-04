# =====================================
# schema/build.py  — assemble the spec
# =====================================
from copy import deepcopy
from .base import deep_base
from .paths_query import get as query_paths
from .paths_hint import get as hint_paths
from .paths_schema_get import get as schema_get_paths


def build_spec(include_hint: bool = True) -> dict:
    spec = deep_base()
    # start with empty paths, then merge fragments
    spec.setdefault("paths", {})
    spec["paths"].update(query_paths())
    if include_hint:
        spec["paths"].update(hint_paths())
    spec["paths"].update(schema_get_paths())
    return spec
