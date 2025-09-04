from collections import OrderedDict
from .base import deep_base
from .paths_query import get as query_paths
from .paths_hint import get as hint_paths
from .paths_schema_get import get as schema_get_paths

def _sorted_map(d: dict) -> dict:
    return OrderedDict(sorted(d.items(), key=lambda kv: kv[0]))

def build_spec(include_hint: bool = True):
    spec = deep_base()
    paths = {}
    paths.update(query_paths())
    if include_hint:
        paths.update(hint_paths())
    paths.update(schema_get_paths())

    # sort paths and component schemas for stable diffs
    spec["paths"] = _sorted_map(paths)

    comps = spec.setdefault("components", {})
    schemas = comps.get("schemas") or {}
    comps["schemas"] = _sorted_map(schemas)

    return spec

spec = build_spec(include_hint=True)
