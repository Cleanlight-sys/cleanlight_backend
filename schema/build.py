# =====================================
# schema/build.py — assemble OpenAPI spec
# =====================================
from .base import base
from .paths_query import get as query_paths
from .paths_hint import get as hint_paths
from .paths_schema_get import get as schema_get_paths

def build_spec():
    spec = base.copy()
    spec.setdefault("paths", {})
    # Merge in a deterministic order
    spec["paths"].update(query_paths())
    spec["paths"].update(hint_paths())         # <- dynamic: pulls from smesvc.hints
    spec["paths"].update(schema_get_paths())
    return spec

# Export a 'spec' name for convenience if other code imports it
spec = build_spec()
