# =====================================
# schema/build.py — assemble OpenAPI spec
# =====================================
from .base import deep_base
from .paths_query import get as query_paths
from .paths_hint import get as hint_paths
from .paths_schema_get import get as schema_get_paths

def build_spec(include_hint: bool = True):
    """
    Assemble the OpenAPI spec.

    include_hint:
      - True  => include /hint path
      - False => omit /hint (useful if you want a lean spec for some clients)
    """
    spec = deep_base()
    spec.setdefault("paths", {})
    # Deterministic order
    spec["paths"].update(query_paths())
    if include_hint:
        spec["paths"].update(hint_paths())
    spec["paths"].update(schema_get_paths())
    return spec

# Default export used by modules that import `spec` directly
spec = build_spec(include_hint=True)
