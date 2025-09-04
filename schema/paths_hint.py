# ================================================
# schema/paths_hint.py — dynamic /hint OpenAPI path
# Generates the /hint schema and examples from smesvc.hints
# ================================================
from typing import Dict, Any

def _load_examples() -> Dict[str, Any]:
    """
    Import at runtime so builds don't fail if smesvc changes independently.
    Returns {"k1": {...}, "k2": {...}} where keys are example names (e.g. "read_all")
    and values are example payloads (e.g. the body you'd POST to /query).
    """
    try:
        from smesvc import hints
        data = hints.get_examples(None)  # {"hint": {...}} or {"hint": None}
        hint = (data or {}).get("hint") or {}
        if not isinstance(hint, dict):
            return {}
        return hint
    except Exception:
        # Fail closed: provide empty examples if SME layer is unavailable.
        return {}

def _request_body_schema(example_keys):
    """
    Keep it permissive but helpful: "target" drives which SME example to return.
    Enumerate known example keys so agents can pick from a valid set.
    """
    # NOTE: we intentionally keep additionalProperties=true,
    # because you may extend the body in the future (e.g., verbosity flags).
    schema = {
        "type": "object",
        "additionalProperties": True,
        "properties": {
            "target": {
                "type": "string",
                "description": "Optional: return only the named example (one of the known targets).",
            }
        }
    }
    if example_keys:
        schema["properties"]["target"]["enum"] = sorted(example_keys)
    return schema

def _request_body_examples(example_keys):
    """
    Provide one request example per target so agents learn the pattern.
    """
    if not example_keys:
        return {
            "all": {
                "summary": "All examples",
                "value": {}
            }
        }
    out = {
        "all": {
            "summary": "Return all examples",
            "value": {}
        }
    }
    for k in sorted(example_keys):
        out[k] = {
            "summary": f"Return only '{k}'",
            "value": {"target": k}
        }
    return out

def _response_examples(hint_map):
    """
    Show the *envelope-shaped* response containing the selected examples.
    Each example is wrapped in { hint: <example> } to reflect the real response.
    """
    # Full set
    base_all = {
        "data": None,
        "echo": {},
        "hint": hint_map,         # entire dict of examples
        "error": None
    }
    out = {
        "all": {
            "summary": "Envelope with all examples",
            "value": base_all
        }
    }
    # Per-target
    for k, v in hint_map.items():
        out[k] = {
            "summary": f"Envelope with only '{k}' example",
            "value": {
                "data": None,
                "echo": {"original_body": {"target": k}},
                "hint": v,
                "error": None
            }
        }
    return out

def get() -> dict:
    """
    Dynamic OpenAPI path for /hint.
    Pulls targets and examples from smesvc.hints to avoid drift.
    """
    hint_map = _load_examples()                      # {"read_all": {...}, ...}
    example_keys = list(hint_map.keys())

    return {
        "/hint": {
            "post": {
                "operationId": "hint",
                "summary": "Return example payloads for actions (dynamic, SME-driven).",
                "description": (
                    "Returns curated example payloads the agent can use when calling /query. "
                    "If 'target' is provided, only that example is returned. "
                    "Targets are discovered at runtime from the SME layer."
                ),
                "requestBody": {
                    "required": False,
                    "content": {
                        "application/json": {
                            "schema": _request_body_schema(example_keys),
                            "examples": _request_body_examples(example_keys)
                        }
                    }
                },
                "responses": {
                    "200": {
                        "description": "OK",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "$ref": "#/components/schemas/Envelope"
                                },
                                "examples": _response_examples(hint_map)
                            }
                        }
                    }
                }
            }
        }
    }
