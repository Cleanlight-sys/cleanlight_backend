# schema/paths_hint.py — dynamic /hint OpenAPI path
from typing import Dict, Any

def _load_examples() -> Dict[str, Any]:
    try:
        from smesvc import hints
        data = hints.get_examples(None)  # expects { "hint": {...} }
        hint = (data or {}).get("hint") or {}
        if not isinstance(hint, dict):
            return {}
        return hint
    except Exception:
        return {}

def _request_body_schema(example_keys):
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
    if not example_keys:
        return { "all": { "summary": "All examples", "value": {} } }
    out = { "all": { "summary": "Return all examples", "value": {} } }
    for k in sorted(example_keys):
        out[k] = { "summary": f"Return only '{k}'", "value": {"target": k} }
    return out

def _response_examples(hint_map):
    base_all = { "data": None, "echo": {}, "hint": hint_map, "error": None }
    out = { "all": { "summary": "Envelope with all examples", "value": base_all } }
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
    hint_map = _load_examples()
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
                                "schema": { "$ref": "#/components/schemas/Envelope" },
                                "examples": _response_examples(hint_map)
                            }
                        }
                    }
                }
            }
        }
    }
