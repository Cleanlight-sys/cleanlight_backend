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

# schema/paths_hint.py — /hint documents the SME “self-aware” envelope
def get() -> dict:
    return {
        "/hint": {
            "post": {
                "operationId": "hint",
                "summary": "Return SME-aware hints (capabilities, coverage, limits, recommended calls).",
                "description": (
                    "Builds a dynamic hint envelope from the backend (smesvc.hints.build_hints). "
                    "Provide an optional 'question' (free text) and/or 'doc' (pattern like '%millinery%') "
                    "to contextualize the recommendations."
                ),
                "requestBody": {
                    "required": False,
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "additionalProperties": True,
                                "properties": {
                                    "question": {
                                        "type": "string",
                                        "description": "User question to bias capabilities/recommendations."
                                    },
                                    "doc": {
                                        "type": "string",
                                        "description": "Doc title/author pattern to bias coverage (e.g., '%millinery%')."
                                    }
                                }
                            },
                            "examples": {
                                "empty": {
                                    "summary": "No context",
                                    "value": {}
                                },
                                "with_question": {
                                    "summary": "Bias by question",
                                    "value": {"question": "Blocking a felt brim with shellac"}
                                },
                                "with_doc": {
                                    "summary": "Bias by doc pattern",
                                    "value": {"doc": "%millinery%"}
                                }
                            }
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
                                "examples": {
                                    "shape": {
                                        "summary": "Response shape",
                                        "value": {
                                            "data": None,
                                            "echo": {},
                                            "hint": {
                                                "capabilities": {
                                                    "docs": 0, "chunks": 0, "graph": 0, "edges": 0, "images": 0, "kcs": 0
                                                },
                                                "coverage": {
                                                    "top_docs": [],
                                                    "recent_docs": []
                                                },
                                                "limits": {"default_top_k": 8, "max_rows": 1000},
                                                "recommend": [
                                                    {
                                                        "title": "Browse graph by label",
                                                        "call": {
                                                            "path": "/query",
                                                            "body": {
                                                                "action": "query",
                                                                "table": "graph",
                                                                "select": "id,doc_id,label,ntype,page",
                                                                "filters": {"label": "ilike.%seam%"},
                                                                "limit": 25
                                                            }
                                                        }
                                                    }
                                                ]
                                            },
                                            "error": None
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

