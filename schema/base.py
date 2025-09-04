# ==============================================
# schema/base.py  — dynamic OpenAPI base (fixed)
# ==============================================
from __future__ import annotations
import os
from copy import deepcopy

base = {
    "openapi": "3.1.0",
    "info": {
        "title": "Cleanlight Agent API",
        "version": "1.8",
        "description": (
            "Single-source schema. All reads via POST /query (action=\"query\"). "
            "Optional /hint for examples. /schema returns the committed base schema."
        ),
    },
    # The server URL is injected from env to make Actions import easier
    "servers": [
        {"url": os.getenv("RENDER_EXTERNAL_URL", "https://cleanlight-backend.onrender.com")}
    ],
    # Components include concrete object schemas so validators are happy
    "components": {
        "schemas": {
            # Uniform response envelope used by routes
            "Envelope": {
                "type": "object",
                "additionalProperties": True,
                "properties": {
                    "data": {},
                    "echo": {"type": "object", "additionalProperties": True},
                    "hint": {"type": "object", "additionalProperties": True},
                    "error": {"type": "string"},
                },
            },
            # Strict object for GET /schema → fixes: "object schema missing properties"
            "SchemaResponse": {
                "type": "object",
                "additionalProperties": True,
                "required": ["openapi", "info", "paths"],
                "properties": {
                    "openapi": {"type": "string"},
                    "info": {
                        "type": "object",
                        "additionalProperties": True,
                        "properties": {
                            "title": {"type": "string"},
                            "version": {"type": "string"},
                            "description": {"type": "string"},
                        },
                    },
                    "servers": {
                        "type": "array",
                        "items": {"type": "object", "additionalProperties": True},
                    },
                    "paths": {"type": "object", "additionalProperties": True},
                    "components": {"type": "object", "additionalProperties": True},
                },
            },
            # Optional reusable request schema for POST /query
            "QueryRequest": {
                "type": "object",
                "additionalProperties": False,
                "required": ["action", "table"],
                "properties": {
                    "action": {"type": "string", "enum": ["query"]},
                    "table": {
                        "type": "string",
                        "enum": ["docs", "chunks", "graph", "edges", "images", "kcs", "bundle"],
                    },
                    "q": {"type": "string", "nullable": True},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 500, "default": 50},
                    "filters": {"type": "object", "additionalProperties": True, "nullable": True},
                    "filters_str": {"type": "string", "nullable": True},
                    "chunk_text_max": {"type": "integer", "minimum": 64, "maximum": 5000, "default": 600},
                },
            },
        }
    },
}


def deep_base() -> dict:
    """Return a deep copy so callers can mutate safely."""
    return deepcopy(base)
