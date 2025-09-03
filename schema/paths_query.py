# schema/paths_query.py — Low-level OpenAPI spec (early-limit + filters)
def get() -> dict:
    """Return the minimal /query path spec matching the provided OpenAPI YAML.
    Why: Keep this endpoint narrowly scoped to a low-level query with early-limit and filters.
    """
    return {
        "/query": {
            "post": {
                "operationId": "query",
                "summary": "Low-level query endpoint (early-limit + filters)",
                "requestBody": {
                    "required": True,
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "required": ["action", "table"],
                                "properties": {
                                    "action": {"type": "string", "enum": ["query"]},
                                    "table": {
                                        "type": "string",
                                        "enum": [
                                            "docs",
                                            "chunks",
                                            "graph",
                                            "edges",
                                            "images",
                                            "kcs",
                                            "bundle",
                                        ],
                                    },
                                    "q": {"type": "string", "nullable": True},
                                    "limit": {
                                        "type": "integer",
                                        "minimum": 1,
                                        "maximum": 500,
                                        "default": 50,
                                    },
                                    "filters": {
                                        "type": "object",
                                        "additionalProperties": True,
                                        "nullable": True,
                                    },
                                    "filters_str": {"type": "string", "nullable": True},
                                    "chunk_text_max": {
                                        "type": "integer",
                                        "minimum": 64,
                                        "maximum": 5000,
                                        "default": 600,
                                    },
                                },
                            }
                        }
                    },
                },
                "responses": {"200": {"description": "OK"}},
            }
        }
    }
