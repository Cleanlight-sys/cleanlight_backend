# ================================================
# schema/paths_hint.py  — POST /hint (optional)
# ================================================

def get() -> dict:
    """OpenAPI path for /hint (optional for Actions; harmless if unused)."""
    return {
        "/hint": {
            "post": {
                "operationId": "hint",
                "summary": "Return example payloads for actions/tables",
                "requestBody": {
                    "required": False,
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "additionalProperties": True,
                                "properties": {
                                    "target": {
                                        "type": "string",
                                        "description": "Action to fetch examples for (or 'all')",
                                        "enum": ["all", "query", "read_all", "read_row", "write", "update", "delete"],
                                    },
                                    "table": {"type": "string", "description": "Optional table name"},
                                    # legacy alias accepted by backend; keep in spec to avoid validator surprises
                                    "action": {"type": "string", "description": "Alias for 'target'"},
                                },
                            }
                        }
                    },
                },
                "responses": {
                    "200": {
                        "description": "OK",
                        "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Envelope"}}},
                    }
                },
            }
        }
    }
