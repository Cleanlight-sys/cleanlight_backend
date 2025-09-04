# ==================================================
# schema/paths_query.py  — POST /query (unchanged shape)
# ==================================================

def get() -> dict:
    """Minimal /query path (query-only actions)."""
    return {
        "/query": {
            "post": {
                "operationId": "query",
                "summary": "Low-level query endpoint (early-limit + filters)",
                "requestBody": {
                    "required": True,
                    "content": {
                        "application/json": {
                            "schema": {"$ref": "#/components/schemas/QueryRequest"}
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
