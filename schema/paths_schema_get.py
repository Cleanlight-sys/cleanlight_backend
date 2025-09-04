# ==================================================
# schema/paths_schema_get.py  — GET /schema (fixed)
# ==================================================

def get() -> dict:
    """Return a validator-clean path item for /schema.

    - Uses **responses** (plural).
    - References components.schemas.SchemaResponse (object with properties).
    """
    return {
        "/schema": {
            "get": {
                "operationId": "Schema_Get",
                "summary": "Return the static base API schema (committed version)",
                "responses": {
                    "200": {
                        "description": "OK",
                        "content": {
                            "application/json": {
                                "schema": {"$ref": "#/components/schemas/SchemaResponse"}
                            }
                        },
                    },
                    "4XX": {"description": "Client error"},
                    "5XX": {"description": "Server error"},
                },
            }
        }
    }
