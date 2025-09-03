# schema/paths_schema_get.py — Static endpoint spec for returning the base schema


def get():
    """Expose a stable read-only endpoint to fetch the committed base schema.
    OperationId chosen to be explicit for GPT Actions routing.
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
                                # We purposely keep this broad: any JSON object
                                "schema": {"type": "object", "additionalProperties": True}
                            }
                        },
                    }
                },
            }
        }
    }
