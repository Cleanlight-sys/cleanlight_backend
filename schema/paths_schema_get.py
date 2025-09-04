def get() -> dict:
    return {
        "/schema": {
            "get": {
                "operationId": "schema_get",
                "summary": "Return the assembled OpenAPI schema.",
                "responses": {
                    "200": {
                        "description": "OK",
                        "content": {
                            "application/json": {
                                "schema": { "$ref": "#/components/schemas/SchemaResponse" }
                            }
                        }
                    }
                }
            }
        }
    }
