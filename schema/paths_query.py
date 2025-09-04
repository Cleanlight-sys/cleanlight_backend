def get() -> dict:
    return {
        "/query": {
            "post": {
                "operationId": "query",
                "summary": "Low-level query endpoint (early-limit + filters).",
                "requestBody": {
                    "required": True,
                    "content": {
                        "application/json": {
                            "schema": { "$ref": "#/components/schemas/QueryRequest" }
                        }
                    }
                },
                "responses": {
                    "200": {
                        "description": "OK",
                        "content": {
                            "application/json": {
                                "schema": { "$ref": "#/components/schemas/Envelope" }
                            }
                        }
                    }
                }
            }
        }
    }
