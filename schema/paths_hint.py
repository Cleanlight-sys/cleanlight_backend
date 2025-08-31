# schema/paths_hint.py

def get():
    """
    OpenAPI path definition for /hint endpoint.
    """
    return {
        "/hint": {
            "post": {
                "summary": "Return example payloads",
                "description": "Provides example payloads for each action or for all actions.",
                "operationId": "hint",
                "requestBody": {
                    "required": False,
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "properties": {
                                    "action": {
                                        "type": "string",
                                        "description": "Target action for which to return example payloads",
                                        "example": "query"
                                    }
                                }
                            }
                        }
                    }
                },
                "responses": {
                    "200": {
                        "description": "Standard wrapped response with hints"
                    }
                }
            }
        }
    }
