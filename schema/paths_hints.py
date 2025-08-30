# schema/paths_hint.py
hint = {
    "/hint": {
        "post": {
            "operationId": "hint",
            "summary": "Get example payloads",
            "requestBody": {
                "required": True,
                "content": {
                    "application/json": {
                        "schema": {
                            "type": "object",
                            "properties": {
                                "target": {
                                    "type": "string",
                                    "enum": [
                                        "read_all",
                                        "read_row",
                                        "write",
                                        "update",
                                        "delete",
                                        "query",
                                        "all"
                                    ]
                                }
                            },
                            "required": ["target"]
                        }
                    }
                }
            },
            "responses": {
                "200": { "description": "Example payloads" }
            }
        }
    }
}
