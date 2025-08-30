# schema/paths_query.py
query = {
    "/query": {
        "post": {
            "operationId": "query",
            "summary": "Unified CRUD + SME gate",
            "x-openai-isConsequential": False,
            "requestBody": {
                "required": True,
                "content": {
                    "application/json": {
                        "schema": {
                            "type": "object",
                            "properties": {
                                "action": {
                                    "type": "string",
                                    "enum": ["read_all", "read_row", "write", "update", "delete", "query"]
                                },
                                "table": {
                                    "type": "string",
                                    "enum": ["docs", "chunks", "graph", "edges"]
                                },
                                "rid": { "type": "string" },
                                "select": { "type": "string" },
                                "filters": {
                                    "type": "object",
                                    "description": "Example: { \"label\": \"ilike.*felt*\" }"
                                },
                                "payload": { "type": "object" },
                                "stream": { "type": "boolean", "default": False },
                                "limit": { "type": "integer", "default": 100 }
                            },
                            "required": ["action", "table"]
                        }
                    }
                }
            },
            "responses": {
                "200": { "description": "Wrapped response" }
            }
        }
    }
}
