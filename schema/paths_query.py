# schema/paths_query.py

def get():
    return {
        "/query": {
            "post": {
                "summary": "Unified query endpoint",
                "description": "Handles read, write, update, delete, and SME queries against docs, graph, chunks, edges.",
                "operationId": "query",
                "requestBody": {
                    "required": True,
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "properties": {
                                    "action": {
                                        "type": "string",
                                        "enum": ["read_all", "read_row", "write", "update", "delete", "query"],
                                        "description": "The CRUD or query action to perform"
                                    },
                                    "table": {
                                        "type": "string",
                                        "enum": ["docs", "chunks", "graph", "edges"],
                                        "description": "The table to operate on"
                                    },
                                    "limit": {
                                        "type": "integer",
                                        "description": "Maximum number of records to return"
                                    },
                                    "stream": {
                                        "type": "boolean",
                                        "description": "Whether to stream results"
                                    },
                                    # Allow *any* other kwargs like filters, rid, etc.
                                },
                                "additionalProperties": True
                            }
                        }
                    }
                },
                "responses": {
                    "200": {
                        "description": "Standard wrapped response"
                    }
                }
            }
        }
    }
