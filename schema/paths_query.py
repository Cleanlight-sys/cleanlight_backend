# schema/paths_query.py — SME-aware OpenAPI spec (description trimmed)

def get():
    return {
        "/query": {
            "post": {
                "summary": "Unified query endpoint (CRUD + SME)",
                "description": (
                    "Handles CRUD and SME queries on docs, graph, chunks, edges. "
                    "For CRUD, include required fields (rid, payload). "
                    "For SME, use filters, q, or rid to fetch bundles with docs, graph, edges, chunks."
                ),
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
                                        "enum": [
                                            "read_all",
                                            "read_row",
                                            "write",
                                            "update",
                                            "delete",
                                            "query"
                                        ],
                                        "description": "The operation to perform"
                                    },
                                    "table": {
                                        "type": "string",
                                        "enum": ["docs", "graph", "edges", "chunks"],
                                        "description": "Target table to operate on"
                                    },
                                    "rid": {
                                        "type": "string",
                                        "description": "Record ID (optional for SME, required for some CRUD)"
                                    },
                                    "filters": {
                                        "type": "object",
                                        "additionalProperties": {"type": "string"},
                                        "description": "PostgREST filter dict, e.g. {\"label\":\"ilike.*felt*\"}"
                                    },
                                    "payload": {
                                        "type": "object",
                                        "description": "Data payload for write/update"
                                    },
                                    "select": {
                                        "type": "string",
                                        "description": "Column selection for read_all/read_row"
                                    },
                                    "q": {
                                        "type": "string",
                                        "description": "Lightweight text search for SME (label/title)"
                                    },
                                    "depth": {
                                        "type": "integer",
                                        "description": "Edge hops to traverse (SME only)",
                                        "default": 0
                                    },
                                    "limit": {
                                        "type": "integer",
                                        "description": "Max records to return",
                                        "default": 100
                                    },
                                    "stream": {
                                        "type": "boolean",
                                        "description": "If true, stream results as JSON array",
                                        "default": False
                                    }
                                },
                                "required": ["action", "table"]
                            }
                        }
                    }
                },
                "responses": {
                    "200": {
                        "description": (
                            "Standard wrapped response. "
                            "For SME queries, returns bundles with docs, graph, chunks, edges, and validation flags."
                        )
                    },
                    "400": {
                        "description": "Invalid input; may include example hints"
                    }
                }
            }
        }
    }
