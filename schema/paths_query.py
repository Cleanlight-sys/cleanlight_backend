# schema/paths_query.py — SME-aware OpenAPI spec

def get():
    return {
        "/query": {
            "post": {
                "summary": "Unified query endpoint (CRUD + SME)",
                "description": (
                    "Handles read, write, update, delete, and SME queries against docs, graph, chunks, edges.\n"
                    "For CRUD actions, payloads must include required fields (rid, payload, etc.).\n"
                    "For SME queries (action=query), you can discover bundles via filters, q (text search), or rid.\n"
                    "SME bundles include docs, graph nodes, edges, and chunks, plus validation diagnostics."
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
                                        "description": "Record ID (required for some CRUD ops; optional for SME queries)"
                                    },
                                    "filters": {
                                        "type": "object",
                                        "additionalProperties": {"type": "string"},
                                        "description": "Filter dict (PostgREST operators allowed, e.g., {'label': 'ilike.*felt*'})"
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
                                        "description": "Lightweight text query for SME discovery (e.g., matches label/title)"
                                    },
                                    "depth": {
                                        "type": "integer",
                                        "description": "Number of edge hops to traverse outward (SME query only)",
                                        "default": 0
                                    },
                                    "limit": {
                                        "type": "integer",
                                        "description": "Maximum number of records to return",
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
                        "description": "Wrapped response. For SME queries, returns one or more bundles:\n"
                                       "{ 'node':..., 'doc':..., 'chunks':[...], 'edges':[...], '__sme_ok__':bool, '__sme_issues__':[...] }"
                    },
                    "400": {
                        "description": "Invalid input, with hint examples"
                    }
                }
            }
        }
    }
