def handle(body):
    action = body.get("action")

    hints = {
        "read_all": {
            "example": {
                "action": "read_all",
                "table": "graph",
                "select": "id,doc_id,label,ntype,page",
                "filters": { "label": "ilike.*felt*" },
                "limit": 100,
                "stream": False
            },
            "notes": "Use filters for WHERE clauses. Set stream=true for continuous results."
        },
        "read_row": {
            "example": {
                "action": "read_row",
                "table": "docs",
                "rid": "ba2f103015526adc",
                "select": "*"
            },
            "notes": "rid is required. Defaults select to *."
        },
        "write": {
            "example": {
                "action": "write",
                "table": "chunks",
                "payload": { "doc_id": "abc123", "content": "New text..." }
            },
            "notes": "Payload must match table schema."
        },
        "update": {
            "example": {
                "action": "update",
                "table": "graph",
                "rid": 52,
                "payload": { "label": "felt hat" }
            },
            "notes": "rid is required. Partial fields accepted."
        },
        "delete": {
            "example": {
                "action": "delete",
                "table": "graph",
                "rid": 52
            },
            "notes": "rid is required. Response confirms deletion."
        },
        "query": {
            "example": {
                "action": "query",
                "table": "docs",
                "rid": "ba2f103015526adc",
                "stream": False
            },
            "notes": "Returns doc, graph_nodes, edges, and chunks. Stream for large bundles."
        },
    }

    if action == "all":
        return hints, None, None

    if action in hints:
        return hints[action], None, None

    return None, "Unknown action", {"code":"BAD_HINT","action":action}
