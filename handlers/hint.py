import json
from flask import jsonify
from config import wrap

def handle(body):
    target = body.get("target")
    echo = {"original_body": body}

    examples = {
        # ---- CRUD actions ----
        "read_all": {
            "action": "read_all",
            "table": "graph",
            "select": "id,doc_id,label,ntype,page",
            "filters": {"label": "ilike.*felt*"}
        },
        "read_row": {
            "action": "read_row",
            "table": "docs",
            "rid": "ba2f103015526adc"
        },
        "write": {
            "action": "write",
            "table": "docs",
            "payload": {
                "title": "Example Doc",
                "meta": {"tags": ["bootstrap"]}
            }
        },
        "update": {
            "action": "update",
            "table": "docs",
            "rid": "123",
            "payload": {"title": "Updated Title"}
        },
        "delete": {
            "action": "delete",
            "table": "docs",
            "rid": "123"
        },

        # ---- SME Query engine examples ----
        "query_graph_q": {
            "action": "query",
            "table": "graph",
            "q": "beaver felt",
            "filters": {"ntype": "eq.concept"},
            "limit": 50,
            "stream": True
        },
        "query_graph_filters": {
            "action": "query",
            "table": "graph",
            "filters": {"label": "ilike.*rabbit*"},
            "limit": 20
        },
        "query_docs_q": {
            "action": "query",
            "table": "docs",
            "q": "felt",
            "limit": 10
        },
        "query_edges": {
            "action": "query",
            "table": "edges",
            "filters": {"etype": "ilike.*contrasts_with*"},
            "stream": True
        },
        "query_traverse": {
            "action": "query",
            "table": "graph",
            "q": "beaver",
            "depth": 1,
            "stream": True
        }
    }

    if target == "all":
        return {"examples": examples}, None, None
    elif target in examples:
        return {"examples": {target: examples[target]}}, None, None
    else:
        return None, (
            "Valid targets: "
            + ", ".join(sorted(examples.keys()))
            + " or 'all'"
        ), {"code": "INVALID_HINT_TARGET"}
