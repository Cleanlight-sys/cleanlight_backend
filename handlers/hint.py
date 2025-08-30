import json
from flask import jsonify
from config import wrap

def handle(body):
    target = body.get("target")
    echo = {"original_body": body}

    examples = {
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
        "query": {
            "action": "query",
            "table": "graph",
            "filters": {"label": "ilike.*beaver*"},
            "stream": True
        }
    }

    if target == "all":
        return jsonify(wrap({"examples": examples}, body))
    elif target in examples:
        return jsonify(wrap({"examples": {target: examples[target]}}, body))
    else:
        return jsonify(wrap(
            None,
            body,
            hint="Valid targets: read_all, read_row, write, update, delete, query, all",
            error={"code": "INVALID_HINT_TARGET"}
        )), 400
