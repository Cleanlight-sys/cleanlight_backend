from flask import Flask, request, jsonify, Response, stream_with_context
from datetime import datetime, timezone
import os, requests, json

import handlers.read_all as read_all
import handlers.read_rows as read_row
import handlers.write as write
import handlers.update as update
import handlers.delete as delete
import handlers.query as query
import handlers.hint as hint

from config import SUPABASE_URL, HEADERS, TABLE_KEYS


app = Flask(__name__)

# --- Helpers ---
def _now(): return datetime.now(timezone.utc).isoformat()
    
def wrap(data=None, body=None, hint=None, error=None):
    echo = {"original_body": body}
    out = {"data": data, "echo": echo}
    if hint is not None: out["hint"] = hint
    if error is not None: out["error"] = error
    return out

# --- Serve schema for agents ---
@app.get("/openapi.json")
def openapi():
    spec = {
        "openapi": "3.1.0",
        "info": {
            "title": "Cleanlight Agent API",
            "version": "1.3",
            "description": "Single-source schema. All operations through `/query`. `/hint` available for examples."
        },
        "servers": [
            { "url": "https://cleanlight-backend.onrender.com" }
        ],
        "paths": {
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
                    "responses": { "200": { "description": "Wrapped response" } }
                }
            },
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
                                            "enum": ["read_all", "read_row", "write", "update", "delete", "query", "all"]
                                        }
                                    },
                                    "required": ["target"]
                                }
                            }
                        }
                    },
                    "responses": { "200": { "description": "Example payloads" } }
                }
            }
        }
    }
    return jsonify(spec)


@app.get("/health")
def health():
    return jsonify({"status": "ok", "time": _now()})

# --- Query dispatch ---
@app.post("/query")
def query_gate():
    body = request.json or {}
    action = body.get("action")
    table  = body.get("table")

    if action == "read_all":
        data, hint, error = read_all.handle(table, body)
        return jsonify(wrap(data, body, hint, error))
    if action == "read_row":
        data, hint, error = read_row.handle(table, body)
        return jsonify(wrap(data, body, hint, error))
    if action == "write":
        data, hint, error = write.handle(table, body)
        return jsonify(wrap(data, body, hint, error))
    if action == "update":
        data, hint, error = update.handle(table, body)
        return jsonify(wrap(data, body, hint, error))
    if action == "delete":
        data, hint, error = delete.handle(table, body)
        return jsonify(wrap(data, body, hint, error))
    if action == "query":
        result = query.handle(table, body)
        if len(result) == 4 and result[3] is True:   # streaming
            generator, hint, error, _ = result
            return Response(stream_with_context(generator), mimetype="application/json")
        else:
            data, hint, error = result
            return jsonify(wrap(data, body, hint, error))

    return jsonify(wrap(None, body, "Unknown action", {"code": "BAD_ACTION"})), 400

@app.post("/hint")
def hint_gate():
    body = request.json or {}
    data, hint_txt, error = hint.handle(body)
    return jsonify(wrap(data, body, hint_txt, error))
    
if __name__ == "__main__":
    app.run(debug=True, port=8000)








