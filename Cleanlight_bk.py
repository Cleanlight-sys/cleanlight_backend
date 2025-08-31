from flask import Flask, request, jsonify, Response, stream_with_context, make_response
from flask_cors import CORS
from datetime import datetime, timezone
import os, requests, json

import handlers.read_all as read_all
import handlers.read_rows as read_row
import handlers.write as write
import handlers.update as update
import handlers.delete as delete
import handlers.query as query
import handlers.hint as hint

from config import SUPABASE_URL, HEADERS, TABLE_KEYS, wrap
from schema import build_spec

app = Flask(__name__)
CORS(app)

# --- Helpers ---
def _now(): 
    return datetime.now(timezone.utc).isoformat()

# --- Schema endpoints ---
@app.get("/openapi.json")
@app.get("/openai.json")  # alias
def serve_openapi():
    """
    Serve OpenAPI schema dynamically (rebuilt from schema/ each time).
    """
    spec = build_spec()
    spec["servers"] = [{"url": os.getenv("RENDER_EXTERNAL_URL", "http://localhost:8000")}]
    response = make_response(jsonify(spec))
    response.headers["Content-Type"] = "application/json"
    return response

@app.get("/health")
def health():
    return {"status": "ok", "time": _now()}

# --- Query dispatch ---
@app.post("/query")
def query_gate():
    """
    Unified query gateway: routes CRUD + SME queries to handlers,
    always wrapped through config.wrap() for consistent responses.
    """
    body = request.json or {}
    action = body.get("action")
    table  = body.get("table")
    stream = body.get("stream", False)

    dispatch = {
        "read_all": read_all.handle,
        "read_row": read_row.handle,
        "write": write.handle,
        "update": update.handle,
        "delete": delete.handle,
    }

    data, error, hint_txt = None, None, None

    if action in dispatch:
        data, error, hint_txt = dispatch[action](table, body)
        return wrap(data, body, hint_txt, error, stream=stream)

    if action == "query":
        data, error, hint_txt = query.handle(table, body)
        return wrap(data, body, hint_txt, error, stream=stream)

    if action == "hint":
        data, error, hint_txt = hint.handle(body)
        return wrap(data, body, hint_txt, error, stream=stream)

    # --- Auto-hint fallback ---
    data, error, hint_txt = hint.handle({"target": "all"})
    return wrap(data, body, hint_txt, error), 400

@app.post("/hint")
def hint_gate():
    body = request.json or {}
    data, error, hint_txt = hint.handle(body)
    return wrap(data, body, hint_txt, error)

if __name__ == "__main__":
    app.run(debug=True, port=8000)
