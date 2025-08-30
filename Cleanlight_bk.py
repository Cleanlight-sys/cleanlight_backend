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
from schema import build_spec

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
@app.get("/openai.json")   # alias for OpenAI
def openapi():
    return jsonify(build_spec())


@app.get("/health")
def health():
    return jsonify({"status": "ok", "time": _now()})

# --- Query dispatch ---
@app.post("/query")
def query_gate():
    body = request.json or {}
    action = body.get("action")
    table  = body.get("table")

    dispatch = {
        "read_all": read_all.handle,
        "read_row": read_row.handle,
        "write": write.handle,
        "update": update.handle,
        "delete": delete.handle,
    }

    if action in dispatch:
        data, hint_txt, error = dispatch[action](table, body)
        return jsonify(wrap(data, body, hint_txt, error))

    if action == "query":
        result = query.handle(table, body)
        if len(result) == 4 and result[3] is True:   # streaming
            generator, hint_txt, error, _ = result
            return Response(stream_with_context(generator), mimetype="application/json")
        else:
            data, hint_txt, error = result
            return jsonify(wrap(data, body, hint_txt, error))

    # --- Auto-hint fallback ---
    data, hint_txt, error = hint.handle({"target": "all"})
    return jsonify(wrap(data, body, hint_txt, error)), 400

@app.post("/hint")
def hint_gate():
    body = request.json or {}
    data, hint_txt, error = hint.handle(body)
    return jsonify(wrap(data, body, hint_txt, error))
    
if __name__ == "__main__":
    app.run(debug=True, port=8000)












