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
    with open("openapi.json", "r") as f:
        spec = json.load(f)
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







