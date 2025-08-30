from flask import Flask, request, jsonify, Response, stream_with_context
from datetime import datetime, timezone
import os, requests, json

import handlers.read_all as read_all
import handlers.read_rows as read_row
import handlers.write as write
import handlers.update as update
import handlers.delete as delete
from config import wrap, SUPABASE_URL, HEADERS, TABLE_KEYS


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
def query():
    body = request.json or {}
    action = body.get("action")
    table  = body.get("table")

    if action == "read_all":
        return read_all.handle(table, body)
    if action == "read_row":
        return read_row.handle(table, body)
    if action == "write":
        return write.handle(table, body)
    if action == "update":
        return update.handle(table, body)
    if action == "delete":
        return delete.handle(table, body)

    return jsonify(wrap(None, body, "Unknown action", {"code":"BAD_ACTION"})), 400

if __name__ == "__main__":
    app.run(debug=True, port=8000)



