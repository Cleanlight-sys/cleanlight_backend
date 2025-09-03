# Cleanlight_bk.py — Flask WSGI app entrypoint for Render
# Purpose: small, stable gateway that dispatches CRUD + query to handlers and serves OpenAPI.
# Why: prior deploys failed due to unfinished triple-quoted blocks and mixed FastAPI/Flask code.

from flask import Flask, request, jsonify
from flask_cors import CORS

# Local modules
from config import wrap
from schema import build_spec
from handlers import read_all, read_rows, write, update, delete, query, hint

app = Flask(__name__)
CORS(app)


@app.get("/")
def root():
    # Health endpoint so Render doesn't 404
    return jsonify({"ok": True, "service": "cleanlight-backend"}), 200


@app.get("/openapi.json")
def openapi_spec():
    return jsonify(build_spec())


@app.post("/hint")
def hint_gate():
    body = request.get_json(silent=True) or {}
    data, error, hint_txt = hint.handle(body)
    return wrap(data, body, hint_txt, error)


@app.post("/query")
def query_gate():
    body = request.get_json(silent=True) or {}
    action = (body.get("action") or "").lower()
    table = body.get("table")

    # Dispatch: support CRUD and low-level query
    if action == "read_all":
        data, error, meta = read_all.handle(table, body)
        return wrap(data, body, meta, error)
    if action == "read_row":
        data, error, meta = read_rows.handle(table, body)
        return wrap(data, body, meta, error)
    if action == "write":
        data, error, meta = write.handle(table, body)
        return wrap(data, body, meta, error)
    if action == "update":
        data, error, meta = update.handle(table, body)
        return wrap(data, body, meta, error)
    if action == "delete":
        data, error, meta = delete.handle(table, body)
        return wrap(data, body, meta, error)
    if action == "query":
        data, error, meta = query.handle(body)
        return wrap(data, body, meta, error)

    # Fallback: helpful error
    return wrap(None, body, {"allowed_actions": [
        "read_all", "read_row", "write", "update", "delete", "query"
    ]}, f"Unknown action: {action!r}")


# Allow local dev: `python Cleanlight_bk.py`
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
