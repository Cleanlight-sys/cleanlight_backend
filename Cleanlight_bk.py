# Cleanlight_bk.py — Flask WSGI app entrypoint for Render
# Purpose: stable gateway; dynamic OpenAPI so bots don't rely on a static file.
# Why: requests to /openai.json or /openapi.json must succeed without rebuilds.

from flask import Flask, request, jsonify
from flask_cors import CORS
import json

# Local modules
from config import wrap
from handlers import read_all, read_rows, write, update, delete, query, hint

app = Flask(__name__)
CORS(app)


def _build_spec_safe() -> dict:
    """Return a best-effort OpenAPI dict.
    Why: avoid tight coupling to a prebuilt openapi.json file.
    """
    # Preferred: project aggregator if present
    try:
        from schema import build_spec as _build  # type: ignore
        spec = _build()
        if isinstance(spec, dict):
            return spec
    except Exception:
        pass

    # Fallback: minimal paths from paths_query
    try:
        from schema.paths_query import get as get_query  # type: ignore
        paths = get_query()
    except Exception:
        paths = {}

    if not isinstance(paths, dict) or not paths:
        paths = {
            "/query": {
                "post": {
                    "operationId": "query",
                    "summary": "Low-level query endpoint (early-limit + filters)",
                    "responses": {"200": {"description": "OK"}},
                }
            }
        }

    return {
        "openapi": "3.0.0",
        "info": {"title": "Cleanlight Backend API", "version": "0.0.0"},
        "paths": paths,
    }


@app.get("/")
def root():
    return jsonify({"ok": True, "service": "cleanlight-backend"}), 200


# Some clients request /openai.json (misspelling) or /.well-known/openapi.json
@app.route("/openapi.json", methods=["GET", "HEAD"])  # canonical
@app.route("/openai.json", methods=["GET", "HEAD"])   # alias for bots
@app.route("/.well-known/openapi.json", methods=["GET", "HEAD"])  # discovery
def openapi_spec():
    spec = _build_spec_safe()
    return app.response_class(
        response=json.dumps(spec, indent=2, ensure_ascii=False),
        status=200,
        mimetype="application/json",
    )


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

    return wrap(None, body, {"allowed_actions": [
        "read_all", "read_row", "write", "update", "delete", "query"
    ]}, f"Unknown action: {action!r}")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
