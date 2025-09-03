# Cleanlight_bk.py — Flask WSGI app (dynamic OpenAPI; no static file)
# Purpose: Always serve OpenAPI from code; do not rely on a file, do not rebuild for spec fetches.
# Notes: Adds canonical + alias routes, ETag/304, HEAD support, strict no-store caching.

from __future__ import annotations  # safe on Py3.8+; remove if you target 3.7

import json
import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, Tuple

from flask import Flask, request, jsonify, Response
from flask_cors import CORS

# Local modules
from config import wrap  # response wrapper
from handlers import read_all, read_rows, write, update, delete, query, hint

app = Flask(__name__)
CORS(app)  # allow bots/tools to fetch the spec cross-origin


# ---------- OpenAPI generation (no static file) ----------

def _build_spec_dict() -> Dict[str, Any]:
    """Return OpenAPI dict from code only. No disk reads.
    Priority: schema.build_spec() → schema.paths_query.get() → minimal stub.
    """
    # Preferred: aggregator if present
    try:
        from schema import build_spec as _build  # type: ignore

        spec = _build()
        if isinstance(spec, dict) and spec.get("paths"):
            return spec
    except Exception:
        pass

    # Fallback: low-level /query path
    paths: Dict[str, Any] = {}
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
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "required": ["action", "table"],
                                    "properties": {
                                        "action": {"type": "string", "enum": ["query"]},
                                        "table": {
                                            "type": "string",
                                            "enum": [
                                                "docs",
                                                "chunks",
                                                "graph",
                                                "edges",
                                                "images",
                                                "kcs",
                                                "bundle",
                                            ],
                                        },
                                        "q": {"type": "string", "nullable": True},
                                        "limit": {
                                            "type": "integer",
                                            "minimum": 1,
                                            "maximum": 500,
                                            "default": 50,
                                        },
                                        "filters": {
                                            "type": "object",
                                            "additionalProperties": True,
                                            "nullable": True,
                                        },
                                        "filters_str": {"type": "string", "nullable": True},
                                        "chunk_text_max": {
                                            "type": "integer",
                                            "minimum": 64,
                                            "maximum": 5000,
                                            "default": 600,
                                        },
                                    },
                                }
                            }
                        },
                    },
                    "responses": {"200": {"description": "OK"}},
                }
            }
        }

    return {
        "openapi": "3.0.0",
        "info": {
            "title": "Cleanlight Backend API",
            "version": "0.0.0",
            "description": "Dynamically generated spec (no static file).",
        },
        "paths": paths,
    }


def _render_spec_response() -> Response:
    """Serialize, add ETag, and honor If-None-Match. Always fresh (no-store)."""
    spec = _build_spec_dict()
    payload = json.dumps(spec, ensure_ascii=False, separators=(",", ":"))

    # Strong ETag based on content
    etag = hashlib.sha256(payload.encode("utf-8")).hexdigest()

    # Conditional GET
    inm = request.headers.get("If-None-Match")
    if inm and inm.strip('"') == etag:
        resp = Response(status=304)
    else:
        resp = Response(payload, status=200, mimetype="application/json")

    # Caching: always serve latest from code; let clients cache but revalidate
    resp.headers["ETag"] = etag
    resp.headers["Cache-Control"] = "no-store, max-age=0, must-revalidate"
    resp.headers["Last-Modified"] = datetime.now(timezone.utc).strftime(
        "%a, %d %b %Y %H:%M:%S GMT"
    )
    # Friendly for humans
    resp.headers["X-OpenAPI-Source"] = "dynamic"
    return resp


# Canonical + common aliases some bots use
@app.route("/openapi.json", methods=["GET", "HEAD"])  # canonical
@app.route("/openai.json", methods=["GET", "HEAD"])   # alias (common typo)
@app.route("/.well-known/openapi.json", methods=["GET", "HEAD"])  # discovery
def openapi_spec() -> Response:
    return _render_spec_response()


# ---------- App health ----------
@app.get("/")
def root() -> Tuple[Response, int]:
    return jsonify({"ok": True, "service": "cleanlight-backend"}), 200

@app.get("/_healthz")
def healthz() -> Tuple[Response, int]:
    return jsonify({"ok": True}), 200


# ---------- Business endpoints ----------
@app.post("/hint")
def hint_gate() -> Response:
    body = request.get_json(silent=True) or {}
    data, error, hint_txt = hint.handle(body)
    return wrap(data, body, hint_txt, error)


@app.post("/query")
def query_gate() -> Response:
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

    return wrap(
        None,
        body,
        {"allowed_actions": ["read_all", "read_row", "write", "update", "delete", "query"]},
        f"Unknown action: {action!r}",
    )


# Local dev: python Cleanlight_bk.py
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
