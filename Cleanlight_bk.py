# Cleanlight_bk.py — Lean WSGI app, dynamic /openapi.json, static /schema endpoint (robust)

import json
import hashlib
from datetime import datetime, timezone
from typing import Tuple

from flask import Flask, request, jsonify, Response
from flask_cors import CORS

from config import wrap
from schema import build_spec
from handlers import read_all, read_rows, write, update, delete, query, hint

app = Flask(__name__)
CORS(app)


# -------- OpenAPI (dynamic) --------

def _spec_response() -> Response:
    spec = build_spec()
    try:
        server_url = request.url_root.rstrip("/")
        spec["servers"] = [{"url": server_url}]
    except Exception:
        pass

    payload = json.dumps(spec, ensure_ascii=False, separators=(",", ":"))
    etag = hashlib.sha256(payload.encode("utf-8")).hexdigest()

    inm = request.headers.get("If-None-Match")
    if inm and inm.strip('"') == etag:
        resp = Response(status=304)
    else:
        resp = Response(payload, status=200, mimetype="application/json")

    resp.headers["ETag"] = etag
    resp.headers["Cache-Control"] = "no-store, max-age=0, must-revalidate"
    resp.headers["Last-Modified"] = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")
    resp.headers["X-OpenAPI-Source"] = "dynamic"
    return resp


@app.route("/openapi.json", methods=["GET", "HEAD"])  # canonical
@app.route("/openai.json", methods=["GET", "HEAD"])   # alias some bots use
@app.route("/.well-known/openapi.json", methods=["GET", "HEAD"])  # discovery

@app.get("/openapi.json")
def openapi():
    spec = build_spec(include_hint=True)
    # Optionally inject the runtime server URL:
    spec["servers"] = [{"url": request.host_url.rstrip("/")}]
    return jsonify(spec)


def openapi_spec() -> Response:
    return _spec_response()


# -------- Static base schema endpoint (robust to base style) --------
@app.get("/schema")
def schema_get() -> Response:
    """Return the committed base schema.
    Works whether `schema/base.py` exposes `get()` or a `base` dict.
    """
    base_schema = None

    # Try function export first
    try:
        from schema.base import get as get_base  # type: ignore
        base_schema = get_base()
    except Exception:
        base_schema = None

    # Fallback to dict export
    if base_schema is None:
        try:
            from schema.base import base as base_obj  # type: ignore
            base_schema = base_obj
        except Exception as e:
            # Clear, JSON error (helps diagnose 500s)
            return (
                jsonify({
                    "error": "Missing base schema",
                    "detail": "schema.base must export `get()` or `base`",
                    "exception": str(e),
                }),
                500,
            )

    try:
        payload = json.dumps(base_schema, ensure_ascii=False, separators=(",", ":"))
    except Exception as e:
        return jsonify({"error": "Base schema is not JSON-serializable", "exception": str(e)}), 500

    return Response(payload, status=200, mimetype="application/json")


# -------- Health --------
@app.get("/")
def root() -> Tuple[Response, int]:
    return jsonify({"ok": True, "service": "cleanlight-backend"}), 200


@app.get("/_healthz")
def healthz() -> Tuple[Response, int]:
    return jsonify({"ok": True}), 200


# -------- Business endpoints --------
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

    return wrap(None, body, {"allowed_actions": [
        "read_all", "read_row", "write", "update", "delete", "query"
    ]}, f"Unknown action: {action!r}")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)

