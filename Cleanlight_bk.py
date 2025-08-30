# Cleanlight_bk.py — Admin Gate
# One endpoint: /query
# Modular handlers can be bolted on later.

from flask import Flask, request, jsonify, Response, stream_with_context
from datetime import datetime, timezone
import os, requests, json

app = Flask(__name__)

# --- Config ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
HEADERS = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}

# --- Table Key Map ---
TABLE_KEYS = {
    "docs": "doc_id",
    "chunks": "id",
    "graph": "id",
    "edges": "id",
}

# --- Helpers ---
def _now():
    return datetime.now(timezone.utc).isoformat()

def wrap(data=None, body=None, hint=None, error=None):
    echo = {"original_body": body}
    out = {"data": data, "echo": echo}
    if hint is not None:
        out["hint"] = hint
    if error is not None:
        out["error"] = error
    return out

# --- Health ---
@app.get("/health")
def health():
    return jsonify({"status": "ok", "time": _now()})

# --- Query ---
@app.post("/query")
def query():
    body = request.json or {}
    action = body.get("action")
    table  = body.get("table")
    select = body.get("select", "*")
    rid    = body.get("rid")
    payload= body.get("payload", {})
    filters= body.get("filters") or payload.get("filters") or {}
    stream = body.get("stream", False)
    limit  = int(body.get("limit", 100))
    echo   = body.get("echo")

    # Guard: table required
    if not table:
        return jsonify(wrap(None, body, "Missing table", {"code": "TABLE_REQUIRED"})), 400
    if table not in TABLE_KEYS:
        return jsonify(wrap(None, body, f"Use one of: {', '.join(TABLE_KEYS.keys())}", {"code": "BAD_TABLE"})), 400

    key_col = TABLE_KEYS.get(table, "id")

    # ---------- READ ALL ----------
    if action == "read_all":
        # build filter string
        qs = []
        if filters:
            for k, v in filters.items():
                qs.append(f"{k}={v}")
        if not stream:
            qs.append(f"limit={limit}")
        filter_qs = "&" + "&".join(qs) if qs else ""
        url = f"{SUPABASE_URL}/rest/v1/{table}?select={select}{filter_qs}"

        if stream:
            r = requests.get(url, headers=HEADERS, stream=True)
            def generate():
                yield '{"data":['
                first = True
                for chunk in r.iter_content(chunk_size=None):
                    if chunk:
                        text = chunk.decode("utf-8")
                        if not first:
                            yield ","
                        yield text
                        first = False
                yield f'], "echo":{json.dumps({"original_body":body})}}'
            return Response(stream_with_context(generate()), mimetype="application/json")
        else:
            r = requests.get(url, headers=HEADERS)
            if r.status_code != 200:
                return jsonify(wrap(None, body, "Supabase error", {"code":"READ_FAIL","detail":r.text})), 500
            return jsonify(wrap(r.json(), body))

    # ---------- READ ROW ----------
    if action == "read_row":
        if not rid:
            return jsonify(wrap(None, body, "Add 'rid': <id>", {"code":"RID_REQUIRED","field":"rid"})), 400
        url = f"{SUPABASE_URL}/rest/v1/{table}?{key_col}=eq.{rid}&select={select}"
        r = requests.get(url, headers=HEADERS)
        rows = r.json()
        if not rows:
            return jsonify(wrap(None, body, "Not found", {"code":"NOT_FOUND","id":rid})), 404
        return jsonify(wrap(rows[0], body))

    # ---------- WRITE ----------
    if action == "write":
        url = f"{SUPABASE_URL}/rest/v1/{table}"
        r = requests.post(url, headers={**HEADERS,"Content-Type":"application/json"}, json=payload)
        if r.status_code not in (200,201):
            return jsonify(wrap(None, body, "Insert failed", {"code":"WRITE_FAIL","detail":r.text})), 500
        return jsonify(wrap(r.json(), body))

    # ---------- UPDATE ----------
    if action == "update":
        if not rid:
            return jsonify(wrap(None, body, "Add 'rid': <id>", {"code":"RID_REQUIRED","field":"rid"})), 400
        url = f"{SUPABASE_URL}/rest/v1/{table}?{key_col}=eq.{rid}"
        r = requests.patch(url, headers={**HEADERS,"Content-Type":"application/json"}, json=payload)
        if r.status_code != 200:
            return jsonify(wrap(None, body, "Update failed", {"code":"UPDATE_FAIL","detail":r.text})), 500
        return jsonify(wrap(r.json(), body))

    # ---------- DELETE ----------
    if action == "delete":
        if not rid:
            return jsonify(wrap(None, body, "Add 'rid': <id>", {"code":"RID_REQUIRED","field":"rid"})), 400
        url = f"{SUPABASE_URL}/rest/v1/{table}?{key_col}=eq.{rid}"
        r = requests.delete(url, headers=HEADERS)
        if r.status_code != 204:
            return jsonify(wrap(None, body, "Delete failed", {"code":"DELETE_FAIL","detail":r.text})), 500
        return jsonify(wrap({"status":"deleted","rid":rid}, body))

    return jsonify(wrap(None, body, "Unknown action", {"code":"BAD_ACTION"})), 400

if __name__ == "__main__":
    app.run(debug=True, port=8000)
