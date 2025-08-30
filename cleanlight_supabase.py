from flask import Flask, request, jsonify
from datetime import datetime, timezone
import os, requests, json

app = Flask(__name__)

# --- Config ---
SUPABASE_URL = os.getenv("SUPABASE_URL")  # e.g. https://<project>.supabase.co
SUPABASE_KEY = os.getenv("SUPABASE_KEY")  # use service_role for full access
HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}"
}

# --- Helpers ---
def _now():
    return datetime.now(timezone.utc).isoformat()

def wrap(data=None, echo=None, hint=None, error=None):
    return {"data": data, "echo": echo, "hint": hint, "error": error}

# --- Health ---
@app.get("/health")
def health():
    return jsonify({"status": "ok", "time": _now()})

# --- Query Proxy ---
@app.post("/query")
def query():
    body = request.json or {}
    table   = body.get("table")
    action  = body.get("action")
    select  = body.get("select", "*")
    rid     = body.get("rid")
    payload = body.get("payload", {})
    echo    = body.get("echo")

    if table not in ("docs", "chunks", "graph", "edges"):
        return jsonify(wrap(None, echo, "Unknown table", {"code": "BAD_TABLE"})), 400

    # --- READ ALL ---
    if action == "read_all":
        url = f"{SUPABASE_URL}/rest/v1/{table}?select={select}"
        resp = requests.get(url, headers=HEADERS)
        if resp.status_code != 200:
            return jsonify(wrap(None, echo, "Supabase error", {"code": "READ_FAIL", "detail": resp.text})), 500
        return jsonify(wrap(resp.json(), echo))

    # --- READ ROW ---
    if action == "read_row":
        if not rid:
            return jsonify(wrap(None, echo, "Missing rid", {"code": "RID_REQUIRED"})), 400
        url = f"{SUPABASE_URL}/rest/v1/{table}?id=eq.{rid}&select={select}"
        resp = requests.get(url, headers=HEADERS)
        rows = resp.json()
        if not rows:
            return jsonify(wrap(None, echo, "Not found", {"code": "NOT_FOUND", "id": rid})), 404
        return jsonify(wrap(rows[0], echo))

    # --- WRITE ---
    if action == "write":
        url = f"{SUPABASE_URL}/rest/v1/{table}"
        resp = requests.post(url, headers={**HEADERS,"Content-Type":"application/json"}, json=payload)
        if resp.status_code not in (200,201):
            return jsonify(wrap(None, echo, "Insert failed", {"code": "WRITE_FAIL", "detail": resp.text})), 500
        return jsonify(wrap(resp.json(), echo))

    # --- UPDATE ---
    if action == "update":
        if not rid:
            return jsonify(wrap(None, echo, "Missing rid", {"code": "RID_REQUIRED"})), 400
        url = f"{SUPABASE_URL}/rest/v1/{table}?id=eq.{rid}"
        resp = requests.patch(url, headers={**HEADERS,"Content-Type":"application/json"}, json=payload)
        if resp.status_code != 200:
            return jsonify(wrap(None, echo, "Update failed", {"code": "UPDATE_FAIL", "detail": resp.text})), 500
        return jsonify(wrap(resp.json(), echo))

    # --- DELETE ---
    if action == "delete":
        if not rid:
            return jsonify(wrap(None, echo, "Missing rid", {"code": "RID_REQUIRED"})), 400
        url = f"{SUPABASE_URL}/rest/v1/{table}?id=eq.{rid}"
        resp = requests.delete(url, headers=HEADERS)
        if resp.status_code != 204:
            return jsonify(wrap(None, echo, "Delete failed", {"code": "DELETE_FAIL", "detail": resp.text})), 500
        return jsonify(wrap({"status":"deleted","rid":rid}, echo))

    return jsonify(wrap(None, echo, "Unknown action", {"code": "BAD_ACTION"})), 400

if __name__ == "__main__":
    app.run(debug=True, port=8000)
