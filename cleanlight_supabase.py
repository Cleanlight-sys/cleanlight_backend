# cleanlight_supabase.py
from flask import Flask, request, jsonify
from datetime import datetime, timezone

app = Flask(__name__)

# Mock in-memory DB (replace with real Supabase client)
DB = {
    "docs": [],
    "chunks": [],
    "graph": [],
    "edges": []
}

def _now():
    return datetime.now(timezone.utc).isoformat()

def wrap(data=None, echo=None, hint=None, error=None):
    return {"data": data, "echo": echo, "hint": hint, "error": error}

@app.get("/health")
def health():
    return jsonify({"status": "ok", "time": _now()})

@app.post("/query")
def query():
    body = request.json or {}
    table, action = body.get("table"), body.get("action")
    rid, payload, echo = body.get("rid"), body.get("payload", {}), body.get("echo")

    if table not in DB:
        return jsonify(wrap(None, echo, "Unknown table", {"code":"BAD_TABLE"})), 400

    # READ ALL
    if action == "read_all":
        select = body.get("select")
        rows = DB[table]
        if select:
            fields = select.split(",")
            rows = [{k: r.get(k) for k in fields if k in r} for r in rows]
        return jsonify(wrap(rows, echo))

    # READ ROW
    if action == "read_row":
        row = next((r for r in DB[table] if r.get("id") == rid or r.get("doc_id") == rid), None)
        if not row:
            return jsonify(wrap(None, echo, "Not found", {"code":"NOT_FOUND"})), 404
        return jsonify(wrap(row, echo))

    # WRITE
    if action == "write":
        new = payload.copy()
        # auto ID if not provided
        pk = "id" if table in ("chunks","graph","edges") else "doc_id"
        if pk not in new:
            new[pk] = len(DB[table]) + 1 if pk == "id" else f"doc_{len(DB[table])+1}"
        if table == "docs":
            new.setdefault("sha256","")
            new.setdefault("meta",{})
        if table == "chunks":
            new.setdefault("sha256","")
            new.setdefault("embedding",[])
        DB[table].append(new)
        return jsonify(wrap(new, echo))

    # UPDATE
    if action == "update":
        for r in DB[table]:
            if r.get("id") == rid or r.get("doc_id") == rid:
                r.update(payload)
                return jsonify(wrap(r, echo))
        return jsonify(wrap(None, echo, "Not found", {"code":"NOT_FOUND"})), 404

    # DELETE
    if action == "delete":
        before = len(DB[table])
        DB[table] = [r for r in DB[table] if not (r.get("id") == rid or r.get("doc_id") == rid)]
        if len(DB[table]) == before:
            return jsonify(wrap(None, echo, "Not found", {"code":"NOT_FOUND"})), 404
        return jsonify(wrap({"status":"deleted","rid":rid}, echo))

    return jsonify(wrap(None, echo, "Unknown action", {"code":"BAD_ACTION"})), 400

if __name__ == "__main__":
    app.run(debug=True, port=8000)
