import requests
from flask import jsonify
from config import wrap, SUPABASE_URL, HEADERS, TABLE_KEYS

def handle(table, body):
    rid = body.get("rid")
    if not rid:
        return jsonify(wrap(None, body, "Add 'rid': <id>", {"code":"RID_REQUIRED"})), 400

    bundle = {}

    # ---- Docs as root ----
    if table == "docs":
        # 1. Doc
        url = f"{SUPABASE_URL}/rest/v1/docs?doc_id=eq.{rid}&select=*"
        r = requests.get(url, headers=HEADERS)
        doc = r.json()
        if not doc:
            return jsonify(wrap(None, body, "Doc not found", {"code":"NOT_FOUND","id":rid})), 404
        bundle["doc"] = doc[0]

        # 2. Graph nodes
        url = f"{SUPABASE_URL}/rest/v1/graph?doc_id=eq.{rid}&select=*"
        g = requests.get(url, headers=HEADERS).json()
        bundle["graph_nodes"] = g

        # 3. Edges for those nodes
        node_ids = [str(n["id"]) for n in g]
        if node_ids:
            id_list = ",".join(node_ids)
            url = f"{SUPABASE_URL}/rest/v1/edges?source=in.({id_list})&select=*"
            e = requests.get(url, headers=HEADERS).json()
            bundle["edges"] = e
        else:
            bundle["edges"] = []

        # 4. Chunks
        url = f"{SUPABASE_URL}/rest/v1/chunks?doc_id=eq.{rid}&select=*"
        c = requests.get(url, headers=HEADERS).json()
        bundle["chunks"] = c

    # ---- Graph node as root ----
    elif table == "graph":
        key_col = TABLE_KEYS["graph"]
        url = f"{SUPABASE_URL}/rest/v1/graph?{key_col}=eq.{rid}&select=*"
        r = requests.get(url, headers=HEADERS)
        node = r.json()
        if not node:
            return jsonify(wrap(None, body, "Graph node not found", {"code":"NOT_FOUND","id":rid})), 404
        node = node[0]
        bundle["graph_node"] = node

        # Parent doc
        doc_id = node.get("doc_id")
        if doc_id:
            url = f"{SUPABASE_URL}/rest/v1/docs?doc_id=eq.{doc_id}&select=*"
            bundle["doc"] = requests.get(url, headers=HEADERS).json()[0]

        # Edges for this node
        url = f"{SUPABASE_URL}/rest/v1/edges?source=eq.{rid}&select=*"
        bundle["edges"] = requests.get(url, headers=HEADERS).json()

    else:
        return jsonify(wrap(None, body, "Query only supports docs or graph as root", {"code":"BAD_TABLE"})), 400

    return jsonify(wrap(bundle, body))
