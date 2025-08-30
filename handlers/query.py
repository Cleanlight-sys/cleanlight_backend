import requests
from config import SUPABASE_URL, HEADERS, TABLE_KEYS

def handle(table, body):
    rid = body.get("rid")
    filters = body.get("filters") or {}
    stream  = body.get("stream", False)
    limit   = int(body.get("limit", 100))

    # ---- Graph starting point ----
    if table == "graph":
        if filters:
            # Resolve matching graph nodes by filter (e.g., label ilike.*felt*)
            qs = []
            for k, v in filters.items():
                qs.append(f"{k}={v}")
            qs.append(f"limit={limit}")
            url = f"{SUPABASE_URL}/rest/v1/graph?{'&'.join(qs)}"
            r = requests.get(url, headers=HEADERS)
            if r.status_code != 200:
                return None, "Graph filter query failed", {"code":"GRAPH_FILTER_FAIL","detail":r.text}
            nodes = r.json()
            if not nodes:
                return None, "No matching graph nodes", {"code":"NO_MATCH"}
            
            bundles = []
            for node in nodes:
                doc_id = node.get("doc_id")
                doc_url   = f"{SUPABASE_URL}/rest/v1/docs?id=eq.{doc_id}"
                edges_url = f"{SUPABASE_URL}/rest/v1/edges?source=eq.{node['id']}"

                doc_r   = requests.get(doc_url, headers=HEADERS)
                edges_r = requests.get(edges_url, headers=HEADERS)

                bundles.append({
                    "graph_node": node,
                    "doc": doc_r.json()[0] if doc_r.status_code == 200 and doc_r.json() else None,
                    "edges": edges_r.json() if edges_r.status_code == 200 else []
                })
            
            return bundles, None, None

        elif rid:
            # Normal single-graph-node lookup
            key_col = TABLE_KEYS.get("graph", "id")
            url = f"{SUPABASE_URL}/rest/v1/graph?{key_col}=eq.{rid}"
            r = requests.get(url, headers=HEADERS)
            if r.status_code != 200 or not r.json():
                return None, "Graph node not found", {"code":"NOT_FOUND","id":rid}
            node = r.json()[0]

            doc_id = node.get("doc_id")
            doc_url   = f"{SUPABASE_URL}/rest/v1/docs?id=eq.{doc_id}"
            edges_url = f"{SUPABASE_URL}/rest/v1/edges?source=eq.{node['id']}"

            doc_r   = requests.get(doc_url, headers=HEADERS)
            edges_r = requests.get(edges_url, headers=HEADERS)

            return {
                "graph_node": node,
                "doc": doc_r.json()[0] if doc_r.status_code == 200 and doc_r.json() else None,
                "edges": edges_r.json() if edges_r.status_code == 200 else []
            }, None, None

        else:
            return None, "Provide either 'rid' or 'filters' for graph query", {"code":"ARGS_REQUIRED"}

    # ---- Doc starting point ----
    if table == "docs":
        if not rid:
            return None, "Doc query requires 'rid'", {"code":"RID_REQUIRED"}
        url = f"{SUPABASE_URL}/rest/v1/docs?id=eq.{rid}"
        r = requests.get(url, headers=HEADERS)
        if r.status_code != 200 or not r.json():
            return None, "Doc not found", {"code":"NOT_FOUND","id":rid}
        doc = r.json()[0]

        graph_url  = f"{SUPABASE_URL}/rest/v1/graph?doc_id=eq.{rid}"
        edges_url  = f"{SUPABASE_URL}/rest/v1/edges?doc_id=eq.{rid}"
        chunks_url = f"{SUPABASE_URL}/rest/v1/chunks?doc_id=eq.{rid}"

        graph_r  = requests.get(graph_url, headers=HEADERS)
        edges_r  = requests.get(edges_url, headers=HEADERS)
        chunks_r = requests.get(chunks_url, headers=HEADERS)

        return {
            "doc": doc,
            "graph_nodes": graph_r.json() if graph_r.status_code == 200 else [],
            "edges": edges_r.json() if edges_r.status_code == 200 else [],
            "chunks": chunks_r.json() if chunks_r.status_code == 200 else []
        }, None, None

    return None, "Unknown table for query", {"code":"BAD_TABLE","table":table}
