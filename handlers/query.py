# handlers/query.py
import requests, json
from config import SUPABASE_URL, HEADERS, TABLE_KEYS

def validate_bundle(bundle):
    """Attach SME diagnostics so we know what's missing/malformed."""
    issues = []
    node = bundle.get("graph_node")
    doc = bundle.get("doc")

    if not node:
        issues.append("MISSING_GRAPH_NODE")
    else:
        if not node.get("label"): issues.append("NODE_NO_LABEL")
        if not node.get("doc_id"): issues.append("NODE_NO_DOC_ID")

    if not doc:
        issues.append("MISSING_DOC")
    else:
        if not doc.get("meta"): issues.append("DOC_NO_META")
        if not doc.get("sha256"): issues.append("DOC_NO_SHA256")

    if not bundle.get("chunks"): issues.append("NO_CHUNKS")
    if not bundle.get("edges"): issues.append("NO_EDGES")

    bundle["__sme_issues__"] = issues
    bundle["__sme_ok__"] = (len(issues) == 0)
    return bundle


def handle(table, body=None, **kwargs):
    """
    Unified SME query handler.
    Returns bundles + SME diagnostics.
    """
    body = {**(body or {}), **kwargs}
    rid     = body.get("rid")
    filters = body.get("filters") or {}
    limit   = int(body.get("limit", 100))

    if table == "graph":
        if filters:
            qs = [f"{k}={v}" for k, v in filters.items()]
            qs.append(f"limit={limit}")
            url = f"{SUPABASE_URL}/rest/v1/graph?{'&'.join(qs)}"
            r = requests.get(url, headers=HEADERS)
            if r.status_code != 200:
                return None, "Graph filter query failed", {"code": "GRAPH_FILTER_FAIL", "detail": r.text}

            nodes = r.json()
            if not nodes:
                return None, "No matching graph nodes", {"code": "NO_MATCH"}

            def gen():
                for node in nodes:
                    doc_id = node.get("doc_id")
                    node_id = node.get("id")
                    doc_r    = requests.get(f"{SUPABASE_URL}/rest/v1/docs?id=eq.{doc_id}", headers=HEADERS)
                    edges_r  = requests.get(f"{SUPABASE_URL}/rest/v1/edges?src_id=eq.{node_id}", headers=HEADERS)
                    chunks_r = requests.get(f"{SUPABASE_URL}/rest/v1/chunks?doc_id=eq.{doc_id}", headers=HEADERS)
                    yield validate_bundle({
                        "graph_node": node,
                        "doc": doc_r.json()[0] if doc_r.status_code == 200 and doc_r.json() else None,
                        "edges": edges_r.json() if edges_r.status_code == 200 else [],
                        "chunks": chunks_r.json() if chunks_r.status_code == 200 else []
                    })
            return gen(), None, None

        elif rid:
            key_col = TABLE_KEYS.get("graph", "id")
            url = f"{SUPABASE_URL}/rest/v1/graph?{key_col}=eq.{rid}"
            r = requests.get(url, headers=HEADERS)
            if r.status_code != 200 or not r.json():
                return None, "Graph node not found", {"code": "NOT_FOUND", "id": rid}
            node = r.json()[0]
            doc_id = node.get("doc_id")
            doc_r    = requests.get(f"{SUPABASE_URL}/rest/v1/docs?id=eq.{doc_id}", headers=HEADERS)
            edges_r  = requests.get(f"{SUPABASE_URL}/rest/v1/edges?src_id=eq.{node['id']}", headers=HEADERS)
            chunks_r = requests.get(f"{SUPABASE_URL}/rest/v1/chunks?doc_id=eq.{doc_id}", headers=HEADERS)

            return validate_bundle({
                "graph_node": node,
                "doc": doc_r.json()[0] if doc_r.status_code == 200 and doc_r.json() else None,
                "edges": edges_r.json() if edges_r.status_code == 200 else [],
                "chunks": chunks_r.json() if chunks_r.status_code == 200 else []
            }), None, None

        return None, "Provide either 'rid' or 'filters' for graph query", {"code":"ARGS_REQUIRED"}

    if table == "docs":
        if not rid:
            return None, "Doc query requires 'rid'", {"code":"RID_REQUIRED"}
        r = requests.get(f"{SUPABASE_URL}/rest/v1/docs?id=eq.{rid}", headers=HEADERS)
        if r.status_code != 200 or not r.json():
            return None, "Doc not found", {"code":"NOT_FOUND","id":rid}
        doc = r.json()[0]
        graph_r  = requests.get(f"{SUPABASE_URL}/rest/v1/graph?doc_id=eq.{rid}", headers=HEADERS)
        edges_r  = requests.get(f"{SUPABASE_URL}/rest/v1/edges?doc_id=eq.{rid}", headers=HEADERS)
        chunks_r = requests.get(f"{SUPABASE_URL}/rest/v1/chunks?doc_id=eq.{rid}", headers=HEADERS)

        return validate_bundle({
            "doc": doc,
            "graph_nodes": graph_r.json() if graph_r.status_code == 200 else [],
            "edges": edges_r.json() if edges_r.status_code == 200 else [],
            "chunks": chunks_r.json() if chunks_r.status_code == 200 else []
        }), None, None

    return None, "Unknown table for query", {"code":"BAD_TABLE","table":table}
