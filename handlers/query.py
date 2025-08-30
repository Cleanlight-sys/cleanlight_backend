import requests, json
from config import SUPABASE_URL, HEADERS, TABLE_KEYS

def handle(table, body):
    rid = body.get("rid")
    stream = body.get("stream", False)
    if not rid:
        return None, "Add 'rid': <id>", {"code":"RID_REQUIRED","field":"rid"}

    if stream:
        return _handle_stream(table, body, rid)
    else:
        return _handle_normal(table, body, rid)

# --- Normal (non-stream) ---
def _handle_normal(table, body, rid):
    bundle = {}

    if table == "docs":
        # Doc
        url = f"{SUPABASE_URL}/rest/v1/docs?doc_id=eq.{rid}&select=*"
        doc = requests.get(url, headers=HEADERS).json()
        if not doc:
            return None, "Doc not found", {"code":"NOT_FOUND","id":rid}
        bundle["doc"] = doc[0]

        # Graph nodes
        url = f"{SUPABASE_URL}/rest/v1/graph?doc_id=eq.{rid}&select=*"
        g = requests.get(url, headers=HEADERS).json()
        bundle["graph_nodes"] = g

        # Edges
        url = f"{SUPABASE_URL}/rest/v1/edges?doc_id=eq.{rid}&select=*"
        e = requests.get(url, headers=HEADERS).json()
        bundle["edges"] = e

        # Chunks
        url = f"{SUPABASE_URL}/rest/v1/chunks?doc_id=eq.{rid}&select=*"
        c = requests.get(url, headers=HEADERS).json()
        bundle["chunks"] = c

    elif table == "graph":
        key_col = TABLE_KEYS["graph"]
        url = f"{SUPABASE_URL}/rest/v1/graph?{key_col}=eq.{rid}&select=*"
        node = requests.get(url, headers=HEADERS).json()
        if not node:
            return None, "Graph node not found", {"code":"NOT_FOUND","id":rid}
        node = node[0]
        bundle["graph_node"] = node

        # Parent doc
        doc_id = node.get("doc_id")
        if doc_id:
            url = f"{SUPABASE_URL}/rest/v1/docs?doc_id=eq.{doc_id}&select=*"
            doc = requests.get(url, headers=HEADERS).json()
            if doc:
                bundle["doc"] = doc[0]

        # Edges
        url = f"{SUPABASE_URL}/rest/v1/edges?source=eq.{rid}&select=*"
        e = requests.get(url, headers=HEADERS).json()
        bundle["edges"] = e

    else:
        return None, "Query only supports docs or graph as root", {"code":"BAD_TABLE"}

    return bundle, None, None

# --- Streaming mode ---
def _handle_stream(table, body, rid):
    def generate():
        yield '{"bundle":{'

        if table == "docs":
            # Doc
            url = f"{SUPABASE_URL}/rest/v1/docs?doc_id=eq.{rid}&select=*"
            doc = requests.get(url, headers=HEADERS).json()
            if not doc:
                yield '}, "echo":' + json.dumps({"original_body": body}) + \
                      ', "hint":"Doc not found", "error":{"code":"NOT_FOUND"}}'
                return
            yield '"doc":' + json.dumps(doc[0]) + ','

            # Graph nodes
            url = f"{SUPABASE_URL}/rest/v1/graph?doc_id=eq.{rid}&select=*"
            g = requests.get(url, headers=HEADERS, stream=True)
            yield '"graph_nodes":['
            first = True
            for chunk in g.iter_content(chunk_size=None):
                if chunk:
                    if not first: yield ","
                    yield chunk.decode("utf-8").strip("[]")
                    first = False
            yield '],'

            # Edges
            url = f"{SUPABASE_URL}/rest/v1/edges?doc_id=eq.{rid}&select=*"
            e = requests.get(url, headers=HEADERS, stream=True)
            yield '"edges":['
            first = True
            for chunk in e.iter_content(chunk_size=None):
                if chunk:
                    if not first: yield ","
                    yield chunk.decode("utf-8").strip("[]")
                    first = False
            yield '],'

            # Chunks
            url = f"{SUPABASE_URL}/rest/v1/chunks?doc_id=eq.{rid}&select=*"
            c = requests.get(url, headers=HEADERS, stream=True)
            yield '"chunks":['
            first = True
            for chunk in c.iter_content(chunk_size=None):
                if chunk:
                    if not first: yield ","
                    yield chunk.decode("utf-8").strip("[]")
                    first = False
            yield ']'

        elif table == "graph":
            key_col = TABLE_KEYS["graph"]
            url = f"{SUPABASE_URL}/rest/v1/graph?{key_col}=eq.{rid}&select=*"
            node = requests.get(url, headers=HEADERS).json()
            if not node:
                yield '}, "echo":' + json.dumps({"original_body": body}) + \
                      ', "hint":"Graph node not found", "error":{"code":"NOT_FOUND"}}'
                return
            node = node[0]
            yield '"graph_node":' + json.dumps(node) + ','

            # Parent doc
            doc_id = node.get("doc_id")
            if doc_id:
                url = f"{SUPABASE_URL}/rest/v1/docs?doc_id=eq.{doc_id}&select=*"
                doc = requests.get(url, headers=HEADERS).json()
                if doc:
                    yield '"doc":' + json.dumps(doc[0]) + ','

            # Edges
            url = f"{SUPABASE_URL}/rest/v1/edges?source=eq.{rid}&select=*"
            e = requests.get(url, headers=HEADERS, stream=True)
            yield '"edges":['
            first = True
            for chunk in e.iter_content(chunk_size=None):
                if chunk:
                    if not first: yield ","
                    yield chunk.decode("utf-8").strip("[]")
                    first = False
            yield ']'

        yield '}, "echo":' + json.dumps({"original_body": body}) + '}'

    return generate(), None, None, True
