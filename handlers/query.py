# handlers/query.py — Instant SME Engine (uniform bundles)

import requests, json
from urllib.parse import quote_plus
from config import SUPABASE_URL, HEADERS, wrap  # TABLE_KEYS not needed here

# ---------------------- helpers

def _qs(pairs): return "&".join(pairs)

def _filter_pairs(filters: dict):
    pairs = []
    for k, v in (filters or {}).items():
        if isinstance(v, str) and any(v.startswith(op) for op in
            ["eq.", "ilike.", "cs.", "fts.", "gt.", "gte.", "lt.", "lte.", "neq."]):
            pairs.append(f"{quote_plus(k)}={quote_plus(v)}")
        else:
            pairs.append(f"{quote_plus(k)}=eq.{quote_plus(str(v))}")
    return pairs

def _supa_get(path_qs):
    url = f"{SUPABASE_URL}/rest/v1/{path_qs}"
    r = requests.get(url, headers=HEADERS)
    if r.status_code != 200:
        return None, {"code": "SUPABASE_GET_FAIL", "status": r.status_code, "detail": r.text}
    try:
        return r.json(), None
    except Exception as e:
        return None, {"code": "JSON_PARSE_FAIL", "detail": str(e)}

def _get_table(table, pairs, select="*"):
    qs = f"{table}?select={quote_plus(select)}"
    if pairs: qs = f"{qs}&{_qs(pairs)}"
    return _supa_get(qs)

def _get_by_id(table, rid, key="id", select="*"):
    pairs = [f"{quote_plus(key)}=eq.{quote_plus(str(rid))}"]
    return _get_table(table, pairs, select)

def _get_edges_by_node_id(node_id, limit=500):
    pairs = [f"or=(src_id.eq.{node_id},dst_id.eq.{node_id})", f"limit={int(limit)}"]
    return _get_table("edges", pairs, select="doc_id,src_id,dst_id,etype")

def _get_chunks_for_doc(doc_id, limit=1000):
    pairs = [f"doc_id=eq.{quote_plus(str(doc_id))}", f"limit={int(limit)}"]
    return _get_table("chunks", pairs, select="doc_id,page_from,page_to,text,sha256")

def _get_graph_for_doc(doc_id, limit=1000):
    pairs = [f"doc_id=eq.{quote_plus(str(doc_id))}", f"limit={int(limit)}"]
    return _get_table("graph", pairs, select="id,doc_id,ntype,label,page,data")

# ---------------------- bundle validation

def _validate_bundle(bundle):
    issues, ok = [], True
    node, doc = bundle.get("node"), bundle.get("doc")
    if node and not node.get("label"): issues.append("graph.node missing label"); ok = False
    if node and not node.get("doc_id"): issues.append("graph.node missing doc_id"); ok = False
    if doc and "meta" not in doc:       issues.append("doc missing meta"); ok = False
    if doc and "sha256" not in doc:     issues.append("doc missing sha256"); ok = False
    if not bundle.get("chunks"):        issues.append("no chunks")
    if bundle.get("edges") is None:     issues.append("edges missing (should be [])"); ok = False
    bundle["__sme_ok__"] = ok
    bundle["__sme_issues__"] = issues
    return bundle

def _make_bundle(node=None, doc=None, chunks=None, edges=None):
    return _validate_bundle({
        "node": node,
        "doc": doc,
        "chunks": chunks or [],
        "edges": edges or []
    })

# ---------------------- bundle builders (with optional chunk text truncation)

def _truncate_chunks(chunks, max_chars):
    if not max_chars: return chunks or []
    out = []
    for c in (chunks or []):
        t = c.get("text")
        if isinstance(t, str) and len(t) > max_chars:
            c = {**c, "text": t[:max_chars] + "…"}
        out.append(c)
    return out

def _bundle_from_graph_row(grow, chunk_limit=1000, edge_limit=500, chunk_text_max=None):
    doc_id = grow.get("doc_id")
    doc, _ = _get_by_id("docs", doc_id, key="doc_id", select="doc_id,title,meta,sha256")
    doc_row = (doc or [None])[0]
    chunks, _ = _get_chunks_for_doc(doc_id, limit=chunk_limit)
    edges, _  = _get_edges_by_node_id(grow.get("id"), limit=edge_limit)
    return _make_bundle(
        node=grow,
        doc=doc_row,
        chunks=_truncate_chunks(chunks, chunk_text_max),
        edges=edges or []
    )

def _bundle_from_doc_row(drow, chunk_limit=1000, edge_limit=500, chunk_text_max=None):
    doc_id = drow.get("doc_id")
    chunks, _ = _get_chunks_for_doc(doc_id, limit=chunk_limit)
    gnodes, _ = _get_graph_for_doc(doc_id, limit=1000)
    edges_union = []
    if gnodes:
        for n in gnodes:
            e, _ = _get_edges_by_node_id(n.get("id"), limit=edge_limit)
            if e: edges_union.extend(e)
    if gnodes:
        for n in gnodes:
            yield _make_bundle(
                node=n,
                doc=drow,
                chunks=_truncate_chunks(chunks, chunk_text_max),
                edges=[e for e in edges_union if e["src_id"] == n["id"] or e["dst_id"] == n["id"]]
            )
    else:
        yield _make_bundle(node=None, doc=drow, chunks=_truncate_chunks(chunks, chunk_text_max), edges=[])

# ---------------------- main handler

def handle(table, body, **kwargs):
    echo   = {"table": table, "body": body}
    rid    = body.get("rid")                # optional for graph/docs
    filters= body.get("filters") or {}
    q      = body.get("q")                  # lightweight text search
    limit  = int(body.get("limit", 100))
    stream = bool(body.get("stream", False))
    chunk_text_max = body.get("chunk_text_max")  # int or None

    if not rid and not filters and not q:
        return None, "Provide 'filters' or 'q' (rid optional for graph/docs).", {
            "code": "ARGS_REQUIRED", "need": ["filters|q|rid"], "table": table
        }

    pairs = _filter_pairs(filters)
    if q:
        if table == "graph": pairs.append(f"label=ilike.*{quote_plus(q)}*")
        elif table == "docs": pairs.append(f"title=ilike.*{quote_plus(q)}*")
        elif table == "edges": pairs.append(f"etype=ilike.*{quote_plus(q)}*")
    pairs.append(f"limit={limit}")

    # ---- graph SME (rid | filters | q)
    if table == "graph":
        rows, err = (_get_by_id("graph", rid, key="id",
                                select="id,doc_id,ntype,label,page,data")
                     if rid else
                     _get_table("graph", pairs,
                                select="id,doc_id,ntype,label,page,data"))
        if err: return None, "Graph query failed", {"code": "GRAPH_QUERY_FAIL", **err}

        def gen():
            for r in (rows or []):
                b = _bundle_from_graph_row(r, chunk_text_max=chunk_text_max)
                if b is not None:  # safety for streaming
                    yield b

        return wrap(gen(), echo=echo, stream=True) if stream else ([b for b in gen()], None, None)

    # ---- docs SME (rid | filters | q)
    if table == "docs":
        rows, err = (_get_by_id("docs", rid, key="doc_id",
                                select="doc_id,title,meta,sha256")
                     if rid else
                     _get_table("docs", pairs,
                                select="doc_id,title,meta,sha256"))
        if err: return None, "Docs query failed", {"code": "DOCS_QUERY_FAIL", **err}

        def gen():
            for d in (rows or []):
                for b in _bundle_from_doc_row(d, chunk_text_max=chunk_text_max):
                    if b is not None:
                        yield b

        return wrap(gen(), echo=echo, stream=True) if stream else ([b for b in gen()], None, None)

    # ---- edges SME (filters | q only; normalize to bundles)
    if table == "edges":
        if rid:
            return None, "Edges do not support 'rid'; use filters or q", {"code": "BAD_ARGS", "table": "edges"}

        rows, err = _get_table("edges", pairs, select="doc_id,src_id,dst_id,etype")
        if err: return None, "Edges query failed", {"code": "EDGES_QUERY_FAIL", **err}

        def gen():
            for e in (rows or []):
                # expand src endpoint
                src_id = e.get("src_id")
                if src_id is not None:
                    src_node, _ = _get_by_id("graph", src_id, key="id",
                                             select="id,doc_id,ntype,label,page,data")
                    if src_node:
                        b = _bundle_from_graph_row(src_node[0], chunk_text_max=chunk_text_max)
                        if b is not None:
                            yield b
                # expand dst endpoint
                dst_id = e.get("dst_id")
                if dst_id is not None:
                    dst_node, _ = _get_by_id("graph", dst_id, key="id",
                                             select="id,doc_id,ntype,label,page,data")
                    if dst_node:
                        b = _bundle_from_graph_row(dst_node[0], chunk_text_max=chunk_text_max)
                        if b is not None:
                            yield b

        return wrap(gen(), echo=echo, stream=True) if stream else ([b for b in gen()], None, None)

    # ---- unknown
    return None, "Unknown table", {"code": "BAD_TABLE", "table": table}
