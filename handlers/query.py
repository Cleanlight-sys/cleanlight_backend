# handlers/query.py — Instant SME Engine (Edges-first Traversal, Filters on All Tables)
# -------------------------------------------------------------------------------
# What this rewrite does:
# 1) Makes `query` a true SME engine:
#    - You can start from GRAPH, DOCS, or EDGES.
#    - You can use `filters` (no ID required) or `rid` if you have it.
#    - You can traverse via EDGES and pull both src/dst bundles.
# 2) Adds lightweight text search `q` (ILIKE) for common fields.
# 3) Supports `depth` (hops) for edge expansion (default=0 => no extra hops).
# 4) Streams results if `stream:true`.
#
# Request body contract (examples below):
# {
#   "action": "query",
#   "table": "graph" | "docs" | "edges",
#   "filters": { ... }          # optional; recommended over rid for SME
#   "rid": "<id>",              # optional; only if you have it
#   "q": "beaver felt",         # optional; adds ILIKE conditions by table
#   "depth": 0 | 1 | 2,         # optional; edge traversal hops (default 0)
#   "limit": 100,               # optional; sane defaults below
#   "stream": true | false      # optional; default false
# }
#
# Returned shape: a sequence (or stream) of SME "bundles":
# {
#   "node": { ...graph row... } | None,
#   "doc":  { ...docs row... }  | None,
#   "chunks": [ ... ],
#   "edges": [ ... ],
#   "__sme_ok__": bool,
#   "__sme_issues__": [str]
# }
# -------------------------------------------------------------------------------

import requests, json
from urllib.parse import quote_plus
from config import SUPABASE_URL, HEADERS, TABLE_KEYS, wrap

# ---------- Utilities

def _qs(pairs):
    """Build a PostgREST querystring from dict of (k, v) that are already operators (e.g., label=ilike.*felt*)."""
    return "&".join(pairs)

def _filter_pairs(filters: dict):
    """Convert a `{col: op.value}` or `{col: 'eq.value'|'ilike.*x*'}` dict into PostgREST `col=<op>` pairs."""
    pairs = []
    for k, v in (filters or {}).items():
        # Allow raw operator strings like "ilike.*felt*"
        if isinstance(v, str) and (v.startswith("eq.") or v.startswith("ilike.") or v.startswith("cs.") or v.startswith("fts.") or v.startswith("gt.") or v.startswith("gte.") or v.startswith("lt.") or v.startswith("lte.") or v.startswith("neq.")):
            pairs.append(f"{quote_plus(k)}={quote_plus(v)}")
        else:
            # default to eq
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
    if pairs:
        qs = f"{qs}&{_qs(pairs)}"
    data, err = _supa_get(qs)
    return data, err

def _get_by_id(table, rid, key="id", select="*"):
    pairs = [f"{quote_plus(key)}=eq.{quote_plus(str(rid))}"]
    return _get_table(table, pairs, select)

def _get_edges_by_node_id(node_id, limit=500):
    pairs = [f"or=(src_id.eq.{node_id},dst_id.eq.{node_id})", f"limit={int(limit)}"]
    return _get_table("edges", pairs, select="id,doc_id,src_id,dst_id,etype")

def _get_chunks_for_doc(doc_id, limit=1000):
    pairs = [f"doc_id=eq.{quote_plus(str(doc_id))}", f"limit={int(limit)}"]
    return _get_table("chunks", pairs, select="doc_id,page_from,page_to,text,sha256")

def _get_graph_for_doc(doc_id, limit=1000):
    pairs = [f"doc_id=eq.{quote_plus(str(doc_id))}", f"limit={int(limit)}"]
    return _get_table("graph", pairs, select="id,doc_id,ntype,label,page,data")

def _validate_bundle(bundle):
    issues = []
    ok = True
    node = bundle.get("node")
    doc  = bundle.get("doc")
    if node and not node.get("label"):
        issues.append("graph.node missing label")
        ok = False
    if node and not node.get("doc_id"):
        issues.append("graph.node missing doc_id")
        ok = False
    if doc and "meta" not in doc:
        issues.append("doc missing meta")
        ok = False
    if doc and "sha256" not in doc:
        issues.append("doc missing sha256")
        ok = False
    if not bundle.get("chunks"):
        issues.append("no chunks")
        # not fatal; we still return
    if bundle.get("edges") is None:
        issues.append("edges missing (should be [])")
        ok = False
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

# ---------- Query builders per table

def _apply_text_query_pairs(table, q: str, pairs: list):
    """Augment filter pairs with ILIKE search for quick SME discovery."""
    if not q: 
        return pairs
    qsafe = quote_plus(f"*.{q}*")  # we'll craft per table below
    if table == "graph":
        # Search by label (ILIKE)
        pairs.append(f"label=ilike.*{quote_plus(q)}*")
    elif table == "docs":
        # Search by title (ILIKE)
        pairs.append(f"title=ilike.*{quote_plus(q)}*")
        # Optional: search meta.tags array with cs. (contains)
        # pairs.append(f"meta->>tags=cs.{{{quote_plus(q)}}}")
    elif table == "edges":
        # Text search doesn't map well; skip or add etype ilike
        pairs.append(f"etype=ilike.*{quote_plus(q)}*")
    return pairs

# ---------- SME expansion

def _bundle_from_graph_row(grow, chunk_limit=1000, edge_limit=500):
    # Graph node → fetch its doc + chunks + edges
    doc_id = grow.get("doc_id")
    doc, err = _get_by_id("docs", doc_id, key="doc_id", select="doc_id,title,meta,sha256")
    if err or not doc:
        return _make_bundle(node=grow, doc=None, chunks=[], edges=[])
    doc_row = doc[0] if isinstance(doc, list) else doc
    chunks, _  = _get_chunks_for_doc(doc_id, limit=chunk_limit)
    edges, _   = _get_edges_by_node_id(grow.get("id"), limit=edge_limit)
    return _make_bundle(node=grow, doc=doc_row, chunks=chunks or [], edges=edges or [])

def _bundle_from_doc_row(drow, chunk_limit=1000, edge_limit=500):
    # Doc → fetch its graph nodes + chunks + edges (via each node)
    doc_id = drow.get("doc_id")
    chunks, _ = _get_chunks_for_doc(doc_id, limit=chunk_limit)
    gnodes, _ = _get_graph_for_doc(doc_id, limit=1000)
    # Optionally union edges of all nodes
    edges_union = []
    if gnodes:
        for n in gnodes:
            e, _ = _get_edges_by_node_id(n.get("id"), limit=edge_limit)
            if e: edges_union.extend(e)
    # We return one bundle per node for doc richness; if no nodes, still return doc bundle
    if gnodes:
        for n in gnodes:
            yield _make_bundle(node=n, doc=drow, chunks=chunks or [], edges=[e for e in edges_union if e["src_id"]==n["id"] or e["dst_id"]==n["id"]])
    else:
        yield _make_bundle(node=None, doc=drow, chunks=chunks or [], edges=[])

def _bundle_from_edge_row(erow, chunk_limit=1000, edge_limit=500):
    # Edge → fetch src and dst node bundles (each with doc+chunks); merge context into one composite bundle
    src_id, dst_id = erow.get("src_id"), erow.get("dst_id")
    src_node, _ = _get_by_id("graph", src_id, key="id", select="id,doc_id,ntype,label,page,data")
    dst_node, _ = _get_by_id("graph", dst_id, key="id", select="id,doc_id,ntype,label,page,data")
    src_bundle = _bundle_from_graph_row(src_node[0]) if src_node else _make_bundle()
    dst_bundle = _bundle_from_graph_row(dst_node[0]) if dst_node else _make_bundle()
    # Composite: keep the edge in edges; union src/dst chunks minimally
    composite = {
        "edge": erow,
        "src": src_bundle,
        "dst": dst_bundle,
    }
    # Validate top-level presence (not traditional bundle)
    composite["__sme_ok__"] = bool(src_node or dst_node)
    composite["__sme_issues__"] = [] if composite["__sme_ok__"] else ["edge points to missing nodes"]
    return composite

# ---------- Traversal (depth > 0)

def _traverse_from_nodes(nodes, depth=1, seen_node_ids=None, edge_limit=500):
    """Simple BFS by edges around a set of starting nodes; returns edges and nodes encountered per hop."""
    if depth <= 0 or not nodes:
        return []
    if seen_node_ids is None:
        seen_node_ids = set(n["id"] for n in nodes if n.get("id") is not None)
    frontier = list(nodes)
    results = []
    for _ in range(depth):
        next_frontier = []
        for n in frontier:
            edges, _ = _get_edges_by_node_id(n.get("id"), limit=edge_limit)
            if not edges: 
                continue
            results.extend(edges)
            # collect neighbor nodes
            for e in edges:
                for neighbor_id in (e.get("src_id"), e.get("dst_id")):
                    if neighbor_id and neighbor_id not in seen_node_ids:
                        neigh, _ = _get_by_id("graph", neighbor_id, key="id", select="id,doc_id,ntype,label,page,data")
                        if neigh:
                            seen_node_ids.add(neighbor_id)
                            next_frontier.append(neigh[0])
        frontier = next_frontier
    return results

# ---------- Main handler

def handle(table, body, **kwargs):
    echo   = {"table": table, "body": body}
    rid    = body.get("rid")
    filters= body.get("filters") or {}
    q      = body.get("q")  # lightweight text query
    depth  = int(body.get("depth", 0))
    limit  = int(body.get("limit", 100))
    stream = bool(body.get("stream", False))

    # Sanity: require either rid or filters for SME discovery — BUT we allow empty filters + q
    if not rid and not filters and not q:
        return None, "Provide 'filters' or 'q' for discovery (rid optional).", {"code": "ARGS_REQUIRED", "need": ["filters|q|rid"], "table": table}

    # ---- Build base pairs (filters + q)
    pairs = _filter_pairs(filters)
    pairs = _apply_text_query_pairs(table, q, pairs)
    pairs.append(f"limit={limit}")

    # ---- Graph SME
    if table == "graph":
        # rid takes precedence if present
        if rid:
            rows, err = _get_by_id("graph", rid, key="id", select="id,doc_id,ntype,label,page,data")
        else:
            rows, err = _get_table("graph", pairs, select="id,doc_id,ntype,label,page,data")
        if err:
            return None, "Graph query failed", {"code": "GRAPH_QUERY_FAIL", **err}

        def generator():
            if not rows:
                return
            # optional traversal by edges
            if depth > 0:
                _ = _traverse_from_nodes(rows, depth=depth)
            for row in rows:
                yield _bundle_from_graph_row(row)

        return wrap(generator(), echo=echo, stream=True) if stream else ( [ _bundle_from_graph_row(r) for r in (rows or []) ], None, None )

    # ---- Docs SME (now supports filters OR rid)
    if table == "docs":
        if rid:
            rows, err = _get_by_id("docs", rid, key="doc_id", select="doc_id,title,meta,sha256")
        else:
            rows, err = _get_table("docs", pairs, select="doc_id,title,meta,sha256")
        if err:
            return None, "Docs query failed", {"code": "DOCS_QUERY_FAIL", **err}

        def generator():
            if not rows:
                return
            # Per-doc, we may choose to traverse via its graph nodes
            for d in rows:
                # Emit one or many bundles (one per node). Stream-friendly.
                for b in _bundle_from_doc_row(d):
                    yield b

        return wrap(generator(), echo=echo, stream=True) if stream else ( [b for d in (rows or []) for b in _bundle_from_doc_row(d) ], None, None )

    # ---- Edges SME (edges-first traversal)
    if table == "edges":
        if rid:
            rows, err = _get_by_id("edges", rid, key="id", select="id,doc_id,src_id,dst_id,etype")
        else:
            rows, err = _get_table("edges", pairs, select="id,doc_id,src_id,dst_id,etype")
        if err:
            return None, "Edges query failed", {"code": "EDGES_QUERY_FAIL", **err}

        def generator():
            if not rows:
                return
            for e in rows:
                yield _bundle_from_edge_row(e)

        return wrap(generator(), echo=echo, stream=True) if stream else ( [ _bundle_from_edge_row(e) for e in (rows or []) ], None, None )

    # ---- Unknown table
    return None, "Unknown table for query (use 'graph', 'docs', or 'edges')", {"code": "BAD_TABLE", "table": table}
