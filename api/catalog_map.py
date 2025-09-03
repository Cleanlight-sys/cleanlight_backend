# api/catalog_map.py
"""Expose /catalog and /map (L0/L1 graph-of-graphs) with follow-up calls.
Why: lets agents explore breadth without flooding.
"""
from __future__ import annotations


from fastapi import APIRouter, HTTPException, Query
from typing import Any, Dict, List
import os


from supabase import create_client


router = APIRouter()




def _sb():
    return create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])




@router.get("/catalog")
def catalog(limit: int = Query(50, ge=1, le=500)):
    sb = _sb()
    docs = (sb.table("docs").select("doc_id,title").limit(limit).execute().data) or []
    topics = (sb.table("prototypes").select("prototype_id,topic,size").ilike("prototype_id","topic:%").order("size", desc=True).limit(50).execute().data) or []
    return {"docs": docs, "topics": topics}




@router.get("/map")
def map_tiles(doc_limit: int = 30, topic_limit: int = 30):
    sb = _sb()
    docs = (sb.table("docs").select("doc_id,title").limit(doc_limit).execute().data) or []
    topics = (sb.table("prototypes").select("prototype_id,topic,size,centroid_384").ilike("prototype_id","topic:%").order("size", desc=True).limit(topic_limit).execute().data) or []


    nodes = ([{"id": f"doc:{d['doc_id']}", "type":"doc", "title": d["title"]} for d in docs] +
             [{"id": t["prototype_id"], "type":"topic", "topic": t["topic"], "size": t["size"]} for t in topics])


    # Lightweight edges by simple coverage: if doc centroid ~ topic centroid (cos > 0.3)
    # Why: avoid heavy joins; gives coarse navigation.
    import math


    def cos(a: List[float], b: List[float]) -> float:
        s = sum(x*y for x, y in zip(a,b));
        na = math.sqrt(sum(x*x for x in a)) or 1.0; nb = math.sqrt(sum(x*x for x in b)) or 1.0
        return s/(na*nb)


    # Load doc centroids
    doc_rows = (sb.table("docs").select("doc_id,embed_384").in_("doc_id", [d["doc_id"] for d in docs]).execute().data) or []
    doc_vec = {r["doc_id"]: r.get("embed_384") for r in doc_rows}


    edges = []
    for t in topics:
        tv = t.get("centroid_384") or []
        if not tv:
            continue
        for d in docs:
            dv = doc_vec.get(d["doc_id"]) or []
            if not dv:
                continue
            w = cos(tv, dv)
            if w >= 0.30:
                edges.append({"src": t["prototype_id"], "dst": f"doc:{d['doc_id']}", "w": round(float(w), 3),
                              "next": {"path": "/query", "body": {"action":"query","table":"graph","filters": {"label": f"ilike.%{t['topic']}%", "doc_id": f"eq.{d['doc_id']}"}, "limit": 50}}})


    return {"nodes": nodes, "edges": edges}
