from flask import Flask, request, jsonify, Response, stream_with_context, make_response
from flask_cors import CORS
from datetime import datetime, timezone
import os, requests, json

import handlers.read_all as read_all
import handlers.read_rows as read_row
import handlers.write as write
import handlers.update as update
import handlers.delete as delete
import handlers.query as query
import handlers.hint as hint

from config import SUPABASE_URL, HEADERS, TABLE_KEYS, wrap
from schema import build_spec

app = Flask(__name__)
CORS(app)

# --- Helpers ---
def _now(): 
    return datetime.now(timezone.utc).isoformat()

# --- Schema endpoints ---
@app.get("/openapi.json")
@app.get("/openai.json")  # alias

#"""OpenAI Action shim: forwards body directly to handlers.query.handle.
#- Accepts filters, filters_str, chunk_text_max.
#- Keeps Action payloads small and safe.
#"""

# from __future__ import annotations

from typing import Any, Dict, Optional, Literal
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from handlers.query import handle

router = APIRouter()

class QueryBody(BaseModel):
    action: Literal["query"]
    table: Literal["docs","chunks","graph","edges","images","kcs","bundle"]
    q: Optional[str] = None
    limit: Optional[int] = Field(50, ge=1, le=500)
    filters: Optional[Dict[str, Any]] = None
    filters_str: Optional[str] = None
    chunk_text_max: Optional[int] = Field(600, ge=64, le=5000)

class AskBody(BaseModel):
    question: str = Field(..., min_length=2)
    doc: Optional[str] = Field(None, description="doc_id or title pattern (e.g., %millinery%)")
    top_k: int = Field(8, ge=1, le=50)
    chunk_text_max: int = Field(600, ge=64, le=5000)
    with_kcs: bool = True

def _doc_filter_to_filters_str(doc: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    if not doc:
        return None, None
    s = doc.strip()
    if len(s) in (32, 40, 64) and all(c in "0123456789abcdef" for c in s.lower()):
        return f"doc_id=eq.{s}", s
    if "%" in s or "*" in s:
        pattern = s.replace("*", "%")
        docs, _err, _ = handle("docs", {"q": pattern.strip("%"), "limit": 1})
        if docs:
            return f"doc_id=eq.{docs[0]['doc_id']}", docs[0]["doc_id"]
        return f"doc_id=ilike.{pattern}", None
    docs, _err, _ = handle("docs", {"q": s, "limit": 1})
    if docs:
        return f"doc_id=eq.{docs[0]['doc_id']}", docs[0]["doc_id"]
    return f"doc_id=ilike.%{s}%", None

def _load_titles(doc_ids: List[str]) -> Dict[str, str]:
    titles: Dict[str, str] = {}
    for did in set([d for d in doc_ids if d]):
        rows, _err, _ = handle("docs", {"filters": {"doc_id": f"eq.{did}"}, "limit": 1})
        if rows:
            titles[did] = rows[0].get("title") or did
    return titles

def _cite_line(title: str, c: Dict[str, Any]) -> str:
    pf, pt = c.get("page_from"), c.get("page_to")
    page = f"p.{pf}" if pf == pt else f"p.{pf}–{pt}"
    text = (c.get("text") or "").replace("\n", " ")
    if len(text) > 180:
        text = text[:180] + "…"
    return f"- [{title}, {page}] {text}"

@router.post("/query")
def query(body: QueryBody):
    payload = body.dict(exclude_none=True)
    payload.pop("action", None)
    table = payload.pop("table")
    data, err, meta = handle(table, payload)
    if err:
        raise HTTPException(400, err)
    return {"data": data, "meta": meta}

@router.post("/ask")
def ask(body: AskBody):
    filters_str, resolved_doc_id = _doc_filter_to_filters_str(body.doc)
    chunk_payload: Dict[str, Any] = {
        "q": body.question,
        "limit": body.top_k,
        "chunk_text_max": body.chunk_text_max,
    }
    if filters_str:
        chunk_payload["filters_str"] = filters_str
    chunks, err, _ = handle("chunks", chunk_payload)
    if err:
        raise HTTPException(400, f"chunks error: {err}")
    kcs: List[Dict[str, Any]] = []
    if body.with_kcs:
        kc_payload: Dict[str, Any] = {"q": body.question, "limit": body.top_k}
        if filters_str:
            kc_payload["filters_str"] = filters_str
        kcs, _err2, _ = handle("kcs", kc_payload)
    doc_titles = _load_titles([c.get("doc_id") for c in chunks])
    citations = "\n".join(_cite_line(doc_titles.get(c.get("doc_id"), c.get("doc_id","")), c) for c in chunks)
    kc_lines = "\n".join(f"- Q: {k.get('q')} | A_ref: {k.get('a_ref')} (p.{k.get('page_hint')})" for k in (kcs or []) if k)
    prompt = (
        "You are a precise subject-matter expert.\n"
        "Answer the question using ONLY the provided sources.\n"
        "Cite like [Title, p.X–Y]. If unclear, say what is missing.\n\n"
        f"Question: {body.question}\n\nSources:\n" + citations + ("\n\nKnowledge Cards:\n" + kc_lines if kc_lines else "")
    )
    return {
        "question": body.question,
        "doc_filter": body.doc,
        "resolved_doc_id": resolved_doc_id,
        "top_k": body.top_k,
        "chunks": chunks,
        "kcs": kcs,
        "compose_prompt": prompt,
    }

@router.post("/query")
def query(body: QueryBody):
    payload = body.dict(exclude_none=True)
    payload.pop("action", None)
    table = payload.pop("table")
    data, err, meta = handle(table, payload)
    if err:
        raise HTTPException(400, err)
    return {"data": data, "meta": meta}

def serve_openapi():
    """
    Serve OpenAPI schema dynamically (rebuilt from schema/ each time).
    """
    spec = build_spec()
    spec["servers"] = [{"url": os.getenv("RENDER_EXTERNAL_URL", "http://localhost:8000")}]
    response = make_response(jsonify(spec))
    response.headers["Content-Type"] = "application/json"
    return response

@app.get("/health")
def health():
    return {"status": "ok", "time": _now()}

# --- Query dispatch ---
@app.post("/query")
def query_gate():
    """
    Unified query gateway: routes CRUD + SME queries to handlers,
    always wrapped through config.wrap() for consistent responses.
    """
    body = request.json or {}
    action = body.get("action")
    table  = body.get("table")
    stream = body.get("stream", False)

    dispatch = {
        "read_all": read_all.handle,
        "read_row": read_row.handle,
        "write": write.handle,
        "update": update.handle,
        "delete": delete.handle,
    }

    data, error, hint_txt = None, None, None

    if action in dispatch:
        data, error, hint_txt = dispatch[action](table, body)
        return wrap(data, body, hint_txt, error, stream=stream)

    if action == "query":
        data, error, hint_txt = query.handle(table, body)
        return wrap(data, body, hint_txt, error, stream=stream)

    if action == "hint":
        data, error, hint_txt = hint.handle(body)
        return wrap(data, body, hint_txt, error, stream=stream)

    # --- Auto-hint fallback ---
    data, error, hint_txt = hint.handle({"target": "all"})
    return wrap(data, body, hint_txt, error), 400

@app.post("/hint")
def hint_gate():
    body = request.json or {}
    data, error, hint_txt = hint.handle(body)
    return wrap(data, body, hint_txt, error)

if __name__ == "__main__":
    app.run(debug=True, port=8000)





