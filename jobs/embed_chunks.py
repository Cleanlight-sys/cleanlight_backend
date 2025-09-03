'''
path: jobs/embed_chunks.py
"""
Embed missing vectors via backend /query API using MiniLM helper.

Why this exists:
- Avoid direct DB creds; reuse backend contract.
- Keep shapes live via backend; we only set field names.

Usage (CPU):
    python -m jobs.embed_chunks \
      --backend http://localhost:8000 \
      --table chunks \
      --limit 200 \
      --batch-size 64

Dry run:
    python -m jobs.embed_chunks --dry-run

Tables supported (defaults):
- chunks: id_field=id, text_field=text, embed_field=embedding_384
- graph:  id_field=id, text_field=label, embed_field=label_embed_384
- docs:   id_field=doc_id, text_field=title, embed_field=embed_384
- kcs:    id_field=id, text_field=q_text, embed_field=q_embed_384

NOTE: Adjust fields if your schema differs; CLI flags override defaults.
"""
'''
from __future__ import annotations

import argparse
import os
import sys
from typing import Dict, Iterable, List, Tuple

import requests

from jobs.embed_minilm import embed_texts


DEFAULTS: Dict[str, Dict[str, str]] = {
    "chunks": {"id_field": "id", "text_field": "text", "embed_field": "embedding_384"},
    "graph": {"id_field": "id", "text_field": "label", "embed_field": "label_embed_384"},
    "docs": {"id_field": "doc_id", "text_field": "title", "embed_field": "embed_384"},
    "kcs": {"id_field": "id", "text_field": "q_text", "embed_field": "q_embed_384"},
}


def _post_query(backend: str, body: dict) -> dict:
    url = backend.rstrip("/") + "/query"
    r = requests.post(url, json=body, timeout=60)
    try:
        r.raise_for_status()
    except Exception as e:  # why: fail fast with body to debug filters/fields
        msg = f"HTTP {r.status_code}: {r.text}"
        raise SystemExit(msg) from e
    return r.json()


def _fetch_missing_batch(
    backend: str,
    table: str,
    id_field: str,
    text_field: str,
    embed_field: str,
    limit: int,
) -> List[dict]:
    select = f"{id_field},{text_field}"
    # PostgREST null filter; backend merges filters_str
    filters_str = f"{embed_field}=is.null"
    body = {
        "action": "query",
        "table": table,
        "limit": limit,
        "filters_str": filters_str,
        "select": select,
    }
    resp = _post_query(backend, body)
    data = resp.get("data") or []
    # Keep only rows that actually carry text; model can embed empty strings but wasteful
    rows = [row for row in data if text_field in row]
    return rows


def _update_embedding_row(
    backend: str,
    table: str,
    rid_value,
    payload: dict,
):
    body = {
        "action": "update",
        "table": table,
        "rid": rid_value,
        "payload": payload,
    }
    return _post_query(backend, body)


def run(
    backend: str,
    table: str,
    id_field: str,
    text_field: str,
    embed_field: str,
    limit: int,
    batch_size: int,
    dry_run: bool,
) -> int:
    processed = 0
    while processed < limit:
        want = min(batch_size, limit - processed)
        rows = _fetch_missing_batch(backend, table, id_field, text_field, embed_field, want)
        if not rows:
            print("No more rows needing embeddings.")
            break

        texts = [str(r.get(text_field, "")) for r in rows]
        vecs = embed_texts(texts, batch_size=batch_size, normalize=True)

        for r, vec in zip(rows, vecs):
            rid = r[id_field]
            payload = {embed_field: vec.tolist()}
            if dry_run:
                print({"table": table, "rid": rid, "set": embed_field, "dim": len(vec)})
            else:
                _ = _update_embedding_row(backend, table, rid, payload)
        processed += len(rows)
        print(f"Processed {processed} / {limit}")

    return 0


def parse_args(argv: List[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Embed missing vectors via backend /query API")
    p.add_argument("--backend", default=os.environ.get("CLEANLIGHT_BACKEND", "http://localhost:8000"))
    p.add_argument("--table", choices=list(DEFAULTS.keys()), default="chunks")
    p.add_argument("--id-field")
    p.add_argument("--text-field")
    p.add_argument("--embed-field")
    p.add_argument("--limit", type=int, default=200)
    p.add_argument("--batch-size", type=int, default=int(os.environ.get("MINILM_BATCH", "64")))
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args(argv)


def main(argv: List[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    d = DEFAULTS[args.table]
    id_field = args.id_field or d["id_field"]
    text_field = args.text_field or d["text_field"]
    embed_field = args.embed_field or d["embed_field"]

    # why: ensure obvious misconfigurations fail early
    if not all([id_field, text_field, embed_field]):
        raise SystemExit("id/text/embed fields must be set")

    return run(
        backend=args.backend,
        table=args.table,
        id_field=id_field,
        text_field=text_field,
        embed_field=embed_field,
        limit=max(args.limit, 1),
        batch_size=max(args.batch_size, 1),
        dry_run=args.dry_run,
    )


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
