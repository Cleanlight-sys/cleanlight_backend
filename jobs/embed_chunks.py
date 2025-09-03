# jobs/embed_chunks.py
            q = q.filter(col, op.replace("=", ""), val)
        r = q.execute(); rows = getattr(r, "data", None) or r.get("data") or []
        if not rows:
            break
        yield rows
        start += batch




def upsert_embeddings(sb: Client, table: str, key_cols: List[str], rows: List[Dict[str, Any]]):
    if not rows:
        return
    res = sb.table(table).upsert(rows, on_conflict=",".join(key_cols), returning="minimal").execute()
    if getattr(res, "error", None):
        raise RuntimeError(str(res.error))




def run(target: List[str], provider: str, batch: int) -> None:
    sb = _get_client()


    if "chunks" in target:
        for page in _iter_rows(sb, "chunks", ["id","doc_id","text" ,"embedding_384"], where="embedding_384.is.null", batch=batch):
            texts = [f"{r['doc_id']} | {r['text'] or ''}" for r in page]
            vecs = embed_texts(texts, provider)
            upsert_embeddings(sb, "chunks", ["id","doc_id"], [
                {"id": r["id"], "doc_id": r["doc_id"], "embedding_384": v} for r, v in zip(page, vecs)
            ])


    if "graph" in target:
        for page in _iter_rows(sb, "graph", ["id","doc_id","label","label_embed_384"], where="label_embed_384.is.null", batch=batch):
            texts = [r.get("label") or "" for r in page]
            vecs = embed_texts(texts, provider)
            upsert_embeddings(sb, "graph", ["id","doc_id"], [
                {"id": r["id"], "doc_id": r["doc_id"], "label_embed_384": v} for r, v in zip(page, vecs)
            ])


    if "kcs" in target:
        for page in _iter_rows(sb, "kcs", ["id","doc_id","q","q_embed_384"], where="q_embed_384.is.null", batch=batch):
            texts = [r.get("q") or "" for r in page]
            vecs = embed_texts(texts, provider)
            upsert_embeddings(sb, "kcs", ["id","doc_id"], [
                {"id": r["id"], "doc_id": r["doc_id"], "q_embed_384": v} for r, v in zip(page, vecs)
            ])


    if "images" in target:
        for page in _iter_rows(sb, "images", ["id","doc_id","caption","caption_embed_384"], where="caption_embed_384.is.null", batch=batch):
            texts = [r.get("caption") or "" for r in page]
            vecs = embed_texts(texts, provider)
            upsert_embeddings(sb, "images", ["id","doc_id"], [
                {"id": r["id"], "doc_id": r["doc_id"], "caption_embed_384": v} for r, v in zip(page, vecs)
            ])




if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", nargs="+", default=["chunks","graph","kcs","images"])
    ap.add_argument("--provider", choices=["minilm","openai"], default="minilm")
    ap.add_argument("--batch", type=int, default=512)
    args = ap.parse_args()
    run(args.target, args.provider, args.batch)
