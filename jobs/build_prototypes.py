# jobs/build_prototypes.py
        for g in page:
            did = g["doc_id"]; label = (g.get("label") or "").strip()
            if not label:
                continue
            # page link
            p = g.get("page")
            v = None
            if p is not None and int(p) in pages_by_doc.get(did, {}):
                v = pages_by_doc[did][int(p)]
            # fallback: use label embedding if we lack chunk embedding on that page
            if not v:
                v = g.get(LABEL_EMBED_KEY)
            if not v:
                continue
            key = (did, label)
            if key not in label_doc_sum:
                label_doc_sum[key] = _zeros(len(v)); label_doc_cnt[key] = 0
            _vec_add(label_doc_sum[key], v); label_doc_cnt[key] += 1


    per_doc_prototypes = []
    for (did, label), vec in label_doc_sum.items():
        cnt = label_doc_cnt[(did, label)]
        label_df[label] += 1
        centroid = _norm([x/cnt for x in vec])
        per_doc_prototypes.append({
            "doc_id": did,
            "prototype_id": f"doc:{did}|topic:{label}",
            "topic": label,
            "centroid_384": centroid,
            "size": cnt
        })


    if per_doc_prototypes:
        sb.table("prototypes").upsert(per_doc_prototypes, on_conflict="prototype_id", returning="minimal").execute()


    # 3) Global topic prototypes (top-N by df)
    top_labels = {label for label, df in sorted(label_df.items(), key=lambda kv: kv[1], reverse=True)[:doc_topic_limit]}


    # Aggregate across docs
    global_sum: Dict[str, List[float]] = {}
    global_cnt: Dict[str, int] = {}
    for row in per_doc_prototypes:
        label = row["topic"]
        if label not in top_labels:
            continue
        v = row["centroid_384"]
        if label not in global_sum:
            global_sum[label] = _zeros(len(v)); global_cnt[label] = 0
        _vec_add(global_sum[label], v); global_cnt[label] += 1


    global_prototypes = [{
        "prototype_id": f"topic:{label}",
        "topic": label,
        "centroid_384": _norm([x/global_cnt[label] for x in vec]),
        "size": global_cnt[label]
    } for label, vec in global_sum.items()]


    if global_prototypes:
        sb.table("prototypes").upsert(global_prototypes, on_conflict="prototype_id", returning="minimal").execute()




if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--topics", type=int, default=200)
    ap.add_argument("--min-chunks", type=int, default=3)
    args = ap.parse_args()
    build(args.topics, args.min_chunks)
