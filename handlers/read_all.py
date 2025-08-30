import requests, json
from flask import jsonify, Response, stream_with_context
from Cleanlight_bk import wrap, SUPABASE_URL, HEADERS

def handle(table, body):
    select = body.get("select", "*")
    filters = body.get("filters") or {}
    stream  = body.get("stream", False)
    limit   = int(body.get("limit", 100))

    # Build query string
    qs = []
    for k, v in filters.items():
        qs.append(f"{k}={v}")
    if not stream:
        qs.append(f"limit={limit}")
    filter_qs = "&" + "&".join(qs) if qs else ""
    url = f"{SUPABASE_URL}/rest/v1/{table}?select={select}{filter_qs}"

    if stream:
        r = requests.get(url, headers=HEADERS, stream=True)

        def generate():
            yield '{"data":['
            first = True
            for chunk in r.iter_content(chunk_size=None):
                if chunk:
                    if not first:
                        yield ","
                    yield chunk.decode("utf-8")
                    first = False
            yield '], "echo":' + json.dumps({"original_body": body}) + '}'

        return Response(stream_with_context(generate()), mimetype="application/json")

    else:
        r = requests.get(url, headers=HEADERS)
        if r.status_code != 200:
            return jsonify(
                wrap(None, body, "Supabase error", {"code": "READ_FAIL", "detail": r.text})
            ), 500
        return jsonify(wrap(r.json(), body))
