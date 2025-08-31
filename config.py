from flask import Response, stream_with_context
import json

SUPABASE_URL = "YOUR_SUPABASE_URL"
HEADERS = {"apikey": "YOUR_API_KEY"}
TABLE_KEYS = {"graph": "id", "docs": "doc_id"}

def wrap(data=None, echo=None, hint=None, error=None, stream=False):
    """
    Standard response wrapper for all handlers.
    - Always returns a consistent envelope.
    - Handles streaming if data is a generator/iterator.
    """
    if error:
        return {"error": error, "echo": echo, "hint": hint}

    # Streaming path: if data is a generator/iterator, stream as JSON array
    if stream and hasattr(data, "__iter__") and not isinstance(data, (dict, list, str, bytes)):
        def generate():
            yield '{"data":['
            first = True
            for item in data:
                if not first:
                    yield ','
                yield json.dumps(item)
                first = False
            yield ']}'
        return Response(stream_with_context(generate()), mimetype="application/json")

    # Normal path: return everything at once
    return {"data": data, "echo": echo, "hint": hint}
