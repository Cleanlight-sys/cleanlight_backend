# handlers/hint.py  — return (data, error, hint)
from smesvc.hints import build_hints

def handle(body):
    body = body or {}
    try:
        # Back-compat: if legacy callers send {"target":"all"}, we still return full hints
        if body.get("target") is not None:
            hint_obj = build_hints(question=None, doc=None)
            return None, None, hint_obj

        question = body.get("question")
        doc = body.get("doc")
        hint_obj = build_hints(question=question, doc=doc)
        return None, None, hint_obj
    except Exception as e:
        # Do NOT raise; return a structured error so the route can envelope it
        return None, f"/hint failed: {e.__class__.__name__}: {e}", None
