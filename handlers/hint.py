# handlers/hint.py
from smesvc.hints import build_hints

def handle(body):
    # Accept optional targeting params for smarter recommendations
    question = (body or {}).get("question")
    doc = (body or {}).get("doc")
    return {"hint": build_hints(question=question, doc=doc)}
