# handlers/hint.py

from smesvc import hints

def handle(body):
    target = body.get("target")
    return hints.get_examples(target)
