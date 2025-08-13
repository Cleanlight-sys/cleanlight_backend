# laws.py — Cleanlight Unified Law Enforcement (full hint-enabled version)

import re
import base64
from datetime import datetime
import zstandard as zstd

from db import get_allowed_tags
from codec import decode_smart10k

class CleanlightLawError(Exception):
    def __init__(self, message, hint=None):
        super().__init__(message)
        self.hint = hint

# ===== Canvas =====
def enforce_canvas_laws(payload: dict, system_delta: bool = False) -> None:
    if not isinstance(payload, dict):
        raise CleanlightLawError("Payload must be an object.",
                                 hint="Send a JSON object with field values.")

    tags      = payload.get("tags", [])
    codex     = payload.get("codex", "") or ""
    mir       = payload.get("mir", "") or ""
    insight   = payload.get("insight", "") or ""
    cognition = payload.get("cognition", None)
    reference = payload.get("reference", "") or ""
    images    = payload.get("images", None)

    _enforce_canonical_tags(tags)
    _enforce_insight(insight)

    if cognition is not None and not system_delta:
        raise CleanlightLawError("Cognition writes require system_delta=true.",
                                 hint="Add 'system_delta': true to your request if writing cognition.")

    _enforce_fact_reason_separation(codex, mir, insight)
    _enforce_reference(reference, insight)
    _enforce_images(images)

def _enforce_canonical_tags(tags):
    if tags is None: return
    if not isinstance(tags, list):
        raise CleanlightLawError("tags must be an array.",
                                 hint="Wrap tag values in a JSON array.")
    allowed = set(get_allowed_tags())
    bad = [t for t in tags if t not in allowed]
    if bad:
        suggestions = [t.lower().replace(" ", "_") for t in bad]
        raise CleanlightLawError(f"Canonical Tag Law: unknown tag(s): {bad}",
                                 hint=f"Use one of: {sorted(allowed)} or try: {suggestions}")

def _compressed_size_bytes(text: str) -> int:
    cctx = zstd.ZstdCompressor()
    return len(cctx.compress(text.encode("utf-8")))

def _semantic_depth_score(text: str) -> float:
    toks = text.lower().split()
    if not toks: return 0.0
    uniq = len(set(toks)) / len(toks)
    connectors = sum(toks.count(c) for c in ["because","therefore","however","if","then"])
    tech = sum(1 for t in toks if len(t) > 8)
    return uniq*0.5 + (connectors/len(toks))*2 + (tech/len(toks))*3

def _enforce_insight(insight: str):
    if not isinstance(insight, str) or not insight.strip():
        raise CleanlightLawError("Insight is required and cannot be empty.",
                                 hint="Provide reasoning text in the 'insight' field.")
    if _compressed_size_bytes(insight) > 3*1024:
        raise CleanlightLawError("Insight exceeds 3KB compressed.",
                                 hint="Shorten your insight text or split into multiple entries.")
    if _semantic_depth_score(insight) < 0.2:
        raise CleanlightLawError("Insight fails depth check (too shallow/repetitive).",
                                 hint="Add more variety, technical terms, or logical connectors.")

def _enforce_fact_reason_separation(codex: str, mir: str, insight: str):
    reason_terms = ["because","therefore","thus","suggests","likely","uncertain","hypothesis"]
    fact_patterns = [r"\b(is|are|was|were)\b", r"\b(measured|recorded|observed)\b", r"\b\d{4}\b"]
    if any(t in (codex or "").lower() for t in reason_terms):
        raise CleanlightLawError("Codex contains reasoning language.",
                                 hint="Move reasoning words into the 'insight' field.")
    il = (insight or "").lower()
    if any(re.search(p, il) for p in fact_patterns):
        raise CleanlightLawError("Insight contains factual statements.",
                                 hint="Move factual data into the 'codex' field.")

def _enforce_reference(reference: str, insight: str):
    if reference and _compressed_size_bytes(reference) > 5*1024:
        raise CleanlightLawError("Reference exceeds 5KB compressed.",
                                 hint="Shorten or compress your reference text.")

def _try_base64_decode(s: str) -> bool:
    try: base64.b64decode(s); return True
    except Exception: return False

def _try_std10k_decode(s: str) -> bool:
    try: decode_smart10k(s); return True
    except Exception: return False

def _enforce_images(images):
    if images is None: return
    items = images if isinstance(images, list) else [images]
    for idx, v in enumerate(items):
        if not isinstance(v, str):
            raise CleanlightLawError(f"images[{idx}] must be string (base64 or smart10k).",
                                     hint="Convert image to base64 before sending.")
        if not (_try_base64_decode(v) or _try_std10k_decode(v)):
            raise CleanlightLawError(f"images[{idx}] invalid encoding.",
                                     hint="Ensure images are encoded in base64 or smart10k format.")
        if len(v) > 10_000_000:
            raise CleanlightLawError(f"images[{idx}] too large.",
                                     hint="Resize or compress the image below 10MB.")

# ===== Tags =====
_TAG_RE = re.compile(r"^[a-z0-9_]+$")

def enforce_tag_laws(payload: dict, action: str, allow_delete: bool = False) -> None:
    if action == "delete":
        if not allow_delete:
            raise CleanlightLawError("Tag deletion requires allow_delete=true.",
                                     hint="Add 'allow_delete': true to your request.")
        return
    if not isinstance(payload, dict):
        raise CleanlightLawError("Payload must be an object.",
                                 hint="Send a JSON object with tag, description, and created_by.")
    tag = payload.get("tag",""); desc = payload.get("description",""); who = payload.get("created_by","")
    if not isinstance(tag, str) or not _TAG_RE.fullmatch(tag):
        raise CleanlightLawError("Tag must be lowercase alphanumeric + underscores.",
                                 hint="Example valid tag: 'system_delta'")
    if not isinstance(desc, str) or len(desc.strip()) < 10:
        raise CleanlightLawError("Tag description must be ≥ 10 characters.",
                                 hint="Write a longer description for the tag.")
    if not isinstance(who, str) or not who.strip():
        raise CleanlightLawError("created_by must be present and non-empty.",
                                 hint="Set created_by to your username or system name.")
    if not payload.get("created_at"):
        payload["created_at"] = datetime.utcnow().isoformat()
