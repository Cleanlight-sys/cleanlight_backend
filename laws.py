# laws.py — Cleanlight Unified Law Enforcement

import re
import base64
from datetime import datetime
import zstandard as zstd

from db import get_allowed_tags
from codec import decode_smart10k

class CleanlightLawError(Exception): pass

# ===== Canvas =====
def enforce_canvas_laws(payload: dict, system_delta: bool = False) -> None:
    if not isinstance(payload, dict):
        raise CleanlightLawError("Payload must be an object.")

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
        raise CleanlightLawError("Cognition writes require system_delta=true.")

    _enforce_fact_reason_separation(codex, mir, insight)
    _enforce_reference(reference, insight)
    _enforce_images(images)

def _enforce_canonical_tags(tags):
    if tags is None: return
    if not isinstance(tags, list):
        raise CleanlightLawError("tags must be an array.")
    allowed = set(get_allowed_tags())
    bad = [t for t in tags if t not in allowed]
    if bad:
        raise CleanlightLawError(f"Canonical Tag Law: unknown tag(s): {bad}")

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
        raise CleanlightLawError("Insight is required and cannot be empty.")
    if _compressed_size_bytes(insight) > 3*1024:
        raise CleanlightLawError("Insight exceeds 3KB compressed.")
    if _semantic_depth_score(insight) < 0.2:
        raise CleanlightLawError("Insight fails depth check (too shallow/repetitive).")

def _enforce_fact_reason_separation(codex: str, mir: str, insight: str):
    reason_terms = ["because","therefore","thus","suggests","likely","uncertain","hypothesis"]
    fact_patterns = [r"\b(is|are|was|were)\b", r"\b(measured|recorded|observed)\b", r"\b\d{4}\b"]
    if any(t in (codex or "").lower() for t in reason_terms):
        raise CleanlightLawError("Codex contains reasoning language.")
    il = (insight or "").lower()
    if any(re.search(p, il) for p in fact_patterns):
        raise CleanlightLawError("Insight contains factual statements.")

def _enforce_reference(reference: str, insight: str):
    if reference and _compressed_size_bytes(reference) > 5*1024:
        raise CleanlightLawError("Reference exceeds 5KB compressed.")

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
            raise CleanlightLawError(f"images[{idx}] must be string (base64 or smart10k).")
        if not (_try_base64_decode(v) or _try_std10k_decode(v)):
            raise CleanlightLawError(f"images[{idx}] invalid encoding.")
        if len(v) > 10_000_000:  # crude guard
            raise CleanlightLawError(f"images[{idx}] too large.")

# ===== Tags =====
_TAG_RE = re.compile(r"^[a-z0-9_]+$")

def enforce_tag_laws(payload: dict, action: str, allow_delete: bool = False) -> None:
    if action == "delete":
        if not allow_delete:
            raise CleanlightLawError("Tag deletion requires allow_delete=true.")
        return
    if not isinstance(payload, dict):
        raise CleanlightLawError("Payload must be an object.")
    tag = payload.get("tag",""); desc = payload.get("description",""); who = payload.get("created_by","")
    if not isinstance(tag, str) or not _TAG_RE.fullmatch(tag):
        raise CleanlightLawError("Tag must be lowercase alphanumeric + underscores.")
    if not isinstance(desc, str) or len(desc.strip()) < 10:
        raise CleanlightLawError("Tag description must be ≥ 10 characters.")
    if not isinstance(who, str) or not who.strip():
        raise CleanlightLawError("created_by must be present and non-empty.")
    if not payload.get("created_at"):
        payload["created_at"] = datetime.utcnow().isoformat()
