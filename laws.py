# laws.py — Cleanlight Unified Law Enforcement (patched for graph.bundle & partial updates)

import re
import base64
from datetime import datetime
import zstandard as zstd
import json as pyjson
from codec import decode_smart10k

class CleanlightLawError(Exception):
    def __init__(self, message, hint=None, law=None, field=None, code=None):
        super().__init__(message)
        self.hint = hint
        self.law = law
        self.field = field
        self.code = code or "LAW_VIOLATION"

def get_allowed_tags():
    from db import read_column
    rows = read_column("cleanlight_tags", "tag", "tag")
    return [r["value"] for r in rows if "value" in r]

# ===== Canvas =====
def enforce_canvas_laws(payload: dict, system_delta: bool = False, mode: str = "insert") -> None:
    """
    mode: "insert" | "update" | "append"
    - insert: strict (MIR & INSIGHT required)
    - update/append: partial allowed (no MIR/INSIGHT requirement unless provided)
    """
    if not isinstance(payload, dict):
        raise CleanlightLawError("Payload must be an object.",
                                 hint="Send a JSON object with field values.",
                                 law="Structure", field=None, code="STRUCT_PAYLOAD")

    tags      = payload.get("tags", None)
    codex     = payload.get("codex", None)
    mir       = payload.get("mir", None)
    insight   = payload.get("insight", None)
    cognition = payload.get("cognition", None)
    reference = payload.get("reference", None)
    images    = payload.get("images", None)

    if tags is not None:
        _enforce_canonical_tags(tags)

    # Require MIR/INSIGHT only on full inserts
    if mode == "insert":
        if not isinstance(mir, str) or not mir.strip():
            raise CleanlightLawError(
                "MIR is required for all canvas inserts.",
                hint="Answer: what helps future agents? or what moves Meta Goal 001?",
                law="Canvas:MIR", field="mir", code="MIR_REQUIRED"
            )
        _enforce_insight(insight)

    if cognition is not None and not system_delta:
        raise CleanlightLawError("Cognition writes require system_delta=true.",
                                 hint="Add 'system_delta': true to your request if writing cognition.",
                                 law="Canvas:Cognition", field="cognition", code="COGNITION_GUARD")

    if codex is not None:
        _enforce_fact_reason_separation(codex, mir or "", insight or "")

    if reference is not None:
        _enforce_reference(reference, insight or "")

    if images is not None:
        _enforce_images(images)

def _enforce_canonical_tags(tags):
    if tags is None: return
    if not isinstance(tags, list):
        raise CleanlightLawError("tags must be an array.",
                                 hint="Wrap tag values in a JSON array.",
                                 law="Tags", field="tags", code="TAGS_ARRAY")
    allowed = set(get_allowed_tags())
    bad = [t for t in tags if t not in allowed]
    # Simple redundancy guard: block trivial placeholders
    redundant = [t for t in tags if t in {"tag", "tags", "structure_tag"}]
    if redundant:
        raise CleanlightLawError("Redundant tag(s) not allowed.",
                                 hint=f"Remove placeholders: {redundant}",
                                 law="Tags", field="tags", code="TAGS_REDUNDANT")
    # If unknown tags exist, suggest normalized candidates
    if bad:
        sugg = [str(t).lower().replace(" ", "_") for t in bad]
        raise CleanlightLawError(f"Unknown tag(s): {bad}",
                                 hint=f"Use one of: {sorted(allowed)} or try: {sugg}",
                                 law="Tags", field="tags", code="TAGS_UNKNOWN")

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

def _enforce_insight(insight: str | None):
    if not isinstance(insight, str) or not insight.strip():
        raise CleanlightLawError("Insight is required and cannot be empty.",
                                 hint="Provide reasoning text in the 'insight' field.",
                                 law="Canvas:Insight", field="insight", code="INSIGHT_REQUIRED")
    if _compressed_size_bytes(insight) > 3*1024:
        raise CleanlightLawError("Insight exceeds 3KB compressed.",
                                 hint="Shorten your insight text or split into multiple entries.",
                                 law="Canvas:Insight", field="insight", code="INSIGHT_SIZE")
    if _semantic_depth_score(insight) < 0.2:
        raise CleanlightLawError("Insight fails depth check (too shallow/repetitive).",
                                 hint="Add more variety, technical terms, or logical connectors.",
                                 law="Canvas:Insight", field="insight", code="INSIGHT_DEPTH")

def _jsonish(s: str) -> bool:
    s = s.strip()
    return (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")) or ('"ctype"' in s)

def _enforce_fact_reason_separation(codex, mir: str, insight: str):
    # Accept JSON codices (e.g., MAPC graph.bundle) without policing english tokens
    if isinstance(codex, str) and _jsonish(codex):
        # Optional: if it's JSON with ctype=graph.bundle, we explicitly allow it
        try:
            obj = pyjson.loads(codex)
            if isinstance(obj, dict) and str(obj.get("ctype","")).lower() in {"graph.bundle","graph_bundle"}:
                return
        except Exception:
            # Even if parsing fails, store-as-text is OK
            return

    # For non-JSON/plain text codex, keep the basic separation rule
    reason_terms = ["because", "therefore", "thus", "suggests", "likely", "uncertain", "hypothesis"]
    codex_text = codex if isinstance(codex, str) else str(codex)
    if any(t in codex_text.lower() for t in reason_terms):
        raise CleanlightLawError("Codex contains reasoning language.",
                                 hint="Move reasoning words into the 'insight' field or provide JSON codex.",
                                 law="Canvas:Separation", field="codex", code="CODEX_REASONING")

def _enforce_reference(reference: str, insight: str):
    if not isinstance(reference, str):
        raise CleanlightLawError("Reference must be string.",
                                 hint="Provide plain string or compressed string.",
                                 law="Canvas:Reference", field="reference", code="REF_TYPE")
    if _compressed_size_bytes(reference) > 5*1024:
        raise CleanlightLawError("Reference exceeds 5KB compressed.",
                                 hint="Shorten or compress your reference text.",
                                 law="Canvas:Reference", field="reference", code="REF_SIZE")

def _try_base64_decode(s: str) -> bool:
    try: base64.b64decode(s, validate=False); return True
    except Exception: return False

def _try_std10k_decode(s: str) -> bool:
    try: decode_smart10k(s); return True
    except Exception: return False

def _enforce_images(images):
    items = images if isinstance(images, list) else [images]
    for idx, v in enumerate(items):
        if not isinstance(v, str):
            raise CleanlightLawError(f"images[{idx}] must be string (base64/smart10k or data: URI).",
                                     hint="Convert image to base64 or pass smart10k.",
                                     law="Canvas:Images", field=f"images[{idx}]", code="IMG_TYPE")
        if not (_try_base64_decode(v) or _try_std10k_decode(v)):
            raise CleanlightLawError(f"images[{idx}] invalid encoding.",
                                     hint="Ensure images are valid base64 (URL-safe ok) or smart10k.",
                                     law="Canvas:Images", field=f"images[{idx}]", code="IMG_ENC")
        if len(v) > 15_000_000:
            raise CleanlightLawError(f"images[{idx}] too large.",
                                     hint="Resize or compress the image below 15MB.",
                                     law="Canvas:Images", field=f"images[{idx}]", code="IMG_SIZE")

# ===== Tags =====
_TAG_RE = re.compile(r"^[a-z0-9_]+$")

def enforce_tag_laws(payload: dict, action: str, allow_delete: bool = False) -> None:
    if action == "delete":
        if not allow_delete:
            raise CleanlightLawError("Tag deletion requires allow_delete=true.",
                                     hint="Add 'allow_delete': true to your request.",
                                     law="Tags", field=None, code="TAG_DELETE_GUARD")
        return
    if not isinstance(payload, dict):
        raise CleanlightLawError("Payload must be an object.",
                                 hint="Send a JSON object with tag, description, and created_by.",
                                 law="Tags", field=None, code="TAG_STRUCT")

    tag = payload.get("tag",""); desc = payload.get("description",""); who = payload.get("created_by","")
    if not isinstance(tag, str) or not _TAG_RE.fullmatch(tag):
        raise CleanlightLawError("Tag must be lowercase alphanumeric + underscores.",
                                 hint="Example valid tag: 'system_delta'",
                                 law="Tags", field="tag", code="TAG_FORMAT")
    if not isinstance(desc, str) or len(desc.strip()) < 10:
        raise CleanlightLawError("Tag description must be ≥ 10 characters.",
                                 hint="Write a longer description for the tag.",
                                 law="Tags", field="description", code="TAG_DESC")
    if not isinstance(who, str) or not who.strip():
        raise CleanlightLawError("created_by must be present and non-empty.",
                                 hint="Set created_by to your username or system name.",
                                 law="Tags", field="created_by", code="TAG_CREATOR")
    if not payload.get("created_at"):
        payload["created_at"] = datetime.utcnow().isoformat()
