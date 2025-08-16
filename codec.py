# codec.py — Cleanlight compression/decompression utilities

import base64
import zlib
import zstandard as zstd
import re

# ---------- Alphabets ----------
def _is_nonchar(cp): return (0xFDD0 <= cp <= 0xFDEF) or (cp & 0xFFFE) == 0xFFFE
def _is_surrogate(cp): return 0xD800 <= cp <= 0xDFFF

def get_base_alphabet(n: int, safe: bool = True) -> str:
    out = []
    for cp in range(0x21 if safe else 0x20, 0x2FFFF):
        if _is_surrogate(cp) or _is_nonchar(cp):
            continue
        ch = chr(cp)
        if safe and ch in {'"', "'", "\\"}:
            continue
        out.append(ch)
        if len(out) == n:
            break
    return ''.join(out)

BASE1K = get_base_alphabet(1000)
BASE10K = get_base_alphabet(10000)
LEGACY_BASE1K = get_base_alphabet(1000, safe=False)
LEGACY_BASE10K = get_base_alphabet(10000, safe=False)

# ---------- BaseN ----------
def int_to_baseN(num: int, alphabet: str) -> str:
    if num == 0: return alphabet[0]
    base = len(alphabet); digits = []
    while num:
        digits.append(alphabet[num % base])
        num //= base
    return ''.join(reversed(digits))

def baseN_to_int(s: str, alphabet: str) -> int:
    amap = {ch: i for i, ch in enumerate(alphabet)}
    base = len(alphabet); num = 0
    for ch in s: num = num * base + amap[ch]
    return num

# ---------- std64 (zlib+base64) ----------
def encode_std64(data: bytes) -> str:
    return base64.b64encode(zlib.compress(data)).decode("ascii")

def decode_std64(s: str) -> bytes:
    return zlib.decompress(base64.b64decode(s))

# ---------- smart1k (zstd+base1k) ----------
def encode_smart1k(text: str) -> str:
    cctx = zstd.ZstdCompressor()
    return int_to_baseN(int.from_bytes(cctx.compress(text.encode("utf-8")), "big"), BASE1K)

def decode_smart1k(s: str) -> str:
    for alph in (BASE1K, LEGACY_BASE1K):
        try:
            as_int = baseN_to_int(s, alph)
            comp = as_int.to_bytes((as_int.bit_length() + 7)//8, "big")
            return zstd.ZstdDecompressor().decompress(comp).decode("utf-8")
        except Exception:
            continue
    return s

# ---------- smart10k (zstd+base10k) ----------
def encode_smart10k(data: bytes) -> str:
    cctx = zstd.ZstdCompressor()
    return int_to_baseN(int.from_bytes(cctx.compress(data), "big"), BASE10K)

def decode_smart10k(s: str) -> bytes:
    for alph in (BASE10K, LEGACY_BASE10K):
        try:
            as_int = baseN_to_int(s, alph)
            comp = as_int.to_bytes((as_int.bit_length() + 7)//8, "big")
            return zstd.ZstdDecompressor().decompress(comp)
        except Exception:
            continue
    return s.encode("utf-8", errors="ignore")

# ---------- Heuristics ----------
def looks_like_baseN(s: str, alphabets=(BASE1K, LEGACY_BASE1K, BASE10K, LEGACY_BASE10K)) -> bool:
    if not isinstance(s, str) or len(s) < 8:
        return False
    return all(ch in alphabets[0] or ch in alphabets[1] or ch in alphabets[2] or ch in alphabets[3] for ch in s)

# ---------- Field-aware encoding (multi-image) ----------
_PASS_THROUGH = {
    "id", "cognition", "tag", "description", "created_by",
    "created_at", "updated_at", "archived_at",
    "tags",
}

_DATA_URI_RE = re.compile(r"^data:.*?;base64,", re.IGNORECASE)

def _normalize_b64(s: str) -> str:
    if not isinstance(s, str):
        raise ValueError("Image item must be string")
    # Strip data URI prefix if present
    s = _DATA_URI_RE.sub("", s)
    # Remove whitespace/newlines
    s = re.sub(r"\s+", "", s)
    # URL-safe → standard
    s = s.replace("-", "+").replace("_", "/")
    # Pad to multiple of 4
    pad = (4 - (len(s) % 4)) % 4
    if pad:
        s += "=" * pad
    return s

def _encode_image_item(s: str) -> str:
    # If it's already smart10k-ish and decompressible, keep as-is
    if looks_like_baseN(s):
        try:
            decode_smart10k(s)
            return s
        except Exception:
            pass
    # Otherwise treat as base64 (normalize & accept URL-safe form)
    raw = base64.b64decode(_normalize_b64(s), validate=False)
    return encode_smart10k(raw)

def _decode_image_item(s10k_str: str) -> str:
    raw = decode_smart10k(s10k_str)
    return base64.b64encode(raw).decode("ascii")

def encode_field(field: str, value):
    if field in _PASS_THROUGH:
        return value

    if field == "images":
        if value is None:
            return None
        if isinstance(value, list):
            return [_encode_image_item(v) for v in value]
        return [_encode_image_item(value)]

    return encode_smart1k(value if isinstance(value, str) else str(value))

def decode_field(field: str, value):
    if field in _PASS_THROUGH:
        return value

    if field == "images":
        if value is None: return None
        if isinstance(value, list):
            out = []
            for v in value:
                try:
                    out.append(_decode_image_item(v))
                except Exception:
                    out.append(v)
            return out
        try:
            return [_decode_image_item(value)]
        except Exception:
            return [value]

    if isinstance(value, str) and looks_like_baseN(value):
        try:
            return decode_smart1k(value)
        except Exception:
            return value

    return value
    
def validate_graph_bundle(obj) -> tuple[bool, list[str]]:
    hints = []
    try:
        if not isinstance(obj, dict):
            return False, ["codex must be JSON object or stringified JSON"]
        if obj.get("ctype") != "graph.bundle":
            hints.append("ctype != graph.bundle")
        if obj.get("schema") not in ("nl-v1",):
            hints.append("unknown schema (nl-v1 expected)")
        if not isinstance(obj.get("modules"), list):
            hints.append("modules missing or not list")
        meta = obj.get("meta", {})
        if "g_nodes" not in meta: hints.append("meta.g_nodes missing")
        if "g_edges" not in meta: hints.append("meta.g_edges missing")
        idx = meta.get("index", {})
        if "node_to_modules" not in idx:
            hints.append("meta.index.node_to_modules missing")
        return (len(hints) == 0, hints)
    except Exception as e:
        return False, [f"validator error: {e}"]
