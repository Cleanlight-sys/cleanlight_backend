# codec.py — Cleanlight compression/decompression utilities

import base64
import zlib
import zstandard as zstd

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
    if not isinstance(s, str) or len(s) < 16: return False
    for alph in alphabets:
        if all(ch in alph for ch in s): return True
    return False

# ---------- Field-aware encoding (multi-image) ----------
def _encode_image_item(b64_str: str) -> str:
    raw = base64.b64decode(b64_str)
    return encode_smart10k(raw)

def _decode_image_item(s10k_str: str) -> str:
    raw = decode_smart10k(s10k_str)
    return base64.b64encode(raw).decode("ascii")

def encode_field(field: str, value):
    if field in ("cognition", "created_at"):
        return value  # Skip encoding for system fields
    if field == "images":
        if value is None: return None
        if isinstance(value, list): return [_encode_image_item(v) for v in value]
        return [_encode_image_item(value)]
    return encode_smart1k(value if isinstance(value, str) else str(value))

def decode_field(field: str, value):
    # Never decode system or plaintext fields
    if field in ("cognition", "created_at", "tag", "description", "created_by"):
        return value

    # Decode image(s) if present
    if field == "images":
        if value is None: return None
        if isinstance(value, list):
            out = []
            for v in value:
                try: out.append(_decode_image_item(v))
                except Exception: out.append(v)
            return out
        try: return [_decode_image_item(value)]
        except Exception: return [value]

    # Decode smart1k if it looks like baseN
    if isinstance(value, str) and looks_like_baseN(value):
        try: return decode_smart1k(value)
        except Exception: return value

    # Return as-is otherwise
    return value

