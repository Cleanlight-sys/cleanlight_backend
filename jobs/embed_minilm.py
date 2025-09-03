# ===============================
# path: jobs/embed_minilm.py
# ===============================
"""MiniLM embedding helper for Cleanlight jobs.

This module provides a small, dependency-light wrapper around
`sentence-transformers/all-MiniLM-L6-v2` (384 dims). It exposes a
single import `embed_texts()` and a CLI for batch embedding files.

Example (Python):
    from jobs.embed_minilm import embed_texts
    vecs = embed_texts(["hello world", "tanning chemistry"], batch_size=64)
    # vecs: numpy.ndarray of shape (2, 384)

Example (CLI, JSONL in/out):
    python -m jobs.embed_minilm \
        --input chunks.jsonl --input-format jsonl --text-field text \
        --output embeds.jsonl --output-format jsonl --batch-size 128

Example (CLI, TXT to NPY):
    python -m jobs.embed_minilm \
        --input lines.txt --input-format txt \
        --output vectors.npy --output-format npy

Why a tiny wrapper?
- Avoids repeated model loads across jobs.
- Consistent normalization.
- Predictable batching and error handling for dirty text.
"""
from __future__ import annotations

import argparse
import os
import sys
import math
from dataclasses import dataclass
from typing import Iterable, List, Optional, Tuple

try:
    import orjson as jsonlib  # faster if available
    _USE_ORJSON = True
except Exception:  # pragma: no cover - fallback
    import json as jsonlib  # type: ignore
    _USE_ORJSON = False

import numpy as np
from tqdm import tqdm

# Lazy model import so `--help` is fast.
_SENTENCE_TRANSFORMERS = None
_MODEL = None


@dataclass(frozen=True)
class MiniLMConfig:
    model_name: str = os.environ.get("MINILM_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
    device: str = os.environ.get("MINILM_DEVICE", "cpu")  # "cpu" or "cuda"
    batch_size: int = int(os.environ.get("MINILM_BATCH", "64"))
    normalize: bool = os.environ.get("MINILM_NORM", "1") not in {"0", "false", "False"}


def _load_model(cfg: MiniLMConfig):
    global _SENTENCE_TRANSFORMERS, _MODEL
    if _MODEL is not None:
        return _MODEL
    # Import lazily here to keep module import cheap
    from sentence_transformers import SentenceTransformer  # type: ignore
    _SENTENCE_TRANSFORMERS = SentenceTransformer
    _MODEL = SentenceTransformer(cfg.model_name, device=cfg.device)
    return _MODEL


def _l2_normalize(x: np.ndarray, eps: float = 1e-12) -> np.ndarray:
    # Normalize rows to unit length; guard zero vectors
    norms = np.linalg.norm(x, ord=2, axis=1, keepdims=True)
    norms = np.maximum(norms, eps)
    return x / norms


def _clean_text(s: Optional[str]) -> str:
    # Keep it simple; avoid None and massive whitespace
    if s is None:
        return ""
    s = str(s)
    return " ".join(s.split())


def embed_texts(texts: Iterable[str], batch_size: int = 64, normalize: bool = True,
                model_name: Optional[str] = None, device: Optional[str] = None) -> np.ndarray:
    """Embed a collection of texts with MiniLM.

    Parameters
    ----------
    texts : Iterable[str]
        Strings to embed.
    batch_size : int
        Per-forward pass batch size.
    normalize : bool
        If True, L2-normalize each embedding row.
    model_name : Optional[str]
        Override model name (defaults to env / config).
    device : Optional[str]
        Override device (cpu/cuda) (defaults to env / config).

    Returns
    -------
    np.ndarray
        Array of shape (N, 384) dtype=float32
    """
    base_cfg = MiniLMConfig()
    if model_name is not None or device is not None or batch_size != base_cfg.batch_size or normalize != base_cfg.normalize:
        cfg = MiniLMConfig(
            model_name=model_name or base_cfg.model_name,
            device=device or base_cfg.device,
            batch_size=batch_size,
            normalize=normalize,
        )
    else:
        cfg = base_cfg

    model = _load_model(cfg)

    # Pre-clean and materialize to a list for length and batching
    items: List[str] = [_clean_text(t) for t in texts]
    n = len(items)
    if n == 0:
        return np.empty((0, 384), dtype=np.float32)

    out = np.empty((n, 384), dtype=np.float32)
    for start in range(0, n, cfg.batch_size):
        end = min(start + cfg.batch_size, n)
        batch = items[start:end]
        # sentence-transformers handles empty strings but returns a valid vector
        vecs = model.encode(batch, batch_size=len(batch), convert_to_numpy=True, normalize_embeddings=False)
        out[start:end, :] = vecs.astype(np.float32, copy=False)

    if cfg.normalize:
        out = _l2_normalize(out)
    return out


# -------------------------
# CLI
# -------------------------

def _read_lines_txt(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8") as f:
        return [ln.rstrip("\n") for ln in f]


def _read_lines_jsonl(path: str, field: str) -> List[str]:
    texts: List[str] = []
    with open(path, "rb") as f:
        for raw in f:
            if not raw.strip():
                continue
            obj = jsonlib.loads(raw)
            texts.append(_clean_text(obj.get(field)))
    return texts


def _write_jsonl_vectors(path: str, vectors: np.ndarray, start_index: int = 0) -> None:
    with open(path, "wb") as f:
        for i in range(vectors.shape[0]):
            row = {"i": i + start_index, "embedding": vectors[i].tolist()}
            data = jsonlib.dumps(row)
            if _USE_ORJSON:
                f.write(data)
            else:
                f.write(data.encode("utf-8"))
            f.write(b"\n")


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="MiniLM embedder (TXT/JSONL → JSONL/NPY)")
    p.add_argument("--input", required=True, help="Input file path (txt or jsonl)")
    p.add_argument("--input-format", choices=["txt", "jsonl"], required=True)
    p.add_argument("--text-field", default="text", help="Field name for JSONL input")

    p.add_argument("--output", required=True, help="Output file path (.jsonl or .npy)")
    p.add_argument("--output-format", choices=["jsonl", "npy"], required=True)

    p.add_argument("--batch-size", type=int, default=64)
    p.add_argument("--model-name", default=None)
    p.add_argument("--device", default=None, choices=[None, "cpu", "cuda"])
    p.add_argument("--no-normalize", action="store_true", help="Disable L2 normalization")
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)
    normalize = not args.no_normalize

    if args.input_format == "txt":
        texts = _read_lines_txt(args.input)
    else:
        texts = _read_lines_jsonl(args.input, args.text_field)

    vecs = embed_texts(
        texts,
        batch_size=args.batch_size,
        normalize=normalize,
        model_name=args.model_name,
        device=args.device,
    )

    if args.output_format == "jsonl":
        _write_jsonl_vectors(args.output, vecs)
    else:
        np.save(args.output, vecs)

    # Print a tiny report to stderr (why: quick sanity check)
    print(f"Embedded {vecs.shape[0]} texts → {vecs.shape[1]} dims; normalize={normalize}", file=sys.stderr)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
