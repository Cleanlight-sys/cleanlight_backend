# schema/__init__.py
"""
Schema package for building and serving the OpenAPI spec.
"""

from .build import build_spec

__all__ = ["build_spec"]
