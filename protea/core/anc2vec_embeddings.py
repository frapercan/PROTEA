"""Anc2Vec GO-term embedding index — thin shim over ``protea_method.anc2vec``.

The implementation lives in the standalone ``protea-method`` library
(F2C extraction, 2026-05-07). This module is a backwards-compatible
shim so existing PROTEA call sites that import from
``protea.core.anc2vec_embeddings`` keep working without changes; new
code should import directly from ``protea_method.anc2vec``.

The PROTEA-specific default path is exported as ``_DEFAULT_PATH`` and
used to instantiate the singleton index when ``get_index()`` is
called without arguments.
"""

from __future__ import annotations

from pathlib import Path

from protea_method.anc2vec import Anc2VecIndex
from protea_method.anc2vec import get_index as _get_index_lib

_DEFAULT_PATH = (
    Path(__file__).resolve().parents[2] / "artifacts" / "anc2vec" / "anc2vec_2020-10.npz"
)


def get_index(path: str | None = None) -> Anc2VecIndex:
    """Return a process-wide singleton index keyed by path.

    Defaults to the PROTEA repo-relative artifact at
    ``artifacts/anc2vec/anc2vec_2020-10.npz`` when no path is provided.
    """
    return _get_index_lib(path or str(_DEFAULT_PATH))


__all__ = ["Anc2VecIndex", "_DEFAULT_PATH", "get_index"]
