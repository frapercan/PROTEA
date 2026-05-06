"""K-nearest-neighbour search — thin shim over ``protea_method.knn_search``.

The numpy + FAISS backends live in the standalone ``protea-method``
library (F2C extraction, 2026-05-07). This module is a
backwards-compatible shim so existing PROTEA call sites that import
from ``protea.core.knn_search`` keep working without changes; new
code should import directly from ``protea_method.knn_search``.

PROTEA's ``OperationTuning.numpy_query_chunk`` configuration is
forwarded to the library via the ``PROTEA_METHOD_NUMPY_QUERY_CHUNK``
environment variable on first call, so the chunk-size knob is
preserved without changing call signatures.
"""

from __future__ import annotations

import os

from protea_method.knn_search import (
    _compute_distance_matrix,
    search_knn as _lib_search_knn,
)


def _sync_chunk_env() -> None:
    """Forward PROTEA's tuning knob to the protea-method env var.

    PROTEA stores the per-chunk query count under
    ``OperationTuning.numpy_query_chunk``; protea-method reads
    ``PROTEA_METHOD_NUMPY_QUERY_CHUNK``. This helper bridges the two
    so that changes via PROTEA's tuning singleton take effect on the
    next ``search_knn`` call without forcing every caller to set the
    env var manually.
    """
    if "PROTEA_METHOD_NUMPY_QUERY_CHUNK" in os.environ:
        return
    try:
        from protea.config.tuning import get_tuning
    except Exception:
        return
    chunk = get_tuning().operation.numpy_query_chunk
    if chunk:
        os.environ["PROTEA_METHOD_NUMPY_QUERY_CHUNK"] = str(int(chunk))


def search_knn(*args, **kwargs):  # type: ignore[no-untyped-def]
    """Pass through to ``protea_method.knn_search.search_knn``.

    Syncs the PROTEA tuning knob into the env var on first call.
    """
    _sync_chunk_env()
    return _lib_search_knn(*args, **kwargs)


__all__ = ["_compute_distance_matrix", "search_knn"]
