"""K-nearest-neighbour search — thin shim over ``protea_method.knn_search``.

The numpy, FAISS, and torch backends live in the standalone
``protea-method`` library (F2C extraction, 2026-05-07). This module is a
backwards-compatible shim so existing PROTEA call sites that import
from ``protea.core.knn_search`` keep working without changes; new
code should import directly from ``protea_method.knn_search``.

PROTEA's ``OperationTuning.numpy_query_chunk`` configuration is
forwarded to the library via the ``PROTEA_METHOD_NUMPY_QUERY_CHUNK``
environment variable on first call, so the chunk-size knob is
preserved without changing call signatures.

Torch backend env vars (pass-through, no PROTEA-side config required):

- ``PROTEA_KNN_DEVICE``: ``"cuda"`` or ``"cpu"``. PROTEA defaults it to
  ``"cpu"`` rather than leaving the library's ``"auto"``, which resolves to
  CUDA whenever a card is visible. See ``_default_device_to_cpu``.
- ``PROTEA_KNN_CHUNK_SIZE``: int, default 4096. Number of query rows
  processed per GPU kernel launch.
"""

from __future__ import annotations

import logging
import os

from protea_method.knn_search import (
    _compute_distance_matrix,
)
from protea_method.knn_search import (
    search_knn as _lib_search_knn,
)

logger = logging.getLogger(__name__)


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


def _default_device_to_cpu() -> None:
    """Make CPU the device a KNN search gets when nobody chose one.

    protea-method defaults to ``"auto"``, which means CUDA whenever a card is
    visible. That default is wrong for this project in both directions.

    It is wrong on correctness. The GPU path is the only one that can run out
    of memory, and OOM recovery is where a search can come back short; it is
    also the only path where matmul precision reorders neighbours near ties,
    which the ``l2`` metric is sensitive to. Neither failure mode exists on
    CPU, so choosing CPU removes them rather than defending against them.

    It is wrong on operations. The card lives on a stateless node that reboots
    without warning, and ``"auto"`` means the absence of configuration silently
    selects the riskier device. Inverting the default makes a GPU search
    something a caller has to ask for, in writing.

    An explicit ``PROTEA_KNN_DEVICE`` still wins, because a deliberate
    experiment should be possible; it is logged so it is never silent.
    """
    chosen = os.environ.setdefault("PROTEA_KNN_DEVICE", "cpu")
    if chosen.lower() != "cpu":
        logger.warning(
            "KNN device is %r, not cpu. This is the path where an out-of-memory "
            "retry can return fewer rows than queries, and where matmul "
            "precision can reorder near-tied neighbours. Set "
            "PROTEA_KNN_DEVICE=cpu unless this run is deliberately measuring "
            "the GPU path.",
            chosen,
        )


def search_knn(*args, **kwargs):  # type: ignore[no-untyped-def]
    """Pass through to ``protea_method.knn_search.search_knn``.

    Syncs the PROTEA tuning knob into the env var on first call, and pins the
    device to CPU unless the caller asked for something else.
    """
    _sync_chunk_env()
    _default_device_to_cpu()
    return _lib_search_knn(*args, **kwargs)


__all__ = ["_compute_distance_matrix", "search_knn"]
