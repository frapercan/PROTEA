"""Feature registry implementation (T2B.1, master plan v3.2 §24).

Imports the canonical singleton :data:`CANONICAL_REGISTRY` lazily via
:func:`get_canonical_registry` so callers that only need a name lookup
do not pay the cost of building the compute table at import time.

The contract lives in :mod:`protea_contracts.feature_registry`; the
concrete implementation here is the in-process registry consumed by
``parquet_export`` and ``_predict_batch`` (wired in T2B.2).
"""

from __future__ import annotations

from protea.core.features.registry import (
    CanonicalFeatureRegistry,
    get_canonical_registry,
)

__all__ = [
    "CanonicalFeatureRegistry",
    "get_canonical_registry",
]
