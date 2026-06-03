"""Feature registry public surface (T2B.1 + T2B.2, master plan v3.2 §24).

Importing this package activates two things:

1. The canonical :class:`CanonicalFeatureRegistry` singleton from
   :mod:`protea.core.features.registry` is built (lazily on first
   :func:`get_canonical_registry` call) and its module-level
   :data:`REGISTRY` constant is bound.
2. T2B.2 wires each feature in :data:`protea_contracts.ALL_FEATURES`
   to its legacy producer reference via
   :func:`protea.core.features._bindings.apply_canonical_bindings`,
   replacing the T2B.1 placeholder raisers.

The contract lives in :mod:`protea_contracts.feature_registry`; the
concrete implementation is the in-process registry consumed by
``parquet_export`` (T2B.2) and ``_predict_batch`` (T2B.3).
"""

from __future__ import annotations

from protea.core.features._bindings import apply_canonical_bindings
from protea.core.features.registry import (
    REGISTRY,
    CanonicalFeatureRegistry,
    get_canonical_registry,
    reset_canonical_registry,
)

# Wire the per-feature compute references onto the module-level
# REGISTRY singleton. Idempotent and import-time so any code path that
# pulls REGISTRY through this package sees a fully-bound registry.
_BOUND_FEATURE_COUNT: int = apply_canonical_bindings(REGISTRY)

__all__ = [
    "REGISTRY",
    "CanonicalFeatureRegistry",
    "apply_canonical_bindings",
    "get_canonical_registry",
    "reset_canonical_registry",
]
