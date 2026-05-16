"""Backward-compat shim for the legacy ``_RerankerMixin`` import path.

The reranker scoring path was extracted into the compositive
:class:`protea.core.operations.predict_go_terms._reranker_scorer.RerankerScorer`
class as part of T2B.4. This module retains the top-level symbol
exports (``load_reranker``, ``apply_reranker``, ``load_settings``,
``get_artifact_store``, ``infer_active_feature_families``) so existing
tests that patch this module path continue to work transparently
without rewriting their mocks. New code should construct
``RerankerScorer`` directly.
"""

from __future__ import annotations

from protea.core.reranker import (
    apply_reranker,
    infer_active_feature_families,
    load_reranker,
)
from protea.infrastructure.settings import load_settings
from protea.infrastructure.storage import get_artifact_store

__all__ = [
    "apply_reranker",
    "get_artifact_store",
    "infer_active_feature_families",
    "load_reranker",
    "load_settings",
]
