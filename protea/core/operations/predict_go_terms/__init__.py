"""Compatibility shim: re-exports every public + test-visible name from
the split submodules so existing imports of
``protea.core.operations.predict_go_terms.*`` keep working.

T2B.6 (2026-05-16) split the monolithic 2096-LOC
``protea/core/operations/predict_go_terms.py`` into this package:

  - ``_common``            constants + dataclasses + helper functions
  - ``_coordinator``       PredictGOTermsOperation (KNN dispatcher)
  - ``_batch_op``          PredictGOTermsBatchOperation (CPU worker)
  - ``_batch_op_reference``  reference-pool loading mixin
  - ``_batch_op_feature``    feature/annotation/embedding loading mixin
  - ``_batch_op_reranker``   reranker scoring mixin
  - ``_aspect_helpers``    aspect-separated KNN pre-search + adapter prep
  - ``_store``             StorePredictionsOperation (DB writer)

Everything imported below is re-exported so external callers keep using
the canonical ``protea.core.operations.predict_go_terms`` module path.
"""

from __future__ import annotations

# External deps re-exported here so callers (and ``mock.patch`` test
# targets) keep using the canonical module path. The submodules also
# import these from their original homes, so swapping out one location
# does not silently miss the other; tests that need to swap behaviour
# should target the submodule directly.
from protea.core.feature_enricher import (
    KnnEnrichmentContext,
    enrich_v6_features,
)
from protea.core.knn_search import search_knn
from protea.core.operations._predict_go_terms_adapter import (
    AdapterInputs,
    AdapterResult,
    call_pipeline_predict,
    call_pipeline_predict_aspect_separated,
)
from protea.core.operations.predict_go_terms._aspect_helpers import (
    _AspectKnnPreSearch,
    _build_aspect_adapter_inputs,
    _load_aspect_go_map_for_hits,
    _load_aspect_separated_annotations,
)
from protea.core.operations.predict_go_terms._batch_op import (
    PredictGOTermsBatchOperation,
    _BatchExecCtx,
    _KnnResult,
    _QueryBatch,
)
from protea.core.operations.predict_go_terms._common import (
    _BATCH_QUEUE,
    _PAIR_FEATURE_KEYS,
    _REF_CACHE,
    _STORE_FLOAT_KEYS,
    _UNIFIED_REF_KEY,
    _WRITE_QUEUE,
    AspectSeparatedKnnContext,
    PredictGOTermsBatchPayload,
    PredictGOTermsPayload,
    StorePredictionsPayload,
    _clean_float,
    _RerankerBinding,
    _row_from_prediction,
    _UnifiedPredictContext,
)
from protea.core.operations.predict_go_terms._coordinator import (
    PredictGOTermsOperation,
)
from protea.core.operations.predict_go_terms._store import StorePredictionsOperation
from protea.core.pca_cache import _load_or_fit_pca_state
from protea.core.reranker import (
    EMBEDDING_PCA_DIM,
    apply_reranker,
    infer_active_feature_families,
    load_reranker,
)
from protea.infrastructure.settings import load_settings
from protea.infrastructure.storage import get_artifact_store

__all__ = (
    "AdapterInputs",
    "AdapterResult",
    "AspectSeparatedKnnContext",
    "EMBEDDING_PCA_DIM",
    "KnnEnrichmentContext",
    "PredictGOTermsBatchOperation",
    "PredictGOTermsBatchPayload",
    "PredictGOTermsOperation",
    "PredictGOTermsPayload",
    "StorePredictionsOperation",
    "StorePredictionsPayload",
    "_AspectKnnPreSearch",
    "_BATCH_QUEUE",
    "_BatchExecCtx",
    "_KnnResult",
    "_PAIR_FEATURE_KEYS",
    "_QueryBatch",
    "_REF_CACHE",
    "_RerankerBinding",
    "_STORE_FLOAT_KEYS",
    "_UNIFIED_REF_KEY",
    "_UnifiedPredictContext",
    "_WRITE_QUEUE",
    "_build_aspect_adapter_inputs",
    "_clean_float",
    "_load_aspect_go_map_for_hits",
    "_load_aspect_separated_annotations",
    "_load_or_fit_pca_state",
    "_row_from_prediction",
    "apply_reranker",
    "call_pipeline_predict",
    "call_pipeline_predict_aspect_separated",
    "enrich_v6_features",
    "get_artifact_store",
    "infer_active_feature_families",
    "load_reranker",
    "load_settings",
    "search_knn",
)
