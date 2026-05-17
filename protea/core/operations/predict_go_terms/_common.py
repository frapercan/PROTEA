"""Shared constants, parameter objects, and pure helpers.

Extracted from the monolithic ``protea/core/operations/predict_go_terms.py``
as part of T2B.6. No behaviour change; only module location.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any

import numpy as np

from protea.core.feature_enricher import NEW_V6_FEATURE_KEYS as _NEW_V6_FEATURE_KEYS
from protea.core.jsonb_dual_write import maybe_jsonb
from protea.infrastructure.orm.models.embedding.go_prediction_features import (
    build_feature_jsonb,
)

# Annotation and stream chunk sizes are configured via OperationTuning
# (annotation_chunk_size, stream_chunk_size) and resolved at call time
# inside the helpers. At 1280 dims x 2 bytes (float16) x 2000 rows
# the streaming reference query fetches ~5 MB per cursor round-trip,
# keeping Python object pressure negligible.

_BATCH_QUEUE = "protea.predictions.batch"
_WRITE_QUEUE = "protea.predictions.write"


@dataclass(frozen=True)
class _RerankerBinding:
    """Resolved RerankerModel artifact pointer + feature schema fingerprint."""

    artifact_uri: str
    feature_schema_sha: str


# GO aspect single-character codes used in GOTerm.aspect are imported from
# the canonical protea.core.domain.aspect module by the submodules that need
# them; not re-exported here to avoid cycles.

# ---------------------------------------------------------------------------
# Process-level reference cache
# Keyed by (embedding_config_id_str, annotation_set_id_str, aspect_separated).
# Value: {"accessions": list[str], "embeddings": np.ndarray (float16)}.
# GO annotations are NOT cached. Loaded lazily per batch for the unique
# neighbors actually found, avoiding ~5-10 GB of Python dicts in memory.
# Embeddings stored as float16 (half of float32); converted to float32
# at KNN time with negligible accuracy loss for cosine similarity.
# Limited to 1 entry; evicts previous reference on config change.
# ---------------------------------------------------------------------------
_REF_CACHE: dict[tuple[str, str, bool], dict[str, Any]] = {}

# Sentinel key under which the aspect-separated reference cache stashes
# the underlying unified pool. The aspect-separated KNN delegation
# (``call_pipeline_predict_aspect_separated``) hands this unified pool
# to ``protea_method.pipeline.predict`` so the partitioned KNN runs
# without a second DB round-trip. Chosen to never collide with the
# single-letter aspect codes in ``_ASPECTS``.
_UNIFIED_REF_KEY = "__unified__"

# ---------------------------------------------------------------------------
# v6 reranker feature constants
# ---------------------------------------------------------------------------

_STORE_FLOAT_KEYS: tuple[str, ...] = (
    "identity_nw",
    "similarity_nw",
    "alignment_score_nw",
    "gaps_pct_nw",
    "alignment_length_nw",
    "identity_sw",
    "similarity_sw",
    "alignment_score_sw",
    "gaps_pct_sw",
    "alignment_length_sw",
    "length_query",
    "length_ref",
    "query_taxonomy_id",
    "ref_taxonomy_id",
    "taxonomic_lca",
    "taxonomic_distance",
    "taxonomic_common_ancestors",
    "vote_count",
    "k_position",
    "go_term_frequency",
    "ref_annotation_density",
    "neighbor_distance_std",
    "neighbor_vote_fraction",
    "neighbor_min_distance",
    "neighbor_mean_distance",
    *_NEW_V6_FEATURE_KEYS,
)


_PAIR_FEATURE_KEYS: tuple[str, ...] = (
    "identity_nw",
    "similarity_nw",
    "alignment_score_nw",
    "gaps_pct_nw",
    "alignment_length_nw",
    "identity_sw",
    "similarity_sw",
    "alignment_score_sw",
    "gaps_pct_sw",
    "alignment_length_sw",
    "length_query",
    "length_ref",
    "query_taxonomy_id",
    "ref_taxonomy_id",
    "taxonomic_lca",
    "taxonomic_distance",
    "taxonomic_common_ancestors",
    "taxonomic_relation",
)


# ---------------------------------------------------------------------------
# Payloads
# ---------------------------------------------------------------------------
# T1.5 of master plan v3: payloads live in protea-contracts.
# Re-export here so existing imports of these classes from this module
# keep working; new code should import from ``protea_contracts``.

from protea_contracts import (  # noqa: E402, F401
    PredictGOTermsBatchPayload,
    PredictGOTermsPayload,
    StorePredictionsPayload,
)


@dataclass(frozen=True)
class _UnifiedPredictContext:
    """Inputs for ``PredictGOTermsBatchOperation._unified_predict_via_pipeline``.

    Bundles the per-batch ids and the cached reference pool so the
    helper signature stays under flake8-bugbear's parameter ceiling.
    """

    p: PredictGOTermsBatchPayload
    annotation_set_id: uuid.UUID
    prediction_set_id: uuid.UUID
    valid_accessions: list[str]
    query_embeddings: np.ndarray
    ref_data: dict[str, Any]


@dataclass(frozen=True)
class AspectSeparatedKnnContext:
    """Inputs for ``PredictGOTermsBatchOperation._run_aspect_separated_knn``.

    Same family as :class:`BatchPredictContext` but for the
    aspect-separated KNN path: one independent index + neighbor set per
    GO aspect (P/F/C). ``ref_data_by_aspect`` is keyed by aspect char
    rather than the unified ``ref_data`` dict that ``_predict_batch``
    consumes; ``annotation_set_id`` is required because aspect-scoped
    GO annotation lookups happen inside the helper.
    """

    valid_accessions: list[str]
    query_embeddings: np.ndarray
    ref_data_by_aspect: dict[str, dict[str, Any]]
    annotation_set_id: uuid.UUID
    prediction_set_id: uuid.UUID
    payload: PredictGOTermsBatchPayload


def _clean_float(value: Any) -> Any:
    """Return ``None`` for NaN / non-finite floats, pass-through otherwise.

    Postgres stores NaN as a real value in double precision columns, but
    LightGBM treats NULL as missing (its native NA handling) while NaN can
    trip numeric safeguards downstream. Keeping NaN out of the DB avoids
    both footguns; feature columns read as ``None`` -> pandas NA -> LightGBM
    missing.
    """
    if value is None:
        return None
    if isinstance(value, float):
        if value != value or value in (float("inf"), float("-inf")):
            return None
    return value


def _row_from_prediction(
    pred: dict[str, Any],
    prediction_set_id: uuid.UUID,
) -> dict[str, Any]:
    """Build a GOPrediction INSERT row from a predict-side prediction dict."""
    row: dict[str, Any] = {
        "prediction_set_id": prediction_set_id,
        "protein_accession": pred["protein_accession"],
        "go_term_id": pred["go_term_id"],
        "ref_protein_accession": pred["ref_protein_accession"],
        "distance": pred["distance"],
        "qualifier": pred.get("qualifier"),
        "evidence_code": pred.get("evidence_code"),
        "taxonomic_relation": pred.get("taxonomic_relation"),
    }
    for key in _STORE_FLOAT_KEYS:
        row[key] = _clean_float(pred.get(key))
    # T3.1a dual-write: mirror every feature value into the JSONB blob.
    # Old typed columns stay authoritative for readers; T3.1b will cut
    # the reader paths over.
    row["features"] = build_feature_jsonb(row)
    # T3.1 dual-write: mirror the prediction tuple (go_term_id, score,
    # evidence) into the ``predictions_jsonb`` blob. Gated by
    # ``PROTEA_GO_PREDICTION_JSONB_WRITE_ENABLED``; when the flag is
    # off (default) ``maybe_jsonb`` returns ``None`` and the column
    # stays NULL.
    row["predictions_jsonb"] = maybe_jsonb(
        [(row["go_term_id"], row["distance"], row.get("evidence_code"))]
    )
    return row
