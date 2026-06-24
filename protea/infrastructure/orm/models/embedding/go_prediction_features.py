"""Dual-write helpers for the ``GOPrediction.features`` JSONB column.

T3.1a of the executor plan ships the JSONB column as a unidirectional
write target: every typed feature column on ``GOPrediction`` is mirrored
into the blob at insert time. Readers stay on the typed columns until
T3.1b. ``from_json`` round-trips the blob into a feature dict so the
T3.1b cut-over can land without changing the helper's contract.

JSONB key naming follows the typed column names (snake_case, identical),
so a future reader cut-over is a column-rename-free swap.
"""
from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any

#: Identity / FK fields on ``GOPrediction`` that are NOT mirrored into
#: the JSONB blob. Everything else on the row is a feature signal.
_IDENTITY_KEYS: frozenset[str] = frozenset(
    {
        "id",
        "prediction_set_id",
        "protein_accession",
        "go_term_id",
        "ref_protein_accession",
    }
)


#: Canonical feature keys mirrored into ``GOPrediction.features``.
#: Order is stable so JSONB dumps diff predictably.
FEATURE_JSONB_KEYS: tuple[str, ...] = (
    "distance",
    "qualifier",
    "evidence_code",
    # NW alignment
    "identity_nw",
    "similarity_nw",
    "alignment_score_nw",
    "gaps_pct_nw",
    "alignment_length_nw",
    # SW alignment
    "identity_sw",
    "similarity_sw",
    "alignment_score_sw",
    "gaps_pct_sw",
    "alignment_length_sw",
    # Sequence lengths
    "length_query",
    "length_ref",
    # Reranker features
    "vote_count",
    "k_position",
    "go_term_frequency",
    "ref_annotation_density",
    "neighbor_distance_std",
    # Consensus features
    "neighbor_vote_fraction",
    "neighbor_min_distance",
    "neighbor_mean_distance",
    # Taxonomy
    "query_taxonomy_id",
    "ref_taxonomy_id",
    "taxonomic_lca",
    "taxonomic_distance",
    "taxonomic_common_ancestors",
    "taxonomic_relation",
    # Anc2Vec semantic coherence
    "anc2vec_neighbor_cos",
    "anc2vec_neighbor_maxcos",
    "anc2vec_has_emb",
    "anc2vec_query_known_cos",
    "anc2vec_query_known_maxcos",
    "anc2vec_query_known_count",
    # Tax-voters consensus
    "tax_voters_same_frac",
    "tax_voters_close_frac",
    "tax_voters_mean_common_ancestors",
    # Sequence-embedding PCA query projection (16 components)
    *tuple(f"emb_pca_query_{i}" for i in range(16)),
)


def _json_safe_float(value: Any) -> Any:
    """Scrub non-finite floats (NaN/Inf) to None so the JSONB stays valid JSON.

    Postgres JSON(B) rejects the NaN/Infinity tokens; the typed float8 columns
    accept them, but the mirrored blob must not carry them (classifier-added
    candidates set distance to NaN). Non-float values pass through unchanged.
    """
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def build_feature_jsonb(typed_row: Mapping[str, Any]) -> dict[str, Any]:
    """Mirror the typed-column values into a JSONB-shaped dict.

    Single source of truth for the T3.1a dual-write: every writer hands
    its already-cleaned ``GOPrediction`` row dict to this helper, and
    the result is what lands in the ``features`` JSONB column.

    Identity columns (``prediction_set_id``, ``protein_accession`` ...)
    are dropped because they are already keys on the row; only the
    feature signals are mirrored. Keys absent from ``typed_row`` map
    to ``None`` so the blob shape stays stable across writer sites.
    """
    return {
        key: _json_safe_float(typed_row.get(key))
        for key in FEATURE_JSONB_KEYS
        if key not in _IDENTITY_KEYS
    }


def from_json(blob: Mapping[str, Any] | None) -> dict[str, Any]:
    """Round-trip a JSONB blob back into a feature dict.

    Reserved for the T3.1b reader cut-over: readers will call this on
    the JSONB column and get the same shape ``build_feature_jsonb``
    produced. ``None`` (legacy / pre-dual-write rows) returns an empty
    dict so callers can treat missing as "no features".
    """
    if blob is None:
        return {}
    return {key: blob.get(key) for key in FEATURE_JSONB_KEYS}
