"""Pure-function helpers extracted from ``run_cafa_evaluation``.

Module-level utilities live here so the operation file
(``run_cafa_evaluation.py``) can stay focused on the orchestrator
class and shrink toward the master-plan v3.2 §3 LOC ceiling.

Re-exported by ``run_cafa_evaluation`` for backwards compatibility
with existing imports (``_NS_LABELS``, ``_NS_SHORT``,
``RunCafaEvaluationOperation``, ``RunCafaEvaluationPayload``).
"""

from __future__ import annotations

import uuid
from typing import Any, NamedTuple

import numpy as np

from protea.core.anc2vec_embeddings import get_index as get_anc2vec_index
from protea.core.domain.aspect import ASPECT_CAFA_CODES, Aspect
from protea.core.utils import job_id_from_payload
from protea.infrastructure.orm.models.annotation.evaluation_result import (
    EvaluationResult,
)
from protea.infrastructure.orm.models.embedding.go_prediction import GOPrediction


def eval_artifact_key(result_id: uuid.UUID, relpath: str) -> str:
    """Canonical MinIO/artifact-store key for a cafaeval output file."""
    return f"eval_artifacts/{result_id}/{relpath.lstrip('/')}"


# Namespace labels used by cafaeval OBO parser. The full names come from
# the obo file; we map them to PROTEA's canonical CAFA codes.
_NS_LABELS: dict[str, str] = {
    "biological_process": Aspect.BIOLOGICAL_PROCESS.cafa,
    "molecular_function": Aspect.MOLECULAR_FUNCTION.cafa,
    "cellular_component": Aspect.CELLULAR_COMPONENT.cafa,
}
_NS_SHORT: set[str] = set(ASPECT_CAFA_CODES)


# Feature columns read straight off the GOPrediction ORM into the reranker
# DataFrame. Kept local (not imported from reranker.py) so this module does
# not pay the LightGBM import cost when no reranker is configured.
_NUMERIC_ORM_COLS: tuple[str, ...] = (
    "distance",
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
    "anc2vec_neighbor_cos",
    "anc2vec_neighbor_maxcos",
    "anc2vec_has_emb",
    "anc2vec_query_known_cos",
    "anc2vec_query_known_maxcos",
    "anc2vec_query_known_count",
    "tax_voters_same_frac",
    "tax_voters_close_frac",
    "tax_voters_mean_common_ancestors",
    *(f"emb_pca_query_{i}" for i in range(16)),
)


# LAFA per-category booster features (INT-2/3/4). As of the signal-store
# code-switch these six have typed GOPrediction columns (they were JSONB-blob
# only before), so the eval reads them straight off the typed columns like the
# base features. The per-category boosters trained on these families, so the
# eval record must carry them or the booster sees them as missing and cannot
# reproduce the predict-time score. ``self_prior`` contributes a single column
# (``self_prior_score``); there is no ``self_prior_present`` in PROTEA. IA is
# handled separately: it maps the typed ``ia`` column to the upper-case ``IA``
# record key (it is an eval-side ``f_micro_w`` weight, not a reranker feature).
_LAFA_TYPED_FEATURE_COLS: tuple[str, ...] = (
    "classifier_score",
    "classifier_present",
    "self_prior_score",
    "association_total",
    "association_cross",
    "association_present",
)


def _record_from_pred(
    pred: GOPrediction,
    go_id: str,
    aspect: str | None = None,
) -> dict[str, Any]:
    """Extract a reranker-ready record from a GOPrediction ORM instance.

    ``aspect`` is only needed when the caller routes by aspect (per-aspect
    models). For category-level reranking pass ``None``.

    Base feature columns come from the typed ORM columns
    (:data:`_NUMERIC_ORM_COLS`). As of the signal-store code-switch the LAFA
    families (:data:`_LAFA_TYPED_FEATURE_COLS`) and IA also read from typed
    columns rather than the ``features`` JSONB blob. When a column is NULL
    (legacy rows / default runs that never computed them) it reads as ``None``,
    which LightGBM routes through its native missing branch. The typed ``ia``
    column is exposed under the upper-case ``IA`` record key.
    """
    record: dict[str, Any] = {
        "protein_accession": pred.protein_accession,
        "go_id": go_id,
        "aspect": aspect or "",
        "qualifier": pred.qualifier or "",
        "evidence_code": pred.evidence_code or "",
        "taxonomic_relation": pred.taxonomic_relation or "",
    }
    for col in _NUMERIC_ORM_COLS:
        record[col] = getattr(pred, col, None)
    for col in _LAFA_TYPED_FEATURE_COLS:
        record[col] = getattr(pred, col, None)
    record["IA"] = getattr(pred, "ia", None)
    return record


def _build_anc2vec_lookup(
    df: Any,
    known_gos: dict[str, set[str]],
) -> tuple[list[str], dict[str, int], np.ndarray, np.ndarray] | None:
    """Build the joint GO-id index and unit-normalised embedding matrix.

    Returns ``(go_list, idx_of_go, all_norm, has_emb_mask)`` or ``None``
    when there is nothing to score (empty df or no known set).
    """
    if df.empty or not known_gos:
        return None

    candidate_go_ids = df["go_id"].unique().tolist()
    unique_proteins = df["protein_accession"].unique().tolist()
    all_go_ids: set[str] = set(candidate_go_ids)
    for q_acc in unique_proteins:
        all_go_ids.update(known_gos.get(q_acc, ()))
    go_list = sorted(all_go_ids)
    if not go_list:
        return None

    idx_of_go: dict[str, int] = {g: i for i, g in enumerate(go_list)}
    emb = get_anc2vec_index().batch(go_list)
    raw_norms = np.linalg.norm(emb, axis=1)
    has_emb_mask = raw_norms > 0.0
    safe_norms = np.where(has_emb_mask, raw_norms, 1.0)[:, None]
    all_norm = (emb / safe_norms).astype(np.float32)
    all_norm[~has_emb_mask] = 0.0
    return go_list, idx_of_go, all_norm, has_emb_mask


def _fill_anc2vec_columns(
    df: Any,
    known_gos: dict[str, set[str]],
    idx_of_go: dict[str, int],
    all_norm: np.ndarray,
    has_emb_mask: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Populate ``cos`` / ``maxcos`` / ``count`` columns row-by-row.

    ``count`` is filled even when no Anc2Vec coverage is available for the
    query's known set so the column stays informative; ``cos`` and
    ``maxcos`` stay NaN for rows without coverage.
    """
    cos_col = np.full(len(df), np.nan, dtype=np.float32)
    maxcos_col = np.full(len(df), np.nan, dtype=np.float32)
    count_col = np.zeros(len(df), dtype=np.float32)

    protein_groups = df.groupby("protein_accession", sort=False).indices
    for q_acc, row_indices in protein_groups.items():
        known = known_gos.get(q_acc, set())
        count_col[row_indices] = float(len(known))
        if not known:
            continue
        known_rows = [idx_of_go[g] for g in known if g in idx_of_go and has_emb_mask[idx_of_go[g]]]
        if not known_rows:
            continue
        kmat = all_norm[known_rows]
        centroid = kmat.mean(axis=0)
        cn = float(np.linalg.norm(centroid))
        centroid_unit = (centroid / cn).astype(np.float32) if cn > 0.0 else None

        for ridx in row_indices:
            go_id = df["go_id"].iat[ridx]
            cand_i = idx_of_go.get(go_id)
            if cand_i is None or not has_emb_mask[cand_i]:
                continue
            cand_vec = all_norm[cand_i]
            if centroid_unit is not None:
                cos_col[ridx] = float(cand_vec @ centroid_unit)
            maxcos_col[ridx] = float((kmat @ cand_vec).max())

    return cos_col, maxcos_col, count_col


def _patch_query_known_features(
    df: Any,
    known_gos: dict[str, set[str]],
) -> None:
    """Overwrite ``anc2vec_query_known_*`` in-place from eval-time known GOs.

    At predict time these columns are stored as NaN / 0 because the query's
    pre-cutoff annotation set is a property of the evaluation split, not the
    prediction set. This helper repairs them for LK / PK evaluation so the
    reranker sees the same query-profile features it was trained on.

    Splits across two private helpers (``_build_anc2vec_lookup`` for the
    embedding matrix and ``_fill_anc2vec_columns`` for the per-row scoring)
    so neither breaches the 60-LOC method ceiling.
    """
    import pandas as pd

    lookup = _build_anc2vec_lookup(df, known_gos)
    if lookup is None:
        return
    _, idx_of_go, all_norm, has_emb_mask = lookup

    cos_col, maxcos_col, count_col = _fill_anc2vec_columns(
        df, known_gos, idx_of_go, all_norm, has_emb_mask
    )

    df["anc2vec_query_known_cos"] = pd.Series(cos_col, index=df.index).replace({np.nan: pd.NA})
    df["anc2vec_query_known_maxcos"] = pd.Series(maxcos_col, index=df.index).replace(
        {np.nan: pd.NA}
    )
    df["anc2vec_query_known_count"] = count_col


class ResultRow(NamedTuple):
    """Everything the stored EvaluationResult row is assembled from.

    A bundle rather than twelve parameters, which is the same reason
    ``AdapterInputs`` exists: the ceiling is six and the row genuinely has
    twelve fields.
    """

    result_id: uuid.UUID
    inputs: Any
    payload: dict[str, Any]
    scoring_config_id: str | None
    first_reranker_id: Any
    reranker_config_snapshot: Any
    results: Any
    #: ``(frame, temporal_window, leakage_role, arms_enabled)``, kept as the
    #: tuple its producer returns rather than unpacked and re-packed. Four
    #: names that always travel together are one thing.
    provenance: tuple[Any, Any, Any, Any]
    #: The depth this result scored, in whichever unit was asked for. Both
    #: None means the whole stored neighbourhood. These are the fields a
    #: reader comes looking for: they say which cut produced these numbers,
    #: and until 2026-08-30 nothing on the row said it, so a five point depth
    #: series rendered as five results at the retrieval depth of 30.
    max_sequence_rank: int | None = None
    max_k_position: int | None = None


def build_result_row(row: ResultRow) -> EvaluationResult:
    """The stored row, assembled in one place."""
    return EvaluationResult(
        id=row.result_id,
        evaluation_set_id=row.inputs.eval_set_id,
        prediction_set_id=row.inputs.pred_set_id,
        scoring_config_id=(
            uuid.UUID(row.scoring_config_id) if row.scoring_config_id else None
        ),
        reranker_model_id=row.first_reranker_id,
        reranker_config=row.reranker_config_snapshot,
        results=row.results,
        frame=row.provenance[0],
        temporal_window=row.provenance[1],
        leakage_role=row.provenance[2],
        arms_enabled=row.provenance[3],
        # On the result itself; job.payload is ON DELETE SET NULL.
        max_sequence_rank=row.max_sequence_rank,
        max_k_position=row.max_k_position,
        job_id=job_id_from_payload(row.payload),
    )
