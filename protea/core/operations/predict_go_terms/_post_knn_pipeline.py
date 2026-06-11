"""Post-KNN pipeline helpers for ``PredictGOTermsBatchOperation``.

F2C.5c extracts the post-KNN dispatch (v6 enrichment, ancestor
expansion, reranker apply) and the synthetic-ancestor FK resolver out
of the orchestrator class so the orchestrator stays under the master
plan §3 class ceiling. Behaviour is unchanged: each helper takes the
``PredictGOTermsBatchOperation`` instance (for DB-bound loader access)
plus an explicit context object, and returns the same shape the inline
method did pre-extraction.

The orchestrator keeps short delegate methods so unit tests that
patch :meth:`PredictGOTermsBatchOperation._apply_v6_features` and
friends keep working without churn.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any

import numpy as np
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn
from protea.core.feature_enricher import KnnEnrichmentContext
from protea.core.operations.predict_go_terms._common import (
    PredictGOTermsBatchPayload,
)
from protea.core.reranker import EMBEDDING_PCA_DIM
from protea.infrastructure.orm.models.annotation.go_term import GOTerm

if TYPE_CHECKING:
    from protea.core.operations.predict_go_terms._batch_op import (
        PredictGOTermsBatchOperation,
        _BatchExecCtx,
        _KnnResult,
    )


def run_post_knn_pipeline(
    op: PredictGOTermsBatchOperation,
    session: Session,
    ctx: _BatchExecCtx,
    knn_result: _KnnResult,
    ref_data: Any,
    emit: EmitFn,
) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    """Apply v6 enrichment, ancestor expansion, and the reranker.

    Returns ``(prediction_dicts, reranker_stats)``. Ancestor expansion
    runs AFTER v6 so synthetic ancestor records inherit the leaf's
    ``anc2vec_`` / ``emb_pca_`` values, mirroring what the dump helper emits;
    without that the lab booster sees a feature distribution it never
    trained on.
    """
    p = ctx.p
    if p.compute_v6_features and knn_result.v6_ctx is not None and knn_result.prediction_dicts:
        op._apply_v6_features(session, ctx, knn_result, ref_data, emit)
    prediction_dicts = knn_result.prediction_dicts
    if p.expand_votes_to_ancestors and prediction_dicts:
        prediction_dicts = op._expand_to_ancestors(session, p, prediction_dicts, emit)
    reranker_stats: dict[str, Any] | None = None
    if p.reranker_model_id and prediction_dicts:
        scorer = op._reranker_scorer
        reranker_stats = scorer.apply_if_aligned(session, prediction_dicts, p, emit)
    return prediction_dicts, reranker_stats


def apply_v6_features(
    op: PredictGOTermsBatchOperation,
    session: Session,
    ctx: _BatchExecCtx,
    knn_result: _KnnResult,
    ref_data: Any,
    emit: EmitFn,
) -> None:
    """Run Anc2Vec / tax_voters / emb_pca enrichment on the prediction
    list in place. PCA is fitted (or loaded from cache) over the full
    unified embedding pool; for aspect-separated mode the per-aspect
    f32 arrays are concatenated first.
    """
    from protea.core.domain.aspect import ASPECT_CODES as _ASPECTS

    # Look up the PCA + v6 helpers through ``_batch_op`` so unit tests
    # that monkeypatch ``_batch_op._load_or_fit_pca_state`` and
    # ``_batch_op.enrich_v6_features`` still flow through this helper
    # (F2C.5c compatibility).
    from protea.core.operations.predict_go_terms import _batch_op

    p = ctx.p
    v6_ctx = knn_result.v6_ctx
    assert v6_ctx is not None  # caller guards on this
    if p.aspect_separated_knn:
        pools = [
            ref_data[a]["embeddings_f32"]
            for a in _ASPECTS
            if ref_data[a].get("embeddings_f32") is not None and ref_data[a]["embeddings_f32"].size
        ]
        pca_pool = np.concatenate(pools, axis=0) if pools else np.empty((0,), dtype=np.float32)
    else:
        # ``embeddings_f32`` may be explicitly None when this run skipped the
        # raw f32 copy (cosine metric + PCA state already cached); coalesce to
        # an empty pool so the (cache-hit) fit ignores it without touching .size.
        pca_pool = ref_data.get("embeddings_f32")
        if pca_pool is None:
            pca_pool = np.empty((0,), dtype=np.float32)

    pca_state = _batch_op._load_or_fit_pca_state(ctx.embedding_config_id, pca_pool)
    _batch_op.enrich_v6_features(
        knn_result.prediction_dicts,
        session=session,
        ctx=KnnEnrichmentContext(
            valid_accessions=knn_result.query_batch.valid_accessions,
            query_embeddings=knn_result.query_batch.query_embeddings,
            neighbors_by_aspect=v6_ctx["neighbors_by_aspect"],
            go_map_by_aspect=v6_ctx["go_map_by_aspect"],
            pair_features=v6_ctx["pair_features"],
            pca_state=pca_state,
        ),
        compute_taxonomy=p.compute_taxonomy,
    )
    emit(
        "predict_go_terms_batch.v6_features_done",
        None,
        {
            "pca_state_fit": pca_state is not None,
            "pca_dim": EMBEDDING_PCA_DIM if pca_state is not None else 0,
            "rows_enriched": len(knn_result.prediction_dicts),
        },
        "info",
    )


def expand_to_ancestors(
    op: PredictGOTermsBatchOperation,
    session: Session,
    p: PredictGOTermsBatchPayload,
    prediction_dicts: list[dict[str, Any]],
    emit: EmitFn,
) -> list[dict[str, Any]]:
    """Expand each leaf prediction to its ancestor closure.

    Mirrors what the offline dump helper emits so live predictions
    carry the same candidate distribution the booster trained on.
    """
    from protea.core.feature_enricher import (
        expand_predictions_to_ancestors,
        load_parent_map,
    )

    snapshot_id = uuid.UUID(p.ontology_snapshot_id)
    parent_map = load_parent_map(session, snapshot_id)
    int_to_str = op._stamp_go_ids(session, prediction_dicts)
    n_before = len(prediction_dicts)
    prediction_dicts = expand_predictions_to_ancestors(
        prediction_dicts,
        parent_map=parent_map,
        k_limit=p.limit_per_entry,
        ia_weights=None,
    )
    prediction_dicts = op._resolve_synthetic_fks(session, prediction_dicts, int_to_str, snapshot_id)
    emit(
        "predict_go_terms_batch.expanded_to_ancestors",
        None,
        {
            "rows_before": n_before,
            "rows_after": len(prediction_dicts),
            "expansion_ratio": (len(prediction_dicts) / n_before if n_before else 0.0),
        },
        "info",
    )
    return prediction_dicts


def stamp_go_ids(
    session: Session,
    prediction_dicts: list[dict[str, Any]],
) -> dict[int, str]:
    """Materialise ``go_id`` strings on each prediction by FK lookup.

    Returns the ``int -> str`` map so the synthetic-ancestor FK
    resolver can reuse it without re-querying.
    """
    from sqlalchemy import select

    unique_int_ids = {rec["go_term_id"] for rec in prediction_dicts if rec.get("go_term_id")}
    id_pairs = session.execute(
        select(GOTerm.id, GOTerm.go_id).where(GOTerm.id.in_(unique_int_ids))
    ).all()
    int_to_str = {gid: go_id for gid, go_id in id_pairs}
    for rec in prediction_dicts:
        gid = rec.get("go_term_id")
        if gid is not None and gid in int_to_str:
            rec["go_id"] = int_to_str[gid]
    return int_to_str


def resolve_synthetic_fks(
    session: Session,
    prediction_dicts: list[dict[str, Any]],
    int_to_str: dict[int, str],
    snapshot_id: uuid.UUID,
) -> list[dict[str, Any]]:
    """Stamp ``go_term_id`` on synthetic ancestor records via the snapshot."""
    from sqlalchemy import select

    leaf_strs = set(int_to_str.values())
    ancestor_strs = {
        rec["go_id"]
        for rec in prediction_dicts
        if rec.get("go_id") and rec["go_id"] not in leaf_strs
    }
    if not ancestor_strs:
        return prediction_dicts
    anc_pairs = session.execute(
        select(GOTerm.id, GOTerm.go_id).where(
            GOTerm.go_id.in_(ancestor_strs),
            GOTerm.ontology_snapshot_id == snapshot_id,
        )
    ).all()
    str_to_int = {go_id: gid for gid, go_id in anc_pairs}
    str_to_int.update({v: k for k, v in int_to_str.items()})
    return [
        {**rec, "go_term_id": str_to_int[rec["go_id"]]}
        for rec in prediction_dicts
        if rec.get("go_id") in str_to_int
    ]


__all__ = (
    "apply_v6_features",
    "expand_to_ancestors",
    "resolve_synthetic_fks",
    "run_post_knn_pipeline",
    "stamp_go_ids",
)
