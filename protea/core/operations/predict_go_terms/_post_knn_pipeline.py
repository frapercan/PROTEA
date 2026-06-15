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
    if getattr(p, "compute_self_prior", False) and prediction_dicts:
        apply_self_prior(
            op,
            session,
            ctx.annotation_set_id,
            knn_result.query_batch.valid_accessions,
            prediction_dicts,
            emit,
        )
    if getattr(p, "compute_association", False) and prediction_dicts:
        apply_association(
            op,
            session,
            ctx.annotation_set_id,
            knn_result.query_batch.valid_accessions,
            prediction_dicts,
            emit,
        )
    reranker_stats: dict[str, Any] | None = None
    if p.reranker_model_id and prediction_dicts:
        scorer = op._reranker_scorer
        reranker_stats = scorer.apply_if_aligned(session, prediction_dicts, p, emit)
    return prediction_dicts, reranker_stats


def apply_self_prior(
    op: PredictGOTermsBatchOperation,
    session: Session,
    annotation_set_id: uuid.UUID,
    valid_accessions: list[str],
    prediction_dicts: list[dict[str, Any]],
    emit: EmitFn,
) -> None:
    """Set ``self_prior_score`` from each query's OWN pre-cutoff annotation.

    The self-prior re-injects the GOA Non-exp signal the KNN scoring
    discards (it excludes the neighbour self-hit by accession). For every
    candidate ``(protein, go_term_id)`` the query already carries among
    its OWN pre-cutoff NON-experimental annotations, ``self_prior_score``
    is set to ``1.0``; all other candidates keep the zero-fill default.

    Leakage guardrails (load-bearing): annotations come from the same
    pre-cutoff ``annotation_set_id`` the KNN reference pool uses (NEVER a
    post-cutoff set); NOT-qualified rows are dropped by
    :meth:`_FeatureLoadingMixin._load_annotations_for`; experimental
    evidence codes are filtered out via
    :func:`protea.core.evidence_codes.is_experimental` so no experimental
    self annotation can leak in.
    """
    accessions = {acc for acc in valid_accessions if acc}
    if not accessions:
        return
    annotations = op._load_annotations_for(session, annotation_set_id, accessions)
    own_nonexp = _own_nonexp_terms(annotations)

    hits = 0
    for rec in prediction_dicts:
        gtid = rec.get("go_term_id")
        if gtid is None:
            continue
        if int(gtid) in own_nonexp.get(rec.get("protein_accession", ""), ()):
            rec["self_prior_score"] = 1.0
            hits += 1

    emit(
        "predict_go_terms_batch.self_prior_done",
        None,
        {
            "queries_with_self_prior": len(own_nonexp),
            "candidates_marked": hits,
        },
        "info",
    )


def _own_nonexp_terms(
    annotations: dict[str, list[dict[str, Any]]],
) -> dict[str, set[int]]:
    """Per-accession set of OWN non-experimental annotated go_term_ids.

    Drops experimental evidence via
    :func:`protea.core.evidence_codes.is_experimental`; NOT-qualified rows
    were already excluded by ``_load_annotations_for``.
    """
    from protea.core.evidence_codes import is_experimental

    own_nonexp: dict[str, set[int]] = {}
    for acc, anns in annotations.items():
        terms: set[int] = set()
        for ann in anns:
            if is_experimental(ann.get("evidence_code") or ""):
                continue
            gtid = ann.get("go_term_id")
            if gtid is not None:
                terms.add(int(gtid))
        if terms:
            own_nonexp[acc] = terms
    return own_nonexp


def _own_exp_terms(
    annotations: dict[str, list[dict[str, Any]]],
) -> dict[str, set[int]]:
    """Per-accession set of OWN EXPERIMENTAL annotated go_term_ids.

    The inverse filter of :func:`_own_nonexp_terms`: keeps only experimental
    evidence via :func:`protea.core.evidence_codes.is_experimental`. NOT-
    qualified rows were already excluded by ``_load_annotations_for``. These
    are the pre-cutoff "known" terms ``K(p)`` the cross-aspect association
    feature transfers from.
    """
    from protea.core.evidence_codes import is_experimental

    own_exp: dict[str, set[int]] = {}
    for acc, anns in annotations.items():
        terms: set[int] = set()
        for ann in anns:
            if not is_experimental(ann.get("evidence_code") or ""):
                continue
            gtid = ann.get("go_term_id")
            if gtid is not None:
                terms.add(int(gtid))
        if terms:
            own_exp[acc] = terms
    return own_exp


def apply_association(
    op: PredictGOTermsBatchOperation,
    session: Session,
    annotation_set_id: uuid.UUID,
    valid_accessions: list[str],
    prediction_dicts: list[dict[str, Any]],
    emit: EmitFn,
) -> None:
    """Set the cross-aspect ``association_*`` features per candidate.

    For each query protein with pre-cutoff EXPERIMENTAL known terms ``K(p)``
    (loaded from the same ``annotation_set_id`` the KNN reference pool uses,
    never a post-cutoff set), each candidate term ``t`` scores from the
    per-set co-occurrence table (``build_go_cooccurrence``):

    - ``association_total``   = sum over ``k in K(p)`` of ``P(t | k)`` where
      ``P(t | k) = cooccurrence(k, t) / freq(k)``.
    - ``association_cross``   = same sum but only over ``k`` whose ontology
      aspect differs from ``t``'s aspect (the genuine prior-knowledge
      transfer that drives PK; same-aspect is largely ancestor-trivial).
    - ``association_present`` = ``1.0`` if ``association_total > 0`` else
      ``0.0``.

    Leakage guardrails mirror :func:`apply_self_prior`: known terms come from
    the pre-cutoff annotation set, NOT-qualified rows are dropped upstream,
    and only experimental codes are kept. If the co-occurrence table is empty
    for this set every candidate stays at the zero-fill default.
    """
    from protea.core.operations.predict_go_terms._association_loader import (
        load_cooccurrence_for_known,
    )

    accessions = {acc for acc in valid_accessions if acc}
    if not accessions:
        return
    annotations = op._load_annotations_for(session, annotation_set_id, accessions)
    own_exp = _own_exp_terms(annotations)
    if not own_exp:
        emit(
            "predict_go_terms_batch.association_done",
            None,
            {"queries_with_known": 0, "candidates_scored": 0},
            "info",
        )
        return

    # Union of all known terms across the batch -> one table lookup.
    all_known: set[int] = set()
    for terms in own_exp.values():
        all_known |= terms
    cooc_by_known, freq = load_cooccurrence_for_known(session, annotation_set_id, all_known)

    # Candidate aspects for every predicted go_term_id.
    candidate_ids = {
        int(rec["go_term_id"]) for rec in prediction_dicts if rec.get("go_term_id") is not None
    }
    aspect_by_id = _load_known_aspects(session, candidate_ids | all_known)

    scored = 0
    for rec in prediction_dicts:
        gtid = rec.get("go_term_id")
        if gtid is None:
            continue
        t = int(gtid)
        known = own_exp.get(rec.get("protein_accession", ""))
        if not known:
            continue
        t_aspect = aspect_by_id.get(t, "")
        total = 0.0
        cross = 0.0
        for k in known:
            f = freq.get(k, 0)
            if f <= 0:
                continue
            count = cooc_by_known.get(k, {}).get(t, 0)
            if count <= 0:
                continue
            p_t_given_k = count / f
            total += p_t_given_k
            if aspect_by_id.get(k, "") != t_aspect:
                cross += p_t_given_k
        if total > 0.0:
            rec["association_total"] = total
            rec["association_cross"] = cross
            rec["association_present"] = 1.0
            scored += 1

    emit(
        "predict_go_terms_batch.association_done",
        None,
        {"queries_with_known": len(own_exp), "candidates_scored": scored},
        "info",
    )


def _load_known_aspects(session: Session, term_ids: set[int]) -> dict[int, str]:
    """``{go_term_id: aspect}`` for the given ids (empty string when NULL)."""
    from sqlalchemy import select

    if not term_ids:
        return {}
    rows = session.execute(select(GOTerm.id, GOTerm.aspect).where(GOTerm.id.in_(term_ids))).all()
    return {gid: (aspect or "") for gid, aspect in rows}


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
    "apply_association",
    "apply_self_prior",
    "apply_v6_features",
    "expand_to_ancestors",
    "resolve_synthetic_fks",
    "run_post_knn_pipeline",
    "stamp_go_ids",
)
