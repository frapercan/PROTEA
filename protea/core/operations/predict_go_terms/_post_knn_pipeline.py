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
from functools import lru_cache
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
    if prediction_dicts:
        _apply_lafa_score_features(op, session, ctx, knn_result, prediction_dicts, emit)
    if getattr(p, "compute_classifier", False):
        prediction_dicts = apply_classifier(
            session,
            uuid.UUID(p.ontology_snapshot_id),
            knn_result.query_batch.valid_accessions,
            prediction_dicts,
            emit,
        )
    reranker_stats: dict[str, Any] | None = None
    if _reranker_requested(p) and prediction_dicts:
        scorer = op._reranker_scorer
        reranker_stats = scorer.apply(session, prediction_dicts, p, emit)
    return prediction_dicts, reranker_stats


def _apply_lafa_score_features(
    op: PredictGOTermsBatchOperation,
    session: Session,
    ctx: _BatchExecCtx,
    knn_result: _KnnResult,
    prediction_dicts: list[dict[str, Any]],
    emit: EmitFn,
) -> None:
    """Apply the LAFA per-candidate score features (self_prior / association / IA).

    Each is gated by its own ``compute_*`` payload flag and mutates
    ``prediction_dicts`` in place. Classifier is handled separately by the
    caller because it can also append NEW candidate rows; these three only
    annotate existing candidates. Extracted from ``run_post_knn_pipeline`` to
    keep it under the master plan §3 method-length ceiling.
    """
    p = ctx.p
    accessions = knn_result.query_batch.valid_accessions
    if getattr(p, "compute_self_prior", False):
        apply_self_prior(op, session, ctx.annotation_set_id, accessions, prediction_dicts, emit)
    if getattr(p, "compute_association", False):
        apply_association(op, session, ctx.annotation_set_id, accessions, prediction_dicts, emit)
    if getattr(p, "compute_ia", False):
        apply_ia(
            session,
            uuid.UUID(p.ontology_snapshot_id),
            prediction_dicts,
            emit,
            ia_file=getattr(p, "ia_file", None),
        )


def _reranker_requested(p: PredictGOTermsBatchPayload) -> bool:
    """True when a single booster OR all three per-category boosters are bound.

    Per-category dispatch (INT-5) does not set ``reranker_model_id``; it carries
    three ``reranker_<cat>_artifact_uri`` pointers instead, so the gate must also
    fire when those are present.
    """
    if p.reranker_model_id:
        return True
    return all(getattr(p, f"reranker_{cat}_artifact_uri", None) for cat in ("nk", "lk", "pk"))


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

    # Snapshot-invariant matching (mirrors ``apply_association``): resolve the own
    # non-exp ids and the candidate ids to the shared go_id string namespace so the
    # match survives the multi-snapshot export (see ``_resolve_self_prior_go_ids``).
    own_nonexp_go, go_id_by_int = _resolve_self_prior_go_ids(
        session, own_nonexp, prediction_dicts
    )

    hits = 0
    for rec in prediction_dicts:
        gtid = rec.get("go_term_id")
        if gtid is None:
            continue
        cand_go = rec.get("go_id") or go_id_by_int.get(int(gtid))
        if cand_go is not None and cand_go in own_nonexp_go.get(
            rec.get("protein_accession", ""), ()
        ):
            rec["self_prior_score"] = 1.0
            hits += 1

    emit(
        "predict_go_terms_batch.self_prior_done",
        None,
        {
            "queries_with_self_prior": len(own_nonexp_go),
            "candidates_marked": hits,
        },
        "info",
    )


def _resolve_self_prior_go_ids(
    session: Session,
    own_nonexp: dict[str, set[int]],
    prediction_dicts: list[dict[str, Any]],
) -> tuple[dict[str, set[str]], dict[int, str]]:
    """Resolve own-non-exp + candidate int ids to snapshot-invariant go_id strings.

    ``self_prior`` marks a candidate that the query already carries among its OWN
    pre-cutoff non-exp annotations. Those own-term ids come from the t0 set's
    ontology snapshot while candidate ids come from the export/predict snapshot, so
    a raw ``go_term_id`` int match silently fails across the multi-snapshot export
    (the 13 rolling t0 sets each carry their own snapshot), leaving the feature
    all-zero in the dump. One GOTerm lookup over the union maps both id-spaces into
    the shared go_id namespace (mirrors :func:`_resolve_association_go_ids`); returns
    ``own_nonexp`` as go_id strings plus the int->go_id resolver for the candidates.
    """
    all_own: set[int] = set()
    for terms in own_nonexp.values():
        all_own |= terms
    candidate_ids = {
        int(rec["go_term_id"]) for rec in prediction_dicts if rec.get("go_term_id") is not None
    }
    go_id_by_int, _aspect = _load_go_id_and_aspect(session, candidate_ids | all_own)
    own_nonexp_go: dict[str, set[str]] = {}
    for acc, terms in own_nonexp.items():
        gos = {go_id_by_int[k] for k in terms if k in go_id_by_int}
        if gos:
            own_nonexp_go[acc] = gos
    return own_nonexp_go, go_id_by_int


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
    (from the same ``annotation_set_id`` the KNN pool uses, never post-cutoff),
    each candidate term ``t`` is scored from the per-set co-occurrence table
    (``build_go_cooccurrence``). The actual scoring lives in
    :func:`_score_association_candidates`: ``association_total`` /
    ``association_cross`` / ``association_present``.

    Leakage guardrails mirror :func:`apply_self_prior`. If the co-occurrence
    table is empty for this set every candidate stays at the zero-fill default.
    """
    from protea.core.operations.predict_go_terms._association_loader import (
        load_cooccurrence_for_known,
    )

    own_exp = _load_own_exp_for_association(op, session, annotation_set_id, valid_accessions)
    if not own_exp:
        emit(
            "predict_go_terms_batch.association_done",
            None,
            {"queries_with_known": 0, "candidates_scored": 0},
            "info",
        )
        return

    go_id_by_int, aspect_by_go, own_exp_go = _resolve_association_go_ids(
        session, own_exp, prediction_dicts
    )
    all_known_go = {go for gos in own_exp_go.values() for go in gos}
    cooc_by_known, freq = load_cooccurrence_for_known(session, annotation_set_id, all_known_go)

    scored = _score_association_candidates(
        prediction_dicts, own_exp_go, cooc_by_known, freq, go_id_by_int, aspect_by_go
    )

    emit(
        "predict_go_terms_batch.association_done",
        None,
        {"queries_with_known": len(own_exp_go), "candidates_scored": scored},
        "info",
    )


def _resolve_association_go_ids(
    session: Session,
    own_exp: dict[str, set[int]],
    prediction_dicts: list[dict[str, Any]],
) -> tuple[dict[int, str], dict[str, str], dict[str, set[str]]]:
    """Resolve known + candidate int ids to snapshot-invariant go_id strings.

    One GOTerm lookup over the union of known ids (t0-set snapshot) and
    candidate ids (export/predict snapshot) yields ``({go_term_id: go_id},
    {go_id: aspect})``; both id-spaces map into the SAME go_id namespace, which
    is what decouples the feature from the cooccurrence table's build snapshot.
    Returns those two maps plus ``own_exp_go`` (each query's known terms as
    go_id strings, the cooccurrence lookup keys).
    """
    all_known: set[int] = set()
    for terms in own_exp.values():
        all_known |= terms
    candidate_ids = {
        int(rec["go_term_id"]) for rec in prediction_dicts if rec.get("go_term_id") is not None
    }
    go_id_by_int, aspect_by_go = _load_go_id_and_aspect(session, candidate_ids | all_known)

    own_exp_go: dict[str, set[str]] = {}
    for acc, terms in own_exp.items():
        gos = {go_id_by_int[k] for k in terms if k in go_id_by_int}
        if gos:
            own_exp_go[acc] = gos
    return go_id_by_int, aspect_by_go, own_exp_go


def _load_own_exp_for_association(
    op: PredictGOTermsBatchOperation,
    session: Session,
    annotation_set_id: uuid.UUID,
    valid_accessions: list[str],
) -> dict[str, set[int]]:
    """Pre-cutoff experimental known terms per query accession (leakage-clean)."""
    accessions = {acc for acc in valid_accessions if acc}
    if not accessions:
        return {}
    annotations = op._load_annotations_for(session, annotation_set_id, accessions)
    return _own_exp_terms(annotations)


def _score_association_candidates(
    prediction_dicts: list[dict[str, Any]],
    own_exp_go: dict[str, set[str]],
    cooc_by_known: dict[str, dict[str, int]],
    freq: dict[str, int],
    go_id_by_int: dict[int, str],
    aspect_by_go: dict[str, str],
) -> int:
    """Write ``association_*`` features per candidate; return rows scored.

    Everything is keyed on the snapshot-invariant ``go_id`` string so a
    candidate scores identically whether or not its snapshot matches the t0
    set's. For each candidate go_id ``t`` and the query's known go_ids ``K(p)``,
    ``association_total`` sums ``P(t | k) = cooccurrence(k, t) / freq(k)``;
    ``association_cross`` sums only the cross-aspect ``k`` (aspect by go_id).

    Hot path: this used to be an O(candidates x known) nested loop with a
    dict-of-dict re-fetch per candidate, which throttled the export build
    (~39 -> ~7 q/s once the feature went live). It is restructured to group
    candidates by protein and accumulate ``P(t | k)`` once per (protein, known)
    pair, doing an O(1) read-back per candidate. Float addition order is pinned
    to ``sorted(known)`` so the per-candidate sums are deterministic and match
    the reference summation order exactly (see ``test_apply_association``).
    """
    # Group candidate recs by protein so each known term's cooccurrence row is
    # walked once per protein instead of once per candidate.
    recs_by_protein: dict[str, list[tuple[dict[str, Any], str]]] = {}
    for rec in prediction_dicts:
        gtid = rec.get("go_term_id")
        if gtid is None:
            continue
        # Prefer the go_id already stamped on the rec; fall back to the resolver.
        t = rec.get("go_id") or go_id_by_int.get(int(gtid))
        if t is None:
            continue
        acc = rec.get("protein_accession", "")
        known = own_exp_go.get(acc)
        if not known:
            continue
        recs_by_protein.setdefault(acc, []).append((rec, t))

    scored = 0
    for acc, rec_pairs in recs_by_protein.items():
        # Candidate go_ids for this protein (dedup; the read-back is O(1) by go_id).
        candidate_gos = {t for _rec, t in rec_pairs}
        assoc_total, assoc_cross = _accumulate_association(
            own_exp_go[acc], candidate_gos, cooc_by_known, freq, aspect_by_go
        )
        for rec, t in rec_pairs:
            total = assoc_total.get(t, 0.0)
            if total > 0.0:
                rec["association_total"] = total
                rec["association_cross"] = assoc_cross.get(t, 0.0)
                rec["association_present"] = 1.0
                scored += 1
    return scored


def _accumulate_association(
    known: set[str],
    candidate_gos: set[str],
    cooc_by_known: dict[str, dict[str, int]],
    freq: dict[str, int],
    aspect_by_go: dict[str, str],
) -> tuple[dict[str, float], dict[str, float]]:
    """Accumulate ``association_total`` / ``association_cross`` for one protein.

    Walks each known term's cooccurrence row ONCE (vs once per candidate in the
    old loop), accumulating ``P(t | k)`` into the per-candidate totals. Float
    additions are ordered by ``sorted(known)`` so totals are deterministic and
    bit-identical to the reference summation order.
    """
    assoc_total: dict[str, float] = {}
    assoc_cross: dict[str, float] = {}
    for k in sorted(known):
        f = freq.get(k, 0)
        if f <= 0:
            continue
        ck = cooc_by_known.get(k)
        if not ck:
            continue
        k_aspect = aspect_by_go.get(k, "")
        # Iterate the smaller side: the protein's candidates vs k's coocs.
        if len(candidate_gos) <= len(ck):
            pairs = ((t, ck.get(t, 0)) for t in candidate_gos)
        else:
            pairs = ((t, c) for t, c in ck.items() if t in candidate_gos)
        for t, count in pairs:
            if count <= 0:
                continue
            p_t_given_k = count / f
            assoc_total[t] = assoc_total.get(t, 0.0) + p_t_given_k
            if k_aspect != aspect_by_go.get(t, ""):
                assoc_cross[t] = assoc_cross.get(t, 0.0) + p_t_given_k
    return assoc_total, assoc_cross


def _load_known_aspects(session: Session, term_ids: set[int]) -> dict[int, str]:
    """``{go_term_id: aspect}`` for the given ids (empty string when NULL).

    Aspect is intrinsic to a GOTerm row, so this int-keyed lookup is safe
    within a single snapshot; it backs the CAFA category split in
    ``_category_dispatch`` (NK / LK / PK), which never crosses snapshots.
    """
    from sqlalchemy import select

    if not term_ids:
        return {}
    rows = session.execute(select(GOTerm.id, GOTerm.aspect).where(GOTerm.id.in_(term_ids))).all()
    return {gid: (aspect or "") for gid, aspect in rows}


def _load_go_id_and_aspect(
    session: Session, term_ids: set[int]
) -> tuple[dict[int, str], dict[str, str]]:
    """Resolve int ids to ``({go_term_id: go_id}, {go_id: aspect})``.

    The aspect is keyed on the snapshot-invariant go_id string so the
    cross-aspect split is itself snapshot-independent (the same go_id carries
    the same aspect across snapshots). Aspect is the empty string when NULL.
    """
    from sqlalchemy import select

    if not term_ids:
        return {}, {}
    rows = session.execute(
        select(GOTerm.id, GOTerm.go_id, GOTerm.aspect).where(GOTerm.id.in_(term_ids))
    ).all()
    go_id_by_int: dict[int, str] = {}
    aspect_by_go: dict[str, str] = {}
    for gid, go_id, aspect in rows:
        go_id_by_int[int(gid)] = go_id
        aspect_by_go[go_id] = aspect or ""
    return go_id_by_int, aspect_by_go


def apply_ia(
    session: Session,
    ontology_snapshot_id: uuid.UUID,
    prediction_dicts: list[dict[str, Any]],
    emit: EmitFn,
    *,
    ia_file: str | None = None,
) -> None:
    """Set the per-candidate ``IA`` feature = information accretion of the term.

    IA(t) is the snapshot-invariant, query-independent information content the
    cafaeval ``f_micro_w`` objective also weights with, so attaching it as a
    booster feature aligns the reranker with the metric. Values come from the
    same go_id-keyed IA table the eval consumes (resolved by
    :func:`_load_ia_feature_map`). When no IA table resolves the feature is left
    unset (LightGBM missing branch) and the run stays bit-exact to a pre-IA
    prediction; matching mirrors :func:`apply_self_prior` (candidate go_id
    string, snapshot-invariant).
    """
    ia_map = _load_ia_feature_map(session, ontology_snapshot_id, ia_file, emit)
    if not ia_map:
        return
    candidate_ids = {
        int(rec["go_term_id"]) for rec in prediction_dicts if rec.get("go_term_id") is not None
    }
    go_id_by_int, _aspect = _load_go_id_and_aspect(session, candidate_ids)
    hits = 0
    for rec in prediction_dicts:
        gtid = rec.get("go_term_id")
        if gtid is None:
            continue
        cand_go = rec.get("go_id") or go_id_by_int.get(int(gtid))
        if cand_go is None:
            continue
        val = ia_map.get(cand_go)
        if val is not None:
            rec["IA"] = float(val)
            hits += 1
    emit(
        "predict_go_terms_batch.ia_done",
        None,
        {"terms_in_ia_map": len(ia_map), "candidates_marked": hits},
        "info",
    )


def _load_ia_feature_map(
    session: Session,
    ontology_snapshot_id: uuid.UUID,
    ia_file: str | None,
    emit: EmitFn,
) -> dict[str, float]:
    """Resolve + parse the go_id-keyed IA table for the ``IA`` feature.

    Priority: explicit ``ia_file`` > ``PROTEA_IA_FEATURE_PATH`` env > the
    ontology snapshot's ``ia_url`` (downloaded once, cached). Returns an empty
    map (with a warning) when none resolves so :func:`apply_ia` no-ops.
    """
    import os

    from protea.infrastructure.orm.models.annotation.ontology_snapshot import (
        OntologySnapshot,
    )

    path = ia_file or os.environ.get("PROTEA_IA_FEATURE_PATH")
    if not path:
        snap = session.get(OntologySnapshot, ontology_snapshot_id)
        url = getattr(snap, "ia_url", None) if snap is not None else None
        if url:
            path = _download_ia_table(url)
    if not path or not os.path.exists(path):
        emit(
            "predict_go_terms_batch.ia_unresolved",
            None,
            {
                "warning": "No IA table resolved (ia_file / PROTEA_IA_FEATURE_PATH / "
                "snapshot.ia_url); the IA feature is left unset.",
            },
            "warning",
        )
        return {}
    return _cached_ia_map(path)


@lru_cache(maxsize=4)
def _cached_ia_map(path: str) -> dict[str, float]:
    """Parse + cache a go_id-keyed IA TSV (raw IA, 0 .. ~19)."""
    from protea.core.operations._run_cafa_artifacts import load_ia_map

    return load_ia_map(path)


@lru_cache(maxsize=4)
def _download_ia_table(url: str) -> str:
    """Download a snapshot ``ia_url`` once to a stable temp path; cache by URL."""
    import hashlib
    import os
    import tempfile

    from protea.core.operations._run_cafa_artifacts import download_tsv

    digest = hashlib.sha256(url.encode()).hexdigest()[:12]
    dest = os.path.join(tempfile.gettempdir(), f"protea_ia_feature_{digest}.tsv")
    if not os.path.exists(dest):
        download_tsv(url, dest)
    return dest


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


def apply_classifier(
    session: Session,
    snapshot_id: uuid.UUID,
    valid_accessions: list[str],
    prediction_dicts: list[dict[str, Any]],
    emit: EmitFn,
) -> list[dict[str, Any]]:
    """Merge full-vocabulary classifier terms as ADDITIONAL candidates.

    The classifier proposes GO terms from the query's 6-PLM embeddings across
    the whole training vocabulary (not just neighbour-carried terms). For a
    ``(protein, go_term_id)`` already present, ``classifier_score`` is set and
    ``classifier_present`` becomes ``1.0``; otherwise a new candidate dict is
    created with the classifier marker. Returns the (possibly grown) list.
    KNN candidates are never removed, so this is a strict union.
    """
    from protea.core.classifier_producer import (
        get_classifier,
        load_concat_features,
        resolve_go_term_ids,
    )

    accessions = [acc for acc in valid_accessions if acc]
    features, valid = load_concat_features(session, accessions)
    if not valid:
        _emit_classifier_done(emit, 0, 0)
        return prediction_dicts
    preds = get_classifier().predict(features, valid)
    go_ids = {pr.go_id for pr in preds}
    gid_by_go = resolve_go_term_ids(session, go_ids, snapshot_id)
    merged, added = _merge_classifier_preds(prediction_dicts, preds, gid_by_go)
    _emit_classifier_done(emit, len(valid), added)
    return merged


def _merge_classifier_preds(
    prediction_dicts: list[dict[str, Any]],
    preds: list[Any],
    gid_by_go: dict[str, int],
) -> tuple[list[dict[str, Any]], int]:
    """Union classifier terms into ``prediction_dicts``; return (list, n_new)."""
    by_key: dict[tuple[str, int], dict[str, Any]] = {}
    for rec in prediction_dicts:
        gtid = rec.get("go_term_id")
        if gtid is not None:
            by_key[(rec.get("protein_accession", ""), int(gtid))] = rec
    added = 0
    for pr in preds:
        gtid = gid_by_go.get(pr.go_id)
        if gtid is None:
            continue
        existing = by_key.get((pr.accession, gtid))
        if existing is not None:
            existing["classifier_score"] = float(pr.score)
            existing["classifier_present"] = 1.0
            continue
        rec = _new_classifier_record(pr.accession, gtid, pr.go_id, float(pr.score))
        by_key[(pr.accession, gtid)] = rec
        prediction_dicts.append(rec)
        added += 1
    return prediction_dicts, added


def _new_classifier_record(
    accession: str, go_term_id: int, go_id: str, score: float
) -> dict[str, Any]:
    """Build a classifier-only candidate dict (KNN features zero/default)."""
    return {
        "protein_accession": accession,
        "go_term_id": go_term_id,
        "go_id": go_id,
        "ref_protein_accession": "classifier",
        "distance": float("nan"),
        "qualifier": "",
        "evidence_code": "",
        "classifier_score": score,
        "classifier_present": 1.0,
        "self_prior_score": 0.0,
        "association_total": 0.0,
        "association_cross": 0.0,
        "association_present": 0.0,
    }


def _emit_classifier_done(emit: EmitFn, queries: int, candidates_added: int) -> None:
    """Emit the classifier completion event."""
    emit(
        "predict_go_terms_batch.classifier_done",
        None,
        {"queries_scored": queries, "candidates_added": candidates_added},
        "info",
    )


__all__ = (
    "apply_association",
    "apply_classifier",
    "apply_self_prior",
    "apply_v6_features",
    "expand_to_ancestors",
    "resolve_synthetic_fks",
    "run_post_knn_pipeline",
    "stamp_go_ids",
)
