"""Full-vocabulary classifier candidate application for the predict path.

The classifier producer (``classifier_producer`` / ``two_tower_classifier``)
proposes GO terms from a query protein's frozen PLM embeddings across the WHOLE
training vocabulary, not just neighbour-carried terms. This module unions those
proposals into the KNN candidate list (a strict union: KNN candidates are never
dropped) and, when opted in, routes them PER CAFA CATEGORY.

Routing (``serve.classifier_impl_by_category``, default off) reproduces the
composite champion classifier pool: NK / LK cells take their candidates from the
M2 anc2vec head and PK cells from the two-tower sparse head (d8979601). The
category is a property of ``(protein, candidate aspect)`` derived from the same
leakage-clean pre-cutoff experimental annotations the reranker category split
uses (see ``_category_dispatch``), so a K protein takes its known-aspect
candidates from the two-tower head and its other-aspect candidates from M2. When
the knob is off the single global ``PROTEA_CLASSIFIER_IMPL`` selection is used
and behaviour is byte-identical to before.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn

if TYPE_CHECKING:
    from protea.core.operations.predict_go_terms._batch_op import (
        PredictGOTermsBatchOperation,
    )


@dataclass(frozen=True)
class ClassifierRouteCtx:
    """Inputs needed to derive the leakage-clean per-protein CAFA category.

    ``op`` supplies the chunked ``_load_annotations_for`` reader and
    ``annotation_set_id`` pins the SAME pre-cutoff set the KNN reference pool
    uses, so the known-aspect derivation is leakage-clean by construction.
    """

    op: PredictGOTermsBatchOperation
    annotation_set_id: uuid.UUID


def apply_classifier(
    session: Session,
    snapshot_id: uuid.UUID,
    valid_accessions: list[str],
    prediction_dicts: list[dict[str, Any]],
    emit: EmitFn,
    *,
    route_ctx: ClassifierRouteCtx | None = None,
) -> list[dict[str, Any]]:
    """Merge full-vocabulary classifier terms as ADDITIONAL candidates.

    For a ``(protein, go_term_id)`` already present, ``classifier_score`` is set
    and ``classifier_present`` becomes ``1.0``; otherwise a new candidate dict is
    created with the classifier marker. Returns the (possibly grown) list.

    When ``serve.classifier_impl_by_category`` is enabled (and ``route_ctx`` is
    supplied so the leakage-clean category can be derived), candidates are routed
    per CAFA category via :func:`apply_classifier_composite`; otherwise the
    single global ``classifier_impl`` selection is used, byte-identical to before.
    """
    if route_ctx is not None and classifier_route_by_category_enabled():
        return apply_classifier_composite(
            route_ctx, session, snapshot_id, valid_accessions, prediction_dicts, emit
        )

    from protea.core.classifier_producer import (
        get_classifier,
        load_classifier_features,
        resolve_go_term_ids,
    )

    accessions = [acc for acc in valid_accessions if acc]
    features, valid = load_classifier_features(session, accessions)
    if not valid:
        _emit_classifier_done(emit, 0, 0)
        return prediction_dicts
    preds = get_classifier().predict(features, valid)
    go_ids = {pr.go_id for pr in preds}
    gid_by_go = resolve_go_term_ids(session, go_ids, snapshot_id)
    merged, added = _merge_classifier_preds(prediction_dicts, preds, gid_by_go)
    _emit_classifier_done(emit, len(valid), added)
    return merged


def classifier_route_by_category_enabled() -> bool:
    """True when the opt-in per-category composite classifier routing is on.

    Reads ``serve.classifier_impl_by_category`` (default False), so serve keeps
    the single global ``PROTEA_CLASSIFIER_IMPL`` selection unless a deploy opts
    in. Kept as a tiny helper so the gate is trivially unit-testable.
    """
    from protea.config.tuning import get_tuning

    return get_tuning().serve.classifier_impl_by_category


def apply_classifier_composite(
    route_ctx: ClassifierRouteCtx,
    session: Session,
    snapshot_id: uuid.UUID,
    valid_accessions: list[str],
    prediction_dicts: list[dict[str, Any]],
    emit: EmitFn,
) -> list[dict[str, Any]]:
    """Route classifier candidates per CAFA category (composite champion pool).

    Runs BOTH heads, then keeps each candidate from the head its category wants:
    PK cells (candidate aspect in the protein's pre-cutoff known aspects) come
    from the two-tower sparse head, NK / LK cells from the M2 anc2vec head. The
    merge into ``prediction_dicts`` is the same strict union the single-impl
    path uses, so downstream reranking is unaffected beyond the candidate pool.
    """
    from protea.core.classifier_producer import _TWO_TOWER_IMPL, resolve_go_term_ids
    from protea.core.operations.predict_go_terms._category_dispatch import (
        _known_aspects_by_protein,
        _own_exp_for,
    )
    from protea.core.operations.predict_go_terms._post_knn_pipeline import (
        _load_known_aspects,
    )

    accessions = [acc for acc in valid_accessions if acc]
    own_exp = _own_exp_for(route_ctx.op, session, route_ctx.annotation_set_id, set(accessions))
    known_aspects = _known_aspects_by_protein(session, own_exp)

    m2_preds = _predict_for_impl(session, accessions, "m2")
    tt_preds = _predict_for_impl(session, accessions, _TWO_TOWER_IMPL)

    go_ids = {pr.go_id for pr in m2_preds} | {pr.go_id for pr in tt_preds}
    gid_by_go = resolve_go_term_ids(session, go_ids, snapshot_id)
    aspect_by_gid = _load_known_aspects(session, set(gid_by_go.values()))

    routed = _route_composite_preds(m2_preds, tt_preds, known_aspects, gid_by_go, aspect_by_gid)
    merged, added = _merge_classifier_preds(prediction_dicts, routed, gid_by_go)
    _emit_classifier_done(emit, len({pr.accession for pr in routed}), added)
    return merged


def _predict_for_impl(session: Session, accessions: list[str], impl: str) -> list[Any]:
    """Classifier predictions from a SPECIFIC head, or ``[]`` when no features."""
    from protea.core.classifier_producer import get_classifier, load_classifier_features

    features, valid = load_classifier_features(session, accessions, impl=impl)
    if not valid:
        return []
    return get_classifier(impl=impl).predict(features, valid)


def _route_composite_preds(
    m2_preds: list[Any],
    tt_preds: list[Any],
    known_aspects: dict[str, frozenset[str]],
    gid_by_go: dict[str, int],
    aspect_by_gid: dict[int, str],
) -> list[Any]:
    """Keep M2 preds for NK / LK cells and two-tower preds for PK cells.

    A candidate is PK when the protein carries a pre-cutoff known term in the
    candidate's aspect; those come from the two-tower head. Everything else
    (NK proteins entirely, plus a K protein's other-aspect candidates) comes
    from M2. The routing is per (protein, candidate aspect), matching the CAFA
    category split the reranker applies downstream.
    """

    def is_pk(pred: Any) -> bool:
        aspects = known_aspects.get(pred.accession)
        if not aspects:
            return False
        gid = gid_by_go.get(pred.go_id)
        if gid is None:
            return False
        aspect = aspect_by_gid.get(gid, "")
        return bool(aspect) and aspect in aspects

    routed = [pr for pr in m2_preds if not is_pk(pr)]
    routed.extend(pr for pr in tt_preds if is_pk(pr))
    return routed


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
    "ClassifierRouteCtx",
    "apply_classifier",
    "apply_classifier_composite",
    "classifier_route_by_category_enabled",
)
