"""Serve-side GO-DAG lineage producer (``compute_lineage_features`` opt-in).

Serve-path analogue of the export-time lineage producer
(``protea.core._knn_transfer_runner._KnnTransferRunner._apply_lineage_features``
-> :func:`protea.core._feature_enricher_helpers.compute_lineage_into`). The
export runner is not on the serve path, so without this pass a served
prediction keeps the builder zero-fill for the four ``lineage_*`` columns; a
lineage-aware booster would then see served-zero where it trained on real
GO-DAG values (a D45-class value skew).

:func:`apply_lineage` is gated by the ``compute_lineage_features`` payload
flag in
:func:`protea.core.operations.predict_go_terms._post_knn_pipeline._apply_lafa_score_features`.
When the flag is off (the default) the pass never runs and the canonical
zero-fill (and the eight existing schema shas) are untouched.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any

from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn

if TYPE_CHECKING:
    from protea.core.operations.predict_go_terms._batch_op import (
        PredictGOTermsBatchOperation,
    )


def apply_lineage(
    op: PredictGOTermsBatchOperation,
    session: Session,
    ontology_snapshot_id: uuid.UUID,
    annotation_set_id: uuid.UUID,
    prediction_dicts: list[dict[str, Any]],
    emit: EmitFn,
) -> None:
    """Fill the four ``lineage_*`` columns per candidate from the GO DAG.

    For each query protein with pre-cutoff EXPERIMENTAL known terms ``K(p)``
    (resolved from the same ``annotation_set_id`` the KNN pool uses, never a
    post-cutoff set), each candidate term ``t`` gets ``is_ancestor_of_known``
    / ``ancestor_of_count`` (how many known terms ``t`` is an ancestor of) and
    ``is_descendant_of_known`` / ``descendant_of_count`` (the inverse). Values
    are byte-identical to the library producer; see
    :func:`protea.core._feature_enricher_helpers.compute_lineage_into`.

    Leakage guardrails mirror ``apply_association``: known terms come from the
    pre-cutoff set only, NOT-qualified rows are dropped by
    ``_load_annotations_for``, and non-experimental evidence is filtered out.
    """
    from protea.core._feature_enricher_helpers import compute_lineage_into

    if not prediction_dicts:
        return
    parents, own_exp_go = _load_lineage_inputs(
        op, session, ontology_snapshot_id, annotation_set_id, prediction_dicts
    )
    cache: dict[str, frozenset[str]] = {}
    recs_by_protein: dict[str, list[dict[str, Any]]] = {}
    for rec in prediction_dicts:
        recs_by_protein.setdefault(rec.get("protein_accession", ""), []).append(rec)

    queries_with_known = 0
    for acc, recs in recs_by_protein.items():
        known = own_exp_go.get(acc, set())
        if known:
            queries_with_known += 1
        compute_lineage_into(recs, parents=parents, known=known, cache=cache)

    emit(
        "predict_go_terms_batch.lineage_done",
        None,
        {"queries_with_known": queries_with_known, "rows_enriched": len(prediction_dicts)},
        "info",
    )


def _load_lineage_inputs(
    op: PredictGOTermsBatchOperation,
    session: Session,
    ontology_snapshot_id: uuid.UUID,
    annotation_set_id: uuid.UUID,
    prediction_dicts: list[dict[str, Any]],
) -> tuple[dict[str, list[str]], dict[str, set[str]]]:
    """Load the GO parent map + per-query experimental known terms.

    Also stamps the snapshot-invariant ``go_id`` string onto any candidate
    record that lacks one, so ``compute_lineage_into`` (which keys on the
    go_id string) traverses the same id-space the closures use. Returns the
    parent map shaped for the producer plus ``{accession: {known go_id}}``.
    """
    from protea.core.feature_enricher import load_parent_map
    from protea.core.operations.predict_go_terms._post_knn_pipeline import (
        _load_own_exp_for_association,
        _resolve_association_go_ids,
    )

    accessions = sorted(
        {r.get("protein_accession", "") for r in prediction_dicts if r.get("protein_accession")}
    )
    own_exp = _load_own_exp_for_association(op, session, annotation_set_id, accessions)
    go_id_by_int, _aspect_by_go, own_exp_go = _resolve_association_go_ids(
        session, own_exp, prediction_dicts
    )
    for rec in prediction_dicts:
        gtid = rec.get("go_term_id")
        if gtid is None or rec.get("go_id"):
            continue
        resolved = go_id_by_int.get(int(gtid))
        if resolved is not None:
            rec["go_id"] = resolved

    parent_map = load_parent_map(session, ontology_snapshot_id)
    parents = {gid: list(ps) for gid, ps in parent_map.items()}
    return parents, own_exp_go


__all__ = ("apply_lineage",)
