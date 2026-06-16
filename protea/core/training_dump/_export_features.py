"""Train/serve feature parity for the dataset export (lafa-integrate INT-6).

The PREDICT path computes the ``self_prior`` / ``association`` / ``classifier``
feature families behind the ``compute_self_prior`` / ``compute_association`` /
``compute_classifier`` flags (see
``protea.core.operations.predict_go_terms._post_knn_pipeline``). The EXPORT
path (``export_research_dataset`` reusing ``TrainRerankerAutoOperation`` in
``dump_only`` mode) historically emitted the zero-fill defaults for those six
columns, so a booster trained on an exported dataset saw zeros for families the
predict path actually serves: a train/serve mismatch (NFR-REPRO).

This module closes that gap by REUSING the exact predict producers on the
export's per-query candidate records. It does not duplicate the scoring math:

* ``apply_self_prior`` / ``apply_association`` are imported verbatim from
  ``_post_knn_pipeline`` (pure list mutators keyed by
  ``(protein_accession, go_term_id)``).
* the classifier reuses ``classifier_producer.load_concat_features`` /
  ``get_classifier`` / ``resolve_go_term_ids`` and MARKS the export candidates
  the classifier agrees with (sets ``classifier_score`` / ``classifier_present``
  in place). The predict-path classifier UNION (appending brand-new
  full-vocabulary candidate rows) is a candidate-recall mechanism orthogonal to
  feature parity and is left to the predict path; the export keeps its KNN +
  ancestor + InterPro candidate set so the parquet schema and row distribution
  stay stable.

Leakage discipline (load-bearing): every feature value is derived ONLY from the
pre-cutoff t0 annotation set (the same ``annotation_set_id`` the KNN reference
pool was built from, never a post-cutoff set), so the features for a row use
only data at or before that row's prediction cutoff. The producers themselves
drop NOT-qualified rows and split experimental vs. non-experimental evidence.
The applier runs per query (all of that query's candidates together), so it is
invoked identically in the list (train) and streaming (test) record paths.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

from protea.core.operations.predict_go_terms._batch_op_feature import _FeatureLoadingMixin
from protea.core.operations.predict_go_terms._post_knn_pipeline import (
    apply_association,
    apply_self_prior,
)

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from protea.core.operations.predict_go_terms._batch_op import (
        PredictGOTermsBatchOperation,
    )


@dataclass(frozen=True)
class ExportParityFlags:
    """Which INT-6 parity feature families the export should compute.

    Each flag independently enables one predict-path producer. All False (the
    default) makes :func:`apply_export_parity_features` a no-op, so the default
    export keeps the leaf builder's zero-fill defaults (bit-identical).
    """

    self_prior: bool = False
    association: bool = False
    classifier: bool = False

    @property
    def any(self) -> bool:
        return self.self_prior or self.association or self.classifier


class _ExportFeatureOp(_FeatureLoadingMixin):
    """Minimal adapter exposing the annotation loader the producers need.

    ``apply_self_prior`` / ``apply_association`` take an ``op`` purely to call
    ``op._load_annotations_for``; that method lives on ``_FeatureLoadingMixin``
    and has no other instance state, so a bare mixin instance is a faithful,
    leakage-equivalent stand-in for the live ``PredictGOTermsBatchOperation``.
    """


def _noop_emit(*_args: Any, **_kwargs: Any) -> None:
    """Swallow producer audit events; the dump pipeline has its own logging."""
    return None


def apply_export_parity_features(
    session: Session,
    t0_annotation_set_id: uuid.UUID,
    ontology_snapshot_id: uuid.UUID,
    valid_accessions: list[str],
    records: list[dict[str, Any]],
    flags: ExportParityFlags,
) -> None:
    """Fill the real self_prior / association / classifier values in place.

    Mutates ``records`` (the export's per-query candidate dicts) so the six
    LAFA columns carry the SAME values the predict path serves, matching what a
    lab booster sees at inference time. Each flag is independent; when all are
    off this is a no-op and the records keep their zero-fill defaults (default
    export stays bit-identical). ``records`` must already carry the six
    zero-filled columns (the leaf builder guarantees this).
    """
    if not flags.any or not records or not valid_accessions:
        return
    # ``_ExportFeatureOp`` only provides ``_load_annotations_for`` (all the
    # producers touch on ``op``); cast to satisfy their stricter annotation.
    op = cast("PredictGOTermsBatchOperation", _ExportFeatureOp())
    if flags.self_prior:
        apply_self_prior(op, session, t0_annotation_set_id, valid_accessions, records, _noop_emit)
    if flags.association:
        apply_association(op, session, t0_annotation_set_id, valid_accessions, records, _noop_emit)
    if flags.classifier:
        _mark_classifier_candidates(session, ontology_snapshot_id, valid_accessions, records)


def _mark_classifier_candidates(
    session: Session,
    ontology_snapshot_id: uuid.UUID,
    valid_accessions: list[str],
    records: list[dict[str, Any]],
) -> None:
    """Set classifier_score / classifier_present on agreed-on export candidates.

    Reuses the predict-path classifier producer's loaders verbatim. Unlike the
    predict path it does NOT append classifier-only candidate rows (that would
    change the export candidate set / parquet row distribution); it only stamps
    the real classifier score onto export candidates the classifier proposes,
    giving the booster the same per-candidate classifier signal at train time.
    """
    from protea.core.classifier_producer import (
        get_classifier,
        load_concat_features,
        resolve_go_term_ids,
    )

    accessions = [acc for acc in valid_accessions if acc]
    features, valid = load_concat_features(session, accessions)
    if not valid:
        return
    preds = get_classifier().predict(features, valid)
    go_ids = {pr.go_id for pr in preds}
    gid_by_go = resolve_go_term_ids(session, go_ids, ontology_snapshot_id)
    by_key: dict[tuple[str, int], dict[str, Any]] = {}
    for rec in records:
        gtid = rec.get("go_term_id")
        if gtid is not None:
            by_key[(rec.get("protein_accession", ""), int(gtid))] = rec
    for pr in preds:
        gtid = gid_by_go.get(pr.go_id)
        if gtid is None:
            continue
        existing = by_key.get((pr.accession, gtid))
        if existing is not None:
            existing["classifier_score"] = float(pr.score)
            existing["classifier_present"] = 1.0


__all__ = ("ExportParityFlags", "apply_export_parity_features")
