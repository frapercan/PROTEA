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
  ``get_classifier`` / ``resolve_go_term_ids`` and UNIONs the classifier's
  full-vocabulary proposals into the export candidate set, exactly like the
  predict path's ``apply_classifier`` / ``_merge_classifier_preds``: existing
  ``(protein, go_id)`` candidates are stamped (``classifier_score`` /
  ``classifier_present``); proposals absent from the KNN + ancestor + InterPro
  set are APPENDED as brand-new full-canonical rows (``knn_present`` False,
  ``distance`` NaN, KNN-derived features NaN). This closes the train/eval pool
  MISMATCH: the predict path serves a ``union(knn, classifier, ...)`` candidate
  pool, so a booster trained on a KNN-only export saw the classifier-added rows
  as out-of-distribution at inference. The new rows carry every canonical
  feature column (the T1.8 boundary holds) and pick up their GT label from the
  SAME downstream ``(protein, go_id)`` labeling the KNN candidates use.

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
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

from protea_contracts import FEATURE_FAMILIES

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
    from protea.core.parquet_export import FamilyProvenance

# Fully-qualified identity of the producer wired for each LAFA family; recorded
# in the dataset manifest provenance so a reader can trace a produced family's
# values back to the exact compute (ADR-D45).
_SELF_PRIOR_PRODUCER = "protea.core.operations.predict_go_terms._post_knn_pipeline.apply_self_prior"
_ASSOCIATION_PRODUCER = (
    "protea.core.operations.predict_go_terms._post_knn_pipeline.apply_association"
)
_CLASSIFIER_PRODUCER = "protea.core.training_dump._export_features._union_classifier_candidates"


@dataclass(frozen=True)
class ExportParityFlags:
    """Which INT-6 parity feature families the export should compute.

    Each flag independently enables one predict-path producer. All False (the
    default) makes :func:`apply_export_parity_features` a no-op, so each family
    stays at the leaf builder's declared-absent default (``NaN``, ADR-D45) and
    is recorded as declared-absent in the dataset manifest.
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


# A factory that builds one full-canonical classifier-only candidate row for a
# ``(protein, go_id, aspect, score)`` the classifier proposes but the KNN +
# ancestor + InterPro candidate set does not already carry. Supplied by the
# runner (it owns the leaf-record builder + aspect map).
ClassifierRecordFactory = Callable[[str, str, str, float], dict[str, Any]]


@dataclass(frozen=True)
class ClassifierUnionSpec:
    """Inputs the classifier candidate-pool union needs beyond the records.

    Bundles the snapshot id (to resolve proposals to term ids), the row
    ``record_factory`` and the ``aspect_by_term_id`` map so
    :func:`apply_export_parity_features` stays under the §3 6-arg ceiling.
    ``record_factory`` ``None`` keeps the legacy STAMP-ONLY behaviour (no new
    rows appended), so the isolated unit-tests and any caller that cannot
    materialise a full canonical row stay safe.

    ``t0_annotation_set_id`` is this cut's t0 annotation set. When set AND the
    two-tower impl is selected with ``PROTEA_TWO_TOWER_GO_CODES_DIR`` configured,
    the classifier scores against the GO co-annotation codes built at THIS cut's
    t0 (per-cut), mirroring the ``association`` feature. ``None`` (the default,
    and the isolated unit-tests) keeps the single fixed-artifact behaviour.
    """

    ontology_snapshot_id: uuid.UUID
    record_factory: ClassifierRecordFactory | None = None
    aspect_by_term_id: dict[int, str] | None = None
    t0_annotation_set_id: uuid.UUID | None = None


def apply_export_parity_features(
    session: Session,
    t0_annotation_set_id: uuid.UUID,
    valid_accessions: list[str],
    records: list[dict[str, Any]],
    flags: ExportParityFlags,
    classifier_union: ClassifierUnionSpec | None = None,
) -> list[dict[str, Any]]:
    """Fill the real self_prior / association / classifier values in place.

    Mutates ``records`` (the export's per-query candidate dicts) so the six
    LAFA columns carry the SAME values the predict path serves, matching what a
    lab booster sees at inference time. Each flag is independent; when all are
    off this is a no-op and every family stays at the leaf builder's
    declared-absent default (``NaN``, ADR-D45), so the default export keeps the
    families declared absent. For a family whose flag IS on, its columns are
    first reset to the true-zero baseline across every record (a producer ran,
    so a non-hit candidate is a genuine ``0``, not a missing measurement) and
    then the producer overwrites its hits. ``records`` must already carry the
    six LAFA columns (the leaf builder guarantees this).

    The classifier step UNIONs the classifier's full-vocabulary proposals into
    the candidate pool exactly like the predict path: existing candidates are
    stamped, proposals absent from the pool are APPENDED as new canonical rows
    via ``classifier_union.record_factory`` (when supplied). This closes the
    train/eval candidate-pool MISMATCH. The self_prior / association producers
    run AFTER the union so the appended rows also pick up their priors when the
    query has known terms.

    Returns the (possibly grown) record list; callers must rebind it. The
    classifier flag requires ``classifier_union`` (the snapshot id is mandatory
    to resolve proposals); when ``flags.classifier`` is off it is ignored.
    """
    if not flags.any or not records or not valid_accessions:
        return records
    # ``_ExportFeatureOp`` only provides ``_load_annotations_for`` (all the
    # producers touch on ``op``); cast to satisfy their stricter annotation.
    op = cast("PredictGOTermsBatchOperation", _ExportFeatureOp())
    # Union the classifier proposals FIRST so the prior producers below see the
    # appended rows (a query's known terms can prime the appended candidate).
    if flags.classifier and classifier_union is not None:
        # ADR-D45: the leaf builder now emits NaN (declared-absent) for the
        # classifier family. Reset it to the true-zero baseline BEFORE the union
        # so an existing candidate the classifier did not propose is a genuine
        # zero (the producer ran and did not vote it), not a missing value; the
        # union then stamps the proposals and appends classifier-only rows.
        _zero_baseline_family(records, FEATURE_FAMILIES["classifier"])
        records = _union_classifier_candidates(session, valid_accessions, records, classifier_union)
    if flags.self_prior:
        _zero_baseline_family(records, FEATURE_FAMILIES["self_prior"])
        apply_self_prior(op, session, t0_annotation_set_id, valid_accessions, records, _noop_emit)
    if flags.association:
        _zero_baseline_family(records, FEATURE_FAMILIES["association"])
        apply_association(op, session, t0_annotation_set_id, valid_accessions, records, _noop_emit)
    return records


def _zero_baseline_family(records: list[dict[str, Any]], columns: list[str]) -> None:
    """Reset one PRODUCED family's columns to the true-zero baseline in place.

    The predict-path producers (``apply_self_prior`` / ``apply_association`` and
    the classifier stamping) only overwrite the candidates they hit and rely on
    the non-hit candidates carrying a well-defined ``0`` (a producer ran and did
    not fire on this candidate). The leaf builder's default is now ``NaN``
    (declared-absent, ADR-D45), so when the export wires a producer for a family
    we first stamp that family's true-zero baseline across every record; the
    producer then overwrites its hits. A DECLARED-ABSENT family is never passed
    here, so its columns stay ``NaN``.
    """
    for rec in records:
        for col in columns:
            rec[col] = 0.0


def build_lafa_family_provenance(flags: ExportParityFlags) -> tuple[FamilyProvenance, ...]:
    """Provenance rows for the three LAFA families given the export flags.

    Each family is ``produced`` (a producer is wired for this export) or
    ``declared_absent`` (no producer; the six columns ship as ``NaN``). The
    export writes these rows into the dataset manifest so a reader learns a
    family's absence from metadata instead of by noticing a column of zeros
    (ADR-D45). ``producer`` names the wired producer when one exists.
    """
    from protea.core.parquet_export import DECLARED_ABSENT, PRODUCED, FamilyProvenance

    wiring = (
        ("classifier", flags.classifier, _CLASSIFIER_PRODUCER),
        ("self_prior", flags.self_prior, _SELF_PRIOR_PRODUCER),
        ("association", flags.association, _ASSOCIATION_PRODUCER),
    )
    return tuple(
        FamilyProvenance(
            family=family,
            state=PRODUCED if on else DECLARED_ABSENT,
            producer=producer if on else None,
        )
        for family, on, producer in wiring
    )


def _union_classifier_candidates(
    session: Session,
    valid_accessions: list[str],
    records: list[dict[str, Any]],
    spec: ClassifierUnionSpec,
) -> list[dict[str, Any]]:
    """Stamp existing candidates and APPEND classifier-only proposals.

    Reuses the predict-path classifier producer's loaders verbatim. The protein
    tower + learned head are t0-independent, so the per-protein output is
    memoised and reused across the 13 train snapshot pairs (+ the test pair). The
    one t0-dependent part is the two-tower's frozen GO co-annotation codes:
    :func:`_resolve_per_cut_go_codes` picks THIS cut's t0 codes (mirroring the
    ``association`` feature) so an earlier pair never sees a later cut's
    co-annotation. ``None`` (no per-cut dir, or M2) keeps the single fixed
    artifact; the cache key carries the codes identity so cuts do not collide.
    """
    from protea.core.classifier_producer import (
        classifier_impl,
        predict_proteins_cached,
    )

    accessions = [acc for acc in valid_accessions if acc]
    go_codes_path = _resolve_per_cut_go_codes(session, spec, classifier_impl())
    preds = predict_proteins_cached(session, accessions, go_codes_path=go_codes_path)
    if not preds:
        return records
    return _merge_classifier_proposals(session, preds, records, spec)


def _merge_classifier_proposals(
    session: Session,
    preds: list[Any],
    records: list[dict[str, Any]],
    spec: ClassifierUnionSpec,
) -> list[dict[str, Any]]:
    """Mirror ``_post_knn_pipeline._merge_classifier_preds`` on export records.

    A proposal matching an existing ``(protein, go_term_id)`` candidate stamps it
    (``classifier_score`` / ``classifier_present``); a proposal with no match
    becomes a new full-canonical row built by ``spec.record_factory``. With no
    factory only the stamping runs (the export keeps its prior KNN-only pool), so
    an isolated caller stays safe. Returns the (possibly grown) list.
    """
    from protea.core.classifier_producer import resolve_go_term_ids

    go_ids = {pr.go_id for pr in preds}
    gid_by_go = resolve_go_term_ids(session, go_ids, spec.ontology_snapshot_id)
    by_key: dict[tuple[str, int], dict[str, Any]] = {}
    for rec in records:
        gtid = rec.get("go_term_id")
        if gtid is not None:
            by_key[(rec.get("protein_accession", ""), int(gtid))] = rec
    aspect_map = spec.aspect_by_term_id or {}
    for pr in preds:
        gtid = gid_by_go.get(pr.go_id)
        if gtid is None:
            continue
        existing = by_key.get((pr.accession, gtid))
        if existing is not None:
            existing["classifier_score"] = float(pr.score)
            existing["classifier_present"] = 1.0
            continue
        if spec.record_factory is None:
            continue
        rec = spec.record_factory(pr.accession, pr.go_id, aspect_map.get(gtid, ""), float(pr.score))
        # Carry the int id transiently so the prior producers (which key by
        # ``go_term_id``) can score the appended row; the runner strips it
        # before emit, leaving the parquet schema unchanged.
        rec["go_term_id"] = gtid
        by_key[(pr.accession, gtid)] = rec
        records.append(rec)
    return records


def _resolve_per_cut_go_codes(session: Session, spec: ClassifierUnionSpec, impl: str) -> str | None:
    """Per-cut two-tower GO codes path for this cut's t0, or ``None``.

    Only the two-tower impl varies its GO codes per cut; the M2 head is fully
    t0-independent (no-op). Returns ``None`` when no t0 is set, the impl is not
    two-tower, or no per-cut artifact is configured/found, so the caller falls
    back to the single fixed artifact (today's behaviour).
    """
    from protea.core.classifier_producer import _TWO_TOWER_IMPL
    from protea.core.two_tower_classifier import resolve_per_cut_go_codes_path

    if spec.t0_annotation_set_id is None or impl != _TWO_TOWER_IMPL:
        return None
    return resolve_per_cut_go_codes_path(session, spec.t0_annotation_set_id)


__all__ = (
    "ClassifierRecordFactory",
    "ClassifierUnionSpec",
    "ExportParityFlags",
    "apply_export_parity_features",
    "build_lafa_family_provenance",
)
