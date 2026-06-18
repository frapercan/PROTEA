"""Unit tests for the INT-6 export train/serve parity applier.

Targets ``protea.core.training_dump._export_features.apply_export_parity_features``.
The export path historically emitted zero-fill defaults for the six LAFA
columns; INT-6 reuses the predict-path producers so the exported feature rows
carry the SAME real values the predict path serves (NFR-REPRO).

The DB-bound pieces (annotation loader, co-occurrence loader, aspect lookup,
classifier model + embeddings) are mocked, so the tests exercise the wiring and
the leakage-clean parity contract against the predict producers as the oracle:

* flags OFF -> records untouched (zeros), so the default export is bit-identical.
* flags ON  -> self_prior / association / classifier_* carry non-zero values
  IDENTICAL to what the predict producers compute on the same synthetic input.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import numpy as np

from protea.core.classifier_producer import ClassifierPrediction
from protea.core.operations.predict_go_terms import _post_knn_pipeline as pkp
from protea.core.training_dump import _export_features as ef
from protea.core.training_dump._export_features import ClassifierUnionSpec, ExportParityFlags

_SET_ID = MagicMock(name="t0_annotation_set_id")
_SNAPSHOT_ID = MagicMock(name="ontology_snapshot_id")


def _records() -> list[dict]:
    """Two export candidates for Q1: a known cross-aspect term + a neighbour."""
    return [
        {
            "protein_accession": "Q1",
            "go_term_id": 99,
            "self_prior_score": 0.0,
            "association_total": 0.0,
            "association_cross": 0.0,
            "association_present": 0.0,
            "classifier_score": 0.0,
            "classifier_present": 0.0,
        },
        {
            "protein_accession": "Q1",
            "go_term_id": 10,
            "self_prior_score": 0.0,
            "association_total": 0.0,
            "association_cross": 0.0,
            "association_present": 0.0,
            "classifier_score": 0.0,
            "classifier_present": 0.0,
        },
    ]


# Synthetic fixture shared by the export applier and the predict oracle.
# Q1 KNOWS term 10 experimentally (aspect F) and non-experimentally too.
_NONEXP_ANN = {"Q1": [{"go_term_id": 10, "evidence_code": "IEA"}]}
_EXP_ANN = {"Q1": [{"go_term_id": 10, "evidence_code": "IDA"}]}
# Co-occurrence + frequency are now keyed on the snapshot-invariant go_id string.
_COOC = {"GO:0000010": {"GO:0000099": 1}}  # known GO:0000010 co-occurs once with GO:0000099
_FREQ = {"GO:0000010": 2}  # P(GO:0000099 | GO:0000010) = 1/2
# int -> go_id resolver + go_id -> aspect (candidate is a DIFFERENT aspect -> cross).
_GO_ID_BY_INT = {10: "GO:0000010", 99: "GO:0000099"}
_ASPECT_BY_GO = {"GO:0000010": "F", "GO:0000099": "P"}
_CLF_PREDS = [ClassifierPrediction("Q1", "GO:0000099", 0.9)]
_GID_BY_GO = {"GO:0000099": 99}


def _run_export(records, flags: ExportParityFlags, *, classifier_record_factory=None):
    """Drive the export applier with every DB call mocked.

    Returns the (possibly grown) record list the applier emits, so a caller can
    assert on classifier-unioned rows when a ``classifier_record_factory`` is
    supplied.
    """
    op = MagicMock()
    op._load_annotations_for.side_effect = lambda *_a, **_k: (
        _EXP_ANN if flags.association else _NONEXP_ANN
    )
    with (
        patch.object(ef, "_ExportFeatureOp", return_value=op),
        patch(
            "protea.core.operations.predict_go_terms._association_loader."
            "load_cooccurrence_for_known",
            return_value=(_COOC, _FREQ),
        ),
        patch.object(pkp, "_load_go_id_and_aspect", return_value=(_GO_ID_BY_INT, _ASPECT_BY_GO)),
        patch(
            "protea.core.classifier_producer.load_concat_features",
            return_value=(np.zeros((1, 8320), dtype=np.float32), ["Q1"]),
        ),
        patch(
            "protea.core.classifier_producer.get_classifier",
            return_value=MagicMock(predict=MagicMock(return_value=_CLF_PREDS)),
        ),
        patch(
            "protea.core.classifier_producer.resolve_go_term_ids",
            return_value=_GID_BY_GO,
        ),
    ):
        return ef.apply_export_parity_features(
            MagicMock(),
            _SET_ID,
            ["Q1"],
            records,
            flags,
            ClassifierUnionSpec(
                ontology_snapshot_id=_SNAPSHOT_ID,
                record_factory=classifier_record_factory,
                aspect_by_term_id={99: "P", 10: "F"},
            ),
        )


def test_flags_off_is_a_noop_zeros_preserved() -> None:
    out = _run_export(_records(), ExportParityFlags())
    for rec in out:
        assert rec["self_prior_score"] == 0.0
        assert rec["association_total"] == 0.0
        assert rec["association_present"] == 0.0
        assert rec["classifier_score"] == 0.0
        assert rec["classifier_present"] == 0.0


def test_self_prior_marks_own_nonexp_known_term() -> None:
    out = _run_export(_records(), ExportParityFlags(self_prior=True))
    by_term = {r["go_term_id"]: r for r in out}
    # Term 10 is in Q1's own non-exp annotations -> self_prior 1.0.
    assert by_term[10]["self_prior_score"] == 1.0
    # Term 99 is not -> stays zero.
    assert by_term[99]["self_prior_score"] == 0.0


def test_association_scores_cross_aspect_candidate() -> None:
    out = _run_export(_records(), ExportParityFlags(association=True))
    by_term = {r["go_term_id"]: r for r in out}
    # P(99|10) = cooc/freq = 1/2; 10 is aspect F, candidate 99 is P -> cross.
    assert by_term[99]["association_total"] == 0.5
    assert by_term[99]["association_cross"] == 0.5
    assert by_term[99]["association_present"] == 1.0


def test_classifier_marks_existing_candidate_only() -> None:
    # No factory supplied -> stamp-only (the isolated, legacy contract): the
    # matching candidate is marked and no new row is appended.
    out = _run_export(_records(), ExportParityFlags(classifier=True))
    by_term = {r["go_term_id"]: r for r in out}
    assert by_term[99]["classifier_score"] == 0.9
    assert by_term[99]["classifier_present"] == 1.0
    assert len(out) == 2


def _fake_classifier_record(q_acc: str, go_id: str, aspect: str, score: float) -> dict:
    """Stand-in for ``_LeafRecordBuilder.build_classifier_only_record``.

    Emits a minimal canonical-shaped classifier-only row (the production factory
    fills the full ~60-column block; the union wiring only needs these markers).
    """
    return {
        "protein_accession": q_acc,
        "go_id": go_id,
        "aspect": aspect,
        "label": 0,
        "distance": float("nan"),
        "knn_present": False,
        "classifier_score": score,
        "classifier_present": 1.0,
        "self_prior_score": 0.0,
        "association_total": 0.0,
        "association_cross": 0.0,
        "association_present": 0.0,
    }


def test_classifier_unions_new_candidate_when_factory_supplied() -> None:
    """REGRESSION (native 0.391 parity): a classifier proposal absent from the
    KNN candidate set is APPENDED as a new classifier-only row, not dropped.

    This proves the export training pool matches the predict pool
    ``union(knn, classifier, ...)``. Without it the booster trains on a
    KNN-only pool but evals on a KNN+classifier pool, capping native f_micro_w
    well below the offline champion.
    """
    # Only term 10 is a KNN candidate; the classifier proposes GO:0000099 (gid
    # 99), which is NOT in the candidate set -> it must be unioned in.
    knn_only = [
        {
            "protein_accession": "Q1",
            "go_term_id": 10,
            "go_id": "GO:0000010",
            "aspect": "F",
            "self_prior_score": 0.0,
            "association_total": 0.0,
            "association_cross": 0.0,
            "association_present": 0.0,
            "classifier_score": 0.0,
            "classifier_present": 0.0,
        },
    ]
    out = _run_export(
        knn_only,
        ExportParityFlags(classifier=True),
        classifier_record_factory=_fake_classifier_record,
    )
    # The classifier-only candidate was appended (KNN row + 1 new row).
    assert len(out) == 2
    new_rows = [r for r in out if r["go_id"] == "GO:0000099"]
    assert len(new_rows) == 1
    added = new_rows[0]
    # Classifier-only row markers: present in the classifier family, absent from
    # KNN evidence (knn_present False, distance NaN). These are exactly the
    # out-of-distribution rows a KNN-only export was missing.
    assert added["classifier_present"] == 1.0
    assert added["classifier_score"] == 0.9
    assert added["knn_present"] is False
    assert np.isnan(added["distance"])
    # The appended row carries the proposal's aspect (resolved via the gid).
    assert added["aspect"] == "P"
    # It carries a transient ``go_term_id`` (the runner strips it pre-emit) so
    # the downstream ``(protein, go_id)`` labelers can label it like a KNN row.
    assert added["go_term_id"] == 99


def test_parity_self_prior_matches_predict_producer() -> None:
    """The export self_prior values must equal the predict producer's, exactly."""
    oracle = _records()
    op = MagicMock()
    op._load_annotations_for.return_value = _NONEXP_ANN
    pkp.apply_self_prior(op, MagicMock(), _SET_ID, ["Q1"], oracle, lambda *_a, **_k: None)

    export = _run_export(_records(), ExportParityFlags(self_prior=True))
    oracle_scores = {r["go_term_id"]: r["self_prior_score"] for r in oracle}
    export_scores = {r["go_term_id"]: r["self_prior_score"] for r in export}
    assert export_scores == oracle_scores


def test_parity_association_matches_predict_producer() -> None:
    """The export association values must equal the predict producer's, exactly."""
    oracle = _records()
    op = MagicMock()
    op._load_annotations_for.return_value = _EXP_ANN
    with (
        patch(
            "protea.core.operations.predict_go_terms._association_loader."
            "load_cooccurrence_for_known",
            return_value=(_COOC, _FREQ),
        ),
        patch.object(pkp, "_load_go_id_and_aspect", return_value=(_GO_ID_BY_INT, _ASPECT_BY_GO)),
    ):
        pkp.apply_association(op, MagicMock(), _SET_ID, ["Q1"], oracle, lambda *_a, **_k: None)

    export = _run_export(_records(), ExportParityFlags(association=True))
    keys = ("association_total", "association_cross", "association_present")
    oracle_v = {r["go_term_id"]: tuple(r[k] for k in keys) for r in oracle}
    export_v = {r["go_term_id"]: tuple(r[k] for k in keys) for r in export}
    assert export_v == oracle_v
