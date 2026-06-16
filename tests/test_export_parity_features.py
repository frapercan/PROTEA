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
from protea.core.training_dump._export_features import ExportParityFlags

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
_COOC = {10: {99: 1}}  # known 10 co-occurs once with candidate 99
_FREQ = {10: 2}  # P(99|10) = 1/2
_ASPECTS = {10: "F", 99: "P"}  # candidate 99 is a DIFFERENT aspect -> cross
_CLF_PREDS = [ClassifierPrediction("Q1", "GO:0000099", 0.9)]
_GID_BY_GO = {"GO:0000099": 99}


def _run_export(records, flags: ExportParityFlags):
    """Drive the export applier with every DB call mocked."""
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
        patch.object(pkp, "_load_known_aspects", return_value=_ASPECTS),
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
        ef.apply_export_parity_features(MagicMock(), _SET_ID, _SNAPSHOT_ID, ["Q1"], records, flags)
    return records


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
    out = _run_export(_records(), ExportParityFlags(classifier=True))
    by_term = {r["go_term_id"]: r for r in out}
    assert by_term[99]["classifier_score"] == 0.9
    assert by_term[99]["classifier_present"] == 1.0
    # No classifier-only candidate is appended (row count unchanged).
    assert len(out) == 2


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
        patch.object(pkp, "_load_known_aspects", return_value=_ASPECTS),
    ):
        pkp.apply_association(op, MagicMock(), _SET_ID, ["Q1"], oracle, lambda *_a, **_k: None)

    export = _run_export(_records(), ExportParityFlags(association=True))
    keys = ("association_total", "association_cross", "association_present")
    oracle_v = {r["go_term_id"]: tuple(r[k] for k in keys) for r in oracle}
    export_v = {r["go_term_id"]: tuple(r[k] for k in keys) for r in export}
    assert export_v == oracle_v
