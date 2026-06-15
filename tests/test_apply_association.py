"""Unit tests for the native cross-aspect association compute (INT-3).

Targets ``apply_association`` in
``protea.core.operations.predict_go_terms._post_knn_pipeline``. The DB-bound
pieces (annotation loader, co-occurrence loader, aspect lookup) are mocked so
the test exercises only the per-candidate scoring math against the offline
oracle: ``association_total = sum_k P(t|k)``, ``association_cross`` restricts to
known terms in a different aspect, ``association_present = 1.0`` iff total > 0.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from protea.core.operations.predict_go_terms import _post_knn_pipeline as pkp


def _emit(*_args, **_kwargs) -> None:
    return None


def _run(prediction_dicts, *, annotations, cooc, freq, aspects):
    """Drive ``apply_association`` with all DB calls mocked.

    ``annotations`` -> what ``op._load_annotations_for`` returns.
    ``cooc`` / ``freq`` -> what ``load_cooccurrence_for_known`` returns.
    ``aspects`` -> ``{term_id: aspect}`` for ``_load_known_aspects``.
    """
    op = MagicMock()
    op._load_annotations_for.return_value = annotations
    set_id = MagicMock()
    accs = sorted({r["protein_accession"] for r in prediction_dicts})
    with (
        patch(
            "protea.core.operations.predict_go_terms._association_loader."
            "load_cooccurrence_for_known",
            return_value=(cooc, freq),
        ),
        patch.object(pkp, "_load_known_aspects", return_value=aspects),
    ):
        pkp.apply_association(op, MagicMock(), set_id, accs, prediction_dicts, _emit)
    return prediction_dicts


def test_association_total_sums_conditional_probabilities() -> None:
    # Q1 knows term 10 (freq 2). Candidate 99: cooc(10,99)=1 -> P=0.5.
    annotations = {"Q1": [{"go_term_id": 10, "evidence_code": "IDA"}]}
    preds = [
        {
            "protein_accession": "Q1",
            "go_term_id": 99,
            "association_total": 0.0,
            "association_cross": 0.0,
            "association_present": 0.0,
        },
    ]
    out = _run(
        preds,
        annotations=annotations,
        cooc={10: {99: 1}},
        freq={10: 2},
        aspects={10: "F", 99: "P"},
    )
    assert out[0]["association_total"] == 0.5
    assert out[0]["association_present"] == 1.0


def test_association_cross_only_counts_different_aspect_known_terms() -> None:
    # Two known terms for Q1: 10 (aspect F), 20 (aspect P). Candidate 99 is P.
    # total = P(99|10)+P(99|20); cross drops same-aspect (20, P) contribution.
    annotations = {
        "Q1": [
            {"go_term_id": 10, "evidence_code": "IDA"},
            {"go_term_id": 20, "evidence_code": "IMP"},
        ]
    }
    preds = [
        {
            "protein_accession": "Q1",
            "go_term_id": 99,
            "association_total": 0.0,
            "association_cross": 0.0,
            "association_present": 0.0,
        },
    ]
    out = _run(
        preds,
        annotations=annotations,
        cooc={10: {99: 1}, 20: {99: 2}},
        freq={10: 2, 20: 2},  # P(99|10)=0.5, P(99|20)=1.0
        aspects={10: "F", 20: "P", 99: "P"},
    )
    assert out[0]["association_total"] == 1.5
    # cross excludes 20 (same aspect P as candidate 99); only 10 (F) counts.
    assert out[0]["association_cross"] == 0.5


def test_no_known_terms_leaves_zero_fill() -> None:
    # Only non-experimental annotation -> not a known term -> all zero.
    annotations = {"Q1": [{"go_term_id": 10, "evidence_code": "IEA"}]}
    preds = [
        {
            "protein_accession": "Q1",
            "go_term_id": 99,
            "association_total": 0.0,
            "association_cross": 0.0,
            "association_present": 0.0,
        },
    ]
    out = _run(preds, annotations=annotations, cooc={}, freq={}, aspects={99: "P"})
    assert out[0]["association_total"] == 0.0
    assert out[0]["association_present"] == 0.0


def test_empty_table_degrades_gracefully() -> None:
    # Known term exists but co-occurrence table is empty for this set.
    annotations = {"Q1": [{"go_term_id": 10, "evidence_code": "IDA"}]}
    preds = [
        {
            "protein_accession": "Q1",
            "go_term_id": 99,
            "association_total": 0.0,
            "association_cross": 0.0,
            "association_present": 0.0,
        },
    ]
    out = _run(preds, annotations=annotations, cooc={}, freq={}, aspects={10: "F", 99: "P"})
    assert out[0]["association_total"] == 0.0
    assert out[0]["association_present"] == 0.0


def test_candidate_with_no_cooccurrence_stays_zero() -> None:
    # Known 10 co-occurs with 99 but not with candidate 77.
    annotations = {"Q1": [{"go_term_id": 10, "evidence_code": "IDA"}]}
    preds = [
        {
            "protein_accession": "Q1",
            "go_term_id": 99,
            "association_total": 0.0,
            "association_cross": 0.0,
            "association_present": 0.0,
        },
        {
            "protein_accession": "Q1",
            "go_term_id": 77,
            "association_total": 0.0,
            "association_cross": 0.0,
            "association_present": 0.0,
        },
    ]
    out = _run(
        preds,
        annotations=annotations,
        cooc={10: {99: 1}},
        freq={10: 2},
        aspects={10: "F", 99: "P", 77: "C"},
    )
    assert out[0]["association_present"] == 1.0
    assert out[1]["association_total"] == 0.0
    assert out[1]["association_present"] == 0.0


def test_own_exp_terms_filters_evidence_and_dedups() -> None:
    annotations = {
        "Q1": [
            {"go_term_id": 1, "evidence_code": "IDA"},  # experimental -> keep
            {"go_term_id": 1, "evidence_code": "IMP"},  # dup term, experimental
            {"go_term_id": 2, "evidence_code": "IEA"},  # non-exp -> drop
        ],
        "Q2": [{"go_term_id": 3, "evidence_code": "ISS"}],  # non-exp -> drop, no entry
    }
    own = pkp._own_exp_terms(annotations)
    assert own == {"Q1": {1}}
