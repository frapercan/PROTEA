"""Unit tests for the native cross-aspect association compute (INT-3).

Targets ``apply_association`` in
``protea.core.operations.predict_go_terms._post_knn_pipeline``. The DB-bound
pieces (annotation loader, co-occurrence loader, int->go_id + aspect lookup) are
mocked so the test exercises only the per-candidate scoring math against the
offline oracle: ``association_total = sum_k P(t|k)``, ``association_cross``
restricts to known terms in a different aspect, ``association_present = 1.0``
iff total > 0.

Snapshot invariance (the bug this slice fixes): GO term integer ids are per
ontology snapshot, so the association feature MUST key on the snapshot-invariant
``go_id`` STRING. These tests drive the producer with known terms whose int ids
live in a DIFFERENT id-space than the candidate int ids (mirroring a t0 set on
one snapshot scoring candidates from another) and assert the score is the same
as when they share one id-space.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from protea.core.operations.predict_go_terms import _post_knn_pipeline as pkp


def _emit(*_args, **_kwargs) -> None:
    return None


def _run(prediction_dicts, *, annotations, cooc, freq, go_id_by_int, aspect_by_go):
    """Drive ``apply_association`` with all DB calls mocked.

    ``annotations``   -> what ``op._load_annotations_for`` returns.
    ``cooc`` / ``freq`` -> what ``load_cooccurrence_for_known`` returns
                          (now keyed on the go_id STRING).
    ``go_id_by_int``  -> ``{go_term_id: go_id}`` resolver (both known + candidate
                          int ids; mirrors snapshot-scoped GOTerm rows).
    ``aspect_by_go``  -> ``{go_id: aspect}`` (snapshot-invariant aspect).
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
        patch.object(pkp, "_load_go_id_and_aspect", return_value=(go_id_by_int, aspect_by_go)),
    ):
        pkp.apply_association(op, MagicMock(), set_id, accs, prediction_dicts, _emit)
    return prediction_dicts


def test_association_total_sums_conditional_probabilities() -> None:
    # Q1 knows term 10 (go GO:0000010, freq 2). Candidate 99 (GO:0000099):
    # cooc(GO:0000010, GO:0000099)=1 -> P=0.5.
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
        cooc={"GO:0000010": {"GO:0000099": 1}},
        freq={"GO:0000010": 2},
        go_id_by_int={10: "GO:0000010", 99: "GO:0000099"},
        aspect_by_go={"GO:0000010": "F", "GO:0000099": "P"},
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
        cooc={"GO:0000010": {"GO:0000099": 1}, "GO:0000020": {"GO:0000099": 2}},
        freq={"GO:0000010": 2, "GO:0000020": 2},  # P(99|10)=0.5, P(99|20)=1.0
        go_id_by_int={10: "GO:0000010", 20: "GO:0000020", 99: "GO:0000099"},
        aspect_by_go={"GO:0000010": "F", "GO:0000020": "P", "GO:0000099": "P"},
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
    out = _run(
        preds,
        annotations=annotations,
        cooc={},
        freq={},
        go_id_by_int={99: "GO:0000099"},
        aspect_by_go={"GO:0000099": "P"},
    )
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
    out = _run(
        preds,
        annotations=annotations,
        cooc={},
        freq={},
        go_id_by_int={10: "GO:0000010", 99: "GO:0000099"},
        aspect_by_go={"GO:0000010": "F", "GO:0000099": "P"},
    )
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
        cooc={"GO:0000010": {"GO:0000099": 1}},
        freq={"GO:0000010": 2},
        go_id_by_int={10: "GO:0000010", 99: "GO:0000099", 77: "GO:0000077"},
        aspect_by_go={"GO:0000010": "F", "GO:0000099": "P", "GO:0000077": "C"},
    )
    assert out[0]["association_present"] == 1.0
    assert out[1]["association_total"] == 0.0
    assert out[1]["association_present"] == 0.0


def test_cross_snapshot_association_is_nonzero_and_matches_single_snapshot() -> None:
    """The fix: known + candidate in DIFFERENT int id-spaces still score.

    The t0 known term and the candidate share a go_id family (GO:0000010 /
    GO:0000099) but, because GOTerm ids are per snapshot, the known int id (510)
    and the candidate int id (99) come from different snapshots. The cooccurrence
    table (built on the t0 snapshot) is keyed on the go_id string, so the score
    is the SAME as the single-snapshot case (``test_association_total_*`` -> 0.5).
    Before the fix the int-keyed match would never fire and total stayed 0.
    """
    # Q1's known term resolves to int 510 (a t0-snapshot GOTerm id).
    annotations = {"Q1": [{"go_term_id": 510, "evidence_code": "IDA"}]}
    # The candidate carries int 99 (an export-snapshot GOTerm id) for the SAME
    # go_id the known term co-occurs with (GO:0000099).
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
        # Cooccurrence is keyed on the go_id STRING (built on the t0 set).
        cooc={"GO:0000010": {"GO:0000099": 1}},
        freq={"GO:0000010": 2},
        # Two DIFFERENT int ids (510 known, 99 candidate) map to go_id strings
        # -> the string keying bridges the snapshot gap.
        go_id_by_int={510: "GO:0000010", 99: "GO:0000099"},
        aspect_by_go={"GO:0000010": "F", "GO:0000099": "P"},
    )
    assert out[0]["association_total"] == 0.5  # identical to the single-snapshot score
    assert out[0]["association_cross"] == 0.5  # known F vs candidate P -> cross
    assert out[0]["association_present"] == 1.0


def test_candidate_go_id_on_record_is_used_when_present() -> None:
    """When the rec already carries ``go_id``, the scorer uses it directly.

    The predict path stamps ``go_id`` on records (ancestor expansion); the
    scorer prefers it over the int resolver, so a candidate whose int id is NOT
    in the resolver still scores from its stamped go_id string.
    """
    annotations = {"Q1": [{"go_term_id": 10, "evidence_code": "IDA"}]}
    preds = [
        {
            "protein_accession": "Q1",
            "go_term_id": 99,
            "go_id": "GO:0000099",
            "association_total": 0.0,
            "association_cross": 0.0,
            "association_present": 0.0,
        },
    ]
    out = _run(
        preds,
        annotations=annotations,
        cooc={"GO:0000010": {"GO:0000099": 1}},
        freq={"GO:0000010": 2},
        # Candidate int 99 deliberately absent from the resolver; rec go_id wins.
        go_id_by_int={10: "GO:0000010"},
        aspect_by_go={"GO:0000010": "F", "GO:0000099": "P"},
    )
    assert out[0]["association_total"] == 0.5
    assert out[0]["association_present"] == 1.0


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
