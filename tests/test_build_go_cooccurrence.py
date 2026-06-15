"""Unit tests for ``BuildGoCooccurrenceOperation`` aggregation (INT-3).

Covers the pure aggregation core (ancestor propagation, per-term frequency,
co-occurrence with the known-term frequency cap) without standing up the DB.
The offline ``protea-reranker-lab/fullgo/assoc_feature.py`` is the semantic
oracle: ``freq[t]`` = distinct proteins carrying ``t`` after propagation, and
``cooccurrence[k, t]`` = distinct proteins carrying both ``k`` and ``t``.
"""

from __future__ import annotations

from protea.core.operations.build_go_cooccurrence import (
    BuildGoCooccurrenceOperation,
    BuildGoCooccurrencePayload,
)


def _op() -> BuildGoCooccurrenceOperation:
    return BuildGoCooccurrenceOperation()


def test_propagate_expands_to_ancestor_closure() -> None:
    # 3 is_a parent of 2, 2 is_a parent of 1.
    parent_map = {1: {2}, 2: {3}}
    terms_by_protein = {"P1": {1}}
    _op()._propagate(terms_by_protein, parent_map)
    assert terms_by_protein["P1"] == {1, 2, 3}


def test_propagate_handles_multiple_parents_and_cycles() -> None:
    # Diamond + a self-referential edge (defensive: cycles must not hang).
    parent_map = {1: {2, 3}, 2: {4}, 3: {4}, 4: {4}}
    terms_by_protein = {"P1": {1}}
    _op()._propagate(terms_by_protein, parent_map)
    assert terms_by_protein["P1"] == {1, 2, 3, 4}


def test_compute_freq_counts_distinct_proteins() -> None:
    terms_by_protein = {"P1": {1, 2}, "P2": {2, 3}, "P3": {2}}
    freq = _op()._compute_freq(terms_by_protein)
    assert freq == {1: 1, 2: 3, 3: 1}


def test_compute_cooccurrence_pairs_distinct_proteins() -> None:
    # P1: {1,2}, P2: {1,2,3}. With all terms known:
    #   (1,2) and (2,1) each appear in both proteins -> 2.
    #   (1,3),(3,1),(2,3),(3,2) only in P2 -> 1.
    terms_by_protein = {"P1": {1, 2}, "P2": {1, 2, 3}}
    known = {1, 2, 3}
    cooc = _op()._compute_cooccurrence(terms_by_protein, known)
    assert cooc[(1, 2)] == 2
    assert cooc[(2, 1)] == 2
    assert cooc[(1, 1)] == 2  # diagonal == freq of the term among proteins carrying it
    assert cooc[(1, 3)] == 1
    assert cooc[(3, 2)] == 1


def test_compute_cooccurrence_respects_known_freq_cap() -> None:
    # Term 2 is common (freq 3 > cap 1): it must NOT appear as a known (k) side,
    # but it may still be a candidate (t) side under a specific known term.
    terms_by_protein = {"P1": {1, 2}, "P2": {2, 3}, "P3": {2}}
    freq = _op()._compute_freq(terms_by_protein)
    known = {t for t, f in freq.items() if f <= 1}
    assert known == {1, 3}
    cooc = _op()._compute_cooccurrence(terms_by_protein, known)
    # No pair has known side == 2.
    assert all(k != 2 for (k, _t) in cooc)
    # But 2 appears as a candidate under known term 1 (P1 carries both).
    assert cooc[(1, 2)] == 1


def test_conditional_probability_matches_oracle() -> None:
    # P(t|k) = cooccurrence(k,t)/freq(k). For k=1 (freq 1), t=2: 1/1 = 1.0.
    terms_by_protein = {"P1": {1, 2}, "P2": {2, 3}, "P3": {2}}
    op = _op()
    freq = op._compute_freq(terms_by_protein)
    cooc = op._compute_cooccurrence(terms_by_protein, {1, 3})
    p_2_given_1 = cooc[(1, 2)] / freq[1]
    assert p_2_given_1 == 1.0


def test_payload_defaults() -> None:
    p = BuildGoCooccurrencePayload.model_validate(
        {"annotation_set_id": "11111111-1111-1111-1111-111111111111"}
    )
    assert p.known_freq_cap == 1000
    assert p.write_batch_size == 50_000


def test_operation_metadata() -> None:
    op = _op()
    assert op.name == "build_go_cooccurrence"
    summary = op.summarize_payload({"annotation_set_id": "abc", "known_freq_cap": 500})
    assert "abc" in summary and "500" in summary
