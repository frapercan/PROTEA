"""Unit tests for ``BuildGoCooccurrenceOperation`` aggregation (INT-3).

Covers the pure aggregation core (ancestor propagation, per-term frequency,
co-occurrence with the known-term frequency cap) without standing up the DB.
The offline ``protea-reranker-lab/fullgo/assoc_feature.py`` is the semantic
oracle: ``freq[t]`` = distinct proteins carrying ``t`` after propagation, and
``cooccurrence[k, t]`` = distinct proteins carrying both ``k`` and ``t``.
"""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock

from protea.core.operations.build_go_cooccurrence import (
    BuildGoCooccurrenceOperation,
    BuildGoCooccurrencePayload,
)
from protea.infrastructure.orm.models.annotation.term_cooccurrence import (
    TermCooccurrence,
    TermFrequency,
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


def _reference_cooccurrence(
    terms_by_protein: dict[str, set[int]],
    known_terms: set[int],
) -> dict[tuple[int, int], int]:
    """Pure-Python triple-loop oracle (the original implementation)."""
    cooc: dict[tuple[int, int], int] = {}
    for terms in terms_by_protein.values():
        known_here = [k for k in terms if k in known_terms]
        if not known_here:
            continue
        for k in known_here:
            for t in terms:
                cooc[(k, t)] = cooc.get((k, t), 0) + 1
    return cooc


def test_compute_cooccurrence_byte_identical_to_reference() -> None:
    # The scipy sparse-matmul implementation must produce EXACTLY the same
    # dict as the original Python triple loop, including the k==t diagonal,
    # terms that are both known and candidate, and proteins with no known
    # term (they contribute nothing). Counts must be plain Python ints.
    terms_by_protein = {
        "P1": {1, 2, 5},
        "P2": {1, 2, 3},
        "P3": {2, 4},  # carries no known term once cap excludes 2 -> contributes nothing
        "P4": {1, 3, 4, 5},
        "P5": {6},  # isolated term, never known here
    }
    known = {1, 3, 4, 5}  # 2 (common) and 6 deliberately excluded from anchors
    expected = _reference_cooccurrence(terms_by_protein, known)
    got = _op()._compute_cooccurrence(terms_by_protein, known)
    assert got == expected
    # Diagonal is present and equals the term's own frequency among carriers.
    assert got[(1, 1)] == 3  # P1, P2, P4 carry term 1
    assert all(isinstance(v, int) and not isinstance(v, bool) for v in got.values())


def test_compute_cooccurrence_empty_input() -> None:
    assert _op()._compute_cooccurrence({}, set()) == {}
    assert _reference_cooccurrence({}, set()) == {}


def test_compute_cooccurrence_no_known_terms() -> None:
    # Proteins exist but none of their terms are known anchors -> empty dict.
    terms_by_protein = {"P1": {7, 8}, "P2": {8, 9}}
    known: set[int] = {100, 200}
    got = _op()._compute_cooccurrence(terms_by_protein, known)
    assert got == _reference_cooccurrence(terms_by_protein, known) == {}


def test_compute_cooccurrence_known_and_candidate_overlap() -> None:
    # A term that is BOTH an anchor (known) and a candidate of another anchor.
    terms_by_protein = {"P1": {1, 2}, "P2": {1, 2}, "P3": {2}}
    known = {1, 2}
    got = _op()._compute_cooccurrence(terms_by_protein, known)
    assert got == _reference_cooccurrence(terms_by_protein, known)
    assert got[(1, 2)] == 2 and got[(2, 1)] == 2 and got[(2, 2)] == 3


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


def _captured_rows(session: MagicMock, model: type) -> list[dict]:
    """Flatten the row dicts passed to ``bulk_insert_mappings`` for ``model``."""
    rows: list[dict] = []
    for call in session.bulk_insert_mappings.call_args_list:
        if call.args and call.args[0] is model:
            rows.extend(call.args[1])
    return rows


def test_write_freq_stamps_snapshot_invariant_go_id() -> None:
    # The freq rows must carry the go_id string so the predict path can match
    # on it across snapshots; the int term_id stays for FK / backward-read.
    session = MagicMock()
    set_id = uuid.uuid4()
    go_id_by_int = {10: "GO:0000010", 20: "GO:0000020"}
    _op()._write_freq(session, set_id, {10: 3, 20: 1}, go_id_by_int, batch_size=50_000)
    rows = {r["term_id"]: r for r in _captured_rows(session, TermFrequency)}
    assert rows[10]["go_id"] == "GO:0000010"
    assert rows[10]["freq"] == 3
    assert rows[20]["go_id"] == "GO:0000020"


def test_write_cooccurrence_stamps_both_go_id_sides() -> None:
    # Both the known (k) and candidate (t) sides carry their go_id strings so
    # the cooccurrence row is snapshot-invariant on each end.
    session = MagicMock()
    set_id = uuid.uuid4()
    go_id_by_int = {10: "GO:0000010", 99: "GO:0000099"}
    cooc = {(10, 99): 2}
    _op()._write_cooccurrence(session, set_id, cooc, go_id_by_int, batch_size=50_000)
    rows = _captured_rows(session, TermCooccurrence)
    assert len(rows) == 1
    row = rows[0]
    assert row["known_term_id"] == 10
    assert row["candidate_term_id"] == 99
    assert row["known_go_id"] == "GO:0000010"
    assert row["candidate_go_id"] == "GO:0000099"
    assert row["cooccurrence_count"] == 2
