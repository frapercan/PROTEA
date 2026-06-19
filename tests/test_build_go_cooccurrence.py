"""Unit tests for ``BuildGoCooccurrenceOperation`` aggregation (INT-3).

Covers the pure aggregation core (ancestor propagation, per-term frequency,
co-occurrence with the known-term frequency cap) without standing up the DB.
The offline ``protea-reranker-lab/fullgo/assoc_feature.py`` is the semantic
oracle: ``freq[t]`` = distinct proteins carrying ``t`` after propagation, and
``cooccurrence[k, t]`` = distinct proteins carrying both ``k`` and ``t``.
"""

from __future__ import annotations

import uuid
from typing import Any
from unittest.mock import MagicMock

from protea.core.operations.build_go_cooccurrence import (
    _COOCCURRENCE_COLUMNS,
    _FREQ_COLUMNS,
    BuildGoCooccurrenceOperation,
    BuildGoCooccurrencePayload,
    _cooccurrence_copy_row,
    _copy_rows,
    _freq_copy_row,
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


# --- COPY row serialization (the byte-identical replacement for the old
#     bulk_insert_mappings persist). These pure helpers are unit-tested here;
#     _copy_rows is exercised with a fake psycopg3 cursor/copy below. ---


def test_freq_copy_row_column_order_and_values() -> None:
    # Row must be in _FREQ_COLUMNS order: (annotation_set_id, term_id, go_id,
    # freq). set_id is a str (UUID stringified), go_id resolved via .get().
    set_id_str = "11111111-1111-1111-1111-111111111111"
    go_id_by_int = {10: "GO:0000010"}
    row = _freq_copy_row(set_id_str, 10, 3, go_id_by_int)
    assert _FREQ_COLUMNS == ("annotation_set_id", "term_id", "go_id", "freq")
    assert row == (set_id_str, 10, "GO:0000010", 3)


def test_freq_copy_row_missing_go_id_is_none() -> None:
    # A term with no go_id mapping must emit None (-> SQL NULL), never a stub.
    row = _freq_copy_row("set-x", 20, 1, {})
    assert row == ("set-x", 20, None, 1)
    assert row[2] is None


def test_cooccurrence_copy_row_column_order_and_values() -> None:
    # Row in _COOCCURRENCE_COLUMNS order with both go_id sides resolved.
    set_id_str = "22222222-2222-2222-2222-222222222222"
    go_id_by_int = {10: "GO:0000010", 99: "GO:0000099"}
    row = _cooccurrence_copy_row(set_id_str, 10, 99, 2, go_id_by_int)
    assert _COOCCURRENCE_COLUMNS == (
        "annotation_set_id",
        "known_term_id",
        "candidate_term_id",
        "known_go_id",
        "candidate_go_id",
        "cooccurrence_count",
    )
    assert row == (set_id_str, 10, 99, "GO:0000010", "GO:0000099", 2)


def test_cooccurrence_copy_row_missing_go_ids_are_none() -> None:
    # Both sides independently fall back to None when unmapped.
    go_id_by_int = {10: "GO:0000010"}  # 99 missing
    row = _cooccurrence_copy_row("set-y", 10, 99, 5, go_id_by_int)
    assert row == ("set-y", 10, 99, "GO:0000010", None, 5)
    assert row[3] == "GO:0000010"  # known side present
    assert row[4] is None  # candidate side missing


def test_cooccurrence_copy_row_diagonal_self_pair() -> None:
    # The k == t diagonal must repeat the same go_id on both sides.
    go_id_by_int = {7: "GO:0000007"}
    row = _cooccurrence_copy_row("set-z", 7, 7, 4, go_id_by_int)
    assert row == ("set-z", 7, 7, "GO:0000007", "GO:0000007", 4)


class _FakeCopy:
    """Captures rows passed to ``cp.write_row`` for one COPY statement."""

    def __init__(self, sink: list[tuple]) -> None:
        self._sink = sink

    def __enter__(self) -> _FakeCopy:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def write_row(self, row: tuple) -> None:
        self._sink.append(row)


class _FakeCursor:
    def __init__(self, sink: list[tuple], stmts: list[str]) -> None:
        self._sink = sink
        self._stmts = stmts

    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def copy(self, stmt: str) -> _FakeCopy:
        self._stmts.append(stmt)
        return _FakeCopy(self._sink)


def _fake_session(sink: list[tuple], stmts: list[str]) -> Any:
    """A session whose raw connection yields cursors recording COPY rows."""
    session = MagicMock()
    raw = MagicMock()
    raw.cursor.side_effect = lambda: _FakeCursor(sink, stmts)
    session.connection.return_value.connection = raw
    return session


def test_write_freq_copies_rows_and_commits_per_chunk() -> None:
    # The freq rows reach COPY in column order, carry the go_id string, and
    # the per-chunk commit semantics are preserved (one commit per chunk).
    sink: list[tuple] = []
    stmts: list[str] = []
    session = _fake_session(sink, stmts)
    set_id = uuid.uuid4()
    go_id_by_int = {10: "GO:0000010", 20: "GO:0000020", 30: "GO:0000030"}
    written = _op()._write_freq(session, set_id, {10: 3, 20: 1, 30: 2}, go_id_by_int, batch_size=2)
    assert written == 3
    by_term = {r[1]: r for r in sink}
    assert by_term[10] == (str(set_id), 10, "GO:0000010", 3)
    assert by_term[20] == (str(set_id), 20, "GO:0000020", 1)
    # COPY targets the term_frequency table with the expected column list.
    assert all(s.startswith("COPY term_frequency (") for s in stmts)
    assert "annotation_set_id, term_id, go_id, freq" in stmts[0]
    # 3 rows, chunk 2 -> 2 chunks -> 2 commits.
    assert session.commit.call_count == 2


def test_write_cooccurrence_copies_rows_and_commits() -> None:
    # Both go_id sides stamped; diagonal and missing-id rows handled; commit
    # fires once for the single chunk.
    sink: list[tuple] = []
    stmts: list[str] = []
    session = _fake_session(sink, stmts)
    set_id = uuid.uuid4()
    go_id_by_int = {10: "GO:0000010", 99: "GO:0000099"}
    cooc = {(10, 99): 2, (10, 10): 4, (10, 7): 1}  # 7 unmapped -> candidate NULL
    written = _op()._write_cooccurrence(session, set_id, cooc, go_id_by_int, batch_size=50_000)
    assert written == 3
    by_pair = {(r[1], r[2]): r for r in sink}
    assert by_pair[(10, 99)] == (str(set_id), 10, 99, "GO:0000010", "GO:0000099", 2)
    assert by_pair[(10, 10)] == (str(set_id), 10, 10, "GO:0000010", "GO:0000010", 4)
    assert by_pair[(10, 7)] == (str(set_id), 10, 7, "GO:0000010", None, 1)
    assert all(s.startswith("COPY term_cooccurrence (") for s in stmts)
    assert session.commit.call_count == 1


def test_copy_rows_streams_lazily_without_materialising_all() -> None:
    # _copy_rows must consume the generator chunk by chunk, not build one big
    # list (memory bound). We assert it pulls exactly the rows it writes.
    sink: list[tuple] = []
    stmts: list[str] = []
    session = _fake_session(sink, stmts)
    pulled = 0

    def gen() -> Any:
        nonlocal pulled
        for i in range(5):
            pulled += 1
            yield ("s", i)

    written = _copy_rows(session, "t", ("a", "b"), gen(), chunk_size=2)
    assert written == 5
    assert pulled == 5
    assert sink == [("s", 0), ("s", 1), ("s", 2), ("s", 3), ("s", 4)]
    # 5 rows, chunk 2 -> 3 chunks -> 3 commits.
    assert session.commit.call_count == 3
