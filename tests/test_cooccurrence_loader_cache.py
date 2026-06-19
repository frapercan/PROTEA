"""Unit tests for the memoized cooccurrence/frequency loader.

Targets ``load_cooccurrence_for_known`` in
``protea.core.operations.predict_go_terms._association_loader``. The export
build calls the loader once per 512-query parity chunk; common GO terms recur
across nearly every chunk's known set, so the same 318M-row
``term_cooccurrence`` rows were re-read every chunk. The loader now memoizes per
``(annotation_set_id, known_go_id)`` so each known term is read from the DB
ONCE and reused across chunks.

These tests drive the loader with a fake session that records which keys each DB
query was asked for, asserting (a) the DB is only queried for the uncached
subset on each call (overlapping known terms served from cache), and (b) the
returned ``(cooc_by_known, freq)`` is byte-identical to a no-cache reference
implementation for any sequence of calls.
"""

from __future__ import annotations

import re
import uuid
from typing import Any

import pytest

from protea.core.operations.predict_go_terms import _association_loader as loader


class _FakeResult:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self._rows = rows

    def all(self) -> list[tuple[Any, ...]]:
        return self._rows


class _FakeSession:
    """Records the ``IN`` lists each cooc/freq query was issued with.

    The loader emits exactly two queries per cache-miss batch (cooccurrence then
    frequency). We pull the ``IN`` clause's literal values out of the compiled
    SQLAlchemy statement so the test can assert which keys hit the DB.
    """

    def __init__(self, cooc: dict[str, dict[str, int]], freq: dict[str, int | None]) -> None:
        self._cooc = cooc
        self._freq = freq
        self.cooc_query_keys: list[set[str]] = []
        self.freq_query_keys: list[set[str]] = []

    def execute(self, stmt: Any) -> _FakeResult:
        # Pull the IN-list literals from the compiled statement.
        wanted = _in_clause_values(stmt)
        text = str(stmt).lower()
        if "term_cooccurrence" in text:
            self.cooc_query_keys.append(set(wanted))
            rows: list[tuple[Any, ...]] = []
            for k in wanted:
                for t, c in self._cooc.get(k, {}).items():
                    rows.append((k, t, c))
            return _FakeResult(rows)
        # frequency query
        self.freq_query_keys.append(set(wanted))
        frows: list[tuple[Any, ...]] = []
        for k in wanted:
            f = self._freq.get(k)
            if f is not None:
                frows.append((k, f))
        return _FakeResult(frows)


def _in_clause_values(stmt: Any) -> list[str]:
    """Extract the go_id IN-list literal values from a compiled select."""
    sql = str(stmt.compile(compile_kwargs={"literal_binds": True}))
    return re.findall(r"GO:\d{7}", sql)


def _reference_load(
    cooc: dict[str, dict[str, int]],
    freq: dict[str, int | None],
    known_go_ids: set[str],
) -> tuple[dict[str, dict[str, int]], dict[str, int]]:
    """No-cache oracle mirroring the original DB-only implementation."""
    cooc_by_known: dict[str, dict[str, int]] = {}
    out_freq: dict[str, int] = {}
    if not known_go_ids:
        return cooc_by_known, out_freq
    for k in known_go_ids:
        row = cooc.get(k, {})
        if row:
            cooc_by_known[k] = {t: int(c) for t, c in row.items()}
        f = freq.get(k)
        if f is not None:
            out_freq[k] = int(f)
    return cooc_by_known, out_freq


@pytest.fixture(autouse=True)
def _clear_cache() -> Any:
    loader.clear_cooccurrence_cache()
    yield
    loader.clear_cooccurrence_cache()


def test_second_call_queries_only_uncached_keys() -> None:
    set_id = uuid.uuid4()
    cooc = {
        "GO:0000001": {"GO:0000099": 1},
        "GO:0000002": {"GO:0000099": 2},
        "GO:0000003": {"GO:0000099": 3},
    }
    freq = {"GO:0000001": 2, "GO:0000002": 4, "GO:0000003": 6}
    sess = _FakeSession(cooc, freq)

    # First call loads {A, B}.
    out1 = loader.load_cooccurrence_for_known(sess, set_id, {"GO:0000001", "GO:0000002"})
    assert sess.cooc_query_keys == [{"GO:0000001", "GO:0000002"}]
    assert sess.freq_query_keys == [{"GO:0000001", "GO:0000002"}]
    assert out1 == _reference_load(cooc, freq, {"GO:0000001", "GO:0000002"})

    # Second call loads {B, C}: only C hits the DB, B served from cache.
    out2 = loader.load_cooccurrence_for_known(sess, set_id, {"GO:0000002", "GO:0000003"})
    assert sess.cooc_query_keys[1] == {"GO:0000003"}
    assert sess.freq_query_keys[1] == {"GO:0000003"}
    assert out2 == _reference_load(cooc, freq, {"GO:0000002", "GO:0000003"})


def test_fully_cached_call_issues_no_query() -> None:
    set_id = uuid.uuid4()
    cooc = {"GO:0000001": {"GO:0000099": 1}}
    freq = {"GO:0000001": 2}
    sess = _FakeSession(cooc, freq)

    loader.load_cooccurrence_for_known(sess, set_id, {"GO:0000001"})
    n_cooc = len(sess.cooc_query_keys)
    n_freq = len(sess.freq_query_keys)

    # Repeat: no new DB queries.
    out = loader.load_cooccurrence_for_known(sess, set_id, {"GO:0000001"})
    assert len(sess.cooc_query_keys) == n_cooc
    assert len(sess.freq_query_keys) == n_freq
    assert out == _reference_load(cooc, freq, {"GO:0000001"})


def test_known_without_cooccurrence_cached_as_empty_not_requeried() -> None:
    set_id = uuid.uuid4()
    # GO:0000005 has neither a cooccurrence row nor a frequency row.
    cooc = {"GO:0000001": {"GO:0000099": 1}}
    freq = {"GO:0000001": 2}
    sess = _FakeSession(cooc, freq)

    out1 = loader.load_cooccurrence_for_known(sess, set_id, {"GO:0000005"})
    assert out1 == ({}, {})
    assert sess.cooc_query_keys == [{"GO:0000005"}]

    # Second call for the same empty key: NOT re-queried.
    out2 = loader.load_cooccurrence_for_known(sess, set_id, {"GO:0000005"})
    assert out2 == ({}, {})
    assert len(sess.cooc_query_keys) == 1
    assert len(sess.freq_query_keys) == 1


def test_empty_input_returns_empty_without_query() -> None:
    set_id = uuid.uuid4()
    sess = _FakeSession({}, {})
    out = loader.load_cooccurrence_for_known(sess, set_id, set())
    assert out == ({}, {})
    assert sess.cooc_query_keys == []
    assert sess.freq_query_keys == []


def test_cache_keyed_by_annotation_set() -> None:
    """The same go_id in a different annotation set is a distinct cache key."""
    set_a = uuid.uuid4()
    set_b = uuid.uuid4()
    cooc = {"GO:0000001": {"GO:0000099": 1}}
    freq = {"GO:0000001": 2}
    sess = _FakeSession(cooc, freq)

    loader.load_cooccurrence_for_known(sess, set_a, {"GO:0000001"})
    assert sess.cooc_query_keys == [{"GO:0000001"}]
    # Same go_id, different set -> must query again.
    loader.load_cooccurrence_for_known(sess, set_b, {"GO:0000001"})
    assert sess.cooc_query_keys == [{"GO:0000001"}, {"GO:0000001"}]


def test_freq_only_no_cooc_returns_freq() -> None:
    """A known term with a frequency row but no cooccurrence row keeps its freq."""
    set_id = uuid.uuid4()
    cooc: dict[str, dict[str, int]] = {}
    freq = {"GO:0000001": 5}
    sess = _FakeSession(cooc, freq)
    out = loader.load_cooccurrence_for_known(sess, set_id, {"GO:0000001"})
    assert out == ({}, {"GO:0000001": 5})


def test_sequence_of_overlapping_calls_matches_reference() -> None:
    """A chunked sequence of overlapping known sets matches the no-cache oracle.

    Mirrors the export build: many chunks, common terms recurring across them.
    Each call's return must equal the reference no-cache load exactly.
    """
    import random

    rng = random.Random(20260619)
    set_id = uuid.uuid4()
    go_ids = [f"GO:{i:07d}" for i in range(1, 31)]
    cands = [f"GO:{i:07d}" for i in range(100, 130)]
    cooc: dict[str, dict[str, int]] = {}
    freq: dict[str, int | None] = {}
    for k in go_ids:
        if rng.random() < 0.2:
            continue  # no rows at all for this known term
        freq[k] = rng.choice([1, 2, 3, 5])
        row = {t: rng.randint(1, 9) for t in cands if rng.random() < 0.3}
        if row:
            cooc[k] = row

    sess = _FakeSession(cooc, freq)
    seen_keys: set[tuple[uuid.UUID, str]] = set()

    for _chunk in range(20):
        known = set(rng.sample(go_ids, rng.randint(1, 8)))
        out = loader.load_cooccurrence_for_known(sess, set_id, known)
        assert out == _reference_load(cooc, freq, known)
        seen_keys |= {(set_id, k) for k in known}

    # Every distinct (set, known) was queried at most once: union of all cooc
    # query key-sets has no duplicates beyond the distinct known terms.
    queried: list[str] = [k for ks in sess.cooc_query_keys for k in ks]
    assert len(queried) == len(set(queried))
    assert set(queried) == {k for _s, k in seen_keys}
