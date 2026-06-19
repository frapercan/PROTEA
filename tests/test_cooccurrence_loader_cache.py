"""Unit tests for the memoized cooccurrence/frequency loader.

Targets ``load_cooccurrence_for_known`` in
``protea.core.operations.predict_go_terms._association_loader``. The export
build calls the loader once per 512-query parity chunk; common GO terms recur
across nearly every chunk's known set, so the same 318M-row
``term_cooccurrence`` rows were re-read every chunk. The loader now memoizes per
``(annotation_set_id, known_go_id)`` so each known term is read from the DB
ONCE and reused across chunks.

These tests drive the loader with a fake psycopg3 connection that records which
keys each ``COPY (SELECT ...) TO STDOUT`` was asked for, asserting (a) the DB is
only queried for the uncached subset on each call (overlapping known terms
served from cache), and (b) the returned ``(cooc_by_known, freq)`` is
byte-identical to a no-cache reference implementation for any sequence of calls.

The loader bulk-reads both tables with COPY on the raw psycopg3 connection (not
SQLAlchemy ``.all()``) to skip the Row-object materialization that profiling
flagged as ~48% of export samples. The known-go_id filter is bound as an
``= ANY(%s)`` array parameter (psycopg3 ``cursor.copy(sql, params)``) rather than
an inlined IN-list literal, so the fake reproduces psycopg3's parameterized COPY
contract: ``cursor.copy(sql, params)`` returns a context manager whose
``.rows()`` yields TEXT rows as tuples of strings (NULLs as ``None``). The go_id
filter values are read out of ``params`` (the bound array), not parsed from SQL.
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest

from protea.core.operations.predict_go_terms import _association_loader as loader


class _FakeCopy:
    """Stand-in for a psycopg3 COPY-TO-STDOUT result.

    ``rows()`` yields TEXT-format rows: each value is a string (the loader casts
    counts via ``int(...)``); a SQL NULL would arrive as ``None`` (psycopg3 maps
    the TEXT ``\\N`` sentinel to ``None`` in ``rows()``).
    """

    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self._rows = rows

    def __enter__(self) -> _FakeCopy:
        return self

    def __exit__(self, *exc: Any) -> None:
        return None

    def rows(self) -> list[tuple[Any, ...]]:
        return self._rows


class _FakeCursor:
    def __init__(self, conn: _FakeRawConnection) -> None:
        self._conn = conn

    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, *exc: Any) -> None:
        return None

    def copy(self, sql: str, params: tuple[Any, ...] | None = None) -> _FakeCopy:
        return self._conn._copy(sql, params)


class _FakeRawConnection:
    """Raw psycopg3 DBAPI connection: serves COPY SELECTs from in-memory dicts.

    The go_id filter list is the SECOND bound parameter (``= ANY(%s)``); the
    fake reads it straight out of ``params`` so the test can assert exactly which
    keys hit the DB on each cache-miss batch. ``params`` is
    ``(annotation_set_id, [go_id, ...])``.
    """

    def __init__(self, cooc: dict[str, dict[str, int]], freq: dict[str, int | None]) -> None:
        self._cooc = cooc
        self._freq = freq
        self.cooc_query_keys: list[set[str]] = []
        self.freq_query_keys: list[set[str]] = []

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)

    def _copy(self, sql: str, params: tuple[Any, ...] | None) -> _FakeCopy:
        assert params is not None, "loader must bind COPY params"
        wanted = list(params[1])
        if "term_cooccurrence" in sql:
            self.cooc_query_keys.append(set(wanted))
            rows: list[tuple[Any, ...]] = []
            for k in wanted:
                for t, c in self._cooc.get(k, {}).items():
                    # TEXT format: every column is a string.
                    rows.append((k, t, str(c)))
            return _FakeCopy(rows)
        # term_frequency query
        self.freq_query_keys.append(set(wanted))
        frows: list[tuple[Any, ...]] = []
        for k in wanted:
            f = self._freq.get(k)
            if f is not None:
                frows.append((k, str(f)))
        return _FakeCopy(frows)


class _FakeConnectionProxy:
    """What ``session.connection()`` returns: a ``.connection`` raw handle."""

    def __init__(self, raw: _FakeRawConnection) -> None:
        self.connection = raw


class _FakeSession:
    """Minimal session exposing ``connection().connection`` (the raw psycopg3)."""

    def __init__(self, cooc: dict[str, dict[str, int]], freq: dict[str, int | None]) -> None:
        self._raw = _FakeRawConnection(cooc, freq)

    def connection(self) -> _FakeConnectionProxy:
        return _FakeConnectionProxy(self._raw)

    @property
    def cooc_query_keys(self) -> list[set[str]]:
        return self._raw.cooc_query_keys

    @property
    def freq_query_keys(self) -> list[set[str]]:
        return self._raw.freq_query_keys


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


def test_copy_text_strings_parsed_to_exact_ints() -> None:
    """COPY TEXT rows arrive as strings; counts/freqs parse to exact ints."""
    set_id = uuid.uuid4()
    cooc = {"GO:0000001": {"GO:0000099": 123456789, "GO:0000100": 1}}
    freq = {"GO:0000001": 987654321}
    sess = _FakeSession(cooc, freq)
    cooc_out, freq_out = loader.load_cooccurrence_for_known(sess, set_id, {"GO:0000001"})
    assert cooc_out == {"GO:0000001": {"GO:0000099": 123456789, "GO:0000100": 1}}
    assert freq_out == {"GO:0000001": 987654321}
    # Values are genuine ints, not the source strings.
    assert all(isinstance(v, int) for v in cooc_out["GO:0000001"].values())
    assert isinstance(freq_out["GO:0000001"], int)


def test_copy_path_matches_reference_oracle() -> None:
    """The COPY-based load equals the no-cache .all()-era oracle byte-for-byte."""
    set_id = uuid.uuid4()
    cooc = {
        "GO:0000001": {"GO:0000099": 4, "GO:0000100": 7},
        "GO:0000002": {"GO:0000099": 2},
    }
    freq = {"GO:0000001": 9, "GO:0000002": 3, "GO:0000004": 5}
    known = {"GO:0000001", "GO:0000002", "GO:0000004"}
    sess = _FakeSession(cooc, freq)
    out = loader.load_cooccurrence_for_known(sess, set_id, known)
    assert out == _reference_load(cooc, freq, known)


def test_null_legacy_rows_excluded_by_sql() -> None:
    """Legacy NULL go_id rows are filtered in the COPY SELECT, not returned.

    The COPY SELECT carries ``known_go_id IS NOT NULL AND candidate_go_id IS NOT
    NULL`` (and ``go_id IS NOT NULL`` for frequency), so NULL legacy rows never
    reach the parser. We assert the SQL the loader issues contains those guards.
    """
    captured: list[str] = []
    set_id = uuid.uuid4()

    class _CapturingRaw(_FakeRawConnection):
        def _copy(self, sql: str, params: tuple[Any, ...] | None) -> _FakeCopy:
            captured.append(sql)
            return super()._copy(sql, params)

    class _CapturingSession(_FakeSession):
        def __init__(self) -> None:
            super().__init__({"GO:0000001": {"GO:0000099": 1}}, {"GO:0000001": 2})
            self._raw = _CapturingRaw({"GO:0000001": {"GO:0000099": 1}}, {"GO:0000001": 2})

    sess = _CapturingSession()
    loader.load_cooccurrence_for_known(sess, set_id, {"GO:0000001"})
    cooc_sql = next(s for s in captured if "term_cooccurrence" in s)
    freq_sql = next(s for s in captured if "term_frequency" in s)
    assert "known_go_id IS NOT NULL" in cooc_sql
    assert "candidate_go_id IS NOT NULL" in cooc_sql
    assert "go_id IS NOT NULL" in freq_sql


def test_known_value_bound_as_param_never_inlined_into_sql() -> None:
    """The known go_id list is a bound array param, never SQL text (no injection).

    With ``= ANY(%s)`` the go_id values are passed through the extended-query
    protocol, so even an injection-shaped token is just data: it is carried in
    ``params``, the COPY SQL string never contains it, and nothing raises.
    """
    captured_sql: list[str] = []
    captured_params: list[tuple[Any, ...] | None] = []
    set_id = uuid.uuid4()
    evil = "'; DROP TABLE x; --"

    class _CapturingRaw(_FakeRawConnection):
        def _copy(self, sql: str, params: tuple[Any, ...] | None) -> _FakeCopy:
            captured_sql.append(sql)
            captured_params.append(params)
            return super()._copy(sql, params)

    class _CapturingSession(_FakeSession):
        def __init__(self) -> None:
            super().__init__({}, {})
            self._raw = _CapturingRaw({}, {})

    sess = _CapturingSession()
    # No raise: the value is data, not SQL.
    out = loader.load_cooccurrence_for_known(sess, set_id, {evil})
    assert out == ({}, {})
    # The injection-shaped token appears ONLY in the bound params, never in SQL.
    for sql in captured_sql:
        assert "DROP TABLE" not in sql
        assert "%s" in sql
    assert any(evil in list(p[1]) for p in captured_params if p is not None)
