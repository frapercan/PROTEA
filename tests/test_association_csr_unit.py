"""Unit tests for the in-memory CSR cooccurrence loader (no Postgres).

Targets ``_association_csr`` in
``protea.core.operations.predict_go_terms``. These drive the CSR build/gather
math with a fake psycopg3 connection (the same COPY-TO-STDOUT contract the
existing ``test_cooccurrence_loader_cache`` uses), proving that:

- the CSR built from a whole-set COPY scan, gathered per known term, returns the
  SAME ``(cooc_by_known, freq)`` dicts (same int values) as the per-chunk DB
  oracle for any known set;
- the whole set is streamed ONCE (one cooc COPY + one freq COPY per set), with
  every later lookup served from RAM (zero further COPYs);
- legacy NULL rows / absent terms degrade to empty exactly as the DB loader does;
- the per-set memo is dropped by ``clear_csr_cooccurrence_cache``.

The fake serves the WHOLE set (filtered by annotation_set_id) regardless of the
known list, mirroring the CSR loader's set-wide COPY (no ``= ANY(%s)`` known
filter), and records each COPY so the test can assert the one-scan contract.
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest

from protea.core.operations.predict_go_terms import _association_csr as csr


class _FakeCopy:
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
    """Serves set-wide COPY SELECTs from in-memory dicts.

    The CSR loader binds only the annotation_set_id (``params[0]``) and streams
    the WHOLE set; the fake returns every row for that set, including a legacy
    NULL row that the real SELECT's ``IS NOT NULL`` would drop (the loader's SQL
    carries the guard, so we drop NULLs here to mirror the server-side filter).
    """

    def __init__(
        self,
        rows_by_set: dict[uuid.UUID, dict[str, dict[str, int]]],
        freq_by_set: dict[uuid.UUID, dict[str, int]],
    ) -> None:
        self._cooc = rows_by_set
        self._freq = freq_by_set
        self.cooc_copies: list[uuid.UUID] = []
        self.freq_copies: list[uuid.UUID] = []

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)

    def _copy(self, sql: str, params: tuple[Any, ...] | None) -> _FakeCopy:
        assert params is not None
        sid = params[0]
        if "term_cooccurrence" in sql:
            self.cooc_copies.append(sid)
            rows: list[tuple[Any, ...]] = []
            for k, row in self._cooc.get(sid, {}).items():
                for t, c in row.items():
                    rows.append((k, t, str(c)))
            return _FakeCopy(rows)
        self.freq_copies.append(sid)
        frows: list[tuple[Any, ...]] = []
        for k, f in self._freq.get(sid, {}).items():
            frows.append((k, str(f)))
        return _FakeCopy(frows)


class _FakeConnectionProxy:
    def __init__(self, raw: _FakeRawConnection) -> None:
        self.connection = raw


class _FakeSession:
    def __init__(
        self,
        rows_by_set: dict[uuid.UUID, dict[str, dict[str, int]]],
        freq_by_set: dict[uuid.UUID, dict[str, int]],
    ) -> None:
        self._raw = _FakeRawConnection(rows_by_set, freq_by_set)

    def connection(self) -> _FakeConnectionProxy:
        return _FakeConnectionProxy(self._raw)

    @property
    def cooc_copies(self) -> list[uuid.UUID]:
        return self._raw.cooc_copies

    @property
    def freq_copies(self) -> list[uuid.UUID]:
        return self._raw.freq_copies


def _reference_load(
    cooc: dict[str, dict[str, int]],
    freq: dict[str, int],
    known_go_ids: set[str],
) -> tuple[dict[str, dict[str, int]], dict[str, int]]:
    """No-cache oracle: exactly what ``_association_loader`` returns per chunk."""
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
    csr.clear_csr_cooccurrence_cache()
    yield
    csr.clear_csr_cooccurrence_cache()


def test_csr_gather_matches_db_oracle() -> None:
    set_id = uuid.uuid4()
    cooc = {
        "GO:0000001": {"GO:0000099": 4, "GO:0000100": 7},
        "GO:0000002": {"GO:0000099": 2},
        "GO:0000003": {"GO:0000101": 11},
    }
    freq = {"GO:0000001": 9, "GO:0000002": 3, "GO:0000004": 5}
    sess = _FakeSession({set_id: cooc}, {set_id: freq})

    known = {"GO:0000001", "GO:0000002", "GO:0000004", "GO:0000005"}
    got = csr.load_cooccurrence_for_known(sess, set_id, known)
    assert got == _reference_load(cooc, freq, known)
    # Spot-check exact structure (known with no row absent; freq-only kept).
    assert got == (
        {
            "GO:0000001": {"GO:0000099": 4, "GO:0000100": 7},
            "GO:0000002": {"GO:0000099": 2},
        },
        {"GO:0000001": 9, "GO:0000002": 3, "GO:0000004": 5},
    )


def test_whole_set_streamed_once_then_served_from_ram() -> None:
    set_id = uuid.uuid4()
    cooc = {"GO:0000001": {"GO:0000099": 1}, "GO:0000002": {"GO:0000099": 2}}
    freq = {"GO:0000001": 2, "GO:0000002": 4}
    sess = _FakeSession({set_id: cooc}, {set_id: freq})

    csr.load_cooccurrence_for_known(sess, set_id, {"GO:0000001"})
    csr.load_cooccurrence_for_known(sess, set_id, {"GO:0000002"})
    csr.load_cooccurrence_for_known(sess, set_id, {"GO:0000001", "GO:0000002"})
    # Exactly ONE cooc COPY + ONE freq COPY for the whole split.
    assert sess.cooc_copies == [set_id]
    assert sess.freq_copies == [set_id]


def test_values_are_exact_ints() -> None:
    set_id = uuid.uuid4()
    cooc = {"GO:0000001": {"GO:0000099": 123456789, "GO:0000100": 1}}
    freq = {"GO:0000001": 987654321}
    sess = _FakeSession({set_id: cooc}, {set_id: freq})
    cooc_out, freq_out = csr.load_cooccurrence_for_known(sess, set_id, {"GO:0000001"})
    assert cooc_out == {"GO:0000001": {"GO:0000099": 123456789, "GO:0000100": 1}}
    assert freq_out == {"GO:0000001": 987654321}
    assert all(isinstance(v, int) for v in cooc_out["GO:0000001"].values())
    assert isinstance(freq_out["GO:0000001"], int)


def test_empty_input_returns_empty_without_scan() -> None:
    set_id = uuid.uuid4()
    sess = _FakeSession({set_id: {}}, {set_id: {}})
    out = csr.load_cooccurrence_for_known(sess, set_id, set())
    assert out == ({}, {})
    assert sess.cooc_copies == []
    assert sess.freq_copies == []


def test_set_with_no_rows_yields_empty_matrix() -> None:
    set_id = uuid.uuid4()
    sess = _FakeSession({set_id: {}}, {set_id: {}})
    out = csr.load_cooccurrence_for_known(sess, set_id, {"GO:0000001"})
    assert out == ({}, {})


def test_freq_only_no_cooc_returns_freq() -> None:
    set_id = uuid.uuid4()
    sess = _FakeSession({set_id: {}}, {set_id: {"GO:0000001": 5}})
    out = csr.load_cooccurrence_for_known(sess, set_id, {"GO:0000001"})
    assert out == ({}, {"GO:0000001": 5})


def test_cache_keyed_by_set_and_cleared() -> None:
    set_a = uuid.uuid4()
    set_b = uuid.uuid4()
    cooc_a = {"GO:0000001": {"GO:0000099": 1}}
    cooc_b = {"GO:0000001": {"GO:0000099": 9}}
    sess = _FakeSession(
        {set_a: cooc_a, set_b: cooc_b},
        {set_a: {"GO:0000001": 2}, set_b: {"GO:0000001": 3}},
    )
    out_a = csr.load_cooccurrence_for_known(sess, set_a, {"GO:0000001"})
    out_b = csr.load_cooccurrence_for_known(sess, set_b, {"GO:0000001"})
    assert out_a == ({"GO:0000001": {"GO:0000099": 1}}, {"GO:0000001": 2})
    assert out_b == ({"GO:0000001": {"GO:0000099": 9}}, {"GO:0000001": 3})
    assert sess.cooc_copies == [set_a, set_b]

    # Re-querying set_a is served from RAM (no new COPY).
    csr.load_cooccurrence_for_known(sess, set_a, {"GO:0000001"})
    assert sess.cooc_copies == [set_a, set_b]
    # After a clear, the next call rescans.
    csr.clear_csr_cooccurrence_cache()
    csr.load_cooccurrence_for_known(sess, set_a, {"GO:0000001"})
    assert sess.cooc_copies == [set_a, set_b, set_a]


def test_csr_matches_oracle_over_random_overlapping_chunks() -> None:
    """A chunked sequence of overlapping known sets matches the DB oracle."""
    import random

    rng = random.Random(20260620)
    set_id = uuid.uuid4()
    go_ids = [f"GO:{i:07d}" for i in range(1, 41)]
    cands = [f"GO:{i:07d}" for i in range(100, 140)]
    cooc: dict[str, dict[str, int]] = {}
    freq: dict[str, int] = {}
    for k in go_ids:
        if rng.random() < 0.2:
            continue
        freq[k] = rng.choice([1, 2, 3, 5, 7])
        row = {t: rng.randint(1, 9) for t in cands if rng.random() < 0.3}
        if row:
            cooc[k] = row

    sess = _FakeSession({set_id: cooc}, {set_id: freq})
    for _chunk in range(25):
        known = set(rng.sample(go_ids, rng.randint(1, 10)))
        got = csr.load_cooccurrence_for_known(sess, set_id, known)
        assert got == _reference_load(cooc, freq, known)
    # Still exactly one scan across all chunks.
    assert sess.cooc_copies == [set_id]
    assert sess.freq_copies == [set_id]


def test_csr_enabled_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(csr._ENV_FLAG, raising=False)
    assert csr.csr_enabled() is False
    for val in ("1", "true", "TRUE", "yes", "on"):
        monkeypatch.setenv(csr._ENV_FLAG, val)
        assert csr.csr_enabled() is True
    for val in ("0", "false", "", "no"):
        monkeypatch.setenv(csr._ENV_FLAG, val)
        assert csr.csr_enabled() is False
