"""In-memory CSR co-occurrence loader for the cross-aspect association feature.

Motivation (measured, I/O-bound diagnosis)
-------------------------------------------
The association feature loads rows from ``term_cooccurrence`` (~41 GB / ~246M
rows over ~40k GO terms) and ``term_frequency``. Even after the COPY-based
per-(set, known_go_id) memo (#659/#660), the *first* read of every distinct
known term in a split is a random index probe into a 41 GB table. During a long
export the Postgres backends sit in ``D`` state (disk-I/O wait) while the worker
idles near 10% CPU: the build is DISK-I/O-bound on thousands of per-chunk random
reads, not CPU-bound.

``term_cooccurrence`` is a sparse matrix ``M[known][candidate] = count`` over the
set's GO vocabulary. Loading it ONCE per annotation_set as an in-memory
``scipy.sparse`` CSR (rows = known terms, cols = candidate terms) plus the
``term_frequency`` vector turns the bottleneck into a single sequential COPY scan
of the set's rows, after which every per-chunk association lookup is served from
RAM with ZERO DB round-trips for the rest of the split.

For the biggest training set (v160) the matrix has ~24M nonzeros, so the CSR is
roughly ``24M * (4 + 4) bytes ~= 200 MB`` (int32 data + int32 indices) plus the
indptr and the go_id<->index maps: it fits RAM easily and is dropped per split
by :func:`clear_csr_cooccurrence_cache` (cooccurrence is t0-specific).

Bit-exactness
-------------
This loader is a drop-in for
``_association_loader.load_cooccurrence_for_known``: it returns the SAME
``(cooc_by_known, freq)`` dict structures with the SAME integer values, gathered
from the CSR instead of queried per chunk. The downstream scoring
(``_accumulate_association``) reads ``cooc_by_known[k][t]`` and ``freq[k]`` as
ints and computes ``count / f``; identical ints in identical dicts yield
bit-identical floats. The same guards are preserved: legacy NULL ``go_id`` rows
are skipped (``IS NOT NULL`` in the COPY SELECT), and only the requested known
terms are surfaced (leakage-clean, t0-only set).
"""

from __future__ import annotations

import os
import uuid

import numpy as np
from scipy.sparse import csr_matrix
from sqlalchemy.orm import Session

# Env gate. When truthy, ``apply_association``'s loader resolves to the CSR path.
# Off by default until a deploy flips it; the path is proven bit-exact under test.
_ENV_FLAG = "PROTEA_ASSOCIATION_CSR"


def csr_enabled() -> bool:
    """True when the CSR association path is enabled via env."""
    return os.environ.get(_ENV_FLAG, "").strip().lower() in {"1", "true", "yes", "on"}


class _SetCooc:
    """In-memory CSR cooccurrence + frequency for ONE annotation_set.

    ``matrix`` is CSR with rows indexed by ``known_idx[known_go_id]`` and columns
    by ``cand_idx[candidate_go_id]``; ``matrix[i, j]`` is the cooccurrence count.
    ``cand_go`` is the inverse column map (col index -> candidate go_id string).
    ``freq`` is the per-known-term frequency (only known go_ids present in
    ``term_frequency`` appear).
    """

    __slots__ = ("matrix", "known_idx", "cand_go", "freq")

    def __init__(
        self,
        matrix: csr_matrix,
        known_idx: dict[str, int],
        cand_go: list[str],
        freq: dict[str, int],
    ) -> None:
        self.matrix = matrix
        self.known_idx = known_idx
        self.cand_go = cand_go
        self.freq = freq

    def row_dict(self, known_go_id: str) -> dict[str, int]:
        """Gather one known term's cooccurrence row as ``{candidate_go_id: count}``.

        Returns ``{}`` when the term has no row (absent from the matrix). Built
        directly from the CSR slice so the values are the exact stored ints.
        """
        i = self.known_idx.get(known_go_id)
        if i is None:
            return {}
        m = self.matrix
        start, end = m.indptr[i], m.indptr[i + 1]
        cols = m.indices[start:end]
        data = m.data[start:end]
        cand_go = self.cand_go
        return {cand_go[c]: int(v) for c, v in zip(cols, data, strict=True)}


# Module-level memo: one built CSR per annotation_set, reused across every parity
# chunk of a split. Cleared per split by ``clear_csr_cooccurrence_cache`` so it
# never grows past the current t0 set's matrix (~200 MB worst case).
_SET_CACHE: dict[uuid.UUID, _SetCooc] = {}


def clear_csr_cooccurrence_cache() -> None:
    """Drop the per-set CSR memo (call between splits; cooccurrence is t0-specific)."""
    _SET_CACHE.clear()


def load_cooccurrence_for_known(
    session: Session,
    annotation_set_id: uuid.UUID,
    known_go_ids: set[str],
) -> tuple[dict[str, dict[str, int]], dict[str, int]]:
    """CSR-backed drop-in for the per-chunk DB loader.

    On the first call for ``annotation_set_id`` the set's whole
    ``term_cooccurrence`` / ``term_frequency`` is streamed ONCE into an in-memory
    CSR (see :func:`_build_set_cooc`); subsequent calls (every parity chunk of the
    split) gather the requested known rows from RAM with no DB round-trip.

    The return value is byte-identical to
    ``_association_loader.load_cooccurrence_for_known``: same ``cooc_by_known``
    (only known terms WITH a non-empty row), same ``freq`` (only known terms with
    a frequency row), same integer values.
    """
    cooc_by_known: dict[str, dict[str, int]] = {}
    freq: dict[str, int] = {}
    if not known_go_ids:
        return cooc_by_known, freq

    built = _SET_CACHE.get(annotation_set_id)
    if built is None:
        built = _build_set_cooc(session, annotation_set_id)
        _SET_CACHE[annotation_set_id] = built

    for k in known_go_ids:
        row = built.row_dict(k)
        if row:
            cooc_by_known[k] = row
        f = built.freq.get(k)
        if f is not None:
            freq[k] = f
    return cooc_by_known, freq


def _build_set_cooc(session: Session, annotation_set_id: uuid.UUID) -> _SetCooc:
    """Stream the set's whole cooccurrence/frequency into an in-memory CSR.

    ONE sequential ``COPY (SELECT ...) TO STDOUT`` per table. Known/candidate
    go_id strings are interned to contiguous integer indices, COO triplets are
    accumulated (see :func:`_stream_coo`), then assembled with
    ``csr_matrix((data, (rows, cols)))``. ``csr_matrix`` sums duplicate
    (row, col) entries; the table is unique per (set, known, candidate) so no
    duplicates exist and each cell equals the single stored count. Legacy NULL
    go_id rows are excluded in the SELECT (``IS NOT NULL``), exactly as the
    per-chunk loader does, so they never reach the matrix.
    """
    raw = session.connection().connection  # psycopg3 DBAPI connection
    known_idx, cand_idx, rows, cols, data = _stream_coo(raw, annotation_set_id)

    n_known = len(known_idx)
    n_cand = len(cand_idx)
    if data:
        matrix = csr_matrix(
            (
                np.asarray(data, dtype=np.int64),
                (np.asarray(rows, dtype=np.int64), np.asarray(cols, dtype=np.int64)),
            ),
            shape=(n_known, n_cand),
        )
    else:
        matrix = csr_matrix((n_known, n_cand), dtype=np.int64)

    # Inverse column map: col index -> candidate go_id string.
    cand_go = [""] * n_cand
    for go, ci in cand_idx.items():
        cand_go[ci] = go

    freq = _load_freq(raw, annotation_set_id)
    return _SetCooc(matrix, known_idx, cand_go, freq)


def _stream_coo(
    raw: object, annotation_set_id: uuid.UUID
) -> tuple[dict[str, int], dict[str, int], list[int], list[int], list[int]]:
    """Stream the set's cooccurrence rows into COO triplets via one COPY scan.

    Returns ``(known_idx, cand_idx, rows, cols, data)`` where ``rows``/``cols``
    are the interned integer indices and ``data`` the counts. go_id strings are
    interned to contiguous indices on the fly so the matrix stays compact.
    """
    known_idx: dict[str, int] = {}
    cand_idx: dict[str, int] = {}
    rows: list[int] = []
    cols: list[int] = []
    data: list[int] = []
    cooc_sql = (
        "COPY (SELECT known_go_id, candidate_go_id, cooccurrence_count "
        "FROM term_cooccurrence "
        "WHERE annotation_set_id = %s "
        "AND known_go_id IS NOT NULL AND candidate_go_id IS NOT NULL"
        ") TO STDOUT"
    )
    with raw.cursor() as cur, cur.copy(cooc_sql, (annotation_set_id,)) as cp:  # type: ignore[attr-defined]
        for known_go, candidate_go, count in cp.rows():
            ki = known_idx.get(known_go)
            if ki is None:
                ki = len(known_idx)
                known_idx[known_go] = ki
            ci = cand_idx.get(candidate_go)
            if ci is None:
                ci = len(cand_idx)
                cand_idx[candidate_go] = ci
            rows.append(ki)
            cols.append(ci)
            data.append(int(count))
    return known_idx, cand_idx, rows, cols, data


def _load_freq(raw: object, annotation_set_id: uuid.UUID) -> dict[str, int]:
    """Stream the set's ``term_frequency`` into ``{go_id: freq}`` via one COPY."""
    freq_sql = (
        "COPY (SELECT go_id, freq FROM term_frequency "
        "WHERE annotation_set_id = %s "
        "AND go_id IS NOT NULL"
        ") TO STDOUT"
    )
    freq: dict[str, int] = {}
    with raw.cursor() as cur, cur.copy(freq_sql, (annotation_set_id,)) as cp:  # type: ignore[attr-defined]
        for go_id, f in cp.rows():
            freq[go_id] = int(f)
    return freq
