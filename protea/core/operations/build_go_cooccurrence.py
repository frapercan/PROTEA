"""Build the per-annotation-set GO term co-occurrence + frequency tables.

This operation (``build_go_cooccurrence``) populates :class:`TermCooccurrence`
and :class:`TermFrequency` for one ``AnnotationSet`` from its propagated
EXPERIMENTAL annotations. It is the offline half of the cross-aspect
association feature (lafa-integrate INT-3); the predict path reads the tables
behind the ``compute_association`` flag and never writes them.

Algorithm (mirrors ``protea-reranker-lab/fullgo/assoc_feature.py``):

1. Load each protein's experimental, non-negated leaf term ids in the set.
2. Propagate each protein's term set to its is_a / part_of ancestor closure
   under the set's ontology snapshot (so co-occurrence is over propagated
   labels, matching the evaluation frame).
3. ``freq(t)`` = number of distinct proteins carrying ``t`` (after
   propagation). Stored in ``term_frequency``.
4. For every protein and every ordered pair ``(k, t)`` where ``k`` is a
   "specific" term (``freq(k) <= known_freq_cap``), accumulate
   ``cooccurrence(k, t)`` = number of distinct proteins carrying both.
   Stored in ``term_cooccurrence`` (the ``k`` side is the known/anchor term,
   the ``t`` side is the candidate). High-frequency ``k`` terms are dropped
   because ``P(t | k)`` for them is ~the marginal and carries no signal.

The build runs entirely on the given annotation set; it never touches a
post-cutoff set, so no leakage can enter the prior knowledge transfer.
"""

from __future__ import annotations

import time
import uuid
from collections import defaultdict
from collections.abc import Iterable, Iterator
from itertools import islice
from typing import Annotated, Any

from pydantic import Field
from sqlalchemy import delete, text
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.core.evidence_codes import ECO_TO_CODE, EXPERIMENTAL
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.term_cooccurrence import (
    TermCooccurrence,
    TermFrequency,
)

# GO + ECO codes that count as experimental (mirrors core.evaluation._EXP_CODES).
_EXP_CODES: list[str] = list(
    EXPERIMENTAL | {eco for eco, go in ECO_TO_CODE.items() if go in EXPERIMENTAL}
)

PositiveInt = Annotated[int, Field(gt=0)]


class BuildGoCooccurrencePayload(ProteaPayload, frozen=True):
    """Inputs for ``build_go_cooccurrence``.

    ``known_freq_cap`` bounds the ``k`` (known/anchor) side: only terms whose
    propagated protein frequency is at or below the cap are stored as known
    terms, matching the offline reference's ``FCAP`` (default 1000). Common
    terms above the cap carry no conditional signal and are skipped.
    """

    annotation_set_id: str
    known_freq_cap: PositiveInt = 1000
    write_batch_size: PositiveInt = 50_000


def _iter_batches(items: Any, size: int) -> Any:
    """Yield ``size``-length tuples from an iterable."""
    it = iter(items)
    while True:
        chunk = tuple(islice(it, size))
        if not chunk:
            return
        yield chunk


# COPY column order: exactly the table column order the rows are written in.
# The row helpers below MUST emit values in this order (psycopg3 adapts each
# Python value, with ``None`` becoming SQL NULL, so the COPY rows are
# byte-identical to what the old ``bulk_insert_mappings`` produced).
_FREQ_COLUMNS = ("annotation_set_id", "term_id", "go_id", "freq")
_COOCCURRENCE_COLUMNS = (
    "annotation_set_id",
    "known_term_id",
    "candidate_term_id",
    "known_go_id",
    "candidate_go_id",
    "cooccurrence_count",
)


def _freq_copy_row(
    set_id_str: str,
    term: int,
    freq_value: int,
    go_id_by_int: dict[int, str],
) -> tuple[str, int, str | None, int]:
    """One ``term_frequency`` COPY row in ``_FREQ_COLUMNS`` order.

    ``go_id`` is resolved via ``go_id_by_int.get(term)`` exactly as the old
    insert; a missing id yields ``None`` (SQL NULL). Values match the prior
    bulk_insert mapping one for one.
    """
    return (set_id_str, term, go_id_by_int.get(term), freq_value)


def _cooccurrence_copy_row(
    set_id_str: str,
    known_term: int,
    candidate_term: int,
    count: int,
    go_id_by_int: dict[int, str],
) -> tuple[str, int, int, str | None, str | None, int]:
    """One ``term_cooccurrence`` COPY row in ``_COOCCURRENCE_COLUMNS`` order.

    The known (``k``) and candidate (``t``) go_id strings are each resolved
    via ``go_id_by_int.get(...)``; a missing id yields ``None`` (SQL NULL).
    Values match the prior bulk_insert mapping one for one.
    """
    return (
        set_id_str,
        known_term,
        candidate_term,
        go_id_by_int.get(known_term),
        go_id_by_int.get(candidate_term),
        count,
    )


def _copy_rows(
    session: Session,
    table: str,
    columns: tuple[str, ...],
    rows: Iterable[tuple[Any, ...]],
    *,
    chunk_size: int,
) -> int:
    """Stream ``rows`` into ``table`` via ``COPY ... FROM STDIN`` (psycopg3).

    Uses the raw DBAPI connection under the SQLAlchemy session so the COPY
    runs inside the session's transaction. Rows are written one at a time
    through psycopg3's binary COPY writer (``cp.write_row``), so memory stays
    bounded regardless of total row count; ``chunk_size`` only governs the
    per-chunk commit cadence (matching the old per-batch commit semantics).
    Returns the number of rows written.
    """
    col_list = ", ".join(columns)
    stmt = f"COPY {table} ({col_list}) FROM STDIN"
    written = 0
    for chunk in _iter_batches(rows, chunk_size):
        # Re-fetch the raw DBAPI connection each chunk: session.commit() below
        # ends the transaction and invalidates the prior reference (psycopg3
        # asserts if a stale post-commit connection is reused for COPY).
        raw = session.connection().connection
        with raw.cursor() as cur, cur.copy(stmt) as cp:
            for row in chunk:
                cp.write_row(row)
        written += len(chunk)
        session.commit()
    return written


class BuildGoCooccurrenceOperation:
    """Populate ``term_cooccurrence`` + ``term_frequency`` for one set."""

    name = "build_go_cooccurrence"
    description = (
        "Build per-annotation-set GO term co-occurrence and frequency tables "
        "from propagated experimental annotations, for the cross-aspect "
        "association feature."
    )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        set_id = (payload or {}).get("annotation_set_id", "?")
        cap = (payload or {}).get("known_freq_cap", 1000)
        return f"annotation_set={set_id} cap={cap}"

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = BuildGoCooccurrencePayload.model_validate(payload or {})
        set_id = uuid.UUID(p.annotation_set_id)
        t0 = time.perf_counter()

        ann_set = session.get(AnnotationSet, set_id)
        if ann_set is None:
            raise ValueError(f"annotation_set {set_id} not found")
        snapshot_id = ann_set.ontology_snapshot_id

        emit(
            "build_go_cooccurrence.start",
            None,
            {"annotation_set_id": str(set_id), "known_freq_cap": p.known_freq_cap},
            "info",
        )

        terms_by_protein = self._load_experimental_terms(session, set_id)
        parent_map = self._load_parent_map_ids(session, snapshot_id)
        self._propagate(terms_by_protein, parent_map)

        freq = self._compute_freq(terms_by_protein)
        emit(
            "build_go_cooccurrence.propagated",
            None,
            {"proteins": len(terms_by_protein), "distinct_terms": len(freq)},
            "info",
        )

        known_terms = {t for t, f in freq.items() if f <= p.known_freq_cap}
        cooc = self._compute_cooccurrence(terms_by_protein, known_terms)
        freq_written, pairs_written = self._persist(session, set_id, freq, cooc, p.write_batch_size)

        session.commit()
        result = {
            "annotation_set_id": str(set_id),
            "proteins": len(terms_by_protein),
            "terms": freq_written,
            "known_terms": len(known_terms),
            "cooccurrence_pairs": pairs_written,
            "elapsed_seconds": time.perf_counter() - t0,
        }
        emit("build_go_cooccurrence.done", None, result, "info")
        return OperationResult(result=result)

    def _load_experimental_terms(self, session: Session, set_id: uuid.UUID) -> dict[str, set[int]]:
        """``{protein_accession: {go_term_id, ...}}`` for experimental, non-NOT rows."""
        rows = session.execute(
            text(
                "SELECT protein_accession, go_term_id "
                "FROM protein_go_annotation "
                "WHERE annotation_set_id = :set_id "
                "AND evidence_code = ANY(:exp_codes) "
                "AND (qualifier IS NULL OR qualifier NOT LIKE '%NOT%')"
            ),
            {"set_id": set_id, "exp_codes": _EXP_CODES},
        ).fetchall()
        by_protein: dict[str, set[int]] = defaultdict(set)
        for acc, gtid in rows:
            by_protein[acc].add(int(gtid))
        return by_protein

    def _load_parent_map_ids(self, session: Session, snapshot_id: uuid.UUID) -> dict[int, set[int]]:
        """``{child_go_term_id: {parent_go_term_id, ...}}`` for is_a + part_of."""
        rows = session.execute(
            text(
                "SELECT child_go_term_id, parent_go_term_id "
                "FROM go_term_relationship "
                "WHERE ontology_snapshot_id = :snap_id "
                "AND relation_type IN ('is_a', 'part_of')"
            ),
            {"snap_id": snapshot_id},
        ).fetchall()
        parent_map: dict[int, set[int]] = defaultdict(set)
        for child, parent in rows:
            parent_map[int(child)].add(int(parent))
        return parent_map

    def _load_go_id_map(self, session: Session, term_ids: set[int]) -> dict[int, str]:
        """``{go_term_id: go_id}`` for the given int ids (the string anchor).

        Resolves each per-snapshot ``GOTerm.id`` to its snapshot-invariant
        ``go_id`` so the written rows carry a key the predict path can match
        across snapshots. Empty input yields an empty map.
        """
        if not term_ids:
            return {}
        rows = session.execute(
            text("SELECT id, go_id FROM go_term WHERE id = ANY(:ids)"),
            {"ids": list(term_ids)},
        ).fetchall()
        return {int(gid): go_id for gid, go_id in rows}

    def _propagate(
        self,
        terms_by_protein: dict[str, set[int]],
        parent_map: dict[int, set[int]],
    ) -> None:
        """Expand each protein's term set to its ancestor closure in place."""
        closure_cache: dict[int, frozenset[int]] = {}

        def ancestors(term: int) -> frozenset[int]:
            cached = closure_cache.get(term)
            if cached is not None:
                return cached
            # Guard against accidental cycles by tracking the visited frontier.
            closure_cache[term] = frozenset()  # placeholder breaks self-recursion
            acc: set[int] = set()
            for parent in parent_map.get(term, ()):  # noqa: B007
                acc.add(parent)
                acc |= ancestors(parent)
            result = frozenset(acc)
            closure_cache[term] = result
            return result

        for acc_set in terms_by_protein.values():
            expanded: set[int] = set(acc_set)
            for term in tuple(acc_set):
                expanded |= ancestors(term)
            acc_set.clear()
            acc_set.update(expanded)

    def _compute_freq(self, terms_by_protein: dict[str, set[int]]) -> dict[int, int]:
        freq: dict[int, int] = defaultdict(int)
        for terms in terms_by_protein.values():
            for t in terms:
                freq[t] += 1
        return dict(freq)

    def _compute_cooccurrence(
        self,
        terms_by_protein: dict[str, set[int]],
        known_terms: set[int],
    ) -> dict[tuple[int, int], int]:
        """``{(known_k, candidate_t): distinct_proteins}`` for k in known_terms.

        ``cooccurrence(k, t)`` is the number of proteins carrying both term k
        and term t (after propagation), restricted to anchor terms k in
        ``known_terms``. This equals ``(M.T @ M)[k, t]`` where M is the
        proteins x terms binary incidence matrix (per protein the term set is
        already deduplicated, so M is binary). We build M as a sparse CSR
        matrix over a contiguous dense term index, take the known-row slice,
        and the matmul yields exactly the same counts (diagonal included: a
        protein co-occurs with itself, so ``(k, k)`` = freq of k among the
        proteins carrying it). Empty input yields an empty dict.
        """
        import numpy as np
        from scipy import sparse

        # Map every distinct term id to a contiguous 0..N-1 column index.
        all_terms = sorted({t for terms in terms_by_protein.values() for t in terms})
        if not all_terms:
            return {}
        col_of = {t: i for i, t in enumerate(all_terms)}

        rows: list[int] = []
        cols: list[int] = []
        for r, terms in enumerate(terms_by_protein.values()):
            for t in terms:
                rows.append(r)
                cols.append(col_of[t])
        n_proteins = len(terms_by_protein)
        n_terms = len(all_terms)
        m = sparse.csr_matrix(
            (np.ones(len(rows), dtype=np.int64), (rows, cols)),
            shape=(n_proteins, n_terms),
        )

        # Known-row slice (anchor side k) times the full matrix (candidate t).
        known_cols = [col_of[k] for k in all_terms if k in known_terms]
        if not known_cols:
            return {}
        cooc_mat = (m[:, known_cols].T @ m).tocoo()

        cooc: dict[tuple[int, int], int] = {}
        for ki, tj, count in zip(cooc_mat.row, cooc_mat.col, cooc_mat.data, strict=True):
            cooc[(all_terms[known_cols[ki]], all_terms[tj])] = int(count)
        return cooc

    def _persist(
        self,
        session: Session,
        set_id: uuid.UUID,
        freq: dict[int, int],
        cooc: dict[tuple[int, int], int],
        batch_size: int,
    ) -> tuple[int, int]:
        """Reset + rewrite both tables for this set; return ``(freq, pairs)``.

        Resolves the snapshot-invariant go_id strings for every int id in play
        first, so each written row carries the key the predict path matches on
        (so cooccurrence is correct even when the candidate snapshot differs
        from this set's snapshot). The delete makes the build idempotent.
        """
        go_id_by_int = self._load_go_id_map(session, set(freq))
        session.execute(delete(TermFrequency).where(TermFrequency.annotation_set_id == set_id))
        session.execute(
            delete(TermCooccurrence).where(TermCooccurrence.annotation_set_id == set_id)
        )
        freq_written = self._write_freq(session, set_id, freq, go_id_by_int, batch_size)
        pairs_written = self._write_cooccurrence(session, set_id, cooc, go_id_by_int, batch_size)
        return freq_written, pairs_written

    def _write_freq(
        self,
        session: Session,
        set_id: uuid.UUID,
        freq: dict[int, int],
        go_id_by_int: dict[int, str],
        batch_size: int,
    ) -> int:
        """COPY the ``term_frequency`` rows for this set (psycopg3 STDIN).

        Replaces ``bulk_insert_mappings`` (~40 min/set at the cooccurrence
        scale) with a streamed ``COPY ... FROM STDIN``. The emitted rows are
        byte-identical to the old mapping: same columns, same values, same
        NULL handling for missing go_id strings.
        """
        set_id_str = str(set_id)

        def rows() -> Iterator[tuple[Any, ...]]:
            for term, f in freq.items():
                yield _freq_copy_row(set_id_str, term, f, go_id_by_int)

        return _copy_rows(
            session,
            TermFrequency.__tablename__,
            _FREQ_COLUMNS,
            rows(),
            chunk_size=batch_size,
        )

    def _write_cooccurrence(
        self,
        session: Session,
        set_id: uuid.UUID,
        cooc: dict[tuple[int, int], int],
        go_id_by_int: dict[int, str],
        batch_size: int,
    ) -> int:
        """COPY the ``term_cooccurrence`` rows for this set (psycopg3 STDIN).

        Replaces ``bulk_insert_mappings`` for the ~24M rows/set that dominated
        the build cost. Streamed row by row so memory stays bounded; the
        emitted rows are byte-identical to the old mapping.
        """
        set_id_str = str(set_id)

        def rows() -> Iterator[tuple[Any, ...]]:
            for (k, t), count in cooc.items():
                yield _cooccurrence_copy_row(set_id_str, k, t, count, go_id_by_int)

        return _copy_rows(
            session,
            TermCooccurrence.__tablename__,
            _COOCCURRENCE_COLUMNS,
            rows(),
            chunk_size=batch_size,
        )
