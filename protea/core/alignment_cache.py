"""Reuse of pairwise alignments across runs.

An alignment depends on the two sequences and on nothing else: not the
embedding model, not K, not the temporal window, not the donor policy. The
rung-1 grid recomputes them anyway, once per (model, K) run, and profiling puts
them at 63% of a batch. Measured recurrence on that grid:

* within one model, K=3's pairs are a strict subset of K=30's (1,239 of 1,239),
  so a model computes 3+5+10+30 = 48 alignments per query where 30 would do;
* across models, 1,063 of 1,216 pairs recurred (87%), because different
  encoders retrieve largely the same neighbours.

Keyed by ``Sequence.sequence_hash``, never by accession. An accession can point
at a different sequence after a UniProt release, and a cache keyed on it would
then answer with an alignment of a sequence that no longer exists under that
name. That is the same failure as an aspect index identified by its cache key
rather than by the pool it was built from (see PROTEA#788): identity by name is
not identity by content.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from collections.abc import Sequence as Seq
from typing import Any

from sqlalchemy import select, tuple_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from protea.infrastructure.orm.models.alignment.sequence_alignment import SequenceAlignment

# The exact keys ``feature_engineering.compute_alignment`` returns. Named here
# so a change on either side is a failing test rather than a silently missing
# feature column.
ALIGNMENT_FIELDS: tuple[str, ...] = (
    "identity_nw",
    "similarity_nw",
    "alignment_score_nw",
    "gaps_pct_nw",
    "alignment_length_nw",
    "identity_sw",
    "similarity_sw",
    "alignment_score_sw",
    "gaps_pct_sw",
    "alignment_length_sw",
    "length_query",
    "length_ref",
)

# Postgres refuses a statement with more than 65535 bind parameters, and the
# limit is on PARAMETERS, not rows. An insert binds one parameter per column,
# so the row budget is the parameter budget divided by the column count.
# Sizing this in rows is how it first shipped, and 5,000 rows x 14 columns =
# 70,000 parameters, which fails at execute time rather than at import.
_PARAM_LIMIT = 65_535
_SAFETY = 0.9

#: Columns per inserted row: the two hash keys plus every metric.
_INSERT_COLUMNS = len(ALIGNMENT_FIELDS) + 2

#: Parameters per looked-up pair: the two hashes of the tuple comparison.
_LOOKUP_COLUMNS = 2

#: Pairs per lookup statement, fixed rather than derived.
#:
#: The read hits a DIFFERENT ceiling from the write. A row-values IN list is
#: parsed as a nested expression tree, so Postgres raises
#: ``StatementTooComplex: stack depth limit exceeded`` long before the 65,535
#: parameter cap is anywhere near. Deriving this from the parameter budget
#: gives 29,490 pairs and fails; the budget is the parser's stack, which is not
#: something the column count can predict.
_LOOKUP_CHUNK = 1_000


def _chunk_for(columns: int) -> int:
    """Rows per statement that keep the bind list under Postgres's ceiling."""
    return max(1, int(_PARAM_LIMIT * _SAFETY) // columns)


def lookup(
    session: Session, pairs: Iterable[tuple[str, str]]
) -> dict[tuple[str, str], dict[str, Any]]:
    """Return the cached alignments among ``pairs``, keyed by (query, ref) hash.

    Absent pairs are simply absent from the result; the caller computes those.
    """
    wanted = list(dict.fromkeys(pairs))
    found: dict[tuple[str, str], dict[str, Any]] = {}
    size = _LOOKUP_CHUNK
    for start in range(0, len(wanted), size):
        chunk = wanted[start : start + size]
        rows = session.execute(
            select(SequenceAlignment).where(
                tuple_(SequenceAlignment.query_hash, SequenceAlignment.ref_hash).in_(chunk)
            )
        ).scalars()
        for row in rows:
            found[(row.query_hash, row.ref_hash)] = {
                field: getattr(row, field) for field in ALIGNMENT_FIELDS
            }
    return found


def store(
    session: Session, computed: Mapping[tuple[str, str], Mapping[str, Any]]
) -> int:
    """Persist newly computed alignments. Returns the number offered.

    Conflicts are ignored rather than updated: two workers computing the same
    pair produce the same numbers, so the first writer wins and the second has
    nothing to correct. Never raises on a race.
    """
    payload = [
        {
            "query_hash": q,
            "ref_hash": r,
            **{field: float(feats[field]) for field in ALIGNMENT_FIELDS},
        }
        for (q, r), feats in computed.items()
        if all(field in feats for field in ALIGNMENT_FIELDS)
    ]
    size = _chunk_for(_INSERT_COLUMNS)
    for start in range(0, len(payload), size):
        chunk = payload[start : start + size]
        session.execute(
            insert(SequenceAlignment)
            .values(chunk)
            .on_conflict_do_nothing(index_elements=["query_hash", "ref_hash"])
        )
    return len(payload)


def hashes_for(sequences: Mapping[str, str]) -> dict[str, str]:
    """Map accession -> sequence hash, using the project's canonical hash.

    Delegates rather than hashing here, so the cache key cannot drift from the
    hash the ``sequence`` table is deduplicated by.
    """
    from protea.infrastructure.orm.models.sequence.sequence import Sequence

    return {acc: Sequence.compute_hash(seq) for acc, seq in sequences.items() if seq}


def missing(
    pairs: Seq[tuple[str, str]], found: Mapping[tuple[str, str], Any]
) -> list[tuple[str, str]]:
    """The pairs that still have to be computed, order preserved."""
    return [p for p in dict.fromkeys(pairs) if p not in found]


class SessionAlignmentCache:
    """Binds the two functions above to one session.

    Exists so the adapter can be handed a cache without learning what a
    ``Session`` is: it holds the port, calls ``lookup`` and ``store``, and
    behaves exactly as before when it holds None instead.
    """

    def __init__(self, session: Session) -> None:
        self._session = session

    def lookup(
        self, pairs: Iterable[tuple[str, str]]
    ) -> dict[tuple[str, str], dict[str, Any]]:
        return lookup(self._session, pairs)

    def store(self, computed: Mapping[tuple[str, str], Mapping[str, Any]]) -> int:
        return store(self._session, computed)
