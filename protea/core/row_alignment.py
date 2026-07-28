"""Joining by key, without the silent last-wins.

Building a lookup keyed by a column and then folding rows into it is the most
common join in this codebase, and it has a failure mode that produces no error
and no warning: when the key is not unique, the last row for a key overwrites
every earlier one. The result is a frame of exactly the expected shape, with
exactly the expected columns, holding values that belong to different rows than
the ones they are attributed to.

This is not hypothetical. A join on a non-unique key silently rewrote 8.3% of
rows in an analysis whose conclusions were then acted on, and nothing about the
output looked wrong. The defect was found by counting, not by reading.

The rule these helpers enforce is that **a join states its expectation and
fails when the data disagrees**. If the key really is unique, saying so costs
one call and the assertion never fires. If it is not, the caller finds out at
the join rather than in a number nobody can reproduce months later.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Hashable, Iterable, Sequence

__all__ = [
    "RowAlignmentError",
    "assert_row_count_preserved",
    "assert_unique_key",
    "lookup_by",
]

#: How many offending keys to name before truncating. Enough to see a pattern,
#: short enough that the message stays readable in a log.
_MAX_REPORTED = 5


class RowAlignmentError(ValueError):
    """A join would have silently dropped or mismatched rows.

    Raised instead of letting a duplicate key overwrite an earlier row, or
    letting a merge change the row count without anyone noticing.
    """


def _describe_duplicates[K: Hashable](counts: Counter[K], total: int) -> str:
    dupes = [(key, n) for key, n in counts.most_common() if n > 1]
    shown = ", ".join(f"{key!r} x{n}" for key, n in dupes[:_MAX_REPORTED])
    if len(dupes) > _MAX_REPORTED:
        shown += f", and {len(dupes) - _MAX_REPORTED} more"
    lost = sum(n - 1 for _, n in dupes)
    pct = 100.0 * lost / total if total else 0.0
    return (
        f"{len(dupes)} duplicated key(s) covering {lost} of {total} rows "
        f"({pct:.1f}% would have been overwritten): {shown}"
    )


def assert_unique_key[T, K: Hashable](
    rows: Iterable[T],
    key: Callable[[T], K],
    *,
    context: str,
) -> None:
    """Raise if ``key`` is not unique across ``rows``.

    ``context`` names the join in the error, because the useful question when
    this fires is which join it was, not that some join somewhere had a
    duplicate. Say what is being joined to what.
    """
    materialised = list(rows)
    counts: Counter[K] = Counter(key(row) for row in materialised)
    if any(n > 1 for n in counts.values()):
        raise RowAlignmentError(
            f"{context}: the join key is not unique. "
            f"{_describe_duplicates(counts, len(materialised))}. "
            f"A lookup built from this key would keep only the last row for each "
            f"duplicate and report a result of the expected shape."
        )


def lookup_by[T, K: Hashable](
    rows: Iterable[T],
    key: Callable[[T], K],
    *,
    context: str,
) -> dict[K, T]:
    """Build a lookup keyed by ``key``, raising rather than overwriting.

    The drop-in for ``{key(r): r for r in rows}``, which is the expression that
    loses rows without saying so. Prefer this wherever a lookup is built from
    data rather than from a literal.
    """
    materialised = list(rows)
    assert_unique_key(materialised, key, context=context)
    return {key(row): row for row in materialised}


def assert_row_count_preserved(
    before: Sequence[object] | int,
    after: Sequence[object] | int,
    *,
    context: str,
) -> None:
    """Raise if a join changed the number of rows.

    The complementary check to :func:`assert_unique_key`. A unique key on one
    side still permits fan-out from the other, and a merge that grows is as
    wrong as one that shrinks: it duplicates the left row across every match
    and every later aggregate then double counts it.
    """
    n_before = before if isinstance(before, int) else len(before)
    n_after = after if isinstance(after, int) else len(after)
    if n_before != n_after:
        direction = "grew" if n_after > n_before else "lost"
        delta = abs(n_after - n_before)
        pct = 100.0 * delta / n_before if n_before else 0.0
        raise RowAlignmentError(
            f"{context}: the join {direction} rows, {n_before} -> {n_after} "
            f"({delta} rows, {pct:.1f}%). A join that changes the row count has "
            f"either dropped observations or fanned one row out across several "
            f"matches, and every aggregate computed after it is wrong."
        )
