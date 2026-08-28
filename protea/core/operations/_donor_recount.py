"""Recount a row's aggregates at a depth, instead of inheriting them.

``vote_count``, ``neighbor_vote_fraction``, ``neighbor_mean_distance`` and
``neighbor_distance_std`` are functions of the neighbourhood the retrieval
used. Cutting that neighbourhood afterwards does not change them, so an arm
cut to depth 2 carries a consensus measured over depth 30 and reports it as
its own. Nothing fails and nothing looks wrong; the numbers are simply about
a different candidate set than the one they are labelled with.

The donor ledger on the row is the detail that makes a recount possible: one
entry per distinct donor, with where it sat and how far it was. At depth d the
voters are the entries at or below d, and the aggregates are taken over those.

Two quantities do not need recounting and are deliberately absent here.
``distance`` and ``neighbor_min_distance`` are minima, and the argument of the
minimum is the term's shallowest donor, which survives every cut that keeps
the row at all. Recomputing them would return the number already stored.

WHAT AN ABSENT LEDGER MEANS. Null arrays say the row was retrieved before the
ledger existed. There is then no honest value for a cut aggregate, and three
answers were available: keep the stored one, which is the defect this module
exists to end; refuse, which makes every historical row unscoreable; or return
nothing and let the signal drop out of the score, which is what
``compute_score`` already does with a None and says out loud in its docstring.
This returns nothing. The caller is responsible for noticing that a whole run
is in that state, because a score built from ten signals on some rows and nine
on others is not comparable within itself, which is worse than either.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

__all__ = ("DepthCut", "RecountedAggregates", "recount_at_depth")


@dataclass(frozen=True)
class DepthCut:
    """A depth, and the unit it is counted in.

    Exactly one of the two is set. Counting in proteins is what
    ``k_position`` has always meant; counting in sequences is what makes
    the arms comparable, because 38,694 sequences in this bank belong to
    more than one protein and one belongs to 114.
    """

    max_k_position: int | None = None
    max_sequence_rank: int | None = None

    def __post_init__(self) -> None:
        if (self.max_k_position is None) == (self.max_sequence_rank is None):
            raise ValueError(
                "a depth is counted either in proteins or in sequences, "
                "and a cut must say which; got "
                f"max_k_position={self.max_k_position!r} and "
                f"max_sequence_rank={self.max_sequence_rank!r}"
            )


@dataclass(frozen=True)
class RecountedAggregates:
    """What the surviving donors of one row add up to.

    ``vote_count`` here counts donors, not annotation rows, which is what
    the column of that name has always counted. The two differ on 37.6
    per cent of pairs in this corpus, so they are not interchangeable and
    this one is named for what it holds.
    """

    donor_count: int
    mean_distance: float | None
    min_distance: float | None
    distance_std: float | None

    def vote_fraction(self, depth: int) -> float | None:
        """Donors that voted, over the depth they were drawn from.

        Bounded by 1 by construction, because a depth admits at most that
        many donors and each is counted once. The stored column of this
        name is not: it divides annotation rows by the retrieval K and
        reaches 4.9.
        """
        if depth <= 0:
            return None
        return self.donor_count / depth


def _ranks_for(row: dict[str, Any], cut: DepthCut) -> tuple[list[int] | None, int]:
    """The rank list the cut is measured against, and the depth itself."""
    if cut.max_sequence_rank is not None:
        return row.get("donor_sequence_ranks"), cut.max_sequence_rank
    return row.get("donor_k_positions"), int(cut.max_k_position or 0)


def recount_at_depth(row: dict[str, Any], cut: DepthCut) -> RecountedAggregates | None:
    """Recompute one row's cut-dependent aggregates over its surviving donors.

    Args:
        row: A prediction row carrying the donor ledger. Missing or null
            ledger columns mean the row predates it.
        cut: The depth, and whether it is counted in proteins or in
            sequences.

    Returns:
        The aggregates over the donors at or below the depth, or None
        when the row carries no ledger in the unit the cut asks for. A
        run whose rows disagree about that is not internally comparable,
        so the caller must decide per run, not per row.

        A ledger that exists but has no donor within the depth returns a
        count of zero with distances of None. That is a real, readable
        state: the term is on the row because a donor gave it, but that
        donor sits deeper than this cut, so the row should not have
        survived the cut either.
    """
    ranks, depth = _ranks_for(row, cut)
    distances = row.get("donor_distances")
    if ranks is None or distances is None or len(ranks) != len(distances):
        return None
    kept = [float(d) for rank, d in zip(ranks, distances, strict=True) if rank <= depth]
    if not kept:
        return RecountedAggregates(0, None, None, None)
    mean = math.fsum(kept) / len(kept)
    variance = math.fsum((d - mean) ** 2 for d in kept) / len(kept)
    return RecountedAggregates(
        donor_count=len(kept),
        mean_distance=mean,
        min_distance=min(kept),
        distance_std=math.sqrt(variance) if len(kept) > 1 else 0.0,
    )
