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

VOTES WEIGHTED BY DISTANCE. Two signals read the ledger at the cut and weigh
each donor by how close it is, both fractions in [0, 1]:

- ``neighbor_similarity_vote``: for a term, the summed similarity ``1 - d/2`` of
  the neighbours within the depth that carry it, over the summed similarity of
  every neighbour within the depth (a similarity-weighted vote, the form of
  DiamondScore with similarities for bit scores);
- ``neighbor_kernel_fraction``: for a term, the sum of ``h / (h + d)`` over the
  neighbours within the depth that carry it, divided by the depth (goPredSim's
  reliability index over k neighbours, with its bandwidth ``h = 0.5``).

The kernel fraction needs the row alone. The similarity vote's denominator is
the neighbourhood, which no row stores: a ledger names only the donors of its
own term. It is rebuilt as the union of the ledgers of one protein's
candidates, in one aspect when the search ranked each aspect's neighbours on
their own (every neighbour of such a search donates at least one term of its
aspect), and across aspects when it ranked one list for all of them. The union
is that neighbourhood only if every neighbour left a candidate, so it is
asserted (:func:`similarity_votes`), and a protein whose ledgers skip a
position stops the computation instead of scoring over a smaller neighbourhood.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

__all__ = (
    "KERNEL_BANDWIDTH",
    "KERNEL_FRACTION",
    "NEIGHBOURHOOD_SIGNALS",
    "SIMILARITY_VOTE",
    "DepthCut",
    "NeighbourhoodIncomplete",
    "NeighbourhoodRequest",
    "RecountedAggregates",
    "kernel_fraction",
    "neighbourhood_request",
    "recount_at_depth",
    "similarity_votes",
)

#: The two signals computed here from the ledger, by the names a scoring
#: configuration weights them under.
SIMILARITY_VOTE = "neighbor_similarity_vote"
KERNEL_FRACTION = "neighbor_kernel_fraction"
NEIGHBOURHOOD_SIGNALS: tuple[str, ...] = (SIMILARITY_VOTE, KERNEL_FRACTION)

#: goPredSim's bandwidth (Littmann et al. 2021): a donor at distance d weighs
#: h / (h + d). The published 0.5 is the only value the scoring uses.
KERNEL_BANDWIDTH = 0.5


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

    ``donor_count`` counts donors, not annotation rows, which is what the
    column named ``vote_count`` has always counted. The two differ on
    37.6 per cent of pairs in this corpus, so they are not
    interchangeable and this one is named for what it holds.

    ``sequence_count`` is the same survivors counted in distinct
    sequences. It is not redundant: two donors sharing a sequence are two
    donors and one sequence, which is the entire reason depth is counted
    in sequences at all.
    """

    donor_count: int
    sequence_count: int | None
    mean_distance: float | None
    min_distance: float | None
    distance_std: float | None
    cut: DepthCut

    def vote_fraction(self) -> float | None:
        """Voters over the depth they were drawn from, in one unit.

        The numerator has to be counted in the same unit as the
        denominator or the result is not a fraction. A cut at sequence
        depth 1 can admit two donors, because two proteins can share one
        sequence, so donors over that depth would give 2.0. Counted in
        sequences it gives 1.0, which is what the depth admitted and what
        it was asked.

        Bounded by 1 either way. The stored column of this name is not:
        it divides annotation rows by the retrieval K and reaches 4.9 on
        104,627 rows.

        Returns None when the row carries no sequence ranks and the cut
        is counted in sequences, since there is then nothing to count in
        that unit rather than a zero.
        """
        if self.cut.max_sequence_rank is not None:
            if self.sequence_count is None or self.cut.max_sequence_rank <= 0:
                return None
            return self.sequence_count / self.cut.max_sequence_rank
        depth = int(self.cut.max_k_position or 0)
        if depth <= 0:
            return None
        return self.donor_count / depth


def _depth(cut: DepthCut) -> int:
    """The depth itself, in the cut's own unit."""
    if cut.max_sequence_rank is not None:
        return int(cut.max_sequence_rank)
    return int(cut.max_k_position or 0)


def _ranks_for(row: Mapping[str, Any], cut: DepthCut) -> tuple[list[int] | None, int]:
    """The rank list the cut is measured against, and the depth itself."""
    by_sequence = cut.max_sequence_rank is not None
    return row.get("donor_sequence_ranks" if by_sequence else "donor_k_positions"), _depth(cut)


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
    survivors = [
        (rank, float(d))
        for rank, d in zip(ranks, distances, strict=True)
        if rank <= depth
    ]
    sequence_ranks = row.get("donor_sequence_ranks")
    kept_sequences = (
        None
        if sequence_ranks is None or len(sequence_ranks) != len(distances)
        else len({
            seq
            for seq, rank in zip(sequence_ranks, ranks, strict=True)
            if rank <= depth
        })
    )
    if not survivors:
        return RecountedAggregates(0, kept_sequences and 0, None, None, None, cut)
    kept = [d for _, d in survivors]
    mean = math.fsum(kept) / len(kept)
    variance = math.fsum((d - mean) ** 2 for d in kept) / len(kept)
    return RecountedAggregates(
        donor_count=len(kept),
        sequence_count=kept_sequences,
        mean_distance=mean,
        min_distance=min(kept),
        distance_std=math.sqrt(variance) if len(kept) > 1 else 0.0,
        cut=cut,
    )


class NeighbourhoodIncomplete(ValueError):
    """A protein's candidates do not name every neighbour within the depth."""


@dataclass(frozen=True)
class NeighbourhoodRequest:
    """The neighbourhood signals a scoring reads, and how the neighbours were ranked.

    ``by_aspect`` is the search's mode as the candidate set recorded it
    (``aspect_separated_knn``): True ranks each aspect's neighbours on their
    own, so a neighbourhood is one protein's in one aspect; False ranks one
    list for every aspect, so it is the protein's. None when the set does not
    say, which only the kernel fraction can do without.
    """

    signals: frozenset[str]
    by_aspect: bool | None


def neighbourhood_request(
    weights: Mapping[str, Any], meta: Mapping[str, Any] | None, cut: DepthCut | None
) -> NeighbourhoodRequest | None:
    """The neighbourhood signals a configuration weights, or None when it weights neither.

    Args:
        weights: The configuration's signal weights.
        meta: The candidate set's record, which says how its neighbours were
            ranked.
        cut: The depth the scoring reads, or None when it names none.

    Raises:
        ValueError: When a signal is weighted with no depth named, since both
            are read at a depth; or when the similarity vote is weighted over a
            set that does not record how its neighbours were ranked, since its
            neighbourhood is then neither known to be one aspect's nor the
            protein's.
    """
    wanted = frozenset(s for s in NEIGHBOURHOOD_SIGNALS if float(weights.get(s) or 0.0) != 0.0)
    if not wanted:
        return None
    if cut is None:
        raise ValueError(
            f"{' and '.join(sorted(wanted))} are read over the neighbourhood at a depth, "
            "and none was named: name max_k_position or max_sequence_rank (the depth "
            "the candidates were written at reads every stored neighbour)"
        )
    mode = (meta or {}).get("aspect_separated_knn")
    if SIMILARITY_VOTE in wanted and not isinstance(mode, bool):
        raise ValueError(
            f"{SIMILARITY_VOTE} divides by the neighbourhood, and the candidate set does "
            "not record how its neighbours were ranked (aspect_separated_knn), so it "
            "cannot be rebuilt as one aspect's or the protein's"
        )
    return NeighbourhoodRequest(signals=wanted, by_aspect=mode if isinstance(mode, bool) else None)


def _positions(row: Mapping[str, Any], cut: DepthCut) -> dict[int, float] | None:
    """Every donor of one row by its position in the cut's unit, with its distance.

    A position holds one donor when depth is counted in proteins, and every
    donor of one sequence when it is counted in sequences. Those donors sit at
    one point of the space, so the position keeps the smallest of their
    distances. None when the row carries no ledger in that unit.
    """
    ranks = _ranks_for(row, cut)[0]
    distances = row.get("donor_distances")
    if ranks is None or distances is None or len(ranks) != len(distances):
        return None
    out: dict[int, float] = {}
    for rank, distance in zip(ranks, distances, strict=True):
        position, d = int(rank), float(distance)
        if d < out.get(position, math.inf):
            out[position] = d
    return out


def kernel_fraction(
    row: Mapping[str, Any], cut: DepthCut, bandwidth: float = KERNEL_BANDWIDTH
) -> float | None:
    """goPredSim's reliability index over the depth, for one row's term.

    The sum of ``h / (h + d)`` over the term's donors within the depth, one per
    position, divided by the depth in the cut's unit, as the vote fraction
    divides its count. None when the row carries no ledger in that unit or the
    depth is not positive.
    """
    positions = _positions(row, cut)
    depth = _depth(cut)
    if positions is None or depth <= 0:
        return None
    within = [bandwidth / (bandwidth + d) for p, d in positions.items() if p <= depth]
    return math.fsum(within) / depth


def similarity_votes(
    rows: Sequence[Mapping[str, Any]], cut: DepthCut, where: str
) -> list[float | None]:
    """The similarity-weighted vote of each row of one neighbourhood, in their order.

    Args:
        rows: Every candidate of one protein in one aspect, when its neighbours
            were ranked aspect by aspect, or of one protein, when they were
            ranked in one list.
        cut: The depth, in proteins or in sequences.
        where: The protein, and the aspect when there is one, for the message.

    Returns:
        For each row, the summed similarity ``1 - d/2`` of its donors within
        the depth over the summed similarity of every neighbour within it. A
        position's similarity is the same in both sums, so a term every
        neighbour carries scores exactly 1. None for a row without a ledger.

    Raises:
        NeighbourhoodIncomplete: When the neighbourhood is not whole. Its
            members are the union of the rows' ledgers, and within the depth
            they must be the positions 1 to ``min(depth, n)``, where ``n`` is
            the deepest position any of the ledgers reaches: the stored rows
            record no count of the neighbours a search returned, so that is the
            deepest one known. A neighbour leaves no candidate when every term
            it donated was dropped, for instance as a denial of the query, and
            its similarity would then be missing from the denominator.
    """
    per_row = [_positions(row, cut) for row in rows]
    depth = _depth(cut)
    union: dict[int, float] = {}
    for positions in per_row:
        for position, d in (positions or {}).items():
            if d < union.get(position, math.inf):
                union[position] = d
    if not union:
        return [None] * len(rows)
    within = {p: d for p, d in union.items() if p <= depth}
    expected = set(range(1, min(depth, max(union)) + 1))
    if set(within) != expected:
        missing, stray = sorted(expected - set(within)), sorted(set(within) - expected)
        found = [f"no candidate names the neighbours at {missing}"] if missing else []
        found += [f"{stray} are not neighbour positions"] if stray else []
        raise NeighbourhoodIncomplete(
            f"{where}: within a depth of {depth} the neighbourhood is the positions 1 to "
            f"{len(expected)}, the deepest a donor ledger reaches being {max(union)}, and "
            f"{' and '.join(found)}, so {SIMILARITY_VOTE} has no whole neighbourhood to "
            "divide by"
        )
    total = math.fsum(1.0 - d / 2.0 for d in within.values())
    if total <= 0.0:
        return [None] * len(rows)
    return [
        None
        if positions is None
        else math.fsum(1.0 - within[p] / 2.0 for p in positions if p <= depth) / total
        for positions in per_row
    ]
