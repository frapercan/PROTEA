"""How much of a window's truth a count map's reading holds past rank 100.

The laboratory's reading kept the 100 best terms per target and aspect. The reading
at the grid floor (``top`` None) keeps every term the evaluator can count instead,
and this measures what that adds: of the true pairs of a window (a target and a term
it gained), how many the reading holds within the first ``rank`` terms of the
target's aspect, how many only past them, and how many not at all. Ranks follow the
reading's own order (score down, then the term).

A measurement read after the run (``scripts/count_map_recall_beyond_rank.py``): the
count map itself never reads the window's truth.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from typing import Any

from protea.core.count_map.reading import TOP

__all__ = ["rank_of_rows", "recall_by_rank"]


def rank_of_rows(
    rows: Iterable[tuple[str, str, float]], aspect_of: Mapping[str, str]
) -> dict[tuple[str, str], int]:
    """(target, term) -> its rank from 1 within the target's aspect, in the reading's order.

    A term the pivot does not place in an aspect has no rank.
    """
    by_target: dict[tuple[str, str], list[tuple[float, str]]] = defaultdict(list)
    for target, term, score in rows:
        aspect = aspect_of.get(term)
        if aspect is not None:
            by_target[(target, aspect)].append((-float(score), term))
    out: dict[tuple[str, str], int] = {}
    for (target, _), scored in by_target.items():
        for rank, (_, term) in enumerate(sorted(scored), start=1):
            out[(target, term)] = rank
    return out


def _counts(found: int, past: int, pairs: int) -> dict[str, Any]:
    share = (lambda n: n / pairs) if pairs else (lambda n: None)
    return {
        "true_pairs": pairs,
        "within_rank": found,
        "past_rank": past,
        "missed": pairs - found - past,
        "recall_within_rank": share(found),
        "recall_at_the_floor": share(found + past),
        "recall_past_rank": share(past),
    }


def recall_by_rank(
    rows: Iterable[tuple[str, str, float]],
    truth: Mapping[str, Iterable[str]],
    aspect_of: Mapping[str, str],
    rank: int = TOP,
) -> dict[str, dict[str, Any]]:
    """Per aspect and for all of them: the true pairs the reading holds within and past ``rank``.

    ``rows`` are the reading's (target, term, score); ``truth`` maps a target to the
    terms it gained; ``aspect_of`` maps a term to its aspect. A true term the pivot
    places in no aspect is not counted.
    """
    ranked = rank_of_rows(rows, aspect_of)
    tally: dict[str, list[int]] = defaultdict(lambda: [0, 0, 0])
    for target, terms in truth.items():
        for term in terms:
            aspect = aspect_of.get(term)
            if aspect is None:
                continue
            where = ranked.get((target, term))
            for key in (aspect, "all"):
                tally[key][2] += 1
                if where is not None:
                    tally[key][0 if where <= rank else 1] += 1
    return {key: _counts(*tally[key]) for key in sorted(tally)}
