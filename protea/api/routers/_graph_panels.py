"""The nine panels: their populations, their levels, and what separation means.

A panel is a knowledge category crossed with an aspect. They are never pooled
and never summed, so everything here is per panel and the module offers no way
to add two of them together.

The population of a panel is reconciled, not stored. ``n_proteins`` on a scored
panel is the count of proteins holding a prediction at the threshold that run
settled on, and ``coverage_at_tau`` is that same count over the panel's
ground-truth cohort, so the cohort is the ratio of the two. Both are persisted
rounded, so one result pins the cohort to a short interval and the intersection
over every result that scored the panel collapses it. When the intervals do not
overlap the results disagree by more than rounding and the answer is None,
never a rounded guess and never a zero.
"""

from __future__ import annotations

import io
import math
from collections.abc import Mapping
from typing import Any

import pandas as pd

from protea.core.domain.aspect import Aspect
from protea.core.domain.category import Category

#: The nine panels in the project's canonical order, taken from the enums that
#: already own the vocabulary rather than respelled here.
PANEL_KEYS: tuple[tuple[str, str], ...] = tuple(
    (category.code, aspect.cafa) for category in Category for aspect in Aspect
)

#: The three aspect codes a stored GO term carries, mapped to the panel names
#: the nine-cell table is written in. Read from the term rather than from the
#: result, so a panel's population does not depend on anything a run chose.
_ASPECT_OF: dict[str, str] = {"P": "BPO", "F": "MFO", "C": "CCO"}

#: The buckets of the ground truth that are scored. ``known`` and ``pk_known``
#: are the excluded-known side and ``removed`` is reported and never scored, so
#: none of the three is a population.
_SCORED_BUCKETS: frozenset[str] = frozenset({"nk", "lk", "pk"})

#: The fields a published result can be told apart by. A level is named by
#: whichever of them actually moved, so a record that varied one thing reads as
#: one word instead of a tuple with two constants in it.
_LEVEL_FIELDS: tuple[str, ...] = (
    "scoring_name",
    "embedding_name",
    "depth",
    "donor_policy",
    "metric",
)


def panel_units_from_groundtruth(
    payload: bytes, aspect_of_term: Mapping[str, str]
) -> dict[tuple[str, str], int]:
    """Count each panel's population from the window's own ground truth.

    A panel holds the proteins that gained at least one term of that aspect in
    that knowledge bucket, which is a property of the ground truth and of the
    ontology pivot. It is therefore counted, not inferred.

    An earlier version derived it instead, by inverting a stored coverage
    against the protein count at the optimum threshold and intersecting the
    intervals that the four-decimal rounding allows. That reads as careful and
    is not: every result inverts the same quantity the same way, so the
    intervals agree with each other while all being wrong together, and the
    guard that returns nothing when they disagree can never fire. It put two of
    the nine panels out by eleven and eight units with no signal that anything
    had happened. Agreement among instruments that share a bias is not a check.
    """
    frame = pd.read_parquet(io.BytesIO(payload))
    frame = frame[frame["bucket"].isin(_SCORED_BUCKETS)]
    aspects = frame["go_id"].map(aspect_of_term)
    frame = frame.assign(aspect=aspects).dropna(subset=["aspect"])
    pairs = frame[["protein_accession", "bucket", "aspect"]].drop_duplicates()
    counted = pairs.groupby(["bucket", "aspect"]).size()
    return {
        (str(bucket).upper(), _ASPECT_OF[str(aspect)]): int(n)
        for (bucket, aspect), n in counted.items()
        if str(aspect) in _ASPECT_OF
    }


#: The paired standard deviation of the within-protein difference, for the
#: cheapest contrast class this project has measured: two arms sharing their
#: retrieval and differing in one downstream knob. Derived in
#: ABLATION-ARCHITECTURE.md from a fold study, and it is the LOW end. Two arms
#: that retrieve different neighbours are noisier, so a panel that cannot
#: resolve at this value cannot resolve at any.
_SIGMA_PAIRED = 0.081

#: (z at 95 per cent + z at 80 per cent power), the constant that turns a
#: population into the smallest difference it can detect.
_Z_SUM = 2.8016


def detectable_effect(units: int | None) -> float | None:
    """The smallest difference this panel could resolve, from its population.

    Pure arithmetic over a number the record already carries, which is why it
    can be shown for a panel nobody has scored: a population states what a panel
    could decide before anything is run, and a panel printed without it invites
    reading a gap of 0.002 in a panel that cannot see 0.008 as a result.

    Computed at the low end of the contrast classes, so it is a floor on the
    floor. Returns nothing when the population is unknown, since an effect
    derived from an absent population would be a number about nothing.
    """
    if not units or units <= 0:
        return None
    return round(_Z_SUM * _SIGMA_PAIRED / math.sqrt(units), 4)


def level_fields(rows: list[dict[str, Any]]) -> tuple[str, ...]:
    """Which fields name a level here, read off what varies across results."""
    varying = tuple(f for f in _LEVEL_FIELDS if len({repr(r.get(f)) for r in rows}) > 1)
    return varying or _LEVEL_FIELDS


def _level_name(row: dict[str, Any], fields: tuple[str, ...]) -> str:
    return " / ".join(str(row[f]) for f in fields if row.get(f) is not None)


def build_panels(
    rows: list[dict[str, Any]],
    units: Mapping[tuple[str, str], int] | None = None,
) -> list[dict[str, Any]]:
    """The nine panels, each with its population and its scored levels.

    Every panel is emitted whether or not it carries a result, because a panel
    nobody scored is a fact about the record, and dropping it would let a
    partial run read as a complete one.

    ``units`` is counted from the ground truth by the caller. A panel whose
    population could not be counted reports ``None``, which is a statement that
    the artefact was unreadable, and never a number standing in for one.
    """
    counted: Mapping[tuple[str, str], int] = units or {}
    fields = level_fields(rows)
    by_panel: dict[tuple[str, str], list[dict[str, Any]]] = {k: [] for k in PANEL_KEYS}
    for row in rows:
        key = (str(row.get("category")), str(row.get("aspect")))
        if key in by_panel:
            by_panel[key].append(row)
    return [
        {
            "category": category,
            "aspect": aspect,
            "units": counted.get((category, aspect)),
            "detectable_effect": detectable_effect(counted.get((category, aspect))),
            "results": [
                {
                    "level": _level_name(r, fields),
                    "f_micro_w": r.get("f_micro_w"),
                    "tau": r.get("tau"),
                }
                for r in sorted(scored, key=lambda r: r.get("f_micro_w") or 0.0, reverse=True)
            ],
        }
        for (category, aspect), scored in by_panel.items()
    ]


def separated_from_floor(rows: list[dict[str, Any]], floor: str) -> bool:
    """Whether some level clears the floor on every panel that carries both.

    Panels are never pooled, so a separation has to hold panel by panel. A panel
    holding only the floor, or only its rivals, cannot testify either way and is
    skipped; if no panel can testify, nothing separated.
    """
    fields = level_fields(rows)
    tested = 0
    for key in PANEL_KEYS:
        here = [r for r in rows if (str(r.get("category")), str(r.get("aspect"))) == key]
        at_floor = [r for r in here if _level_name(r, fields) == floor]
        rivals = [r for r in here if _level_name(r, fields) != floor]
        if not at_floor or not rivals:
            continue
        tested += 1
        if max(r.get("f_micro_w") or 0.0 for r in rivals) <= max(
            r.get("f_micro_w") or 0.0 for r in at_floor
        ):
            return False
    return tested > 0
