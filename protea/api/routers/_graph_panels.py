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
#: Every field a level can differ in. A level is named by whichever of these
#: actually moved, so a record varying one thing reads as one word.
#:
#: The list has to be complete or the naming lies. When the donor policy was
#: missing, two arms differing only in it rendered under one name: a panel's
#: head became ambiguous, and the spread silently absorbed the bank effect into
#: a number a reader takes for scoring variation. A level named by fewer fields
#: than it varies in is the same defect as an axis compared on two.
_LEVEL_FIELDS: tuple[str, ...] = (
    "scoring_name",
    "embedding_name",
    "depth",
    "donor_policy",
    # A stored donor policy is byte-identical either side of the 2026-08-29
    # change that moved its evidence codes from gating pool admission to gating
    # donation, so the policy alone names two incompatible experiments with one
    # string. The revision separates them, and separates the next change of the
    # same kind without anyone having to see it coming.
    "self_exclusion",
    "features",
    "code_revision",
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
#: retrieval and differing in one downstream knob.
#:
#: MEASURED ON THIS CAMPAIGN, 2026-08-30. Two evaluations of prediction set
#: 9995651a at sequence depth 30, identical in everything but the scoring
#: configuration (composite against embedding_plus_alignment), compared per
#: protein at each arm's own best threshold:
#:
#:     NK.BPO  n=1509  0.1816      LK.BPO  n=1214  0.2094
#:     NK.CCO  n=1116  0.2581      LK.CCO  n= 821  0.2528
#:     NK.MFO  n=1129  0.3746      LK.MFO  n= 943  0.4051
#:     PK.BPO  n=5810  0.1157      PK.CCO  n=3201  0.2248
#:     PK.MFO  n=3292  0.2565      median  0.2528
#:
#: THE VALUE WAS 0.081, WHICH IS BELOW ALL NINE. It was attributed to a fold
#: study in ABLATION-ARCHITECTURE.md, a file on no trunk, and it made
#: detectable_effect report a floor about three times finer than this record
#: supports: 0.0017 against 0.0054 at a population of 17,438. The panel was
#: calling differences resolvable that sit inside the spread of the contrast
#: class it names.
#:
#: THIS IS NOT MEASUREMENT NOISE, and the distinction matters for anyone
#: tempted to shrink it again. Evaluation here is deterministic, 117 of 117
#: metrics reproduce exactly, so re-running an arm gives a difference of
#: exactly zero. What this measures is the real spread of the paired
#: difference between two arms of one contrast class, which is the quantity a
#: detectable effect has to clear.
#:
#: The minimum is taken rather than the median, keeping the original intent:
#: it is the LOW end, so a panel that cannot resolve at this value cannot
#: resolve at any. Two arms that retrieve different neighbours are noisier
#: still, which is what _SIGMA_ROUTING below is for.
_SIGMA_PAIRED = 0.1157

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


#: The paired standard deviation of the contrast a ROUTING decision makes: two
#: arms that retrieve different neighbours, rather than two arms that share a
#: retrieval and differ in one downstream knob.
#:
#: MEASURED 2026-08-30, and the measurement says this class is not one number.
#: Five pairs from the depth series on 9995651a, same scoring, same evaluation,
#: differing only in retrieval depth, compared per protein across nine panels:
#:
#:     arms      min      median     max
#:     30 v 20   0.0638   0.1215    0.1836
#:     20 v 10   0.0770   0.1510    0.2385
#:     10 v  5   0.1029   0.1810    0.2619
#:      5 v  2   0.1357   0.2304    0.3141
#:     30 v  2   0.1521   0.2726    0.3983
#:
#: Both the minimum and the median rise monotonically with how far apart the
#: two retrievals are, so a single constant models something that varies by a
#: factor of six across the pairs this campaign actually contains. Part of that
#: rise is real effect rather than spread: depth is monotone, so arms further
#: apart genuinely differ more, and the wide pairs measure signal as much as
#: scatter. The narrow pairs are the ones that behave like a floor.
#:
#: THE VALUE IS KEPT AT 0.13. It sits inside the measured range and within nine
#: per cent of the adjacent-arm median of 0.1215, which is the conservative use
#: of it. Its stated derivation, reproducing a population floor of 332 declared
#: in SURVIVOR-CASCADE.md, is still unverifiable: that file is on no trunk and
#: lives only in agent-farm PR #257.
#:
#: A PRIOR HELD HERE WAS WRONG AND IS RECORDED SO IT IS NOT REPEATED. Retrieval
#: was assumed to be uniformly noisier than configuration, which would have put
#: this well above _SIGMA_PAIRED's 0.1157. Adjacent retrieval arms are QUIETER
#: than two scoring configurations, not louder. Changing one knob downstream
#: can move more per protein than shortening the neighbour list by ten.
_SIGMA_ROUTING = 0.13

#: The difference a floor is asked to resolve, in weighted micro F. Two points
#: is the smallest gap this project has ever been willing to act on: the median
#: gap between adjacent scoring presets on the previous campaign was 0.0030,
#: and a floor set there would demand populations no panel in this record has.
_TARGET_EFFECT = 0.02


def population_floor(sigma: float, target: float = _TARGET_EFFECT) -> int:
    """Smallest population that can resolve ``target`` at 95 per cent and 80.

    The inverse of :func:`detectable_effect`, and deliberately not rounded
    down: a floor that a population merely approaches is not a floor.
    """
    return math.ceil((_Z_SUM * sigma / target) ** 2)


def contrast_floors() -> dict[str, Any]:
    """The population floors, one per contrast class, and what fixes them.

    Published rather than left implicit because a surface that marks a cell as
    thin has to be able to say against WHAT, and a floor invented on the client
    is a floor nobody can audit. Both classes travel together: the same cell is
    usually reportable and unroutable at once, and a page that showed only one
    of the two numbers would read as if there were only one question.

    Ordered by population, cheapest first, so a reader meets the permissive
    floor before the strict one.
    """
    classes = [
        {
            "key": "reporting",
            "sigma_paired": _SIGMA_PAIRED,
            "population": population_floor(_SIGMA_PAIRED),
            "contrast": ("two arms that share their retrieval and differ in one downstream knob"),
        },
        {
            "key": "routing",
            "sigma_paired": _SIGMA_ROUTING,
            "population": population_floor(_SIGMA_ROUTING),
            "contrast": (
                "two arms that retrieve different neighbours, which is the "
                "contrast a routing decision makes"
            ),
        },
    ]
    return {
        "target_effect": _TARGET_EFFECT,
        "z_sum": _Z_SUM,
        "classes": sorted(classes, key=lambda c: c["population"]),
    }


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
