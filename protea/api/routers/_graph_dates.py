"""Where each rung of the graph's ladder was climbed, read off the rows that climbed it.

Apart from the node builders for the reason ``_graph_edges`` is: every one of
them depends on this and none of it depends on any of them. What lives here is
the reading of DATES, and the one property that makes the temporal axis
trustworthy is spelled out in each function rather than in a document beside
them: every date is keyed by the same function the builder counts its levels
with, so the axis and the count are two readings of one set of rows and not two
sets.

Nothing here decides WHICH rungs were climbed. That is
``_graph_edges.climbed``, derived from the same fields the strength is, and the
split is deliberate: a builder may not say where its node stands, only when the
rows that put it there arrived.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import Any

from protea.api.routers._graph_edges import dates

#: Sorts an undated row LAST. A row the record cannot date must not be able to
#: claim the earliest rung by sorting as the empty string, which is what every
#: ``or ""`` key in a date sort quietly does.
_UNDATED_LAST = "9999-99-99"


def _climbed_on(
    level_dates: Iterable[str | None],
    *,
    power: str | None = None,
    floor: str | None = None,
    separation: str | None = None,
) -> tuple[tuple[str, str], ...]:
    """The date each rung of the ladder was climbed, read off the rows that climbed it.

    ``level_dates`` are the dates of the rows behind ``instantiated``, in any
    order: the earliest is when this node got an artifact at all and the next is
    when it got a second one to compare against. Undated entries are DROPPED
    before the ordering rather than sorted to one end, so a level the record
    cannot date cannot take the first rung from one it can.

    Where fewer dates are known than levels are counted, the rungs are still
    reported and the ones this cannot place come out undated. That is the
    honest report and not a gap: the count is the count, and a date the record
    does not hold is not one this surface may supply from the nearest row.
    """
    known = sorted(d for d in level_dates if d)
    return dates(
        first_level=known[0] if known else None,
        contrast=known[1] if len(known) > 1 else None,
        power=power,
        floor=floor,
        separation=separation,
    )


def _nth(level_dates: Iterable[str | None], n: int) -> str | None:
    """The ``n``-th earliest date among the ones the record holds.

    Undated entries are dropped, so this answers over what is KNOWN and returns
    ``None`` where fewer than ``n`` dates are known. It is the same reading
    :func:`_climbed_on` takes of its own argument, named once so a node whose
    levels and scored levels are the same rows can say so by passing the same
    number twice instead of by a comment claiming it.
    """
    known = sorted(d for d in level_dates if d)
    return known[n - 1] if len(known) >= n else None


def _level_dates(
    rows: list[dict[str, Any]], level_of: Callable[[dict[str, Any]], Any]
) -> list[str | None]:
    """The earliest date on each distinct level, from the rows that instantiate it.

    A level is held by as many rows as happened to instantiate it, and the day it
    came into existence is the EARLIEST of those rather than any one of them.

    ``level_of`` is the same function the builder counts its levels with, passed
    in rather than reimplemented, so the returned list carries exactly one entry
    per counted level by construction. That is the property that keeps the axis
    from drifting from the count: not a test, an identity.
    """
    first: dict[Any, str | None] = {}
    for row in sorted(rows, key=lambda r: r.get("created_on") or _UNDATED_LAST):
        key = level_of(row)
        if key not in first:
            first[key] = row.get("created_on")
    return list(first.values())


def _second_level_scored(
    results: list[dict[str, Any]], level_of: Callable[[dict[str, Any]], Any]
) -> str | None:
    """The date a SECOND distinct level first carried a number.

    Walked in date order and not indexed, because ``scored`` counts DISTINCT
    levels: the day it reached two is the day the second distinct one appeared,
    which is not the second result and not the second result's level either when
    the first level produced several of them. Taking ``results[1]`` instead would
    date this rung by whichever row happened to land second, and on this record
    one retrieval setting produced three prediction sets.

    ``level_of`` maps a RESULT to the level it was read under, and it takes the
    whole row rather than a field name for the reason the levels themselves are
    keyed by a function: on two of these nodes a level is several fields, and
    naming it by one of them counts two rows of one level as two levels. That is
    the defect this codebase keeps finding, and an axis is a worse place to hide
    it than a count, because a date looks like a fact about one day.
    """
    seen: list[Any] = []
    for row in sorted(results, key=lambda r: r.get("created_on") or _UNDATED_LAST):
        level = level_of(row)
        if level is None or level in seen:
            continue
        seen.append(level)
        if len(seen) == 2:
            return row.get("created_on")
    return None


def _levels_scored(results: list[dict[str, Any]], level_of: Callable[[dict[str, Any]], Any]) -> int:
    """How many DISTINCT levels carried a number.

    Replaces counting one identifying field of them. ``bank`` counted distinct
    ``annotation_set_id`` while instantiating a level as (corpus, donor policy),
    and ``retriever`` counted distinct ``prediction_set_id`` while instantiating
    one from eight fields: both could report the power to compare two levels
    with one level scored several times. Keyed by the same function that counts
    the levels, so the two numbers are about one thing.
    """
    return len({level_of(r) for r in results} - {None})


def _level_by_set(
    sets: list[dict[str, Any]], level_of: Callable[[dict[str, Any]], Any]
) -> Callable[[dict[str, Any]], Any]:
    """A result's level, by way of the prediction set that produced it.

    A result names its prediction set and nothing else about the retrieval or
    the bank, so the level has to be looked up. A result whose prediction set is
    not in ``sets`` maps to ``None`` and is counted as no level at all rather
    than as one of its own.
    """
    by_id = {p["id"]: level_of(p) for p in sets}
    return lambda row: by_id.get(row.get("prediction_set_id"))


def _read_on(results: list[dict[str, Any]]) -> str | None:
    """The earliest date a declared comparison could have been read.

    The last of its inputs. A separation is read off the panels, and the panels
    are built from every result, so the comparison cannot predate the newest one
    it was read over. It is a BOUND and not a timestamp, which is why nothing
    here claims it is the date the reading was taken: the record holds no row
    for the reading itself.
    """
    known = sorted(r["created_on"] for r in results if r.get("created_on"))
    return known[-1] if known else None


_RETRIEVER_FIELDS: tuple[str, ...] = (
    "depth",
    "distance_threshold",
    "metric",
    "search_backend",
    "aspect_separated",
    "expand_to_ancestors",
    # Who may be a neighbour, which is this node and not the bank: the donor
    # policy decides which annotations may donate, this decides who is retrieved
    # at all. Left out, two runs differing only in whether a protein could
    # retrieve itself rendered as one level.
    "exclude_self_neighbour",
)


def _policy_is_empty(row: dict[str, Any]) -> bool:
    """Whether the donor policy in force is the one a field holds unset.

    No reviewed-only restriction, no evidence filter, no excluded reference
    prefixes. That is not a permissive policy somebody chose, it is the absence
    of a policy, and the difference is the whole of this node's verdict.
    """
    return (
        (row.get("donor_reviewed_only") or "false") == "false"
        and row.get("donor_evidence_codes") is None
        and (row.get("donor_exclusions") or "[]") == "[]"
    )


def _bank_level(row: dict[str, Any]) -> tuple[str, bool]:
    """What makes one bank a different level from another: the corpus and the policy.

    Named rather than written inline because the builder counts levels with it
    AND dates them with it, and two spellings of one key is how a count and an
    axis come to be about different things.
    """
    return (row["annotation_set_id"], _policy_is_empty(row))


def _retriever_level(row: dict[str, Any]) -> tuple[str, ...]:
    """What makes one retrieval setting a different level, field by field.

    ``repr`` and not ``str`` for the reason the rest of this module uses it: it
    keeps ``None`` apart from the string "None", and a depth that was never
    recorded is not a depth of that name.
    """
    return tuple(repr(row.get(f)) for f in _RETRIEVER_FIELDS)


# ── Where each node's dates come from, one named answer per node ──────────────
#
# One function per builder rather than five lines inside each one, and they live
# here rather than there because the question they answer is this module's: WHICH
# ROWS dated this node. A builder asking for its dates in one line cannot key
# them by anything but the function that counts its levels, which is the property
# the whole axis rests on.
#
# Each returns the keyword arguments ``_floor_for`` takes, splatted at the call
# site, so a builder cannot fetch the levels without the power date either.


def frame_dates(used: list[dict[str, Any]]) -> dict[str, Any]:
    """The frame's dates: its levels, and no power date.

    NO POWER DATE, and the reason is a fact about this node that looked like a
    coincidence. ``scored`` on the frame is gated on THIS window's results while
    ``first_used_on`` on an accretion row is the first result ANYWHERE that the
    table weighted, so the two are about different sets of rows. On the live
    record two tables carry a date and the newest window carries no result at
    all, which is a node with a contrast and no power: dating the rung from the
    levels put a date on a rung this node has not climbed, and the guard in
    ``crossings_of`` refused it. The rung is reported undated when it is
    climbed, which is the honest report -- no row says when.
    """
    return {"levels": [a["first_used_on"] for a in used]}


def substrate_dates(used: list[dict[str, Any]], results: list[dict[str, Any]]) -> dict[str, Any]:
    """The substrate's dates. Its level IS the embedding configuration.

    The only one of the five whose level is a single field, so the power date is
    keyed on that field directly rather than through the prediction set.
    """
    return {
        "levels": [r["first_used_on"] for r in used],
        "power": _second_level_scored(results, lambda r: r.get("embedding_config_id")),
    }


def bank_dates(sets: list[dict[str, Any]], results: list[dict[str, Any]]) -> dict[str, Any]:
    """The bank's dates, keyed by (corpus, donor policy)."""
    return {
        "levels": _level_dates(sets, _bank_level),
        "power": _second_level_scored(results, _level_by_set(sets, _bank_level)),
    }


def retriever_dates(sets: list[dict[str, Any]], results: list[dict[str, Any]]) -> dict[str, Any]:
    """The retriever's dates, keyed by all eight fields of a setting."""
    return {
        "levels": _level_dates(sets, _retriever_level),
        "power": _second_level_scored(results, _level_by_set(sets, _retriever_level)),
    }


def scoring_dated(
    used: list[dict[str, Any]], declared: Any, read_over: list[dict[str, Any]]
) -> tuple[tuple[str, str], ...]:
    """Scoring's dates, assembled here because that node reads its own floor.

    ``_scoring_node`` cannot go through ``_floor_for``: it needs the verdict and
    the refusal as named locals for its crossed-ladder prose. So it gets the
    whole ``dated`` tuple instead, from the same three sources as the others --
    its own rows, the declaring run, and the last input a reading could have
    been taken over.

    A scoring configuration counts as a level only once it has SCORED something
    (``used`` filters on ``results > 0``), and ``first_used_on`` is the first
    result it scored. So on this node the level and the scored level genuinely
    ARE one row, which is the claim that turned out to be false on the frame.

    ``read_over`` is empty when nothing separated, so no separation date is
    offered for a comparison that did not clear its floor.
    """
    on = [c["first_used_on"] for c in used]
    return _climbed_on(
        on,
        power=_nth(on, 2),
        floor=declared.declared_on,
        separation=_read_on(read_over),
    )


def comparison_dated(
    declared: Any,
    levels: Iterable[str | None],
    power: str | None,
    read_over: list[dict[str, Any]],
) -> tuple[tuple[str, str], ...]:
    """Every rung's date for a node that reads its floor through ``_floor_for``.

    The builder's own two or three dates, plus the two the comparison owns: the
    day the declaring run was created, and the bound on when a reading could
    have been taken. ``read_over`` is empty unless something separated, so no
    separation date is offered for a comparison that did not clear its floor.
    """
    return _climbed_on(
        levels, power=power, floor=declared.declared_on, separation=_read_on(read_over)
    )
