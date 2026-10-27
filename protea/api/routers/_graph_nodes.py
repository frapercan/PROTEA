"""One builder per node of the graph, in pipeline order.\n\nEach answers the same question about a different field: what does the record\nhold here, how many levels could it have held, and how firmly is the value\nstanding. They are together because they share that shape and apart from the\nassembly because the assembly should read as a list of nodes and nothing else."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from datetime import date
from typing import Any

from protea.api.routers._graph_edges import (
    REFUSED,
    UNDECLARED,
    Built,
    DeclaredFloor,
    Edge,
    _node,
    dates,
    held_values,
    split_fields,
)
from protea.api.routers._graph_panels import (
    CrossedDepthAxes,
    CrossedFrames,
    reading_against_floor,
    separated_from_floor,
)

#
# One per node. Each returns the node, the artifact it cannot produce, and the
# precondition for producing it; the last two are read only when the node comes
# out blocked. Every count in them is derived from the rows, never asserted.


def _window_span(head: dict[str, Any]) -> dict[str, Any] | None:
    """When the window opened, when it closed, and how long it ran.

    Returns nothing when either publication date is missing rather than
    computing a span from one end, because a window with one date is not a
    shorter window, it is an undated one.
    """
    start, end = head.get("window_from_date"), head.get("window_to_date")
    if not start or not end:
        return None
    days = (date.fromisoformat(end) - date.fromisoformat(start)).days
    return {"from": start, "to": end, "days": days, "months": round(days / 30.44, 1)}


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


def _floor_for(
    key: str,
    record: dict[str, list[dict[str, Any]]],
    floors: dict[str, DeclaredFloor],
    levels: Iterable[str | None] = (),
    power: str | None = None,
) -> dict[str, Any]:
    """The floor declared for this node, whether it was cleared, and why it was not asked.

    Returned as the keyword arguments an :class:`Edge` takes, so a builder spends
    one line on the whole comparison. Three named locals unpacked into three
    keyword lines is four lines per builder, which put ``_frame_node`` and
    ``_bank_node`` one line over the 60-LOC smell budget the moment the refusal
    joined the pair -- the budget this helper existed to protect.

    Handing back one object is the pairing argument rather than a dodge of it: a
    floor, the verdict on it and the account of there being no verdict are never
    read apart, and a builder that fetched one without the others would be
    exactly the half-wiring this helper was written to end. Splatted, it cannot
    fetch one without the others at all. :func:`_scoring_node` still reads them
    itself because it also needs the crossed-ladder reason in its own prose,
    which is why what reaches the payload is pinned node by node and not once
    over this helper.

    The refusal used to be dropped right here. :func:`_separation` catches
    CrossedFrames and CrossedDepthAxes so the page still serves, and this helper
    threw the caught text away, so for nine of the ten nodes a comparison whose
    floor REFUSED came out of ``strength_of`` as a bare ``chosen`` -- the same
    word as a comparison that ran with power and lost. No reader could tell the
    two apart and nothing in the response said a refusal had happened at all.

    THE DATES RIDE ALONG for the same pairing argument. Two of the five rungs of
    the ladder are this comparison -- the floor was declared on a day and the
    reading could not predate its last input -- and the other three are the
    builder's own rows, which is why ``levels`` and ``power`` are passed in
    rather than found here. Assembled in one place because an axis built half
    here and half in ten builders is ten chances for a node to report a rung it
    cannot date as one it never climbed.

    The READING arrives already answered, in ``floors``, rather than being
    computed here beside the verdict. It is the one half of the comparison this
    helper cannot work out from what it is handed: deciding whether the panels
    had the power to resolve the declared effect needs their populations, and
    those are counted from the window's own ground truth by the endpoint. See
    :func:`declared_floors`, which is where they are.
    """
    declared = floors.get(key) or UNDECLARED
    separated, refusal = _separation(record["panels"], declared.floor)
    return {
        "floor": declared.floor,
        "separated": separated,
        "refusal": refusal,
        "reading": declared.reading,
        "dated": _climbed_on(
            levels,
            power=power,
            floor=declared.declared_on,
            separation=_read_on(record["results"]) if separated else None,
        ),
    }


def _frame_node(
    record: dict[str, list[dict[str, Any]]],
    head: dict[str, Any] | None,
    floors: dict[str, DeclaredFloor],
) -> Built:
    """The frame's own decision: which accretion regime weights the terms.

    Eligibility is the whole of the argument here. An accretion table computed
    over a different ontology snapshot is not an alternative that lost, it is
    not an alternative: it weights terms this frame does not contain. So the
    available levels are the tables built on the declared pivot, and when
    exactly one of them exists and it is the one in use, the frame fixed the
    regime rather than leaving it open. That is a ``chosen``. Should a second
    eligible table ever be built, this node drops to ``inherited`` on the same
    evidence, which is correct: an alternative now exists and nobody weighed it.
    """
    pivot = head["pivot_snapshot_id"] if head else None
    eligible = [a for a in record["accretion"] if pivot and a["ontology_snapshot_id"] == pivot]
    used = [a for a in record["accretion"] if a["in_use"]]
    pool = eligible or record["accretion"]
    results = [r for r in record["results"] if head and r["evaluation_set_id"] == head["id"]]
    used_dates = [a["first_used_on"] for a in used]
    edge = Edge(
        produced=head is not None,
        instantiated=len(used),
        available=len(pool),
        scored=len(used) if results else 0,
        results=len(results),
        forced=len(eligible) == 1 and bool(used) and used[0]["id"] == eligible[0]["id"],
        # NO POWER DATE, and the reason is a fact about this node that looked
        # like a coincidence. ``scored`` here is gated on THIS window's results
        # while ``first_used_on`` on an accretion row is the first result
        # anywhere that the table weighted, so the two are about different sets
        # of rows: on the live record two tables carry a date and the newest
        # window carries no result at all, which is a node that has a contrast
        # and no power. Dating the rung from the levels would have put a date on
        # a rung this node has not climbed. The rung is reported undated when it
        # is climbed, which is the honest report: no row says when.
        **_floor_for("frame", record, floors, levels=used_dates),
    )
    if head is None:
        reason = "No evaluation set exists, so there is no window for a number to be read in."
    elif not used:
        reason = (
            f"{len(record['accretion'])} accretion tables exist and no producing job names one, "
            "so the weighting behind the published numbers is not recoverable from the record."
        )
    elif edge.forced:
        reason = (
            "One accretion table is built on the declared pivot and it is the one every "
            "published result was weighted by, so the frame fixes the regime rather than "
            "leaving it open."
        )
    else:
        reason = (
            f"{len(pool)} accretion tables are eligible on the declared pivot and "
            f"{len(used)} is in use. Nothing in the record says the others were weighed."
        )
    fields = ("window_from", "window_to", "window_role", "mode", "pivot_version")
    return (
        _node(
            "frame",
            edge,
            reason,
            split_fields(record["evaluation_sets"], fields),
            held_values(record["evaluation_sets"], fields),
        ),
        "a frame to read numbers in",
        "an evaluation set with a pivot snapshot and an accretion table",
    )


_SUBSTRATE_FIELDS: tuple[str, ...] = (
    "model_name",
    "model_backend",
    "layer_indices",
    "layer_agg",
    "pooling",
    "normalize",
    "normalize_residues",
    "max_length",
    "use_chunking",
    "chunk_size",
    "chunk_overlap",
)


def _substrate_node(
    record: dict[str, list[dict[str, Any]]], floors: dict[str, DeclaredFloor]
) -> Built:
    """Which representation the neighbourhood is computed in.

    Available counts the configurations that hold stored embeddings, not the
    configurations that have rows. A registered configuration nobody ever
    embedded is not an untried alternative, it is an unbuilt one, and counting
    it would overstate what was passed over.
    """
    rows = record["substrates"]
    used = [r for r in rows if r["in_use"]]
    producible = [r for r in rows if r["producible"]]
    used_ids = {r["id"] for r in used}
    results = [r for r in record["results"] if r["embedding_config_id"] in used_ids]
    edge = Edge(
        produced=bool(producible),
        instantiated=len(used),
        available=len(producible),
        scored=len({r["embedding_config_id"] for r in results}),
        results=len(results),
        # Never forced. Nothing in the frame names a representation: the window,
        # the pivot and the accretion regime are all silent about it, so a
        # single instantiated level here is a value nobody chose rather than one
        # the frame fixed.
        forced=False,
        **_floor_for(
            "substrate",
            record,
            floors,
            levels=[r["first_used_on"] for r in used],
            power=_second_level_scored(results, lambda r: r.get("embedding_config_id")),
        ),
    )
    if not used:
        reason = "No prediction set names a representation, so nothing has been retrieved against."
    elif len(producible) <= 1:
        reason = (
            "Only one representation holds stored embeddings, so there was never an alternative "
            "to weigh and nothing decided this one."
        )
    else:
        names = ", ".join(sorted(r["label"] for r in used))
        reason = (
            f"{len(producible)} representations hold stored embeddings and {len(used)} has ever "
            f"been retrieved against ({names}). The rest are built and were never tried, and "
            "nothing in the frame selects the one that was."
        )
    return (
        _node(
            "substrate",
            edge,
            reason,
            split_fields(used, _SUBSTRATE_FIELDS),
            held_values(used, _SUBSTRATE_FIELDS),
        ),
        "a representation to retrieve in",
        "an embedding configuration with stored embeddings",
    )


def _bank_level(row: dict[str, Any]) -> tuple[str, bool]:
    """What makes one bank a different level from another: the corpus and the policy.

    Named rather than written inline because the builder counts levels with it
    AND dates them with it, and two spellings of one key is how a count and an
    axis come to be about different things.
    """
    return (row["annotation_set_id"], _policy_is_empty(row))


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


_BANK_FIELDS: tuple[str, ...] = (
    "bank_source",
    "bank_version",
    "donor_reviewed_only",
    "donor_evidence_codes",
    "donor_exclusions",
)


def _bank_node(
    record: dict[str, list[dict[str, Any]]],
    head: dict[str, Any] | None,
    floors: dict[str, DeclaredFloor],
) -> Built:
    """Which corpus the donors come from, under which donor policy.

    The two are one node because they cannot be decided apart: a corpus and the
    filter applied to it define one bank between them. The corpus is fixed by
    the frame, since the window's lower endpoint is the only annotation set a
    donor may come from without reading the future. The policy is not fixed by
    anything, and when it is the empty one the node as a whole is inherited: a
    group of inseparable fields is held no more firmly than its loosest member.
    """
    sets = record["prediction_sets"]
    lower = head["bank_annotation_set_id"] if head else None
    eligible = [b for b in record["banks"] if lower and b["id"] == lower]
    levels = {_bank_level(p) for p in sets}
    bank_of = _level_by_set(sets, _bank_level)
    used_ids = {p["annotation_set_id"] for p in sets}
    pinned = bool(eligible) and used_ids <= {b["id"] for b in eligible}
    policy_set = bool(sets) and not all(_policy_is_empty(p) for p in sets)
    edge = Edge(
        produced=bool(record["banks"]),
        instantiated=len(levels),
        available=len(eligible or record["banks"]),
        scored=_levels_scored(record["results"], bank_of),
        results=len(record["results"]),
        forced=pinned and policy_set,
        **_floor_for(
            "bank",
            record,
            floors,
            levels=_level_dates(sets, _bank_level),
            power=_second_level_scored(record["results"], bank_of),
        ),
    )
    if not sets:
        reason = "No prediction set names a corpus, so no bank has been drawn from."
    elif pinned and not policy_set:
        reason = (
            "The corpus is the window's lower endpoint, which the frame fixes. The donor policy "
            "in force is the empty one: no reviewed-only restriction, no evidence filter, no "
            "excluded reference prefixes. That is the value the field holds when nobody sets it, "
            "and corpus and policy cannot be decided apart."
        )
    elif pinned:
        reason = (
            "The corpus is the window's lower endpoint, which the frame fixes, and the donor "
            "policy in force was set rather than left empty."
        )
    else:
        reason = (
            f"{len(used_ids)} of {len(record['banks'])} registered corpora have been drawn from, "
            "and none of them is the window's lower endpoint that the frame would fix."
        )
    return (
        _node(
            "bank",
            edge,
            reason,
            split_fields(sets, _BANK_FIELDS),
            held_values(sets, _BANK_FIELDS),
        ),
        "a corpus to draw donors from",
        "an annotation set at the window's lower endpoint",
    )


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


def _donor_required(record: dict[str, list[dict[str, Any]]]) -> bool:
    """Whether the candidate table can hold a term that arrived without a donor.

    Read from the catalog rather than from a row count. A NOT NULL column says
    the shape of the record forbids the other mechanism; an all-donor sample
    says only that nobody ran it.

    It is also the only affordable way to ask. Counting the donorless rows means
    ``WHERE ref_protein_accession IS NULL``, which the planner does not fold
    against the constraint: it plans a parallel sequential scan of the largest
    table (47M planner units, measured 2026-09-14) for a zero the column states.
    """
    return any(row.get("is_nullable") == "NO" for row in record["donor_column"])


#: Why three nodes have nowhere to write their artifact while every candidate
#: must name a donor. Written once: they argue it off one catalog row, and a
#: second copy is the one that goes stale.
_NO_DONORLESS_ROW = (
    "The candidate column naming the donor is NOT NULL, so a term that arrived without one has "
    "nowhere to be written, and neither has an answer carrying two flows. That is the shape of "
    "the record rather than its contents: no run moves it, only a migration."
)

#: What a node the record cannot express is waiting for. A column and never a
#: table: a reader sent to load one would be producing rows that could not show
#: up here, which is the errand this word exists to stop.
_NULLABLE_DONOR = "a candidate column that can hold a row with no donor"


def _retriever_level(row: dict[str, Any]) -> tuple[str, ...]:
    """What makes one retrieval setting a different level, field by field.

    ``repr`` and not ``str`` for the reason the rest of this module uses it: it
    keeps ``None`` apart from the string "None", and a depth that was never
    recorded is not a depth of that name.
    """
    return tuple(repr(row.get(f)) for f in _RETRIEVER_FIELDS)


def _retriever_node(
    record: dict[str, list[dict[str, Any]]], floors: dict[str, DeclaredFloor]
) -> Built:
    """How candidates are drawn from the bank, and how deep.

    There is no catalogue of retrievers to choose from. Unlike a representation
    or a weighting, a retriever setting has no registry table: a level comes
    into existence only when a prediction set instantiates one, so available and
    instantiated are the same number by construction and this node cannot report
    an alternative it passed over. That, plus a candidate schema that requires a
    donor on every row, is why it is inherited and not chosen: nothing decided
    the setting, and nothing could have decided it against.
    """
    sets = record["prediction_sets"]
    levels = {_retriever_level(p) for p in sets}
    setting_of = _level_by_set(sets, _retriever_level)
    candidates = sum(int(c["candidates"]) for c in record["candidates"])
    depths = sorted({p["depth"] for p in sets if p["depth"]})
    edge = Edge(
        produced=bool(sets),
        instantiated=len(levels),
        available=len(levels),
        scored=_levels_scored(record["results"], setting_of),
        results=len(record["results"]),
        forced=False,
        **_floor_for(
            "retriever",
            record,
            floors,
            levels=_level_dates(sets, _retriever_level),
            power=_second_level_scored(record["results"], setting_of),
        ),
    )
    if not sets:
        reason = "No prediction set exists, so nothing has been retrieved."
    else:
        donor = (
            "Every candidate the record can hold carries a donor accession, because the column "
            "is NOT NULL: a term that arrived without one cannot be stored here at all. "
            if _donor_required(record)
            else "The candidate column that names a donor is nullable, so a term without one "
            "could be stored and none has been. "
        )
        setting = "setting" if len(levels) == 1 else "settings"
        reason = (
            f"{donor}{candidates} candidates sit under {len(levels)} retrieval {setting} at "
            f"depth {', '.join(depths) or 'unrecorded'}. No registry of retrievers exists, so "
            "there was never a level to choose against: the setting is the one it has always "
            "been."
        )
    return (
        _node(
            "retriever",
            edge,
            reason,
            split_fields(sets, _RETRIEVER_FIELDS),
            held_values(sets, _RETRIEVER_FIELDS),
        ),
        "a way of drawing candidates from the bank",
        "a prediction set",
    )


def _artifacts(record: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    return record["artifacts"][0] if record["artifacts"] else {}


def _generator_node(
    record: dict[str, list[dict[str, Any]]], floors: dict[str, DeclaredFloor]
) -> Built:
    """Whether any candidate arrives without a donor.

    A generator is the second half of a second flow: a source that proposes a
    term from the query alone, with no neighbour to inherit it from. Two things
    decide this node and both are read rather than assumed: whether the
    candidate column that names the donor still forbids a donorless row, and
    whether the tables a domain-based generator would draw on hold anything.

    THE WORD IS DISTINGUISHED HERE AND THE COUNT IS NOT READ, because those two
    facts are not the same kind. While the donor column is NOT NULL the artifact
    has nowhere to be written, so filling interpro_annotation moves this node's
    sentence and not its word; with the column nullable the block is an ordinary
    one and those rows end it. The donorless candidates are counted in neither
    case, for the reason :func:`_donor_required` gives.
    """
    art = _artifacts(record)
    interpro = int(art.get("interpro_annotation") or 0)
    mappings = int(art.get("interpro_go_mapping") or 0)
    models = int(art.get("reranker_model") or 0)
    expressible = not _donor_required(record)
    # One second source is designed for: InterPro domains carried onto GO terms
    # through the mapping table. Both halves, because a domain table with
    # nothing to map through proposes nothing; read and not written in, so the
    # denominator moves the moment either table is loaded.
    available = int(bool(interpro and mappings))
    edge = Edge(
        produced=available > 0,
        instantiated=0,
        available=available,
        expressible=expressible,
        **_floor_for("generator", record, floors),
    )
    counts = (
        f"interpro_annotation holds {interpro} rows, interpro_go_mapping {mappings}, "
        f"reranker_model {models}."
    )
    reason = (
        "No candidate arrives without a donor. The candidate column naming the donor is "
        f"nullable, so one could be stored, and the artifacts a second source would draw on "
        f"are absent. {counts}"
        if expressible
        else f"No candidate CAN arrive without a donor. {_NO_DONORLESS_ROW} The artifacts a "
        f"second source would draw on would not move it if every one of them were full. {counts}"
    )
    return (
        _node("generator", edge, reason, ([], [])),
        "a candidate that owes nothing to a donor",
        "rows in interpro_annotation and interpro_go_mapping"
        if expressible
        else f"{_NULLABLE_DONOR}, and then rows in interpro_annotation and interpro_go_mapping",
    )


_SCORING_FIELDS: tuple[str, ...] = ("formula", "weights", "evidence_weights", "params")


def _separation(panels: list[dict[str, Any]], floor: str | None) -> tuple[bool | None, str | None]:
    """Whether the declared floor was cleared, or why it could not be asked.

    A refusal is caught rather than allowed to escape, and it is caught on this
    side of the node because this is the only place that can show it. Letting it
    reach the endpoint would turn a comparison nobody should have declared into
    a 500 on a page that is otherwise entirely readable, and the reader would
    learn nothing about why. Returned as a reason instead, with the separation
    at None, which already means the record established nothing.
    """
    if not floor or not panels:
        return None, None
    try:
        return separated_from_floor(panels, floor), None
    except (CrossedDepthAxes, CrossedFrames) as refusal:
        return None, str(refusal)


def declared_floors(
    rows: list[dict[str, Any]],
    panels: list[dict[str, Any]],
    units: Mapping[tuple[str, str], int] | None = None,
) -> dict[str, DeclaredFloor]:
    """Every floor the record declares, read once, where the populations are.

    Read here rather than inside the builders because the power half of the
    reading needs the panel populations, and those are counted from the window's
    ground truth by the caller. Handing them to ten builders instead would be one
    more argument a future node can be written without, and a node that cannot
    see a declared floor is how nine of these ten published ``chosen`` with one
    declared for them until 2026-09-02.

    A row declares a comparison only when it names both the node and the level:
    an ``experiment_run`` that names one without the other says which question it
    meant to ask and never asks it, and answering it anyway would invent the half
    it left out.

    A refusal is caught here for the same reason :func:`_separation` catches it
    one layer down: letting it escape turns a comparison nobody should have
    declared into a 500 on a page that is otherwise entirely readable. What is
    kept here is only the WORD, ``refused``, because the TEXT of the refusal is
    already published per node by :func:`_floor_for`, and a comparison that was
    refused must not fall through to a word that says it was read.
    """
    out: dict[str, DeclaredFloor] = {}
    for row in (r for r in rows if r["node"] and r["floor"]):
        key, floor = row["node"], row["floor"]
        try:
            reading = reading_against_floor(panels, floor, units)
        except (CrossedDepthAxes, CrossedFrames):
            reading = REFUSED
        out[key] = DeclaredFloor(floor=floor, reading=reading, declared_on=row.get("declared_on"))
    return out


def _scoring_reason(
    configs: list[dict[str, Any]],
    used: list[dict[str, Any]],
    floor: str | None,
    separated: bool | None,
    crossed: str | None,
) -> str:
    """Why the scoring node stands where it does, in one sentence.

    The order of the branches is the order the questions arise in: whether
    anything scored, whether more than one thing did, whether a floor was named,
    whether the floor could be asked at all, and only then what it answered.
    """
    if not used:
        return f"{len(configs)} weightings are registered and none of them has a surviving result."
    if len(used) == 1:
        return (
            f"One of {len(configs)} registered weightings carries a result. With nothing to "
            "contrast it against, its numbers are a reading and not a separation."
        )
    if floor is None:
        return (
            f"{len(used)} weightings scored the same candidates in the same frame, so the "
            "contrast is real and its spread is in the panels below. No floor is declared for "
            "it anywhere in the record, so the spread cannot be called a separation."
        )
    if crossed is not None:
        return crossed
    verdict = "clears" if separated else "does not clear"
    return (
        f"{len(used)} weightings scored the same candidates in the same frame against the "
        f"declared floor '{floor}', and the best of them {verdict} it on every panel that "
        "carries both."
    )


def _scoring_node(
    record: dict[str, list[dict[str, Any]]], floors: dict[str, DeclaredFloor]
) -> Built:
    """Which weighting turns a candidate into a score.

    This is the one node in the record with a live contrast, and it is still not
    a measurement. Several weightings scored the same candidates in the same
    frame, so the spread in the panels below is real and attributable. What is
    missing is a floor: nothing in the record says which level the others are
    supposed to beat, so the spread is a spread and not a separation. Naming one
    is a single ``experiment_run`` row, and this node reads it the moment it
    lands.
    """
    configs = record["scoring"]
    used = [c for c in configs if int(c["results"] or 0) > 0]
    declared = floors.get("scoring") or UNDECLARED
    floor = declared.floor
    separated, crossed = _separation(record["panels"], floor)
    used_dates = [c["first_used_on"] for c in used]
    edge = Edge(
        produced=bool(configs),
        instantiated=len(used),
        available=len(configs),
        scored=len(used),
        results=sum(int(c["results"] or 0) for c in used),
        # Never forced, for the same reason the substrate is not: the frame says
        # nothing about which weighting a candidate is scored under.
        forced=False,
        floor=floor,
        separated=separated,
        refusal=crossed,
        reading=declared.reading,
        # Built here rather than through ``_floor_for`` because this node reads
        # the floor itself, for the crossed-ladder prose below. Same three
        # sources: its own rows, the declaring run, and the last input a reading
        # could have been taken over. A scoring configuration counts as a level
        # only once it has scored something, so here too the level and the
        # scored level are one row.
        dated=_climbed_on(
            used_dates,
            power=_nth(used_dates, 2),
            floor=declared.declared_on,
            separation=_read_on(record["results"]) if separated else None,
        ),
    )
    return (
        _node(
            "scoring",
            edge,
            _scoring_reason(configs, used, floor, separated, crossed),
            split_fields(used, _SCORING_FIELDS),
            held_values(used, _SCORING_FIELDS),
        ),
        "a weighting to score candidates with",
        "a scoring configuration with a surviving result",
    )


def _features_reason(models: int, selections: list[str], families: list[str]) -> str:
    """Where the features node stands, in one sentence.

    Three branches in the order the questions arise in: whether any consumer
    exists, whether one recorded what it consumed, and only then how many
    selections that comes to. The middle one is worth having because a consumer
    that records no schema leaves a level UNRECOVERABLE rather than
    uninstantiated, and the model count read as a level count would report
    selections nobody can name.
    """
    named = ", ".join(families) if families else "none"
    if not models:
        return (
            f"Candidates carry the feature families their run asked for ({named}), but choosing "
            "among them is a decision only a consumer makes and there is none: reranker_model "
            f"holds {models} rows."
        )
    if not selections:
        return (
            f"{models} consumer(s) exist and none records the feature schema it was fitted "
            f"against, so no selection can be named from the record. The candidates carry {named}."
        )
    return (
        f"{len(selections)} feature selection(s) reached a consumer, each named by the schema "
        f"digest its booster was fitted against, out of the families the candidates carry "
        f"({named})."
    )


def _features_node(
    record: dict[str, list[dict[str, Any]]], floors: dict[str, DeclaredFloor]
) -> Built:
    """Which per-candidate features enter a model.

    The features exist. Every candidate row carries the families the run asked
    for, and they are stored, not derived at read time. What does not exist is a
    consumer, and a feature selection is a decision only a consumer can make:
    with no model to feed, no subset has ever been chosen over another.

    THIS NODE READS ITS COUNT RATHER THAN DISTINGUISHING ITS WORD, because
    unlike the three structural ones it has a row to count: a level here is one
    selection, named in exactly one place, the schema digest the booster was
    fitted against. The literal zero that stood here printed ``blocked`` beside
    a live family list whatever ``reranker_model`` held, so an empty table read
    like a node no table could move. Read, one row ends it.
    """
    art = _artifacts(record)
    models = int(art.get("reranker_model") or 0)
    selections = sorted(
        {str(r["schema_sha"]) for r in record["feature_selections"] if r.get("schema_sha")}
    )
    families = sorted(
        {
            f.strip()
            for p in record["prediction_sets"]
            for f in (p["features"] or "").split(",")
            if f.strip()
        }
    )
    edge = Edge(
        produced=models > 0,
        instantiated=len(selections),
        available=len(families),
        **_floor_for("features", record, floors),
    )
    return (
        _node("features", edge, _features_reason(models, selections, families), ([], [])),
        "a selection of features to feed a model",
        "a row in reranker_model to consume them, recording the schema it consumed",
    )


def _reranking_node(
    record: dict[str, list[dict[str, Any]]], floors: dict[str, DeclaredFloor]
) -> Built:
    """Whether a model reorders the candidates."""
    art = _artifacts(record)
    models = int(art.get("reranker_model") or 0)
    reranked = int(art.get("reranked_results") or 0)
    used = len({r["reranker_model_id"] for r in record["results"] if r["reranker_model_id"]})
    edge = Edge(
        produced=models > 0,
        instantiated=used,
        available=models,
        results=reranked,
        **_floor_for("reranking", record, floors),
    )
    reason = (
        f"No model exists to reorder with: reranker_model holds {models} rows and {reranked} "
        "published results name one. The candidate order is the retriever's, unchanged."
    )
    return (
        _node("reranking", edge, reason, ([], [])),
        "a model that reorders candidates",
        "a row in reranker_model",
    )


def _flow_count(record: dict[str, list[dict[str, Any]]]) -> int:
    """How many flows the record instantiates.

    A flow is a source plus a propagation mechanism. The sources are the corpora
    the prediction sets draw from; the mechanism is one, donor transfer, for as
    long as the candidate schema requires a donor on every row. Running the same
    mechanism over another representation is the same flow configured
    differently and is not counted twice here. Should that column ever become
    nullable this count turns into a lower bound, because a second mechanism
    would then be storable and nothing on the row would distinguish it.
    """
    return len({p["bank_source"] for p in record["prediction_sets"] if p["bank_source"]})


def _combination_node(
    record: dict[str, list[dict[str, Any]]], floors: dict[str, DeclaredFloor]
) -> Built:
    """How two or more flows are merged into one answer.

    One flow is instantiated, so there is nothing to merge today and a second
    corpus would change that. But a merged answer also has nowhere to be
    written: not a decision nobody has taken yet but one the record cannot
    express, which is the other word. Nothing here names a merge rule either, so
    the zero below is a gap named, and a migration has to bring the count.
    """
    flows = _flow_count(record)
    sources = sorted({p["bank_source"] for p in record["prediction_sets"] if p["bank_source"]})
    expressible = not _donor_required(record)
    edge = Edge(
        produced=flows >= 2,
        instantiated=0,
        available=flows,
        expressible=expressible,
        **_floor_for("combination", record, floors),
    )
    reason = (
        f"{flows} {'flow is' if flows == 1 else 'flows are'} instantiated, so there is nothing "
        f"to combine. Every prediction set draws from a single kind of source "
        f"({', '.join(sources) or 'none'}) through a single propagation mechanism, donor "
        "transfer, which is the only one the candidate schema can express."
    )
    if not expressible:
        reason = f"{reason} {_NO_DONORLESS_ROW}"
    return (
        _node("combination", edge, reason, ([], [])),
        "a rule for merging two flows",
        "a second flow, which the generator node has to produce first"
        if expressible
        else f"{_NULLABLE_DONOR}, so that one answer can carry two flows at all",
    )


def _routing_node(
    record: dict[str, list[dict[str, Any]]], floors: dict[str, DeclaredFloor]
) -> Built:
    """Which flow answers which panel.

    The one node here with nothing to count at all: a per-panel choice of flow
    has to be recorded where a panel can be seen, and no table this endpoint
    reads names the flow behind an answer. Counting prediction sets would count
    something else, so the gap is named rather than filled and the word turns on
    what can be read: every panel is answered by the one flow there is.
    """
    flows = _flow_count(record)
    expressible = not _donor_required(record)
    edge = Edge(
        produced=flows >= 2,
        instantiated=0,
        available=flows,
        expressible=expressible,
        **_floor_for("routing", record, floors),
    )
    # One opener, two tails. The sentence turns on the same fact the word does,
    # and writing it out twice is how the two halves drift apart.
    tail = (
        "is blocked for the same reason."
        if expressible
        else f"cannot be expressed for the same reason. {_NO_DONORLESS_ROW} No column on a "
        "published result names the flow that answered a panel either, so even two flows "
        "would leave this node with nothing to count."
    )
    reason = (
        f"Routing picks a flow per panel and there {'is' if flows == 1 else 'are'} {flows} of "
        f"them, so no panel has a choice to make. It is downstream of combination, which {tail}"
    )
    return (
        _node("routing", edge, reason, ([], [])),
        "a per-panel choice of flow",
        "a combination of two or more flows"
        if expressible
        else f"{_NULLABLE_DONOR}, and a column on the published result naming the flow that "
        "answered each panel",
    )
