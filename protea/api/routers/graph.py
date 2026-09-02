"""The experiment graph: which decisions have been taken, and on what evidence.

WHAT THIS REPLACES. The retired ladder surface described the campaign as an
ordered sequence of steps, each one a grid of jobs, and counted the grid from
the job table. That table is append-only and outlived a wipe that removed the
evaluation results, so the count went on reporting a complete measurement with
no surviving evidence behind it. Nothing here counts a job. Every number is read
off an artifact that still exists, and a node with no artifact says so rather
than borrowing a number from the queue.

THE MODEL. A FLOW is an annotation source plus a mechanism that propagates it.
Running the same mechanism over a different representation is not a second flow,
it is the same flow configured differently. A NODE is one decision over a field,
or over a group of fields that cannot be decided apart. The nodes are, in
pipeline order: frame, substrate, bank, retriever, generator, scoring, features,
re-ranking, combination, routing. The EDGE into a node says how firmly that
decision is held and takes one of five values:

``measured``
    a declared comparison separated against its floor.
``chosen``
    the comparison ran with power and did not separate; a level was selected and
    recorded.
``inherited``
    nobody ever decided. The value is the one it has always been.
``unpowered``
    the comparison could not have resolved anything. Settled from the shape of
    the comparison, before any metric is read.
``blocked``
    a level cannot be produced at all, because its artifact has no producer.

A PANEL is one of nine regions, a knowledge category (NK, LK, PK) crossed with
an aspect (BPO, MFO, CCO). Panels are never pooled and never summed: the
cardinality of this record is a vector over the nine, not a scalar.

WHY A SINGLE LEVEL IS NEVER A MEASUREMENT. One instantiated level means no
contrast existed, so no separation can have happened whatever the numbers look
like. Such a node is ``inherited`` when nothing fixed the value and ``chosen``
when the frame's own definition fixed it. Two or more levels with fewer than two
of them scored is ``unpowered``: a contrast needs two scored levels, and that is
knowable before reading a single metric. Two or more scored levels reach
``measured`` only when a floor is declared for the comparison and some level
clears it on every panel that carries both. No table in this schema has a column
for a floor, so the one place a floor can be declared is an ``experiment_run``
whose ``config`` names ``graph_node`` and ``floor``. Until such a row exists no
node here can be a measurement, and that is the honest report rather than a
defect in the endpoint.

WHERE THE PANEL POPULATIONS COME FROM. Not from the ground-truth parquet, which
this endpoint never opens, and not from ``evaluation_set.stats``, which counts
proteins per knowledge category across all three aspects at once and so cannot
answer a per-panel question at all. They are reconciled out of the stored
metrics. ``n_proteins`` on a panel is the count of proteins holding a prediction
at the threshold that run settled on, and ``coverage_at_tau`` is that same count
over the panel's ground-truth cohort, so the cohort is the ratio of the two.
Both are rounded when persisted, so each scored result pins the cohort to a
short interval rather than to a number, and intersecting the intervals of every
result that scored the panel collapses it. When the intervals do not overlap the
endpoint reports null, never a rounded guess and never a zero.

NOTHING IS WRITTEN. Every statement behind this is a SELECT, and they all live
in ``_graph_reads``. ``session_scope`` is deliberately not used here because it
commits on exit, and a reporting surface has no business opening a write path.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session, sessionmaker

from protea.api.deps import get_session_factory, get_settings
from protea.api.routers._graph_edges import BLOCKED, Built
from protea.api.routers._graph_nodes import (
    _bank_node,
    _combination_node,
    _features_node,
    _frame_node,
    _generator_node,
    _reranking_node,
    _retriever_node,
    _routing_node,
    _scoring_node,
    _substrate_node,
    _window_span,
)
from protea.api.routers._graph_panels import (
    build_panels,
    contrast_floors,
    panel_units_from_groundtruth,
)
from protea.api.routers._graph_reads import read_pivot_aspects, read_record
from protea.api.routers._graph_representations import build_representations
from protea.infrastructure.settings import Settings
from protea.infrastructure.storage.factory import (
    ArtifactStoreUnavailable,
    get_artifact_store,
)

router = APIRouter(prefix="/graph", tags=["graph"])

# The edge vocabulary, closed at five. Spelled once so a typo in a builder is an
# import-time NameError rather than a word nobody downstream recognises.

# ── The frame ─────────────────────────────────────────────────────────────────


def build_frame(record: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    """The frame every number below is read in, and whether it is declared.

    ``declared`` is not "a frame exists". It is true only when the record leaves
    a reader nothing to guess: exactly one evaluation set, so there is a single
    window under measurement; both ends of that window resolve to a release; a
    pivot snapshot, an accretion table and a query set all resolve; and no
    published result is unsealed. A result is sealed when its ``frame`` column
    says which scoring frame it lives in. Today every one of them is null, which
    is why the block below can be fully populated and still report false: the
    frame is recoverable, but no published number states it.
    """
    heads = record["evaluation_sets"]
    head = heads[0] if heads else None
    accretion = next((a for a in record["accretion"] if a["in_use"]), None)
    query_set = next((q for q in record["query_sets"] if q["in_use"]), None)
    results = record["results"]
    sealed = sum(1 for r in results if r["frame"])
    unsealed = len(results) - sealed
    ends = (head["window_from"], head["window_to"]) if head else (None, None)
    window = f"{ends[0]}->{ends[1]}" if all(ends) else None
    # The release numbers name two files and date nothing. Fourteen months
    # separate 220 from 227, and how long a window ran is the first thing asked
    # of a temporal benchmark: it bounds how much annotation could accumulate,
    # and therefore what any panel drawn from it could possibly resolve.
    span = _window_span(head) if head else None
    pivot = (
        {"id": head["pivot_snapshot_id"], "version": head["pivot_version"]}
        if head and head["pivot_snapshot_id"]
        else None
    )
    return {
        "declared": bool(
            head is not None
            and len(heads) == 1
            and window
            and pivot
            and accretion
            and query_set
            and unsealed == 0
        ),
        "evaluation_set_id": head["id"] if head else None,
        "window": window,
        "window_span": span,
        "window_role": head["window_role"] if head else None,
        "mode": head["mode"] if head else None,
        "pivot_snapshot": pivot,
        "information_accretion_set": (
            {"id": accretion["id"], "regime": accretion["regime"], "sha256": accretion["sha256"]}
            if accretion
            else None
        ),
        "query_set": (
            {"id": query_set["id"], "name": query_set["name"], "entries": query_set["entries"]}
            if query_set
            else None
        ),
        "sealed_rows": sealed,
        "unsealed_rows": unsealed,
    }


# ── Assembly ──────────────────────────────────────────────────────────────────


def build_timeline(
    marks: list[dict[str, Any]], head: dict[str, Any] | None
) -> dict[str, Any] | None:
    """The frame laid out on a date axis, with each release's part in it named.

    A table can say the window runs from 220 to 227 and the pivot is the July
    graph. Only an axis shows what that arrangement means: the pivot sits inside
    the window it reconciles rather than at either end, an ontology contemporary
    with the window's opening exists and was not the one chosen, and a release
    beyond the closing end is the cohort nobody is allowed to look at yet.

    Every role is derived from dates the record already holds. Nothing is
    positioned by assumption, and a release the record cannot date does not
    appear at all rather than appearing at a guessed end.
    """
    if not head or not marks:
        return None
    start, end = head.get("window_from_date"), head.get("window_to_date")
    pivot_version = head.get("pivot_version")
    out: list[dict[str, Any]] = []
    for mark in marks:
        when = mark.get("date")
        if not when:
            continue
        inside = bool(start and end and start <= when <= end)
        if when == start:
            role = "window_start"
        elif when == end:
            role = "window_end"
        elif mark["version"] == pivot_version:
            role = "pivot"
        elif start and when < start:
            role = "before"
        elif end and when > end:
            role = "beyond"
        else:
            role = "inside"
        out.append(
            {
                "kind": mark["kind"],
                "label": mark["label"],
                "date": when,
                "role": role,
                "in_window": inside,
                "is_pivot": mark["version"] == pivot_version,
            }
        )
    return {
        "window": {"from": start, "to": end},
        "marks": sorted(out, key=lambda m: m["date"]),
    }


def build_graph(
    record: dict[str, list[dict[str, Any]]],
    units: Mapping[tuple[str, str], int] | None = None,
) -> dict[str, Any]:
    """Turn the rows into the graph. A pure function of what was read.

    The ``blocked`` list is derived from the nodes rather than compiled beside
    them, so a node cannot report itself blocked and then be missing from the
    list, and cannot appear in the list without the reason its own entry
    carries. The two say the same thing because they are built from one source.
    """
    head = record["evaluation_sets"][0] if record["evaluation_sets"] else None
    floors = {f["node"]: f["floor"] for f in record["floors"] if f["node"] and f["floor"]}
    # EVERY builder gets the floors, not one of them. Until 2026-09-02 this list
    # handed the dict to _scoring_node alone, so nine of the ten nodes could not
    # see a declared floor at all and `strength_of` returned CHOSEN for them
    # whatever anyone declared. The vocabulary said `measured` was reachable and
    # for nine nodes it was not, which is a surface asserting a state it cannot
    # produce -- the same defect this endpoint exists to end.
    built: list[Built] = [
        _frame_node(record, head, floors),
        _substrate_node(record, floors),
        _bank_node(record, head, floors),
        _retriever_node(record, floors),
        _generator_node(record, floors),
        _scoring_node(record, floors),
        _features_node(record, floors),
        _reranking_node(record, floors),
        _combination_node(record, floors),
        _routing_node(record, floors),
    ]
    return {
        "frame": build_frame(record),
        "nodes": [node for node, _, _ in built],
        # The Substrate node's denominator, expanded. It is beside the nodes
        # rather than inside one because every other node's levels are named by
        # their fields and this node's are not: a representation is a model, a
        # layer, a pooling and a coverage over the corpus, and none of that fits
        # in a field list.
        "representations": build_representations(record),
        "panels": build_panels(record["panels"], units),
        # The floors the panels and every finer cell are read against. A
        # constant of the design rather than a fact about this record, which is
        # why it is served with the record instead of being restated by every
        # surface that has to mark a cell as too thin to decide anything on.
        "floors": contrast_floors(),
        "timeline": build_timeline(
            record.get("timeline", []),
            record["evaluation_sets"][0] if record["evaluation_sets"] else None,
        ),
        "blocked": [
            {
                "node": node["key"],
                "what": what,
                "why": node["blocked_reason"],
                "precondition": precondition,
            }
            for node, what, precondition in built
            if node["strength"] == BLOCKED
        ],
    }


@router.get("")
def get_graph(
    factory: sessionmaker[Session] = Depends(get_session_factory),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    """The experiment graph as the record currently supports it.

    Reads and returns. The session is opened directly rather than through
    ``session_scope``, which commits on exit; there is nothing to commit and a
    reporting surface should not hold a transaction open that could.
    """
    with factory() as session:
        record = read_record(session)
        aspect_of_term = read_pivot_aspects(session, record)
    units = _counted_units(record, aspect_of_term, settings)
    return build_graph(record, units)


def _counted_units(
    record: dict[str, list[dict[str, Any]]],
    aspect_of_term: dict[str, str],
    settings: Settings,
) -> Mapping[tuple[str, str], int] | None:
    """Count the panel populations from the window's stored ground truth.

    Returns ``None`` when the artefact cannot be read, so every panel reports an
    absent population rather than one standing in for it. That is the whole
    point: the population is the one quantity on this page that must not be
    inferred, because a plausible wrong number here is indistinguishable from a
    right one and silently rescales every panel that reads it.
    """
    head = record["evaluation_sets"][0] if record["evaluation_sets"] else None
    uri = (head or {}).get("groundtruth_uri")
    if not uri or not aspect_of_term:
        return None
    try:
        store = get_artifact_store(settings)
        payload = store.get(uri.split("/", 3)[3] if uri.startswith("s3://") else uri)
    except (ArtifactStoreUnavailable, OSError, ValueError, KeyError):
        return None
    return panel_units_from_groundtruth(payload, aspect_of_term)
