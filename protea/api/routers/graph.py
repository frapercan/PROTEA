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
from dataclasses import dataclass
from typing import Any

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session, sessionmaker

from protea.api.deps import get_session_factory, get_settings
from protea.api.routers._graph_panels import (
    build_panels,
    panel_units_from_groundtruth,
    separated_from_floor,
)
from protea.api.routers._graph_reads import read_pivot_aspects, read_record
from protea.infrastructure.settings import Settings
from protea.infrastructure.storage.factory import (
    ArtifactStoreUnavailable,
    get_artifact_store,
)

router = APIRouter(prefix="/graph", tags=["graph"])

# The edge vocabulary, closed at five. Spelled once so a typo in a builder is an
# import-time NameError rather than a word nobody downstream recognises.
MEASURED = "measured"
CHOSEN = "chosen"
INHERITED = "inherited"
UNPOWERED = "unpowered"
BLOCKED = "blocked"

# ── Edges ─────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Edge:
    """Everything the record says about one node's decision.

    ``produced`` is false when the node's artifact has no producer at all.
    ``forced`` is true when the frame's own definition fixes the level, which is
    what separates a recorded choice from a value nobody ever chose. ``floor``
    is the level a declared comparison measures against, and ``separated``
    whether the comparison cleared it.
    """

    produced: bool = True
    instantiated: int = 0
    available: int = 0
    scored: int = 0
    results: int = 0
    forced: bool = False
    floor: str | None = None
    separated: bool | None = None


def strength_of(edge: Edge) -> str:
    """The one word that says how firmly a decision is held.

    The order of the tests is the argument. Production comes first, because an
    artifact with no producer cannot have levels to compare. A single level
    comes next and can never reach a measurement whatever it scored: with
    nothing to contrast against, a number is a reading and not a separation.
    Power comes before evidence, since whether a comparison could resolve
    anything is a fact about its shape and is settled before a metric is read.
    """
    if not edge.produced or edge.instantiated == 0:
        return BLOCKED
    if edge.instantiated == 1:
        return CHOSEN if edge.forced else INHERITED
    if edge.scored < 2:
        return UNPOWERED
    if edge.floor is None or edge.separated is None:
        return CHOSEN
    return MEASURED if edge.separated else CHOSEN


@dataclass(frozen=True)
class Spec:
    """A node's fixed identity: what it is, where it sits, what it asks."""

    key: str
    title: str
    stage: int
    question: str


SPECS: tuple[Spec, ...] = (
    Spec("frame", "Frame", 0, "Which window, pivot and accretion regime every number is read in."),
    Spec("substrate", "Substrate", 1, "Which representation the neighbourhood is computed in."),
    Spec("bank", "Bank", 2, "Which corpus the donors come from, and under which donor policy."),
    Spec("retriever", "Retriever", 3, "How candidates are drawn from the bank, and how deep."),
    Spec("generator", "Generator", 4, "Whether any candidate arrives without a donor."),
    Spec("scoring", "Scoring", 5, "Which weighting turns a candidate into a score."),
    Spec("features", "Features", 6, "Which per-candidate features enter a model."),
    Spec("reranking", "Re-ranking", 7, "Whether a model reorders the candidates."),
    Spec("combination", "Combination", 8, "How two or more flows are merged into one answer."),
    Spec("routing", "Routing", 9, "Which flow answers which panel."),
)

_SPEC_BY_KEY: dict[str, Spec] = {s.key: s for s in SPECS}

#: What a builder hands back: the node, the artifact a blocked node cannot
#: produce, and what would have to exist first. The last two are used only when
#: the strength comes out ``blocked``, which is how every blocked node is
#: guaranteed to reach the response with a reason attached.
Built = tuple[dict[str, Any], str, str]


def _node(key: str, edge: Edge, reason: str, fields: tuple[list[str], list[str]]) -> dict[str, Any]:
    """Assemble one node of the response.

    ``blocked_reason`` carries the reason the node stands where it does, not
    only the reason it is blocked. It is always present when the strength is
    ``blocked``, which is the contract a reader depends on, and it is null only
    for a node that reached ``measured`` and therefore needs no account of
    itself beyond the measurement.
    """
    spec = _SPEC_BY_KEY[key]
    strength = strength_of(edge)
    varying, constant = fields
    return {
        "key": spec.key,
        "title": spec.title,
        "stage": spec.stage,
        "question": spec.question,
        "strength": strength,
        "levels_instantiated": edge.instantiated,
        "levels_available": edge.available,
        "varying_fields": varying,
        "constant_fields": constant,
        "blocked_reason": None if strength == MEASURED else reason,
        "results": edge.results,
    }


def split_fields(
    rows: list[dict[str, Any]], fields: tuple[str, ...]
) -> tuple[list[str], list[str]]:
    """Which of ``fields`` actually differ across ``rows``, and which do not.

    Read off the instantiated rows rather than off a declaration, so a node that
    grows an axis says so the moment the row lands, and a node that names an
    axis it never moved gets no credit for it. With nothing instantiated neither
    list has members: a field is constant only once something has held it.
    """
    if not rows:
        return [], []
    varying: list[str] = []
    constant: list[str] = []
    for field in fields:
        values = {repr(row.get(field)) for row in rows}
        (varying if len(values) > 1 else constant).append(field)
    return varying, constant


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


# ── Node builders ─────────────────────────────────────────────────────────────
#
# One per node. Each returns the node, the artifact it cannot produce, and the
# precondition for producing it; the last two are read only when the node comes
# out blocked. Every count in them is derived from the rows, never asserted.


def _frame_node(record: dict[str, list[dict[str, Any]]], head: dict[str, Any] | None) -> Built:
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
    edge = Edge(
        produced=head is not None,
        instantiated=len(used),
        available=len(pool),
        scored=len(used) if results else 0,
        results=len(results),
        forced=len(eligible) == 1 and bool(used) and used[0]["id"] == eligible[0]["id"],
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
        _node("frame", edge, reason, split_fields(record["evaluation_sets"], fields)),
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


def _substrate_node(record: dict[str, list[dict[str, Any]]]) -> Built:
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
        _node("substrate", edge, reason, split_fields(used, _SUBSTRATE_FIELDS)),
        "a representation to retrieve in",
        "an embedding configuration with stored embeddings",
    )


_BANK_FIELDS: tuple[str, ...] = (
    "bank_source",
    "bank_version",
    "donor_reviewed_only",
    "donor_evidence_codes",
    "donor_exclusions",
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


def _bank_node(record: dict[str, list[dict[str, Any]]], head: dict[str, Any] | None) -> Built:
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
    levels = {(p["annotation_set_id"], _policy_is_empty(p)) for p in sets}
    used_ids = {p["annotation_set_id"] for p in sets}
    pinned = bool(eligible) and used_ids <= {b["id"] for b in eligible}
    policy_set = bool(sets) and not all(_policy_is_empty(p) for p in sets)
    edge = Edge(
        produced=bool(record["banks"]),
        instantiated=len(levels),
        available=len(eligible or record["banks"]),
        scored=len({r["annotation_set_id"] for r in record["results"]}),
        results=len(record["results"]),
        forced=pinned and policy_set,
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
        _node("bank", edge, reason, split_fields(sets, _BANK_FIELDS)),
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
)


def _donor_required(record: dict[str, list[dict[str, Any]]]) -> bool:
    """Whether the candidate table can hold a term that arrived without a donor.

    Read from the catalog rather than from a row count. A NOT NULL column says
    the shape of the record forbids the other mechanism; an all-donor sample
    says only that nobody ran it.
    """
    return any(row.get("is_nullable") == "NO" for row in record["donor_column"])


def _retriever_node(record: dict[str, list[dict[str, Any]]]) -> Built:
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
    levels = {tuple(repr(p.get(f)) for f in _RETRIEVER_FIELDS) for p in sets}
    candidates = sum(int(c["candidates"]) for c in record["candidates"])
    depths = sorted({p["depth"] for p in sets if p["depth"]})
    edge = Edge(
        produced=bool(sets),
        instantiated=len(levels),
        available=len(levels),
        scored=len({r["prediction_set_id"] for r in record["results"]}),
        results=len(record["results"]),
        forced=False,
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
        _node("retriever", edge, reason, split_fields(sets, _RETRIEVER_FIELDS)),
        "a way of drawing candidates from the bank",
        "a prediction set",
    )


def _artifacts(record: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    return record["artifacts"][0] if record["artifacts"] else {}


def _generator_node(record: dict[str, list[dict[str, Any]]]) -> Built:
    """Whether any candidate arrives without a donor.

    A generator is the second half of a second flow: a source that proposes a
    term from the query alone, with no neighbour to inherit it from. Two things
    decide this node and both are read rather than assumed: whether the
    candidate column that names the donor still forbids a donorless row, and
    whether the tables a domain-based generator would draw on hold anything.
    """
    art = _artifacts(record)
    interpro = int(art.get("interpro_annotation") or 0)
    mappings = int(art.get("interpro_go_mapping") or 0)
    models = int(art.get("reranker_model") or 0)
    edge = Edge(produced=bool(interpro and mappings), instantiated=0, available=0)
    schema = (
        "the candidate column naming the donor is NOT NULL, so a term that arrived without one "
        "has nowhere to be written, and "
        if _donor_required(record)
        else ""
    )
    reason = (
        f"No candidate arrives without a donor: {schema}the artifacts a second source would "
        f"draw on are absent. interpro_annotation holds {interpro} rows, interpro_go_mapping "
        f"{mappings}, reranker_model {models}."
    )
    return (
        _node("generator", edge, reason, ([], [])),
        "a candidate that owes nothing to a donor",
        "rows in interpro_annotation and interpro_go_mapping",
    )


_SCORING_FIELDS: tuple[str, ...] = ("formula", "weights", "evidence_weights", "params")


def _scoring_node(record: dict[str, list[dict[str, Any]]], floors: dict[str, str]) -> Built:
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
    floor = floors.get("scoring")
    panels = record["panels"]
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
        separated=separated_from_floor(panels, floor) if floor and panels else None,
    )
    if not used:
        reason = (
            f"{len(configs)} weightings are registered and none of them has a surviving result."
        )
    elif len(used) == 1:
        reason = (
            f"One of {len(configs)} registered weightings carries a result. With nothing to "
            "contrast it against, its numbers are a reading and not a separation."
        )
    elif floor is None:
        reason = (
            f"{len(used)} weightings scored the same candidates in the same frame, so the "
            "contrast is real and its spread is in the panels below. No floor is declared for "
            "it anywhere in the record, so the spread cannot be called a separation."
        )
    else:
        verdict = "clears" if edge.separated else "does not clear"
        reason = (
            f"{len(used)} weightings scored the same candidates in the same frame against the "
            f"declared floor '{floor}', and the best of them {verdict} it on every panel that "
            "carries both."
        )
    return (
        _node("scoring", edge, reason, split_fields(used, _SCORING_FIELDS)),
        "a weighting to score candidates with",
        "a scoring configuration with a surviving result",
    )


def _features_node(record: dict[str, list[dict[str, Any]]]) -> Built:
    """Which per-candidate features enter a model.

    The features exist. Every candidate row carries the families the run asked
    for, and they are stored, not derived at read time. What does not exist is a
    consumer, and a feature selection is a decision only a consumer can make:
    with no model to feed, no subset has ever been chosen over another, so the
    node has zero instantiated levels rather than one.
    """
    art = _artifacts(record)
    models = int(art.get("reranker_model") or 0)
    families = sorted(
        {
            f.strip()
            for p in record["prediction_sets"]
            for f in (p["features"] or "").split(",")
            if f.strip()
        }
    )
    edge = Edge(produced=models > 0, instantiated=0, available=len(families))
    named = ", ".join(families) if families else "none"
    reason = (
        f"Candidates carry the feature families their run asked for ({named}), but choosing "
        f"among them is a decision only a consumer makes and there is none: reranker_model "
        f"holds {models} rows."
    )
    return (
        _node("features", edge, reason, ([], [])),
        "a selection of features to feed a model",
        "a row in reranker_model to consume them",
    )


def _reranking_node(record: dict[str, list[dict[str, Any]]]) -> Built:
    """Whether a model reorders the candidates."""
    art = _artifacts(record)
    models = int(art.get("reranker_model") or 0)
    reranked = int(art.get("reranked_results") or 0)
    used = len({r["reranker_model_id"] for r in record["results"] if r["reranker_model_id"]})
    edge = Edge(produced=models > 0, instantiated=used, available=models, results=reranked)
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


def _combination_node(record: dict[str, list[dict[str, Any]]]) -> Built:
    """How two or more flows are merged into one answer."""
    flows = _flow_count(record)
    sources = sorted({p["bank_source"] for p in record["prediction_sets"] if p["bank_source"]})
    edge = Edge(produced=flows >= 2, instantiated=0, available=flows)
    reason = (
        f"{flows} {'flow is' if flows == 1 else 'flows are'} instantiated, so there is nothing "
        f"to combine. Every prediction set draws from a single kind of source "
        f"({', '.join(sources) or 'none'}) through a single propagation mechanism, donor "
        "transfer, which is the only one the candidate schema can express."
    )
    return (
        _node("combination", edge, reason, ([], [])),
        "a rule for merging two flows",
        "a second flow, which the generator node has to produce first",
    )


def _routing_node(record: dict[str, list[dict[str, Any]]]) -> Built:
    """Which flow answers which panel."""
    flows = _flow_count(record)
    edge = Edge(produced=flows >= 2, instantiated=0, available=flows)
    reason = (
        f"Routing picks a flow per panel and there {'is' if flows == 1 else 'are'} {flows} of "
        "them, so no panel has a choice to make. It is downstream of combination, which is "
        "blocked for the same reason."
    )
    return (
        _node("routing", edge, reason, ([], [])),
        "a per-panel choice of flow",
        "a combination of two or more flows",
    )


# ── Assembly ──────────────────────────────────────────────────────────────────


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
    built: list[Built] = [
        _frame_node(record, head),
        _substrate_node(record),
        _bank_node(record, head),
        _retriever_node(record),
        _generator_node(record),
        _scoring_node(record, floors),
        _features_node(record),
        _reranking_node(record),
        _combination_node(record),
        _routing_node(record),
    ]
    return {
        "frame": build_frame(record),
        "nodes": [node for node, _, _ in built],
        "panels": build_panels(record["panels"], units),
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
