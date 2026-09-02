"""One builder per node of the graph, in pipeline order.\n\nEach answers the same question about a different field: what does the record\nhold here, how many levels could it have held, and how firmly is the value\nstanding. They are together because they share that shape and apart from the\nassembly because the assembly should read as a list of nodes and nothing else."""

from __future__ import annotations

from datetime import date
from typing import Any

from protea.api.routers._graph_edges import (
    Built,
    Edge,
    _node,
    held_values,
    split_fields,
)
from protea.api.routers._graph_panels import CrossedDepthAxes, separated_from_floor

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
    # A refusal is caught rather than allowed to escape, and it is caught here
    # because this is the only place that can show it. Letting it reach the
    # endpoint would turn a comparison nobody should have declared into a 500
    # on a page that is otherwise entirely readable, and the reader would learn
    # nothing about why. Held as a reason instead, and the separation drops to
    # None, which already means the record established nothing.
    crossed: str | None = None
    separated: bool | None = None
    if floor and panels:
        try:
            separated = separated_from_floor(panels, floor)
        except CrossedDepthAxes as refusal:
            crossed = str(refusal)
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
    elif crossed is not None:
        reason = crossed
    else:
        verdict = "clears" if edge.separated else "does not clear"
        reason = (
            f"{len(used)} weightings scored the same candidates in the same frame against the "
            f"declared floor '{floor}', and the best of them {verdict} it on every panel that "
            "carries both."
        )
    return (
        _node(
            "scoring",
            edge,
            reason,
            split_fields(used, _SCORING_FIELDS),
            held_values(used, _SCORING_FIELDS),
        ),
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
