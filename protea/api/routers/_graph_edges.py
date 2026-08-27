"""What an edge is, and the one function that decides how firmly it is held.

Apart from the node builders because every one of them depends on this and none
of it depends on any of them. The order of the tests in ``strength_of`` is the
argument the whole surface rests on, so it lives where it can be read without
scrolling past ten builders to find it.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

MEASURED = "measured"
CHOSEN = "chosen"
INHERITED = "inherited"
UNPOWERED = "unpowered"
BLOCKED = "blocked"


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


def _node(
    key: str,
    edge: Edge,
    reason: str,
    fields: tuple[list[str], list[str]],
    held: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    """Assemble one node of the response.

    ``blocked_reason`` carries the reason the node stands where it does, not
    only the reason it is blocked. It is always present when the strength is
    ``blocked``, which is the contract a reader depends on, and it is null only
    for a node that reached ``measured`` and therefore needs no account of
    itself beyond the measurement.

    ``held`` is the value the node currently stands at, named field by field.
    A strength says how firmly a decision is held and says nothing about what
    was decided, and a reader who cannot see the value cannot tell an inherited
    default from a deliberate choice that happens to be unmeasured. Both read
    ``inherited`` and only one of them is a surprise.
    """
    spec = _SPEC_BY_KEY[key]
    strength = strength_of(edge)
    varying, constant = fields
    return {
        "held": held or [],
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


#: Longest value worth printing inline beside a node. Past this a value is a
#: paragraph rather than a fact, and it crowds out the row it belongs to.
_VALUE_LIMIT = 120


def held_values(rows: list[dict[str, Any]], fields: tuple[str, ...]) -> list[dict[str, str]]:
    """The value each field stands at, or every value it took if it varied.

    Emitted so a reader can see WHAT was decided beside how firmly it is held.
    A node reading ``inherited`` with no visible value is indistinguishable
    from one nobody has looked at, and those are different situations: the
    first is a default nobody chose, the second is a choice nobody measured.

    Fields absent from every row are dropped rather than shown empty, because a
    field the record cannot speak to is not a value standing at nothing.
    """
    out: list[dict[str, str]] = []
    for field in fields:
        seen = sorted({str(r[field]) for r in rows if r.get(field) is not None})
        if not seen:
            continue
        text_value = " · ".join(seen[:4]) + ("…" if len(seen) > 4 else "")
        # A value long enough to need wrapping stops being a value a reader can
        # take in at a glance and becomes a paragraph competing with the row it
        # sits in. The scoring weights are a nested object and run to hundreds
        # of characters; the field is worth naming, the blob is not worth
        # printing, and the levels below carry the same information usably.
        if len(text_value) > _VALUE_LIMIT:
            text_value = f"{len(seen)} value(s), too long to show"
        out.append({"field": field, "value": text_value})
    return out


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
