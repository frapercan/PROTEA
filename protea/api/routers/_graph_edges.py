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
INEXPRESSIBLE = "inexpressible"

#: The two words that say a node has no artifact to show. Kept together because
#: every surface that lists what cannot answer has to list both, and apart as
#: two words because they ask the reader for different things: a ``blocked``
#: node is waiting for rows, an ``inexpressible`` one is waiting for a
#: migration. A reader who cannot tell them apart goes looking for data no run
#: could produce.
BLOCK_WORDS: tuple[str, ...] = (BLOCKED, INEXPRESSIBLE)


class StaleStructuralBlock(ValueError):
    """A node says its artifact cannot be written while the record holds one.

    Raised rather than rendered. The whole point of ``expressible`` is that it
    is read from the catalog and not asserted; the moment a builder claims the
    shape forbids an artifact and hands over instantiated levels of it, one of
    the two came from a constant that outlived the schema, and publishing
    either would be the defect this field was added to end.
    """


@dataclass(frozen=True)
class Edge:
    """Everything the record says about one node's decision.

    ``produced`` is false when the node's artifact has no producer at all.
    ``forced`` is true when the frame's own definition fixes the level, which is
    what separates a recorded choice from a value nobody ever chose. ``floor``
    is the level a declared comparison measures against, and ``separated``
    whether the comparison cleared it.

    ``expressible`` is the one field here that is about the SHAPE of the record
    rather than its content. It is false only where the catalog positively
    forbids the artifact -- a NOT NULL column that leaves it nowhere to be
    written -- and it exists because four nodes used to report a blocked edge
    from a literal zero, printing live counts beside a word that no number of
    rows could have changed. It must be read, never asserted: a builder that
    hardcodes it is back to the same defect one field along.
    """

    produced: bool = True
    instantiated: int = 0
    available: int = 0
    scored: int = 0
    results: int = 0
    forced: bool = False
    floor: str | None = None
    separated: bool | None = None
    expressible: bool = True


def strength_of(edge: Edge) -> str:
    """The one word that says how firmly a decision is held.

    The order of the tests is the argument. Expressibility comes first, ahead
    even of production, because it is the only test about the shape of the
    record instead of its content: a node whose artifact has nowhere to be
    written is not waiting for a producer to be run, and telling a reader it is
    sends them after work no run can do. Production comes next, because an
    artifact with no producer cannot have levels to compare. A single level
    comes after that and can never reach a measurement whatever it scored: with
    nothing to contrast against, a number is a reading and not a separation.
    Power comes before evidence, since whether a comparison could resolve
    anything is a fact about its shape and is settled before a metric is read.

    The refusal at the top fires before any word is produced, which is where it
    has to be: the next mistake in this area is a migration that makes an
    artifact storable, a builder that starts counting the rows, and a structural
    claim beside it that nobody re-read. That would print ``inexpressible`` next
    to a live count of the thing it says cannot exist.
    """
    if edge.instantiated and not edge.expressible:
        raise StaleStructuralBlock(
            f"{edge.instantiated} level(s) are instantiated under a node that says the record "
            "cannot express one. The structural claim outlived the schema it was read from; "
            "re-read it from the catalog rather than publishing a word the rows contradict."
        )
    if not edge.expressible:
        return INEXPRESSIBLE
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
    only the reason it is blocked. It is always present when the strength is one
    of ``BLOCK_WORDS``, which is the contract a reader depends on, and it is
    null only for a node that reached ``measured`` and therefore needs no
    account of itself beyond the measurement.

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
