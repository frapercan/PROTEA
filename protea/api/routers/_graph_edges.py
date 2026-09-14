"""What an edge is, and the one function that decides how firmly it is held.

Apart from the node builders because every one of them depends on this and none
of it depends on any of them. The order of the tests in ``strength_of`` is the
argument the whole surface rests on, so it lives where it can be read without
scrolling past ten builders to find it.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

# The reading vocabulary is the instrument's, taken from where it is defined
# rather than respelled here, for the reason the panel keys are taken from the
# enums that own them: a surface that keeps its own copy of somebody else's
# words drifts from them silently. The served process already holds this module,
# since the operation catalog imports it, so the reach costs nothing at import.
from protea.core.operations._paired_panels_panel import TALLY_KEYS

MEASURED = "measured"
#: A comparison that was read with the power to resolve the difference this
#: project acts on, and came back with its levels inside each other's noise.
#:
#: THE SIXTH WORD, AND WHY IT IS A WORD. ``compare_paired_panels`` keeps two
#: nulls apart because they are two facts: ``null_with_power`` looked, could
#: have seen the declared effect, and found none, while ``null_unread`` had no
#: declared effect to look for. This scale had five words and both nulls landed
#: on ``chosen``, so a null the campaign MEASURED read exactly like a question
#: nobody asked. Over the whole-panel readings in ``job_event`` that is 23
#: measured nulls published in the same word as 2,299 unasked questions.
#:
#: A flag beside ``chosen`` would have left the word itself wrong, and the word
#: is what every surface renders: the page prints it, styles it and legends it.
#: A sixth word is also what leaves the ORDER OF THE TESTS in ``strength_of``
#: alone -- it is a different answer to the last question asked there, not a new
#: question inserted before it, and the four tests ahead of it still settle
#: production, contrast and power before any metric is read.
INDISTINGUISHABLE = "indistinguishable"
CHOSEN = "chosen"
INHERITED = "inherited"
UNPOWERED = "unpowered"
BLOCKED = "blocked"

# ── The reading a declared comparison came back with ──────────────────────────
#
# The instrument's vocabulary, named here so that a producer which misspells one
# is an import-time NameError rather than a word no strength answers to, and
# checked against the instrument's own tuple below so that a bucket added there
# cannot be silently missing here.
RESOLVED = "resolved"
NULL_WITH_POWER = "null_with_power"
NULL_UNREAD = "null_unread"
UNDERPOWERED = "underpowered"
REFUSED = "refused"
NOT_COMPUTED = "not_computed"

#: What each reading publishes as. Exactly one strength per reading, which is
#: what makes this a function a test can walk: add a seventh bucket to the tally
#: and the walk over ``TALLY_KEYS`` fails instead of the surface inventing a
#: word for it.
#:
#: THREE READINGS PUBLISH AS ``chosen`` AND THAT IS NOT THE COLLAPSE THIS FIXES.
#: A strength says how firmly a decision is held. A comparison nobody declared,
#: one whose precondition was refused and one no panel could answer all leave a
#: level standing with nothing established about it, which is what ``chosen``
#: means; what separates them is WHY nothing was established, and that is the
#: node's reason to give, not the scale's. The distinction the scale itself had
#: to carry is the one it could not: a null read against declared power is a
#: finding about two levels, and ``indistinguishable`` is that finding.
#:
#: ``underpowered`` publishes as ``unpowered`` beside the structural test that
#: already produces that word. They are the same statement -- this comparison
#: could not have resolved anything -- reached once from the shape of the
#: contrast (fewer than two scored levels) and once from the populations that
#: testified. One word, because a reader asking "could this have answered?" gets
#: one answer.
STRENGTH_OF_READING: dict[str, str] = {
    RESOLVED: MEASURED,
    NULL_WITH_POWER: INDISTINGUISHABLE,
    UNDERPOWERED: UNPOWERED,
    NULL_UNREAD: CHOSEN,
    REFUSED: CHOSEN,
    NOT_COMPUTED: CHOSEN,
}


class UnpublishableReading(ValueError):
    """A reading the firmness scale has no word for.

    Raised, and raised before any strength reaches a reader, because the
    alternative is what this surface already did with the sixth word: publish it
    as ``chosen``, which is a real word meaning something else. A default here
    does not lose a reading, it relabels it.
    """


def refuse_unstateable_readings(table: Mapping[str, str], counted: Iterable[str]) -> None:
    """Refuse a scale that cannot state every reading the instrument counts.

    Called at import below, so a seventh bucket in the tally stops this module
    loading rather than reaching a reader relabelled: a scale that cannot state
    its own vocabulary has no business serving it.

    It is a NAMED FUNCTION and not the bare ``if`` it used to be because a
    refusal that only ever happens at import is a refusal no test has seen
    raise. Reproducing it needs a fabricated tally and a module reload, so the
    check went untested while reading as though it were covered, which is the
    same trap as a guard that logs: the rule looks guarded and nothing pins the
    guard. Called with the real pair one line below, so the happy path is not a
    claim either.

    Both directions, because each is a different defect. A counted reading with
    no strength is the one that relabels a finding. A strength named for a
    reading nothing counts is a word this surface can never be asked for, and it
    would sit in the table looking like coverage.
    """
    tally, scale = set(counted), set(table)
    if scale == tally:
        return
    raise UnpublishableReading(
        "the panel tally and the firmness scale disagree about which readings exist. "
        f"Counted with no strength to publish as: {sorted(tally - scale)}. "
        f"Named here and never counted: {sorted(scale - tally)}. "
        "A reading with no strength is published as the strength that happens to be left, "
        "which is how a measured null became a recorded choice."
    )


refuse_unstateable_readings(STRENGTH_OF_READING, TALLY_KEYS)


def strength_of_reading(reading: str) -> str:
    """The one strength a reading publishes as, or a refusal to publish it.

    The refusal is the point. Every caller here holds a word that came from the
    tally's vocabulary or from nowhere, and a word from nowhere must not be
    answered with the nearest strength to hand.
    """
    try:
        return STRENGTH_OF_READING[reading]
    except KeyError:
        raise UnpublishableReading(
            f"reading {reading!r} is not one of the panel tally's words "
            f"({', '.join(TALLY_KEYS)}), so no strength states it. Publishing the nearest "
            "one would report a comparison that was never read as one that was."
        ) from None


@dataclass(frozen=True)
class Edge:
    """Everything the record says about one node's decision.

    ``produced`` is false when the node's artifact has no producer at all.
    ``forced`` is true when the frame's own definition fixes the level, which is
    what separates a recorded choice from a value nobody ever chose. ``reading``
    is the answer to the comparison the node declared, in the panel tally's own
    words, and it is what the strength is published from: a node with no
    declared comparison reads ``null_unread``, which is the instrument's word
    for a null that cannot be read because nothing said what would have counted
    as an effect.
    """

    produced: bool = True
    instantiated: int = 0
    available: int = 0
    scored: int = 0
    results: int = 0
    forced: bool = False
    reading: str = NULL_UNREAD


@dataclass(frozen=True)
class Declared:
    """A comparison a node declared, and the whole of what the record answers.

    The floor and its answer never travel apart. A builder that fetched one
    without the other is exactly the half-wiring that let nine nodes publish
    ``chosen`` with a floor declared for them, and this is the object that makes
    that impossible to write.

    ``reading`` is what the strength comes from. ``separated`` and ``refusal``
    are kept because a node's sentence about itself says more than one word can:
    which level was the floor, whether the best of the rest cleared it, and
    which comparison the surface declined to answer.
    """

    floor: str | None = None
    separated: bool | None = None
    reading: str = NULL_UNREAD
    refusal: str | None = None


#: Where a node with nothing declared for it stands. Its reading is the
#: instrument's word for a null nobody can read, which is the honest report: the
#: record holds no statement of what this node's levels were supposed to beat.
UNDECLARED = Declared()


def strength_of(edge: Edge) -> str:
    """The one word that says how firmly a decision is held.

    The order of the tests is the argument. Production comes first, because an
    artifact with no producer cannot have levels to compare. A single level
    comes next and can never reach a measurement whatever it scored: with
    nothing to contrast against, a number is a reading and not a separation.
    Power comes before evidence, since whether a comparison could resolve
    anything is a fact about its shape and is settled before a metric is read.
    Only then the reading, which is the one answer this function does not
    compute: it is handed the word the comparison came back in and publishes the
    strength that word states, and refuses a word no strength states.
    """
    if not edge.produced or edge.instantiated == 0:
        return BLOCKED
    if edge.instantiated == 1:
        return CHOSEN if edge.forced else INHERITED
    if edge.scored < 2:
        return UNPOWERED
    return strength_of_reading(edge.reading)


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
    itself beyond the measurement. A node that reached ``indistinguishable``
    still owes one: a measured null is a finding about two named levels, and the
    floor it was read against is not in the word.

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
