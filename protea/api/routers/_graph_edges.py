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
# enums that own them: a surface keeping its own copy of somebody else's words
# drifts from them silently. The served process already holds this module, since
# the operation catalog imports it, so the reach costs nothing at import.
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
INDISTINGUISHABLE = "indistinguishable"
CHOSEN = "chosen"
INHERITED = "inherited"
UNPOWERED = "unpowered"
#: A level with no producer. What ends it is rows.
BLOCKED = "blocked"
#: A node whose artifact the SHAPE of the record forbids, so nothing the record
#: comes to hold moves it.
#:
#: THE SEVENTH WORD, AND WHY IT IS A WORD. Four builders printed ``blocked``
#: while meaning this. Their edges were constructed with ``instantiated=0``
#: written in, so ``strength_of`` answered on its first test and returned
#: ``blocked`` before it read anything, and the reasons printed beside it
#: carried live counts -- interpro rows, feature families, flows -- that the
#: word would have printed the same beside a million of. A reader could not tell
#: NO DATA YET from NO DATA WOULD DO, and the two send them to different work:
#: one to run something, the other to write a migration.
#:
#: It is ONE END of that repair and not the whole of it. Where a node has a row
#: it can count, the fix is to count it and let ``blocked`` mean what it says;
#: this word is for the nodes whose artifact has nowhere to be written at all,
#: where there is no row to count and never will be until the schema moves.
INEXPRESSIBLE = "inexpressible"

#: The two words that say a node has no artifact to show. Together because every
#: surface that lists what cannot answer has to list both, and two words because
#: they ask the reader for two different things.
BLOCK_WORDS: tuple[str, ...] = (BLOCKED, INEXPRESSIBLE)


class StaleStructuralBlock(ValueError):
    """A node says its artifact cannot be written while the record holds one.

    Raised rather than rendered. The whole point of ``expressible`` is that it
    is READ from the catalog and never asserted; the moment a builder claims the
    shape forbids an artifact and hands over instantiated levels of it, one of
    the two came from a constant that outlived the schema, and publishing either
    is the defect the field was added to end.
    """

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
#: what makes this a table a test can walk: add a seventh bucket to the tally and
#: the walk over ``TALLY_KEYS`` fails instead of the surface inventing a word for
#: it.
#:
#: FOUR READINGS PUBLISH AS ``chosen`` AND THAT IS NOT THE COLLAPSE THIS FIXES.
#: A strength says how firmly a decision is held. A comparison nobody declared,
#: one whose precondition was refused, one no panel could answer and one whose
#: panels were too thin to answer all leave a level standing with nothing
#: established about it, which is what ``chosen`` means; what separates them is
#: WHY nothing was established, and that is the node's reason to give, and the
#: business of ``floor``, ``separated`` and ``floor_refusal``, not the scale's.
#: The distinction the scale itself had to carry is the one it could not: a null
#: read against declared power is a finding about two levels, and
#: ``indistinguishable`` is that finding.
STRENGTH_OF_READING: dict[str, str] = {
    RESOLVED: MEASURED,
    NULL_WITH_POWER: INDISTINGUISHABLE,
    NULL_UNREAD: CHOSEN,
    UNDERPOWERED: CHOSEN,
    REFUSED: CHOSEN,
    NOT_COMPUTED: CHOSEN,
}

#: The two readings that state what verdict the floor got, and the verdict each
#: one states. The other four say the comparison was never read against a floor
#: at all, so they constrain nothing. Used by :meth:`Edge.__post_init__` to
#: refuse an edge whose word and whose verdict disagree.
_VERDICT_OF_READING: dict[str, bool] = {RESOLVED: True, NULL_WITH_POWER: False}


def refuse_unstateable_readings(table: Mapping[str, str], counted: Iterable[str]) -> None:
    """Refuse a scale that cannot state every reading the instrument counts.

    Called at import below, so a seventh bucket in the tally stops this module
    loading rather than reaching a reader relabelled: a scale that cannot state
    its own vocabulary has no business serving it.

    It is a NAMED FUNCTION and not the bare ``if`` it could have been because a
    refusal that only ever happens at import is a refusal no test has seen raise.
    Reproducing it needs a fabricated tally and a module reload, so the check
    would go untested while reading as though it were covered, which is the same
    trap as a guard that logs. Called with the real pair one line below, so the
    happy path is not a claim either.

    Both directions, because each is a different defect. A counted reading with
    no strength is the one that relabels a finding. A strength named for a
    reading nothing counts is a word this surface can never be asked for, and it
    would sit in the table looking like coverage.
    """
    tally, scale = set(counted), set(table)
    if scale == tally:
        return
    raise ValueError(
        "the panel tally and the firmness scale disagree about which readings exist. "
        f"Counted with no strength to publish as: {sorted(tally - scale)}. "
        f"Named here and never counted: {sorted(scale - tally)}. "
        "A reading with no strength is published as the strength that happens to be left, "
        "which is how a measured null became a recorded choice."
    )


refuse_unstateable_readings(STRENGTH_OF_READING, TALLY_KEYS)


@dataclass(frozen=True)
class DeclaredFloor:
    """A floor the record declares, and the word the panels answered it in.

    Two fields and not four. The VERDICT and the REFUSAL are not here because
    they are still read per node by ``_graph_nodes._separation``, exactly where
    the change that published them put them; what has to be computed once, by
    the caller, is the READING, because deciding whether a panel had the power to
    resolve the declared effect needs the panel populations and those are counted
    from the window's own ground truth by the endpoint. Handing them to ten
    builders instead would be one more argument a future node can be written
    without, and a node that cannot see a declared floor is how nine of these ten
    published ``chosen`` with one declared for them until 2026-09-02.
    """

    floor: str | None = None
    reading: str = NULL_UNREAD


#: Where a node with nothing declared for it stands. Its reading is the
#: instrument's word for a null nobody can read, which is the honest report: the
#: record holds no statement of what this node's levels were supposed to beat.
UNDECLARED = DeclaredFloor()


@dataclass(frozen=True)
class Edge:
    """Everything the record says about one node's decision.

    ``produced`` is false when the node's artifact has no producer at all.
    ``forced`` is true when the frame's own definition fixes the level, which is
    what separates a recorded choice from a value nobody ever chose. ``floor``
    is the level a declared comparison measures against, ``separated`` whether
    the comparison cleared it, and ``refusal`` the text of the refusal when the
    comparison could not be asked at all. The three travel together because a
    ``separated`` of None means two different things -- nothing was declared, or
    what was declared was refused -- and only the third says which.

    ``reading`` is the word the panel tally answered the declared comparison in,
    and it is the one thing here the strength cannot be derived from the other
    fields: ``separated`` false is a yes-or-no, and its no is several facts that
    only the tally tells apart. It sits BESIDE the three above and does not
    replace any of them. They report the comparison; it reports the answer.

    ``expressible`` is the one field here about the SHAPE of the record rather
    than its content, and the only one no key of the response carries: nothing
    reads it but :func:`strength_of`. It is false only where the catalog
    positively forbids the artifact -- a NOT NULL column that leaves it nowhere
    to be written -- and it exists because four nodes reported a blocked edge
    from a literal zero, printing live counts beside a word no number of rows
    could have changed. It must be READ, never asserted: a builder that
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
    refusal: str | None = None
    reading: str = NULL_UNREAD
    expressible: bool = True

    def __post_init__(self) -> None:
        """Refuse an edge that carries a verdict and a refusal at the same time.

        Raised rather than reconciled, because there is nothing to reconcile: a
        comparison that was refused HAS no verdict, and a verdict means nothing
        refused, so an edge holding both asks a reader to choose which half of
        its own payload to believe. Checked at construction because that is the
        last point before ``strength_of`` reads the pair and ``_node`` publishes
        it, and a node that reached the response holding both would be the same
        unreadable answer that publishing the refusal was written to end.

        A refusal with no floor is refused for the same reason: a refusal is an
        account of one declared comparison, and where nothing was declared there
        is no comparison for it to be an account of.

        THE READING IS HELD TO THE SAME RULE, which is why this guard was
        extended rather than joined by a second one. ``resolved`` and
        ``null_with_power`` are the two words that state what verdict the floor
        got, so an edge carrying either of them alongside a floor that was never
        declared, or a verdict that says the opposite, is the same unreadable
        payload in a third key: it would publish a finding about a comparison
        whose own record denies it. A word the tally does not count is refused
        here too, so ``strength_of`` never has to answer one with the nearest
        strength to hand -- a default there does not lose a reading, it relabels
        it, and a relabelled reading is exactly the sixth word's original defect.
        """
        if self.refusal is not None and self.separated is not None:
            raise ValueError(
                f"an edge cannot hold both a verdict ({self.separated}) and a refusal "
                f"({self.refusal!r}). A refused comparison has no verdict and a verdict "
                "means nothing was refused; publishing both leaves a reader to guess "
                "which of the two the number behind this node came from."
            )
        if self.refusal is not None and self.floor is None:
            raise ValueError(
                f"an edge holds the refusal {self.refusal!r} and no floor. A refusal is "
                "an account of one declared comparison, so with no floor declared there "
                "is nothing it can be an account of."
            )
        if self.reading not in STRENGTH_OF_READING:
            raise ValueError(
                f"reading {self.reading!r} is not one of the panel tally's words "
                f"({', '.join(TALLY_KEYS)}), so no strength states it. Publishing the "
                "nearest one would report a comparison that was never read as one that was."
            )
        stated = _VERDICT_OF_READING.get(self.reading)
        if stated is not None and (
            self.floor is None or self.separated is None or bool(self.separated) is not stated
        ):
            raise ValueError(
                f"an edge reads {self.reading!r}, which states that a declared floor was "
                f"{'cleared' if stated else 'not cleared'}, and reports floor="
                f"{self.floor!r} separated={self.separated}. The word and the verdict are "
                "two reports of one comparison; an edge that lets them disagree publishes "
                "a finding the rest of its own payload denies."
            )


def strength_of(edge: Edge) -> str:
    """The one word that says how firmly a decision is held.

    The order of the tests is the argument. Expressibility comes first, ahead
    even of production, because it is the only test about the SHAPE of the
    record instead of its content: a node whose artifact has nowhere to be
    written is not waiting for a producer to be run, and telling a reader it is
    sends them after work no run can do. Production comes next, because an
    artifact with no producer cannot have levels to compare. A single level
    comes after that and can never reach a measurement whatever it scored: with
    nothing to contrast against, a number is a reading and not a separation.
    Power comes before evidence, since whether a comparison could resolve
    anything is a fact about its shape and is settled before a metric is read.

    The refusal at the top fires before any word is produced. The next mistake
    here is a migration that makes an artifact storable, a builder that counts
    the rows, and a structural claim beside it nobody re-read: that would print
    ``inexpressible`` next to a live count of the thing it says cannot exist. It
    sits here rather than in :meth:`Edge.__post_init__` beside the other three
    because ``expressible`` reaches no key of the response: this is its reader.

    WHERE THE SIXTH WORD ENTERS, AND WHY IT LEAVES THAT ORDER MEANING WHAT IT
    MEANT. It enters LAST, on the no side of the threshold test, and nowhere
    earlier. The tests ahead of it are unchanged and still settle shape,
    production, contrast and power before any metric is read, and the reading is
    never consulted until they have all passed and the floor has been asked and
    answered. So it cannot move a node that has no producer, no contrast, no
    second scored level or no declared floor, and it cannot turn a separation
    into anything but ``measured``: it is a different answer to the last question
    asked here, not a new question inserted before it.

    What it refines is the NO of that last test. ``separated`` false was the
    whole of it, and its no was several facts the panel tally has told apart
    since it was written: the panels could have shown the difference this project
    acts on and did not, they could not have shown it, or no panel carried both
    sides. All of them published as ``chosen``, which is also what a node nobody
    declared a floor for publishes as, so a null the campaign measured was
    indistinguishable from a question nobody asked. The table says which reading
    publishes as what, and it is the authority rather than a document beside one:
    this line is the only place a strength is read out of it.
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
    if edge.separated:
        return MEASURED
    return STRENGTH_OF_READING[edge.reading]


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
    account of itself beyond the measurement. A node the record cannot express
    owes one most of all: what it is waiting for is a migration, and a reader
    not told that goes and produces rows that could not be stored. A node that
    reached ``indistinguishable`` still owes one too: a measured null is a
    finding about two named levels, and which floor it was read against is not
    in the word.

    ``held`` is the value the node currently stands at, named field by field.
    A strength says how firmly a decision is held and says nothing about what
    was decided, and a reader who cannot see the value cannot tell an inherited
    default from a deliberate choice that happens to be unmeasured. Both read
    ``inherited`` and only one of them is a surprise.

    ``floor``, ``separated`` and ``floor_refusal`` are the comparison the
    strength was decided on, which the strength itself cannot report. ``chosen``
    is a sink of four situations: a single level the frame's own definition
    fixed, a powered contrast with no floor declared for it, one whose declared
    floor REFUSED to be compared, and one that was compared and did not
    separate. Until these three keys existed the response held nothing that told
    them apart, and the refusal in particular reached nobody: it is caught so
    the page keeps serving, and a caught refusal that is not published is a
    silent None, which is the failure this surface has already been burnt by.
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
        "floor": edge.floor,
        "separated": edge.separated,
        "floor_refusal": edge.refusal,
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
