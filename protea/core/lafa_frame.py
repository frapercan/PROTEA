"""LAFA's frame: a window's truth built the way LAFA builds its own windows.

THE REFERENCE. LAFA publishes the code that builds its windows: the workflow in
``github.com/anphan0828/CAFA_forever`` (``modules/local/timepoint_build.nf`` and
``modules/local/evaluation.nf``) and the package it calls,
``github.com/anphan0828/democafa_package``, read on 2026-11-20 at the commits
:data:`LAFA_WORKFLOW` and :data:`LAFA_PACKAGE` name. This module follows that
workflow step by step on PROTEA's own tables. It is written from the reading
and copies nothing: the package carries no licence.

EACH END ALONE, UNDER ITS OWN ONTOLOGY (``timepoint_build.nf``):

1. the experimental rows: the thirteen codes, and no row whose qualifier says
   ``NOT`` (``retrieve_terms -sgc 'Experimental,IC,TAS'``);
2. every id as that end's ontology names it: a secondary id (``alt_id``) becomes
   its primary, then an obsolete term with a ``replaced_by`` becomes its
   successor, and what is still not a live term of that ontology is dropped;
3. propagated to its ancestors through ``is_a`` and ``part_of`` inside one
   aspect (``propagate_and_ia``).

THE WINDOW (``evaluation.nf``, ``classify_ground_truth``):

4. the terms of interest are the ids live in both ontologies, and BOTH ends are
   trimmed to them before anything is compared;
5. a protein whose trimmed F side at t1 holds nothing but "protein binding" and
   its ancestors in the t0 ontology loses that F side; t0 is left alone;
6. a gain is a t1 term the protein did not hold at t0. A protein with nothing
   at t0 is NK and every t1 term of it is truth. Otherwise, per aspect with
   gains: LK when t0 held nothing in that aspect, PK when it did, and PK's
   known terms are the trimmed t0 terms of that aspect.

The targets, Swiss-Prot at both ends with the same sequence, are the window's
population (``swissprot_same_sequence``, :mod:`protea.core._window_population`),
which the operation applies after this module as it applies every population:
the classification is per protein, so restricting before or after it is the
same.

ONE DEPARTURE, THE AUTHOR'S (2026-11-20). LAFA drops a ``NOT`` row and keeps
every positive annotation the row contradicts. Here a ``NOT`` row takes truth
away: its term and every term under it leave the DIRECT annotations of both
ends before step 3, so the ancestors that only a negated annotation supported
never enter the truth either. Which ``NOT`` rows count is the window's
``not_codes``; the named frame says ``experimental``. PROTEA's reconciled path
negates after its closure instead, which keeps those ancestors; see
:func:`protea.core.evaluation.compute_evaluation_data_reconciled`.
"""

from __future__ import annotations

import uuid
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from typing import Any, NamedTuple

from sqlalchemy.orm import Session

from protea.core._binding_rule import BindingStep, drop_cafa_binding_only_f
from protea.core._lafa_frame_data import EndOntology, load_end_ontology, load_end_rows
from protea.core.evaluation import EvaluationData, _classify_protein_deltas

#: The two commits this module was written from. Stamped on every window it
#: builds, so a reader knows which version of LAFA's rules made the truth.
LAFA_WORKFLOW = "github.com/anphan0828/CAFA_forever@2ce245715aa9"
LAFA_PACKAGE = "github.com/anphan0828/democafa_package@1102e34ec2cd"

#: The profile and the gain rule only this frame builds, and the binding rule
#: it applies (:mod:`protea.core._truth_rules`, :mod:`protea.core._binding_rule`).
PROFILE = "own_graph"
GAIN_RULE = "both_ends_trimmed"
BINDING_RULE = "cafa"

PROTEIN_BINDING = "GO:0005515"

#: Step 5 runs at t1, on the propagated and trimmed F side.
_AT_T1 = BindingStep(on_direct_terms=False, on_propagated_f=True)

#: ``{protein: {aspect: {go_id}}}``, the shape of one end's truth.
ByAspect = dict[str, dict[str, set[str]]]


class _Walks:
    """Ancestors and descendants in one ontology, each term walked once.

    A visited set, not recursion: one GO release (2024-06-10) has cycles, and
    a walk has to end on it as on every other.
    """

    def __init__(self, ontology: EndOntology) -> None:
        self._up: Mapping[str, Iterable[str]] = ontology.parents
        down: dict[str, set[str]] = defaultdict(set)
        for child, parents in ontology.parents.items():
            for parent in parents:
                down[parent].add(child)
        self._down: Mapping[str, Iterable[str]] = down
        self._cache: dict[tuple[bool, str], frozenset[str]] = {}

    def ancestors(self, go_id: str) -> frozenset[str]:
        """``go_id`` and every term above it."""
        return self._walk(go_id, upward=True)

    def descendants(self, go_id: str) -> frozenset[str]:
        """``go_id`` and every term under it."""
        return self._walk(go_id, upward=False)

    def _walk(self, start: str, *, upward: bool) -> frozenset[str]:
        found = self._cache.get((upward, start))
        if found is None:
            edges = self._up if upward else self._down
            seen = {start}
            stack = [start]
            while stack:
                for step in edges.get(stack.pop(), ()):
                    if step not in seen:
                        seen.add(step)
                        stack.append(step)
            found = self._cache[(upward, start)] = frozenset(seen)
        return found


def named(
    ontology: EndOntology, go_ids: Iterable[str], counts: Counter[str], kind: str
) -> set[str]:
    """Step 2: each id as this end's ontology names it; what it cannot name is dropped.

    The two renamings run in LAFA's order and one step each: ``alt_id`` first,
    then ``replaced_by``. A secondary id of an obsolete term therefore reaches
    the successor, and a successor that is itself obsolete is dropped, not
    followed. ``counts`` gains one per renaming and per drop, under ``kind``.
    """
    out: set[str] = set()
    for raw in go_ids:
        go_id = ontology.alt_ids.get(raw, raw)
        if go_id != raw:
            counts[f"{kind}_renamed_alt_id"] += 1
        successor = ontology.replaced_by.get(go_id)
        if successor is not None:
            go_id = successor
            counts[f"{kind}_renamed_replaced_by"] += 1
        if go_id in ontology.live:
            out.add(go_id)
        else:
            counts[f"{kind}_dropped_not_live"] += 1
    return out


def _direct_terms(
    ontology: EndOntology,
    walks: _Walks,
    positives: Mapping[str, Iterable[str]],
    negations: Mapping[str, Iterable[str]],
    counts: Counter[str],
    end: str,
) -> dict[str, set[str]]:
    """Steps 1 and 2 at one end, then the author's negation: each protein's direct terms."""
    out: dict[str, set[str]] = {}
    for protein, raw_ids in positives.items():
        direct = named(ontology, raw_ids, counts, f"{end}_positive")
        denied = negations.get(protein)
        if denied:
            negated: set[str] = set()
            for go_id in named(ontology, denied, counts, f"{end}_not"):
                negated |= walks.descendants(go_id)
            if direct & negated:
                counts[f"{end}_negated_proteins"] += 1
                counts[f"{end}_negated_direct_terms"] += len(direct & negated)
                direct -= negated
        if direct:
            out[protein] = direct
    return out


def _propagated(ontology: EndOntology, walks: _Walks, direct: Mapping[str, set[str]]) -> ByAspect:
    """Step 3: every direct term and its ancestors, by aspect."""
    truth: ByAspect = {}
    for protein, terms in direct.items():
        by_aspect: dict[str, set[str]] = defaultdict(set)
        for go_id in terms:
            for term in walks.ancestors(go_id):
                by_aspect[ontology.live[term]].add(term)
        truth[protein] = dict(by_aspect)
    return truth


def end_truth(
    ontology: EndOntology,
    positives: Mapping[str, Iterable[str]],
    negations: Mapping[str, Iterable[str]],
    counts: Counter[str],
    end: str,
) -> ByAspect:
    """Steps 1 to 3 at one end, with the author's negation before the propagation.

    ``positives`` are the end's experimental rows without ``NOT``, by protein;
    ``negations`` the ``NOT`` ids of BOTH ends, by protein, which this end names
    under its own ontology before taking each one and everything under it out
    of the protein's direct terms.
    """
    walks = _Walks(ontology)
    return _propagated(
        ontology, walks, _direct_terms(ontology, walks, positives, negations, counts, end)
    )


def end_state(
    ontology: EndOntology,
    positives: Mapping[str, Iterable[str]],
    negations: Mapping[str, Iterable[str]],
    counts: Counter[str],
    end: str,
) -> tuple[ByAspect, dict[str, set[str]]]:
    """One end as a state: its truth, as :func:`end_truth` builds it, and what each protein is denied.

    The denied terms are the ones the negation takes out: each ``NOT`` id of the
    protein named under this end's ontology, and every term under it. A truth
    needs only the first half; a state that a method fits on or reads needs
    both, so it never gives a protein a term the end says it does not have.
    """
    walks = _Walks(ontology)
    truth = _propagated(
        ontology, walks, _direct_terms(ontology, walks, positives, negations, counts, end)
    )
    denied: dict[str, set[str]] = {}
    for protein, go_ids in negations.items():
        # Named again with a counter of its own: _direct_terms has already
        # counted the renamings of every protein that holds a positive term.
        terms = {
            term
            for go_id in named(ontology, go_ids, Counter(), f"{end}_not")
            for term in walks.descendants(go_id)
        }
        if terms:
            denied[protein] = terms
    return truth, denied


def _made_by_the_ontology(
    pk: Mapping[str, set[str]],
    direct_t0: Mapping[str, set[str]],
    direct_t1: Mapping[str, set[str]],
    walks_t1: _Walks,
) -> Counter[str]:
    """PK gains no new direct annotation explains: made by an edit to the ontology alone.

    A gain is explained when it is an ancestor, at t1, of a direct term the
    protein did not hold at t0. Counted and kept, as LAFA keeps them; the
    frame's page lists this among the known flaws.
    """
    # Both keys on every window, zero included: a missing count reads as "not measured".
    found: Counter[str] = Counter(
        {"pk_pairs_made_by_the_ontology": 0, "pk_proteins_made_by_the_ontology": 0}
    )
    for protein, gains in pk.items():
        explained: set[str] = set()
        for go_id in direct_t1.get(protein, set()) - direct_t0.get(protein, set()):
            explained |= walks_t1.ancestors(go_id)
        unexplained = gains - explained
        found["pk_pairs_made_by_the_ontology"] += len(unexplained)
        if unexplained == gains:
            found["pk_proteins_made_by_the_ontology"] += 1
    return found


def terms_of_interest(t0: EndOntology, t1: EndOntology) -> frozenset[str]:
    """Step 4's terms: the ids live in both ontologies."""
    return frozenset(t0.live.keys() & t1.live.keys())


def trimmed(truth: ByAspect, keep: frozenset[str]) -> ByAspect:
    """Step 4's trim for one end: every term outside ``keep`` leaves, then every empty aspect."""
    out: ByAspect = {}
    for protein, by_aspect in truth.items():
        kept = {aspect: inside for aspect, terms in by_aspect.items() if (inside := terms & keep)}
        if kept:
            out[protein] = kept
    return out


def binding_terms(t0: EndOntology) -> frozenset[str]:
    """Step 5's set: "protein binding" and its ancestors in the t0 ontology.

    LAFA reads it the same way, from its pivot graph, as
    ``nx.descendants(F, "GO:0005515")`` on an obonet graph whose edges run from
    child to parent, which returns the ancestors: "binding" and the
    molecular-function root, the three of :data:`protea.core._binding_rule.CAFA_BINDING_TERMS`.
    """
    if PROTEIN_BINDING not in t0.live:
        raise ValueError(
            f"{PROTEIN_BINDING} is not a live term of the t0 ontology, so LAFA's binding "
            "rule has no set to test the F side against"
        )
    return _Walks(t0).ancestors(PROTEIN_BINDING)


class LafaInputs(NamedTuple):
    """What the frame reads from the store: both ontologies and both ends' rows."""

    t0: EndOntology
    t1: EndOntology
    t0_positives: Mapping[str, set[str]]
    t1_positives: Mapping[str, set[str]]
    #: ``NOT`` ids of both ends, unioned per protein: a term a protein is known
    #: not to have at either end is truth at neither.
    negations: Mapping[str, set[str]]


def _pairs(truth: ByAspect) -> int:
    return sum(len(terms) for by_aspect in truth.values() for terms in by_aspect.values())


def lafa_truth(inputs: LafaInputs) -> tuple[EvaluationData, dict[str, Any]]:
    """Steps 1 to 6 on what the store holds, and the counts of what each step did."""
    counts: Counter[str] = Counter()
    walks = _Walks(inputs.t0), _Walks(inputs.t1)
    direct = (
        _direct_terms(inputs.t0, walks[0], inputs.t0_positives, inputs.negations, counts, "t0"),
        _direct_terms(inputs.t1, walks[1], inputs.t1_positives, inputs.negations, counts, "t1"),
    )
    old = _propagated(inputs.t0, walks[0], direct[0])
    new = _propagated(inputs.t1, walks[1], direct[1])
    toi = terms_of_interest(inputs.t0, inputs.t1)
    for end, truth in (("t0", old), ("t1", new)):
        counts[f"{end}_pairs_before_trim"] = _pairs(truth)
    old, new = trimmed(old, toi), trimmed(new, toi)
    for end, truth in (("t0", old), ("t1", new)):
        counts[f"{end}_pairs_after_trim"] = _pairs(truth)
    binding_dropped = drop_cafa_binding_only_f(new, _AT_T1, binding_terms(inputs.t0))
    nk, lk, pk, pk_known, removed = _classify_protein_deltas(old, new)
    counts.update(_made_by_the_ontology(pk, direct[0], direct[1], walks[1]))
    data = EvaluationData(
        nk=nk,
        lk=lk,
        pk=pk,
        known={protein: set().union(*by_aspect.values()) for protein, by_aspect in old.items()},
        pk_known=pk_known,
        removed=removed,
        binding_dropped={"old_exp": 0, "new_exp": binding_dropped},
    )
    stats = {
        "lafa_workflow": LAFA_WORKFLOW,
        "lafa_package": LAFA_PACKAGE,
        "terms_of_interest": len(toi),
        **{f"lafa_{name}": count for name, count in sorted(counts.items())},
    }
    return data, stats


class LafaWindow(NamedTuple):
    """The window a LAFA build reads: its two corpora, the ontology of each, the asked rules."""

    old_set_id: uuid.UUID
    new_set_id: uuid.UUID
    old_native: uuid.UUID
    new_native: uuid.UUID
    truth_profile: str
    toi_rule: str
    not_codes: str
    binding_rule: str


def refuse_unless_lafa(window: LafaWindow) -> None:
    """Refuse a window that asks this frame for a rule it does not apply."""
    expected = {"truth_profile": PROFILE, "toi_rule": GAIN_RULE, "binding_rule": BINDING_RULE}
    wrong = {
        name: getattr(window, name)
        for name, rule in expected.items()
        if getattr(window, name) != rule
    }
    if wrong:
        raise ValueError(f"LAFA's frame applies {expected}; this window asks for {wrong}")


def compute_evaluation_data_lafa(
    session: Session, window: LafaWindow
) -> tuple[EvaluationData, dict[str, Any]]:
    """LAFA's truth of ``window``, read from the store, and the counts of each step."""
    refuse_unless_lafa(window)
    experimental_only = window.not_codes == "experimental"
    t0_positives, t0_negations = load_end_rows(session, window.old_set_id, experimental_only)
    t1_positives, t1_negations = load_end_rows(session, window.new_set_id, experimental_only)
    negations: dict[str, set[str]] = defaultdict(set)
    for side in (t0_negations, t1_negations):
        for protein, go_ids in side.items():
            negations[protein] |= go_ids
    inputs = LafaInputs(
        load_end_ontology(session, window.old_native),
        load_end_ontology(session, window.new_native),
        t0_positives,
        t1_positives,
        dict(negations),
    )
    return lafa_truth(inputs)


__all__ = [
    "BINDING_RULE",
    "GAIN_RULE",
    "LAFA_PACKAGE",
    "LAFA_WORKFLOW",
    "PROFILE",
    "EndOntology",
    "LafaInputs",
    "LafaWindow",
    "binding_terms",
    "compute_evaluation_data_lafa",
    "end_truth",
    "lafa_truth",
    "named",
    "refuse_unless_lafa",
    "terms_of_interest",
    "trimmed",
]
