"""The two choices a window makes about its own truth, and what they need.

A window is five ids and two rules. The ids say which corpora and which
graphs; the rules say what the delta IS:

* ``truth_profile`` -- whether a t1 term with no node at t0 is dropped by the
  closure (``as_closed``) or replaced by its first live ancestors
  (``projected``);
* ``toi_rule`` -- whether the GAIN may contain anything the pivot can name
  (``pivot_terms``) or only what is live at both ends (``live_in_both_ends``).

Both are part of the window's identity in the database, because both change
the delta, and two windows that differ in either are two different truths.

WHY THIS IS ITS OWN MODULE. Every resolver here reads the SAME thing from the
window -- what its two ends say -- and every one of them has the same trap to
avoid: the native snapshot fields cannot answer it. Keeping them together is
what makes that one paragraph instead of three.
"""

from __future__ import annotations

import uuid
from typing import NamedTuple

from sqlalchemy.orm import Session

from protea.core._binding_rule import PROTEIN_BINDING
from protea.core._evaluation_graph import (
    _bfs_closure,
    _live_terms_with_aspect,
    _load_children_by_go_id,
    _load_parents_by_go_id,
)
from protea.core._truth_projection import Graph
from protea.core._truth_projection import Request as ProjectionRequest
from protea.core._truth_remap import Remap, alias_rows, build_remap
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet

#: How the t1 side's truth is built, by the name a payload asks for it with.
#:
#: ``as_closed`` is what the platform has always done: a term the pivot cannot
#: express is dropped by the closure intersection, and the gain disappears
#: with it. It stays the default, so no stored window moves.
#:
#: ``projected`` adds the lab's rule (see :mod:`protea.core._truth_projection`):
#: a direct t1 term with no node at t0 is replaced by its first live ancestors
#: before the binding rule and the closure. On the 211 -> 227 window the two
#: profiles differ by 646 proteins and 2,610 entries, which is the whole of
#: the difference between PROTEA's truth and the lab's.
TRUTH_PROFILES: tuple[str, ...] = ("as_closed", "projected")

#: Which terms the GAIN is allowed to contain, by the name a payload asks for
#: it with. Same vocabulary as ``run_cafa_evaluation``'s ``toi_rule``, because
#: it is the same rule asked at two moments: there it decides what cafaeval is
#: told to score, here it decides what counts as a gain in the first place.
#:
#: ``pivot_terms`` (the default) is what the platform has always done: the
#: gain is whatever the pivot can name, and the restriction to the terms of
#: interest happens later, at scoring time.
#:
#: ``live_in_both_ends`` restricts it BEFORE classifying, which is what the
#: lab's frame does. The difference is not the figure -- cafaeval discards a
#: protein with no truth inside the terms of interest either way -- it is the
#: COUNTS: a protein whose only gain is a term obsolete by t1 is not a target
#: at all. Measured on 211 -> 227: one protein, ``Q9FNK4``, whose four gains
#: were alive in 2022 and are obsolete in 2025.
GAIN_RULES: tuple[str, ...] = ("pivot_terms", "live_in_both_ends")

#: Which NOT annotations may take truth away, by the name a payload asks for
#: it with.
#:
#: ``any`` is what the platform has always done: ``_load_not_raw_go_ids``
#: reads every NOT row whatever its evidence code, so a ``NOT|IBA`` removes
#: the term and its descendants from a truth whose positives are experimental
#: only. It stays the default, so no stored window moves.
#:
#: ``experimental`` restricts the negation to the thirteen experimental
#: codes, which is what the lab's frame does and what Francisco declared for
#: the long validation window on 2026-10-27: a truth built from experiment
#: should be denied by experiment, and an inferred NOT stays available as an
#: INPUT to the methods instead of removing ground truth.
#:
#: Measured on 211 -> 227 with everything else held fixed -- same remap, same
#: projection, same two graphs -- the two rules are:
#:
#: ===================  =======  ==============  =====
#: counter              ``any``  experimental    lab
#: ===================  =======  ==============  =====
#: delta_proteins        40.534          40.531  40.531
#: lk_proteins            4.669           4.667   4.667
#: projected_entries      2.619           2.610   2.610
#: projected_pairs        3.830           3.821   3.821
#: projected_proteins     2.237           2.228   2.228
#: ===================  =======  ==============  =====
#:
#: NK and PK do not move. Three proteins and nine projected entries is the
#: whole of it, and it is the difference between reproducing the lab's truth
#: exactly and missing it by three.
#:
#: The lab's own choice here was inherited rather than taken -- their store
#: was built from experimental exports and the other codes were never loaded
#: -- which is why this is a declared field and not a constant.
NOT_CODE_RULES: tuple[str, ...] = ("any", "experimental")

class ReconciledWindow(NamedTuple):
    """The five ids a reconciled delta is built from, plus how to build it.

    One object for the same reason :class:`EvalContext` is one: the five
    travel together through every lookup, every refusal and every insert, and
    a signature that took them loose is a signature somebody can call with
    four. The profile joins them because it decides what the delta IS, not
    how it is stored: two profiles over the same five ids are two different
    truths, which is why it is part of the window's identity in the database
    too.
    """

    old_annotation_set_id: uuid.UUID
    new_annotation_set_id: uuid.UUID
    old_native_snapshot_id: uuid.UUID
    new_native_snapshot_id: uuid.UUID
    pivot_snapshot_id: uuid.UUID
    truth_profile: str = TRUTH_PROFILES[0]
    toi_rule: str = GAIN_RULES[0]
    not_codes: str = NOT_CODE_RULES[0]


def _window_end_snapshots(session: Session, window: ReconciledWindow) -> tuple[uuid.UUID, uuid.UUID]:
    """The ontology each end's corpus was loaded under.

    NOT ``old_native_snapshot_id`` / ``new_native_snapshot_id``. Those two do
    two jobs -- which graph closes each side, and which ontologies the window
    spans -- and asking for a closure under t0's graph alone means setting
    both to t0, which erases t1. The 211 -> 227 validation window has that
    done, and reading the native fields there made the projection climb t0's
    graph and project nothing at all.

    Twin of ``_run_cafa_setup._window_end_snapshots``, which answers the same
    question for the terms of interest. That module imports this one.
    """
    ends = []
    for set_id in (window.old_annotation_set_id, window.new_annotation_set_id):
        row = session.get(AnnotationSet, set_id)
        if row is None:
            raise ValueError(
                f"EvaluationSet names an AnnotationSet that is gone ({set_id}); neither "
                "the projection nor the terms of interest can be resolved from its ends"
            )
        ends.append(row.ontology_snapshot_id)
    return ends[0], ends[1]

def _terms_of_interest(session: Session, window: ReconciledWindow) -> set[str]:
    """The ids non-obsolete in the ontology of BOTH ends.

    The same rule ``_run_cafa_setup`` resolves for cafaeval's ``-toi``, asked
    here because the gain has to be restricted BEFORE the delta is
    classified: a protein whose only gain is a term obsolete by t1 is not a
    target, and classifying first would have made it one. On 211 -> 227 this
    is 39,028 terms against the pivot's 47,328.
    """
    old_snapshot, new_snapshot = _window_end_snapshots(session, window)
    old_live, _ = _live_terms_with_aspect(session, old_snapshot)
    new_live, _ = _live_terms_with_aspect(session, new_snapshot)
    return old_live & new_live

def _remap_of_t0(session: Session, window: ReconciledWindow) -> Remap:
    """The t0 snapshot's own statements about ids it no longer uses as primary.

    FROM t0 AND ONLY t0, which is the lab's answer and the one that makes
    sense: the question is what a t1 id is called in the space the window is
    scored in. t1's own alias statements are about t1's space and would
    answer a question nobody asked.

    REFUSES WITHOUT THE MAP, and that refusal is what keeps the profile name
    honest. ``projected`` means "remapped, then projected"; a build that
    found no alias rows would silently mean "projected only", and the same
    four letters would name two different truths in two different windows.
    Load them with ``load_ontology_aliases``, which reads the ARCHIVED OBO
    and verifies its sha256.
    """
    old_snapshot, _ = _window_end_snapshots(session, window)
    rows = alias_rows(session, old_snapshot)
    if not rows:
        raise ValueError(
            f"ontology snapshot {old_snapshot} has no rows in go_term_alias, so a "
            "term obsolete at t0 with a published successor cannot be told from one "
            "with none. Run load_ontology_aliases on it: without the map this "
            "profile would quietly mean 'projected without remapping', which is a "
            "different truth under the same name"
        )
    live, _ = _live_terms_with_aspect(session, old_snapshot)
    return build_remap(rows, live)


def _projection_request(session: Session, window: ReconciledWindow) -> ProjectionRequest:
    """Which graph the projection climbs, and which terms it may land on.

    READ FROM THE ANNOTATION SETS, NOT FROM THE WINDOW'S NATIVE FIELDS, and
    this is not a stylistic preference: it is the defect this function exists
    to avoid, measured.

    ``old_native_snapshot_id`` and ``new_native_snapshot_id`` do two jobs.
    They say which graph closes each side, and they are the only other record
    of which ontologies the window spans. Asking for a closure under t0's
    graph alone -- supported, and done on the 211 -> 227 validation window --
    means setting BOTH to t0, which erases t1.

    The first wiring of this used them, and on that window it climbed the
    2022-07-01 graph while believing it was climbing t1's. Asking "is this
    term live at t1" of t0's graph answers no for every term t0 does not
    have, so all 2,619 candidates were classified as obsolete-at-t1, nothing
    projected at all, and the populations did not move. The lab projects
    2,610 of those and remaps the other 9 through ``replaced_by``, which is
    how the number told us which half was wrong: the terms were identified
    correctly and the graph was not.

    ``AnnotationSet.ontology_snapshot_id`` is fixed when the corpus is
    loaded and is independent of any later closure choice, so it answers the
    question the native fields were overloaded with. Same reason, same fix
    and same regression test as the terms of interest in
    ``_run_cafa_setup._window_end_snapshots``.
    """
    old_snapshot, new_snapshot = _window_end_snapshots(session, window)
    target, _ = _live_terms_with_aspect(session, old_snapshot)
    live, aspect = _live_terms_with_aspect(session, new_snapshot)
    return ProjectionRequest(
        graph=Graph(
            parents=_load_parents_by_go_id(session, new_snapshot),
            aspect=aspect,
            live=live,
            binding=frozenset(
                _bfs_closure({PROTEIN_BINDING}, _load_children_by_go_id(session, new_snapshot))
            ),
        ),
        target=target,
        remap=_remap_of_t0(session, window),
        # Resolved here, with the graph and the landings, because all three
        # come from the SAME two ends and the trap is the same for all three:
        # the native fields cannot answer which ontologies the window spans.
    )


def _keep_within_toi(
    new_by_ns: dict[str, dict[str, set[str]]],
    old_by_ns: dict[str, dict[str, set[str]]],
    toi: set[str],
) -> dict[str, dict[str, set[str]]]:
    """``kept = (new & toi) | (new & old)``, per protein and aspect.

    The lab's rule, from ``sfjepa.world.truth.single_truth``: "only the gains
    are cut: the categories come from the whole known state at q, and removed
    stays old - new". Three consequences, and each one is a decision:

    * only the GAIN is restricted. A protein whose only gain is a term
      obsolete by t1 stops being a target, which is the one protein
      (``Q9FNK4``) between PROTEA's delta and the lab's that is not a
      ``replaced_by`` case;
    * the CLASSIFICATION still sees t0 whole. A term outside the terms of
      interest that the protein already had at t0 keeps its aspect non-empty,
      so the protein is PK and not LK. That is why ``old_by_ns`` is read here
      and not trimmed;
    * a term already known at t0 stays in ``kept`` even when it is outside
      the terms of interest, so ``removed = old - new`` does not acquire
      losses that the restriction invented.

    ``known`` and ``pk_known`` are not touched at all: they are ``old``, and
    cafaeval's own ``-toi`` restricts them at scoring time.
    """
    kept: dict[str, dict[str, set[str]]] = {}
    for protein, ns_map in new_by_ns.items():
        old_ns = old_by_ns.get(protein, {})
        trimmed: dict[str, set[str]] = {}
        for ns, terms in ns_map.items():
            survivors = (terms & toi) | (terms & old_ns.get(ns, set()))
            if survivors:
                trimmed[ns] = survivors
        if trimmed:
            kept[protein] = trimmed
    return kept


__all__ = [
    "GAIN_RULES",
    "TRUTH_PROFILES",
    "ProjectionRequest",
    "ReconciledWindow",
]
