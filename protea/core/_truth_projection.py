"""Project a t1 term with no node at t0 onto its first live ancestors.

THE PROBLEM THIS SOLVES. A temporal window is scored in one ontology, the
one the pivot names, and the terms a protein gained at t1 are not all
expressible in it: the ontology moved between the two ends. Today those terms
are simply dropped, by the ``closure & pivot_go_ids`` intersection, and the
annotation disappears with them. The protein looks like it gained nothing
where it gained something the older ontology could only say less precisely.

THE RULE, AS THE LAB STATES IT (``sfjepa.world.truth.first_live_ancestors`` /
``project_sets``, decision 25), and it is narrow on purpose:

* only a DIRECT, experimental, non-NOT annotation of the t1 side projects.
  NOT terms never do, and neither does an inherited one: the ancestors are
  already there by the True Path Rule;
* only a term that is LIVE at t1 and has NO node at t0. A term alive at both
  ends needs no projection, and one already obsolete at t1 is not a gain the
  window should carry;
* climb the parents of the T1 graph, never the pivot's. The edges that
  explain where the new term sits are the ones the new ontology has;
* same ASPECT only. A cross-aspect parent would move an annotation between
  the three CAFA tasks, which are scored separately;
* stop at the FIRST live term on each path, and take ALL of them. Not the
  nearest one: a term can descend from two live ancestors by different
  routes, and keeping one of the two would be a choice nothing justifies.
  Not every live ancestor either, because the ones above a stop arrive by the
  True Path Rule anyway;
* if no path reaches a live term, DISCARD and count. A projection with no
  landing is a gain the old ontology cannot express at all, and inventing one
  would be worse than losing it.

The projected ancestors replace the new term as DIRECT annotations, so they
then pass through the binding rule and the closure exactly like any other
direct annotation. There is deliberately no special case for an ancestor the
protein already had at t0: the delta subtracts the two sides afterwards, and
a term present at both ends vanishes there. Writing that case here would
duplicate the subtraction in a second place.

WHAT IT CANNOT DO, AND IT IS MEASURED. The lab also REMAPS obsolete terms
through ``alt_id`` and ``replaced_by`` before asking whether a node exists at
t0, and PROTEA records neither: ``go_term`` carries ``go_id``, ``name``,
``aspect``, ``definition`` and ``is_obsolete``, and nothing else. On the
211 -> 227 window their own counters say ``new_exp_replaced_by`` is 9, so the
residue is 9 entries this projection will treat as having no node when the
lab finds one through a successor. That is a declared difference, not a
hidden one, and closing it needs the two columns.
"""

from __future__ import annotations

from typing import NamedTuple


class Graph(NamedTuple):
    """The t1 side's ontology, as the projection needs to read it.

    One object because the four parts have to come from the SAME snapshot. The
    first version took them loose and the aspect map arrived from the pivot
    while the parents came from t1, which silently restricted the climb by the
    wrong aspects for every term whose aspect had changed.
    """

    #: ``{child_go_id: {parent_go_id}}`` over ``is_a`` and ``part_of``.
    parents: dict[str, set[str]]
    #: ``{go_id: aspect}`` for the same snapshot.
    aspect: dict[str, str]
    #: The non-obsolete ids of the same snapshot: what "live at t1" means.
    live: set[str]
    #: The descendant closure of ``GO:0005515``, for the binding counter.
    binding: frozenset[str] = frozenset()


class Projection(NamedTuple):
    """What the projection did, in numbers that can be compared with the lab's."""

    #: (protein, term) entries that were projected away.
    entries: int = 0
    #: (protein, ancestor) pairs the projection produced.
    pairs: int = 0
    #: Entries no path could land: the gain is lost and counted.
    discarded: int = 0
    #: Proteins whose direct set changed.
    proteins: int = 0
    #: Terms that were not live at t1, so they never qualified to project.
    not_live_at_t1: int = 0
    #: Projected ancestors that landed inside the protein-binding subtree.
    #: The lab reports zero for this window; a non-zero would mean the
    #: projection is feeding the binding rule terms it did not see before.
    binding_landings: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "projected_entries": self.entries,
            "projected_pairs": self.pairs,
            "projected_discarded": self.discarded,
            "projected_proteins": self.proteins,
            "projected_not_live_at_t1": self.not_live_at_t1,
            "projected_binding_landings": self.binding_landings,
        }


def first_live_ancestors(term: str, graph: Graph, target: set[str]) -> set[str]:
    """The first ``target`` terms above ``term``, one per path, same aspect.

    A breadth-first climb that does NOT expand a node once it has landed: the
    ancestors above a landing are reachable from it by the True Path Rule, so
    taking them too would add nothing and would bury the stop.
    """
    aspect = graph.aspect.get(term)
    if aspect is None:
        return set()
    landed: set[str] = set()
    seen: set[str] = {term}
    frontier = [
        parent
        for parent in graph.parents.get(term, ())
        if graph.aspect.get(parent) == aspect
    ]
    while frontier:
        node = frontier.pop()
        if node in seen:
            continue
        seen.add(node)
        if node in target:
            landed.add(node)
            continue
        frontier.extend(
            parent
            for parent in graph.parents.get(node, ())
            if parent not in seen and graph.aspect.get(parent) == aspect
        )
    return landed


def project_direct_annotations(
    raw: dict[str, set[str]], graph: Graph, target: set[str]
) -> Projection:
    """Rewrite ``raw`` in place, replacing unexpressible terms by their landings.

    ``raw`` is ``{protein: {direct go_id}}`` on the t1 side, before the binding
    rule and before any propagation, which is the only point where the
    substitution means what the rule says.
    """
    entries = pairs = discarded = proteins = not_live = binding_hits = 0
    for protein, terms in raw.items():
        keep: set[str] = set()
        changed = False
        for term in terms:
            if term in target:
                keep.add(term)
                continue
            if term not in graph.live:
                # Obsolete at its own end: not a gain to carry forward, and
                # not something to project either. Left as it was, so the
                # closure trims it exactly as it does today.
                not_live += 1
                keep.add(term)
                continue
            landings = first_live_ancestors(term, graph, target)
            entries += 1
            changed = True
            if not landings:
                discarded += 1
                continue
            pairs += len(landings)
            binding_hits += len(landings & graph.binding)
            keep |= landings
        if changed:
            proteins += 1
            raw[protein] = keep
    return Projection(
        entries=entries,
        pairs=pairs,
        discarded=discarded,
        proteins=proteins,
        not_live_at_t1=not_live,
        binding_landings=binding_hits,
    )
