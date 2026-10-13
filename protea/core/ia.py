"""Information Accretion (Clark and Radivojac 2013) computation.

IA(v) = -log2( P(v | parents(v)) )
      = -log2( |proteins[v]| / |intersect proteins[p] for p in parents(v)| )

This is the term-weight CAFA / cafaeval / LAFA use to compute the IA-weighted
``f_micro_w`` headline metric. PROTEA computes its OWN IA from an
``OntologySnapshot`` plus a t0 annotation corpus; that IA MUST reproduce
LAFA's IA on the shared Sep_2025 t0 (validation gate, FIX-METRIC-IA), so this
function is a faithful reimplementation of LAFA's reference ``calc_ia``
(``protea-lafa-knn/ontology.py``).

Faithfulness details that matter for sub-1e-3 parity:

* **Dummy protein.** LAFA prepends one synthetic protein annotated with every
  term before counting. It contributes ``+1`` to every term's protein count
  and to every parent intersection, which (a) keeps the ratio defined when a
  term's parents are never co-annotated and (b) makes roots collapse to
  exactly 0 (the dummy plus full propagation guarantees
  ``count(term) == count(parents)`` at a root). We reproduce it with the
  ``+1`` terms below rather than materialising the protein.
* **Exact zero.** When ``count(term) == count(parents)`` IA is returned as
  exactly ``0.0`` to avoid a ``-log2(1.0)`` floating-point residual and to
  match LAFA's ``if prots_with_term == prots_with_parents: return 0`` guard.
* **Parents.** The True Path Rule edges (``is_a`` + ``part_of``); a term with
  no parents (a root) is 0.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Iterator, Mapping

PROPAGATE_RELATIONS = ("is_a", "part_of")


def term_ia(
    go_id: str,
    parents: frozenset[str] | set[str] | None,
    proteins_per_term: Mapping[str, set[str]],
) -> float:
    """IA of a single propagated term.

    ``proteins_per_term`` maps a GO id to the set of proteins annotated with
    it AFTER True-Path-Rule propagation (so a protein annotated to a child is
    also present on every ancestor). A synthetic dummy protein annotated with
    every term is modelled by the ``+ 1`` terms, matching LAFA's reference.
    """
    return term_ia_against(proteins_per_term.get(go_id, set()), parents, proteins_per_term)


def term_ia_against(
    own: set[str],
    parents: frozenset[str] | set[str] | None,
    proteins_per_term: Mapping[str, set[str]],
) -> float:
    """The IA of a protein set measured against a set of parent terms.

    Split out of :func:`term_ia` so the cycle path in
    :func:`ia_over_components` measures a whole component against its external
    parents through the SAME arithmetic. Two copies of this formula would be two
    places for the dummy protein and the exact-zero guard to drift apart, and
    parity with LAFA is checked to sub-1e-3.
    """
    if not parents:
        return 0.0
    parent_sets = [proteins_per_term.get(p) for p in parents]
    # A parent with no annotated proteins still carries the dummy protein, so
    # its effective set is {dummy}; the intersection with it is {dummy}. Treat
    # a missing parent set as empty (only the dummy remains).
    present = [ps for ps in parent_sets if ps]
    if len(present) < len(parent_sets):
        # At least one parent is annotated by the dummy only; the intersection
        # across all parents then collapses to just the dummy.
        denom = 1
    elif present:
        denom = len(set.intersection(*present)) + 1
    else:
        denom = 1
    num = len(own) + 1
    if num >= denom:
        # num == denom -> exact 0; num > denom should not happen under correct
        # propagation (a child cannot have more proteins than its parents), but
        # guard against corpus/ontology drift by clamping to 0 like LAFA.
        return 0.0
    return -math.log2(num / denom)


def build_ancestors(parents: Mapping[str, set[str]]) -> dict[str, frozenset[str]]:
    """Memoised ancestor closure (including self) for every child in
    ``parents`` and every term reachable through it."""
    cache: dict[str, frozenset[str]] = {}

    def ancestors(go_id: str) -> frozenset[str]:
        cached = cache.get(go_id)
        if cached is not None:
            return cached
        acc = {go_id}
        stack = list(parents.get(go_id, ()))
        while stack:
            p = stack.pop()
            if p in acc:
                continue
            acc.add(p)
            stack.extend(parents.get(p, ()))
        fs = frozenset(acc)
        cache[go_id] = fs
        return fs

    seen = set(parents) | {p for ps in parents.values() for p in ps}
    for go_id in seen:
        ancestors(go_id)
    return cache


def propagate_annotations(
    annotations: Iterable[tuple[str, str]],
    ancestors: Mapping[str, frozenset[str]],
) -> dict[str, set[str]]:
    """Propagate (protein, term) pairs up the DAG.

    For each annotation the protein is added to the term and all its
    ancestors. Terms absent from ``ancestors`` (not in the target snapshot)
    are skipped. Returns ``proteins_per_term``.
    """
    proteins_per_term: dict[str, set[str]] = {}
    for protein, go_id in annotations:
        anc = ancestors.get(go_id)
        if anc is None:
            continue
        for a in anc:
            proteins_per_term.setdefault(a, set()).add(protein)
    return proteins_per_term


def information_accretion(
    terms: Iterable[str],
    parents: Mapping[str, set[str]],
    annotations: Iterable[tuple[str, str]],
) -> dict[str, float]:
    """Full IA over a term universe.

    ``terms``: every GO id in the target snapshot.
    ``parents``: child go_id -> set(direct parent go_id) on the True-Path
    edges of the target snapshot.
    ``annotations``: (protein_accession, go_id) corpus pairs (unpropagated).

    Returns ``go_id -> IA`` for every term in ``terms`` (0.0 for roots and
    terms with no corpus support), faithfully reproducing LAFA's IA.
    """
    ancestors = build_ancestors(parents)
    # ensure every requested term has an ancestor entry (isolated terms = self)
    for t in terms:
        if t not in ancestors:
            ancestors[t] = frozenset({t})
    proteins_per_term = propagate_annotations(annotations, ancestors)
    return {t: term_ia(t, parents.get(t), proteins_per_term) for t in terms}


def strongly_connected_components(
    parents: Mapping[str, set[str]],
) -> list[frozenset[str]]:
    """Every set of terms that are ancestors of one another, size two or more.

    Tarjan's algorithm, iterative because a GO branch is deep enough to exhaust
    the recursion limit. Singletons are omitted: a term is trivially its own
    component, and saying so would make every caller filter. One linear pass,
    and on an acyclic snapshot it returns an empty list.

    What a component means for IA is in :func:`ia_over_components`.
    """
    index: dict[str, int] = {}
    low: dict[str, int] = {}
    on_stack: dict[str, bool] = {}
    stack: list[str] = []
    out: list[frozenset[str]] = []
    counter = 0

    nodes = set(parents) | {p for ps in parents.values() for p in ps}
    for root in nodes:
        if root in index:
            continue
        index[root] = low[root] = counter
        counter += 1
        stack.append(root)
        on_stack[root] = True
        work: list[tuple[str, Iterator[str]]] = [(root, iter(parents.get(root, ())))]
        while work:
            v, it = work[-1]
            descended = False
            for w in it:
                if w not in index:
                    index[w] = low[w] = counter
                    counter += 1
                    stack.append(w)
                    on_stack[w] = True
                    work.append((w, iter(parents.get(w, ()))))
                    descended = True
                    break
                if on_stack.get(w):
                    low[v] = min(low[v], index[w])
            if descended:
                continue
            work.pop()
            if work:
                low[work[-1][0]] = min(low[work[-1][0]], low[v])
            if low[v] == index[v]:
                member: list[str] = []
                while True:
                    w = stack.pop()
                    on_stack[w] = False
                    member.append(w)
                    if w == v:
                        break
                if len(member) > 1:
                    out.append(frozenset(member))
    return out


def ia_over_components(
    terms: Iterable[str],
    parents: Mapping[str, set[str]],
    proteins_per_term: Mapping[str, set[str]],
) -> dict[str, float]:
    """IA for every term, treating each cycle as one concept.

    ON AN ACYCLIC SNAPSHOT THIS IS :func:`term_ia` FOR EVERY TERM, with no
    arithmetic in between, which is the property that keeps 60 of the 61
    snapshots byte-identical to what they were before cycles were handled at all.

    WHY A CYCLE NEEDS ITS OWN RULE. Propagation closes over reachability, so the
    members of a cycle are ancestors of one another and carry the SAME protein
    set. Every member therefore has a parent whose set equals its own, the ratio
    is 1, and ``term_ia`` returns exactly 0.0 for all of them -- not approximately
    zero, exactly. Since ``f_micro_w`` weights by IA, those terms would carry no
    weight at all: not mismeasured, invisible. Verified on release 221, where all
    six terms of the two triangles have an internal parent and identical
    descendant sets (9 and 4).

    THE RULE. A component is one concept. Its IA is computed against its EXTERNAL
    parents, the ones outside the component, and divided equally among its
    members. Equal division is not a convention chosen for tidiness: ``f_micro_w``
    SUMS the IA of the terms a protein carries, and a protein carrying any member
    of a cycle carries all of them, so the members must sum to the component's own
    information content. Any split summing to it preserves that; equal is the only
    one that does not require picking a representative.

    The condensed graph is a DAG by construction, so this needs no ordering: a
    component's external parents are either singletons or members of another
    component, and in both cases their protein sets are already propagated.
    """
    components = strongly_connected_components(parents)
    if not components:
        return {t: term_ia(t, parents.get(t), proteins_per_term) for t in terms}

    component_of: dict[str, frozenset[str]] = {}
    for comp in components:
        for member in comp:
            component_of[member] = comp

    shared: dict[frozenset[str], float] = {}
    for comp in components:
        external: set[str] = set()
        for member in comp:
            external |= parents.get(member, set()) - comp
        # The component stands in for its members, so the numerator is any
        # member's set: they are equal by construction, and
        # ``_gate_components`` in the operation refuses the run if they are not.
        representative = next(iter(comp))
        whole = term_ia_against(
            proteins_per_term.get(representative, set()), external, proteins_per_term
        )
        shared[comp] = whole / len(comp)

    out: dict[str, float] = {}
    for t in terms:
        suyo = component_of.get(t)
        out[t] = shared[suyo] if suyo is not None else term_ia(t, parents.get(t), proteins_per_term)
    return out
