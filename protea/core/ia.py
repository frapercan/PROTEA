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
from collections.abc import Iterable, Mapping

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
    num = len(proteins_per_term.get(go_id, set())) + 1
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
