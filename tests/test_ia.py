"""IA-gen correctness gate (FIX-METRIC-IA).

PROTEA computes its own Information Accretion; it MUST reproduce LAFA's IA on
a shared term universe within max abs diff < 1e-3 (the IA-weighting on
``f_micro_w`` is meaningless otherwise). LAFA's reference is
``protea-lafa-knn/ontology.py::calc_ia``: a dummy protein annotated with every
term is prepended, then ``IA(v) = -log2(count(v) / count(parents(v)))`` with
parents = direct ``is_a``/``part_of`` parents and an exact-zero rule when the
counts are equal.

These tests reimplement that reference inline (the same networkx
``descendants_at_distance`` + dummy-row counting LAFA uses) and assert
``protea.core.ia`` agrees with it on synthetic DAGs + random corpora, plus a
few hand-computed values.
"""

from __future__ import annotations

import math
import random

import networkx as nx

from protea.core.ia import information_accretion, term_ia


def _lafa_reference_ia(
    terms: list[str],
    parents: dict[str, set[str]],
    propagated: dict[str, set[str]],
) -> dict[str, float]:
    """LAFA ``calc_ia`` reference, dummy protein included.

    ``propagated`` maps term -> set(protein) AFTER True-Path propagation.
    Build a child->parent DiGraph so ``descendants_at_distance(g, t, 1)`` are
    the direct parents (matching obonet's edge direction in LAFA). Prepend a
    dummy protein annotated with every term.
    """
    g = nx.DiGraph()
    g.add_nodes_from(terms)
    for child, ps in parents.items():
        for p in ps:
            g.add_edge(child, p)

    dummy = "__DUMMY__"
    counts = {t: set(propagated.get(t, set())) | {dummy} for t in terms}

    out: dict[str, float] = {}
    for t in terms:
        direct_parents = nx.descendants_at_distance(g, t, 1)
        prots_with_term = len(counts[t])
        if not direct_parents:
            # root: parents empty -> LAFA counts proteins matching the empty
            # parent set, which (under full propagation) equals every protein,
            # so the ratio is 1 -> IA 0.
            out[t] = 0.0
            continue
        inter: set[str] = set.intersection(*[counts[p] for p in direct_parents])
        prots_with_parents = len(inter)
        if prots_with_term == prots_with_parents:
            out[t] = 0.0
        else:
            out[t] = -math.log2(prots_with_term / prots_with_parents)
    return out


def _propagate(
    annotations: list[tuple[str, str]], parents: dict[str, set[str]], terms: list[str]
) -> dict[str, set[str]]:
    """Reference TPR propagation: a (protein, term) pair lands on term and all
    ancestors (BFS over parent edges)."""
    prop: dict[str, set[str]] = {t: set() for t in terms}
    for protein, go in annotations:
        seen = set()
        stack = [go]
        while stack:
            cur = stack.pop()
            if cur in seen or cur not in prop:
                continue
            seen.add(cur)
            prop[cur].add(protein)
            stack.extend(parents.get(cur, ()))
    return prop


def test_root_is_zero():
    parents: dict[str, set[str]] = {}
    assert term_ia("GO:ROOT", parents.get("GO:ROOT"), {"GO:ROOT": {"p1", "p2"}}) == 0.0


def test_hand_computed_single_parent():
    # child annotated by 1 protein, parent by 3. With the dummy protein:
    # num = 1 + 1 = 2, denom = 3 + 1 = 4 -> IA = -log2(2/4) = 1.0
    parents = {"C": {"P"}}
    prop = {"C": {"a"}, "P": {"a", "b", "c"}}
    assert abs(term_ia("C", parents["C"], prop) - 1.0) < 1e-12


def test_equal_counts_returns_exact_zero():
    # child and parent annotated by the same proteins -> exact 0 (no -0.0 noise)
    parents = {"C": {"P"}}
    prop = {"C": {"a", "b"}, "P": {"a", "b"}}
    assert term_ia("C", parents["C"], prop) == 0.0


def test_multi_parent_intersection():
    # two parents; denominator is the intersection of their protein sets + dummy
    parents = {"C": {"P1", "P2"}}
    prop = {
        "C": {"a"},
        "P1": {"a", "b", "c"},
        "P2": {"a", "b"},
    }
    # intersection(P1,P2) = {a,b} -> denom = 2 + 1 = 3; num = 1 + 1 = 2
    expected = -math.log2(2 / 3)
    assert abs(term_ia("C", parents["C"], prop) - expected) < 1e-12


def _random_dag(n_terms: int, seed: int) -> tuple[list[str], dict[str, set[str]]]:
    rng = random.Random(seed)
    terms = [f"GO:{i:07d}" for i in range(n_terms)]
    parents: dict[str, set[str]] = {}
    # term i can only parent to lower indices -> acyclic; index 0 is a root
    for i in range(1, n_terms):
        n_parents = rng.choice([1, 1, 2])
        ps = {terms[rng.randint(0, i - 1)] for _ in range(n_parents)}
        parents[terms[i]] = ps
    return terms, parents


def test_reproduces_lafa_reference_within_tolerance():
    """The hard gate: own IA matches LAFA's reference algorithm on a random
    DAG + corpus within max abs diff < 1e-3 (acceptance threshold)."""
    for seed in range(8):
        terms, parents = _random_dag(60, seed)
        rng = random.Random(1000 + seed)
        proteins = [f"prot{j}" for j in range(40)]
        annotations = [
            (rng.choice(proteins), rng.choice(terms)) for _ in range(300)
        ]
        propagated = _propagate(annotations, parents, terms)

        ours = information_accretion(terms, parents, annotations)
        ref = _lafa_reference_ia(terms, parents, propagated)

        max_diff = max(abs(ours[t] - ref[t]) for t in terms)
        assert max_diff < 1e-3, f"seed={seed} max abs IA diff {max_diff} >= 1e-3"


def test_information_accretion_propagation_is_monotone():
    # after propagation a parent's protein set is a superset of the child's,
    # so a child's IA is non-negative (sanity, matches LAFA assert ia>=0)
    terms, parents = _random_dag(40, seed=7)
    rng = random.Random(99)
    annotations = [(f"p{rng.randint(0, 20)}", rng.choice(terms)) for _ in range(200)]
    ia = information_accretion(terms, parents, annotations)
    assert all(v >= 0.0 for v in ia.values())
