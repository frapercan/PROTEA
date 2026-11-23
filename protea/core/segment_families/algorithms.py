"""The label-free steps that turn segment cores and their hits into families (ADR-D65, decision 2).

Ported from the laboratory's ``familias_tramos.py`` (card 2 of 2026-11-13) and
``piloto_mmseqs.py`` (card 1, the calibration pilot), with the same rules:

- a segment's core drops every run of non-match residues (insert or background)
  longer than the segment's own number of match residues;
- every core of a decoy component larger than ``MASK_ABOVE`` (decoys: reversed
  donor cores against the donor cores, both coverages at least one half, at the
  permissive scope) is masked;
- a link is a hit within the scope between two unmasked donor cores covering
  at least half of both; a hit that covers only one side is a partial link;
- groups come from a greedy set cover over the links (the core with most
  unassigned neighbours first, ties by index), so every member is linked to its
  representative directly and nothing chains;
- families join groups whose profile finds the other's representative within
  the profile step's scope, calibrated on the reversed representatives so that
  at most ``REVERSED_KEPT`` reversed hits remain;
- clans are the components of the families joined by partial links;
- a target's segment takes the family of its best link to an unmasked donor core.

Hits arrive as integer-indexed arrays (``Hits``); nothing here reads a label.
"""

from __future__ import annotations

import heapq
from collections.abc import Iterable
from dataclasses import dataclass

import numpy as np

#: The strict scope: E-value times the donor cores below this (pilot 1's excess).
STRICT_CONSTANT = 8.599e-5
#: The permissive scope: E-value below this over the donor cores (one false link per theme).
FULL_BUDGET = 32768.0
#: A decoy component larger than this masks every core in it.
MASK_ABOVE = 10
#: Both coverages of a link, and of a profile's or a target's hit.
COVERAGE = 0.5
#: How many reversed representatives the profile step's constant lets through.
REVERSED_KEPT = 3


@dataclass(frozen=True)
class Hits:
    """MMseqs2 hits as arrays: query and target indices, E-value, bits and both coverages."""

    q: np.ndarray
    t: np.ndarray
    e: np.ndarray
    bits: np.ndarray
    qcov: np.ndarray
    tcov: np.ndarray

    def take(self, keep: np.ndarray) -> Hits:
        return Hits(
            self.q[keep],
            self.t[keep],
            self.e[keep],
            self.bits[keep],
            self.qcov[keep],
            self.tcov[keep],
        )

    def __len__(self) -> int:
        return len(self.q)


def scope_evalue(scope: str, donor_cores: int) -> float:
    """The E-value a link must stay below at ``scope``."""
    if scope == "strict":
        return STRICT_CONSTANT / donor_cores
    if scope == "permissive":
        return FULL_BUDGET / donor_cores
    raise ValueError(f"unknown scope {scope!r}")


def core_mask(columns: np.ndarray) -> np.ndarray:
    """The residues a segment's core keeps: every non-match run longer than its match count goes."""
    match = np.asarray(columns) >= 0
    n_match = int(match.sum())
    keep = np.ones(len(match), dtype=bool)
    i = 0
    while i < len(match):
        if match[i]:
            i += 1
            continue
        j = i
        while j < len(match) and not match[j]:
            j += 1
        if j - i > n_match:
            keep[i:j] = False
        i = j
    return keep


def components(pairs: Iterable[tuple[int, int]]) -> list[set[int]]:
    """The connected components of the pairs (union-find with path halving)."""
    parent: dict[int, int] = {}

    def find(x: int) -> int:
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for a, b in pairs:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb
    out: dict[int, set[int]] = {}
    for x in list(parent):
        out.setdefault(find(x), set()).add(x)
    return list(out.values())


def set_cover(nodes: Iterable[int], pairs: Iterable[tuple[int, int]]) -> dict[int, int]:
    """Greedy set cover: node -> its representative, every member linked to it directly."""
    nb: dict[int, set[int]] = {n: set() for n in nodes}
    for a, b in pairs:
        if a != b and a in nb and b in nb:
            nb[a].add(b)
            nb[b].add(a)
    rep: dict[int, int] = {}
    left = set(nb)
    heap = [(-len(nb[n]), n) for n in nb]
    heapq.heapify(heap)
    while heap:
        negd, n = heapq.heappop(heap)
        if n not in left:
            continue
        d = sum(1 for x in nb[n] if x in left)
        if d != -negd:  # a stale degree: pushed back with the true one
            heapq.heappush(heap, (-d, n))
            continue
        rep[n] = n
        left.discard(n)
        for x in nb[n]:
            if x in left:
                rep[x] = n
                left.discard(x)
    return rep


def both_covered(hits: Hits) -> np.ndarray:
    return (hits.qcov >= COVERAGE) & (hits.tcov >= COVERAGE)


def decoy_mask(decoys: Hits, donor_cores: int) -> np.ndarray:
    """Donor cores in a decoy component of more than ``MASK_ABOVE`` cores, at the permissive scope.

    A decoy hit's query is a reversed core, node ``-1 - q``; its target is a donor core.
    """
    keep = (decoys.e < scope_evalue("permissive", donor_cores)) & both_covered(decoys)
    keep &= (decoys.q >= 0) & (decoys.t >= 0)
    kept = decoys.take(keep)
    masked = np.zeros(donor_cores, dtype=bool)
    for comp in components(zip((-1 - kept.q).tolist(), kept.t.tolist(), strict=True)):
        if len(comp) > MASK_ABOVE:
            masked[[(-1 - x) if x < 0 else x for x in comp]] = True
    return masked


def links(real: Hits, emax: float, masked: np.ndarray) -> tuple[Hits, Hits]:
    """(links, partial links) between unmasked donor cores within the scope."""
    keep = (real.e < emax) & (real.q != real.t) & (real.q >= 0) & (real.t >= 0)
    keep &= ~masked[np.maximum(real.q, 0)] & ~masked[np.maximum(real.t, 0)]
    kept = real.take(keep)
    q_ok, t_ok = kept.qcov >= COVERAGE, kept.tcov >= COVERAGE
    return kept.take(q_ok & t_ok), kept.take(q_ok != t_ok)


def profile_evalue(reversed_hits: Hits, nominal: float) -> float:
    """The profile step's constant: the nominal one, lowered until at most three reversed hits remain."""
    e = np.sort(reversed_hits.take((reversed_hits.e < 0.1) & both_covered(reversed_hits)).e)
    return min(nominal, float(e[REVERSED_KEPT]) if len(e) > REVERSED_KEPT else float("inf"))


def join_families(
    representatives: Iterable[int], profile_hits: Hits, e_profile: float
) -> dict[int, int]:
    """Representative -> family (the smallest representative of its component of profile links)."""
    keep = (profile_hits.e < e_profile) & (profile_hits.q != profile_hits.t)
    keep &= (profile_hits.q >= 0) & (profile_hits.t >= 0) & both_covered(profile_hits)
    joined = profile_hits.take(keep)
    family = {r: r for r in representatives}
    for comp in components(zip(joined.q.tolist(), joined.t.tolist(), strict=True)):
        root = min(comp)
        for x in comp:
            family[x] = root
    return family


def clans(family_of_core: np.ndarray, partial: Hits) -> dict[int, int]:
    """Family -> clan (the smallest family of its component of partial links)."""
    a, b = family_of_core[partial.q], family_of_core[partial.t]
    ok = (a >= 0) & (b >= 0) & (a != b)
    out: dict[int, int] = {}
    for comp in components(zip(a[ok].tolist(), b[ok].tolist(), strict=True)):
        root = min(comp)
        for x in comp:
            out[x] = root
    return out


def target_families(
    queries: Hits, emax: float, masked: np.ndarray, family_of_core: np.ndarray, n_targets: int
) -> np.ndarray:
    """Per target core, the family of its best link to an unmasked donor core (-1: none)."""
    keep = (queries.e < emax) & (queries.q >= 0) & (queries.t >= 0) & both_covered(queries)
    kept = queries.take(keep)
    kept = kept.take(~masked[kept.t])
    order = np.lexsort((kept.t, kept.e, -kept.bits, kept.q))
    q_sorted = kept.q[order]
    first = order[np.r_[True, q_sorted[1:] != q_sorted[:-1]]] if len(order) else order
    out = np.full(n_targets, -1, dtype=np.int64)
    out[kept.q[first]] = family_of_core[kept.t[first]]
    return out


__all__ = [
    "COVERAGE",
    "FULL_BUDGET",
    "MASK_ABOVE",
    "REVERSED_KEPT",
    "STRICT_CONSTANT",
    "Hits",
    "both_covered",
    "clans",
    "components",
    "core_mask",
    "decoy_mask",
    "join_families",
    "links",
    "profile_evalue",
    "scope_evalue",
    "set_cover",
    "target_families",
]
