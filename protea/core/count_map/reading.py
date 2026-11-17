"""The reading of a count map: every target scored with its witness (ADR-D65, decision 4).

A target's score for term t is the largest ``p(u, t)`` over its units, each unit
read without the target's exclusion group: the donors that share its sequence,
which are the target itself when it is a donor, its merged aliases and its
identical twins. Those donors carry exactly the target's units (units are a
function of the sequence), so for every unit of the target the group takes the
same states and the same count out.

Then, as the laboratory's reading did: the aspect's prior candidates (the
``top`` terms by base rate, scored at pi) are added, every score is floored at
pi(t) (a floored row loses its witness), the terms the target is denied at the
cut (its NOT terms and their descendants) are taken out, and the ``top`` best
terms per target are kept. Each row keeps its witness: the unit, and K and N
after the exclusion. Ported from the laboratory's ``mapa_cuentas.py``.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass

import numpy as np
import scipy.sparse as sp

#: The best terms kept per target and aspect, and the prior's candidates.
TOP = 100
#: How many support entries one chunk of pairs may expand to.
CHUNK_SUPPORT = 30_000_000


@dataclass(frozen=True)
class CountMap:
    """One aspect's map: K (units x terms), N per unit, pi per term and m."""

    counts: sp.csr_matrix
    carried: np.ndarray
    base_rate: np.ndarray
    strength: float


@dataclass(frozen=True)
class Targets:
    """One aspect's targets: their units, what their exclusion group takes out, their denied terms.

    ``units`` (targets x units) is binary; ``excluded`` (targets x terms) sums the
    states of the aspect's donors that share the target's sequence, ``excluded_n``
    counts them; ``denied`` (targets x terms) marks the NOT terms at the cut and
    their descendants.
    """

    units: sp.csr_matrix
    excluded: sp.csr_matrix
    excluded_n: np.ndarray
    denied: sp.csr_matrix


@dataclass(frozen=True)
class Rows:
    """Scored rows: target and term indices, score, and the witness (unit -1 is the prior)."""

    target: np.ndarray
    term: np.ndarray
    score: np.ndarray
    unit: np.ndarray
    k: np.ndarray
    n: np.ndarray

    def take(self, index: np.ndarray) -> Rows:
        return Rows(*(getattr(self, f)[index] for f in _FIELDS))

    def __len__(self) -> int:
        return len(self.target)


_FIELDS = ("target", "term", "score", "unit", "k", "n")


def concat(parts: list[Rows]) -> Rows:
    if not parts:
        return Rows(*(np.zeros(0, d) for d in (np.int64, np.int64, float, np.int64, float, float)))
    return Rows(*(np.concatenate([getattr(p, f) for p in parts]) for f in _FIELDS))


def _chunks(indptr: np.ndarray, lens: np.ndarray) -> Iterator[tuple[int, int]]:
    """Pair ranges cut at target boundaries, each expanding to about ``CHUNK_SUPPORT`` entries."""
    cum = np.concatenate([[0], np.cumsum(lens)])
    lo = 0
    for i in range(1, len(indptr)):
        end = int(indptr[i])
        if end > lo and cum[end] - cum[lo] > CHUNK_SUPPORT:
            yield lo, end
            lo = end
    if int(indptr[-1]) > lo:
        yield lo, int(indptr[-1])


def _scored(cmap: CountMap, targets: Targets, q_c: np.ndarray, u_c: np.ndarray) -> Rows:
    """The best unit per (target, term) over a chunk of (target, unit) pairs."""
    pi, m, terms = cmap.base_rate, cmap.strength, cmap.counts.shape[1]
    kept = (cmap.counts[u_c] - targets.excluded[q_c]).tocsr()
    kept.eliminate_zeros()  # a term every carrier of which is excluded drops out
    kept.sort_indices()
    if kept.nnz and kept.data.min() < 0:
        raise ValueError("an exclusion took out more than its unit counted")
    r_ = np.repeat(np.arange(len(u_c)), np.diff(kept.indptr))
    t_ = kept.indices.astype(np.int64)
    k_ = kept.data
    n_ = (cmap.carried[u_c] - targets.excluded_n[q_c])[r_]
    v_ = (k_ + m * pi[t_]) / (n_ + m)
    key = q_c[r_] * terms + t_
    o = np.lexsort((-v_, key))
    first = o[np.r_[True, key[o][1:] != key[o][:-1]]] if len(o) else o
    return Rows(q_c[r_][first], t_[first], v_[first], u_c[r_][first], k_[first], n_[first])


def _with_prior(part: Rows, qs: np.ndarray, base_rate: np.ndarray, top: int) -> Rows:
    """The prior's candidates added and every score floored at pi; ties keep the unit's row."""
    prior = np.argsort(-base_rate, kind="stable")[:top]
    nan = np.full(len(qs) * len(prior), np.nan)
    candidates = Rows(
        np.repeat(qs, len(prior)),
        np.tile(prior, len(qs)),
        base_rate[np.tile(prior, len(qs))],
        np.full(len(qs) * len(prior), -1, dtype=np.int64),
        nan,
        nan.copy(),
    )
    floor = base_rate[part.term]
    low = part.score < floor
    part = Rows(
        part.target,
        part.term,
        np.where(low, floor, part.score),
        np.where(low, -1, part.unit),
        np.where(low, np.nan, part.k),
        np.where(low, np.nan, part.n),
    )
    both = concat([part, candidates])
    o = np.lexsort((-both.score, both.term, both.target))  # stable: the unit's row first on a tie
    key = both.target[o] * (len(base_rate) + 1) + both.term[o]
    return both.take(o[np.r_[True, key[1:] != key[:-1]]] if len(o) else o)


def _finish(part: Rows, qs: np.ndarray, cmap: CountMap, denied: np.ndarray, top: int) -> Rows:
    """Prior and floor, the denied terms out, the ``top`` best per target."""
    rows = _with_prior(part, qs, cmap.base_rate, top)
    terms = cmap.counts.shape[1]
    if len(denied):
        key = rows.target * terms + rows.term
        hit = np.minimum(np.searchsorted(denied, key), len(denied) - 1)
        rows = rows.take(np.flatnonzero(denied[hit] != key))
    o = np.lexsort((rows.term, -rows.score, rows.target))
    target = rows.target[o]
    starts = np.r_[0, np.flatnonzero(target[1:] != target[:-1]) + 1] if len(o) else o
    rank = np.arange(len(o)) - np.repeat(starts, np.diff(np.r_[starts, len(o)]))
    return rows.take(o[rank < top])


def read(cmap: CountMap, targets: Targets, top: int = TOP) -> Rows:
    """Every target's ``top`` best terms of the aspect, with their witness."""
    units = targets.units.tocsr()
    units.sort_indices()
    terms = cmap.counts.shape[1]
    dn = targets.denied.tocoo()
    denied = np.sort(dn.row.astype(np.int64) * terms + dn.col.astype(np.int64))
    q_of = np.repeat(np.arange(units.shape[0], dtype=np.int64), np.diff(units.indptr))
    u_of = units.indices.astype(np.int64)
    lens = np.diff(cmap.counts.indptr)[u_of]
    parts = []
    for lo, hi in _chunks(units.indptr, lens):
        part = _scored(cmap, targets, q_of[lo:hi], u_of[lo:hi])
        parts.append(_finish(part, np.unique(q_of[lo:hi]), cmap, denied, top))
    without = np.setdiff1d(np.arange(units.shape[0]), np.unique(q_of))
    if len(without):
        parts.append(_finish(concat([]), without, cmap, denied, top))
    return concat(parts)


__all__ = ["CHUNK_SUPPORT", "TOP", "CountMap", "Rows", "Targets", "concat", "read"]
