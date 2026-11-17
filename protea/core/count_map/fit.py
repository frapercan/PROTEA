"""The strength of a count map, fitted on its donors alone (ADR-D65, decision 3).

For unit u and term t of one aspect the map reads

    p(u, t) = (K_u(t) + m * pi(t)) / (N_u + m)

N_u counts the donors that carry u, K_u(t) those whose closed state holds t,
pi(t) is the aspect's base rate over its donors and m one strength per aspect.
m maximises the bits of every donor's own state under the map read without the
donor's exclusion group (the donors that share its sequence): for every pair of
a unit and a donor that carries it, over every term of the aspect, as a
Bernoulli. The window's targets never enter.

Everything that does not depend on m is built once. A pair (unit, group) with
N' = N_u minus the group's donors in u scores a term off the unit's support at
``c * pi(t)``, with ``c = m / (N' + m)``; ``F`` sums that over every term of the
aspect for each distinct N', ``A`` corrects it on the unit's support, and the
group's own terms take their exact leave-group-out value. Ported from the
laboratory's ``mapa_cuentas.py`` (class ``Fit``), which checked it against a
brute force; the tests keep that brute force.
"""

from __future__ import annotations

import math
from typing import Any

import numpy as np
import scipy.sparse as sp

#: Where a probability is clipped below one before its complement's logarithm.
EPS = 1e-12
#: The search's bounds and its grid: 29 points on log m, then 30 golden steps.
LOWEST, HIGHEST, GRID, GOLDEN_STEPS = 1e-3, 1e4, 29, 30
#: How many support entries one step of the sum holds: bounded temporaries.
STEP = 20_000_000


def unit_counts(carriers: sp.csr_matrix, states: sp.csr_matrix) -> tuple[sp.csr_matrix, np.ndarray]:
    """K (units x terms, float64, sorted indices) and N (donors per unit)."""
    counts = (carriers.T.tocsr().astype(np.float64) @ states.astype(np.float64)).tocsr()
    counts.sort_indices()
    carried = np.asarray(carriers.sum(0)).ravel().astype(np.float64)
    return counts, carried


class StrengthFit:
    """The leave-group-out bits of every donor's state, as a function of m."""

    def __init__(
        self,
        carriers: sp.csr_matrix,
        states: sp.csr_matrix,
        group: np.ndarray,
        base_rate: np.ndarray,
    ) -> None:
        """``carriers`` (donors x units) and ``states`` (donors x terms) are binary."""
        self.base_rate = np.asarray(base_rate, dtype=np.float64)
        self.terms = int(states.shape[1])
        self.counts, self.carried = unit_counts(carriers, states)
        self._pairs(carriers, states, np.asarray(group, dtype=np.int64))
        self._combos()
        self.np_vals, self.np_idx = np.unique(self.np_r, return_inverse=True)
        self.pi_vals, self.pi_cnt = np.unique(self.base_rate, return_counts=True)

    def _pairs(self, carriers: sp.csr_matrix, states: sp.csr_matrix, group: np.ndarray) -> None:
        """The (unit, group) pairs, their sizes and the group's own terms in each."""
        coo = carriers.tocoo()
        donor, unit = coo.row.astype(np.int64), coo.col.astype(np.int64)
        width = int(group.max()) + 1 if len(group) else 1
        uniq, pair_of, n_ug = np.unique(
            unit * width + group[donor], return_inverse=True, return_counts=True
        )
        self.u_r = (uniq // width).astype(np.int64)
        self.n_r = n_ug.astype(np.float64)
        self.np_r = self.carried[self.u_r] - self.n_r
        incidence = sp.csr_matrix(
            (np.ones(len(donor)), (pair_of, donor)), shape=(len(uniq), carriers.shape[0])
        )
        own = (incidence @ states.astype(np.float64)).tocsr()
        own.sort_indices()
        self.e_r = np.repeat(np.arange(own.shape[0]), np.diff(own.indptr))
        self.e_t = own.indices.astype(np.int64)
        self.e_kug = own.data
        counts, terms = self.counts, self.terms
        keys = np.repeat(np.arange(counts.shape[0], dtype=np.int64), np.diff(counts.indptr))
        keys = keys * terms + counts.indices.astype(np.int64)
        wanted = self.u_r[self.e_r] * terms + self.e_t
        pos = np.searchsorted(keys, wanted)
        if len(pos) and not np.all(keys[np.minimum(pos, len(keys) - 1)] == wanted):
            raise ValueError("a group's term lies outside its unit's support")
        self.e_ku = counts.data[pos]

    def _combos(self) -> None:
        """The distinct (unit, N') combinations and each one's expansion over its unit's support."""
        counts, carried = self.counts, self.carried
        width = int(carried.max()) + 2 if len(carried) else 2
        cuniq, self.combo_r = np.unique(
            self.u_r * width + self.np_r.astype(np.int64), return_inverse=True
        )
        cu = cuniq // width
        lens = np.diff(counts.indptr)[cu]
        self.x_combo = np.repeat(np.arange(len(cuniq), dtype=np.int32), lens)
        offs = np.arange(int(lens.sum()), dtype=np.int64) - np.repeat(np.cumsum(lens) - lens, lens)
        src = np.repeat(counts.indptr[cu], lens) + offs
        self.x_t = counts.indices[src].astype(np.int32)
        self.x_k = counts.data[src].astype(np.float32)  # counts: exact in float32
        self.x_np = (cuniq % width).astype(np.float32)
        self.n_combo = len(cuniq)

    def sizes(self) -> dict[str, int]:
        """What the fit holds, for the record."""
        return {
            "units": int(self.counts.shape[0]),
            "terms": self.terms,
            "support": int(self.counts.nnz),
            "pairs": int(len(self.u_r)),
            "group_terms": int(len(self.e_t)),
            "combos": int(self.n_combo),
            "expanded": int(len(self.x_t)),
        }

    def _support_correction(self, m: float) -> np.ndarray:
        """A per combination: on its unit's support, the true value against ``c * pi``."""
        pi, out = self.base_rate, np.zeros(self.n_combo)
        for lo in range(0, len(self.x_t), STEP):
            xc, xt = self.x_combo[lo : lo + STEP], self.x_t[lo : lo + STEP]
            den = self.x_np[xc].astype(np.float64) + m
            q0 = np.minimum((self.x_k[lo : lo + STEP] + m * pi[xt]) / den, 1.0 - EPS)
            values = np.log2(1.0 - q0) - np.log2(1.0 - (m / den) * pi[xt])
            out += np.bincount(xc, weights=values, minlength=self.n_combo)
        return out

    def bits(self, m: float) -> float:
        """The log-likelihood in bits of every donor's state, its group out of each of its units."""
        pi = self.base_rate
        c = m / (self.np_vals + m)
        prior = (self.pi_cnt[None, :] * np.log2(1.0 - c[:, None] * self.pi_vals[None, :])).sum(1)
        base = (self.n_r * (prior[self.np_idx] + self._support_correction(m)[self.combo_r])).sum()
        den = self.np_r[self.e_r] + m
        q1 = (self.e_ku - self.e_kug + m * pi[self.e_t]) / den
        q0 = np.minimum((self.e_ku + m * pi[self.e_t]) / den, 1.0 - EPS)
        delta = np.log2(1.0 - q1) - np.log2(1.0 - q0)
        own = self.e_kug * (np.log2(q1) - np.log2(1.0 - q1))
        return float(base + (self.n_r[self.e_r] * delta).sum() + own.sum())

    def best(self) -> tuple[float, float, dict[str, Any]]:
        """m with the most bits: a grid on log m, then a golden search around its best point."""
        xs = np.linspace(math.log(LOWEST), math.log(HIGHEST), GRID)
        grid = [self.bits(math.exp(x)) for x in xs]
        i = int(np.argmax(grid))
        a, b = float(xs[max(i - 1, 0)]), float(xs[min(i + 1, len(xs) - 1)])
        ratio = (math.sqrt(5) - 1) / 2
        c_, d_ = b - ratio * (b - a), a + ratio * (b - a)
        fc, fd = self.bits(math.exp(c_)), self.bits(math.exp(d_))
        for _ in range(GOLDEN_STEPS):
            if fc > fd:
                b, d_, fd = d_, c_, fc
                c_ = b - ratio * (b - a)
                fc = self.bits(math.exp(c_))
            else:
                a, c_, fc = c_, d_, fd
                d_ = a + ratio * (b - a)
                fd = self.bits(math.exp(d_))
        m = math.exp((a + b) / 2)
        return m, self.bits(m), {"grid_m": [math.exp(x) for x in xs], "grid_bits": grid}


__all__ = ["EPS", "StrengthFit", "unit_counts"]
