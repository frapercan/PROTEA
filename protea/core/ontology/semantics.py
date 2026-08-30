"""How similar two terms are, measured on the ontology and a corpus.

Resnik similarity: the information content of the most informative common
ancestor. Two terms are as close as the most specific thing that is true of
both. It is grounded rather than lexical, which is what makes it a better axis
of difficulty than asking whether two names happen to share a word.

WHAT IT IS AND IS NOT. Resnik is SYMMETRIC and subsumption is not. It cannot
replace the order objective, which is the only thing that knows a parent from a
child and which reaches 98.97 per cent on reversed pairs. What it can do is
grade it: say how far apart a false pair deserves to be pushed, instead of
treating every false pair alike.

IC IS A CORPUS QUANTITY, SO IT HAS A DATE. IC(t) = -log P(t) with P estimated
from annotations, which here means the t0 bank and nothing later. It is also
NOT the information accretion already stored in the database: accretion is
-log P(t | parents(t)), conditional, and Resnik needs the marginal.

THE CIRCULARITY TO AVOID. IC is annotation frequency, and the frequency prior
was measured reaching median rank 106 of 40,214 on a held-out term. A model
trained to reproduce Resnik has partly been trained to reproduce that prior, so
Resnik may shape the training and must never be the thing the result is scored
on.

Measured on GO snapshot 36038118 against bank cbb35a32, 556,306 annotated
proteins, IC from 0.104 at the root to 13.229, median 10.185:

    Resnik(parent, child)    p10 3.84   p50 6.97   p90 11.03
    Resnik(sibling, sibling) p10 3.23   p50 6.34   p90 10.01
    Resnik(random, random)   p10 0.00   p50 0.00   p90  1.72

Two things follow. A uniformly drawn negative is trivially separable, so it
teaches almost nothing. And "sibling" is not one difficulty: within siblings
the spread runs 0.18 to 13.23, an interquartile range of 3.51 nats, which a
single fixed margin throws away.
"""

from __future__ import annotations

import numpy as np

from protea.core.ontology.dag import Dag


class Semantics:
    """Information content over a DAG, and Resnik similarity from it."""

    def __init__(self, dag: Dag, ic: dict[str, float]) -> None:
        self.dag = dag
        #: A term nobody has ever been annotated with is maximally informative,
        #: not undefined. 8,804 of GO's 40,214 terms have no carrier in the t0
        #: bank, and dropping them would silently exclude a fifth of the
        #: ontology from every comparison.
        self.max_ic = max(ic.values()) if ic else 0.0
        self._ic = ic
        self._ranked: dict[str, tuple[tuple[str, float], ...]] = {}
        #: Structured negatives repeat across epochs, so the second pass
        #: over them is free. Without this the sampler, not the model,
        #: is what a training run spends its time on.
        self._cache: dict[tuple[str, str], float] = {}

    def ic(self, term: str) -> float:
        return self._ic.get(term, self.max_ic)

    def _by_ic(self, term: str) -> tuple[tuple[str, float], ...]:
        """A term's ancestors and itself, most informative first.

        Ordered once so that Resnik is a walk that stops at the first hit
        rather than a maximum over a full intersection. The median term has
        twelve ancestors, so the walk is short.
        """
        hit = self._ranked.get(term)
        if hit is None:
            items = [(a, self.ic(a)) for a in (self.dag.ancestors(term) | {term})]
            hit = tuple(sorted(items, key=lambda kv: -kv[1]))
            self._ranked[term] = hit
        return hit

    def resnik(self, a: str, b: str) -> float:
        """The information content of the most informative common ancestor.

        Zero when the two share nothing, which for GO means they sit in
        different aspects and there is no common ancestor at all.
        """
        hit = self._cache.get((a, b))
        if hit is not None:
            return hit
        upper_b = self.dag.ancestors(b) | {b}
        out = 0.0
        for term, value in self._by_ic(a):
            if term in upper_b:
                out = value
                break
        self._cache[(a, b)] = out
        return out

    def resnik_bulk(self, pairs: list[tuple[str, str]]) -> np.ndarray:
        return np.array([self.resnik(a, b) for a, b in pairs], dtype=np.float32)

    def margins(self, pairs: list[tuple[str, str]], full: float) -> np.ndarray:
        """How far a false pair deserves to be pushed apart.

        Full margin when the two share nothing, and approaching zero as they
        approach being the same thing. Forcing a sibling that differs only in
        the last step of the ontology to sit a full margin away is asking the
        encoder to assert something the ontology does not.
        """
        r = self.resnik_bulk(pairs)
        return (full * (1.0 - r / max(self.max_ic, 1e-9))).astype(np.float32)


def information_content(carriers: dict[str, int], n_proteins: int) -> dict[str, float]:
    """IC(t) = -log P(t), with P the share of annotated proteins carrying t.

    ``carriers`` must already be propagated up the DAG: a protein annotated
    with a term carries every ancestor of it, and counting only direct
    annotations would make every internal term look rarer than it is.
    """
    if n_proteins <= 0:
        raise ValueError("information content needs a corpus to be estimated from")
    return {
        g: float(-np.log(max(n, 1) / n_proteins)) for g, n in carriers.items() if n > 0
    }
