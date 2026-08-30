"""Fitting an order encoder, and the negatives that decide what it learns.

The loss is max-margin over a contrast: a true (ancestor, descendant) pair
should have zero penalty, and a corrupted one should have at least ``margin``.
Everything interesting is in how the corrupted pairs are drawn.
"""

from __future__ import annotations

import random
from collections.abc import Callable

import numpy as np
import torch
from torch import Tensor

from protea.core.ontology.dag import Dag
from protea.core.ontology.order_encoder import OrderEncoder, TrainConfig
from protea.core.ontology.semantics import Semantics


class NegativeSampler:
    """Corrupted pairs, checked against the true closure before being used.

    A uniformly drawn term is almost never an ancestor of the term it replaces,
    so most implementations skip the check. That is a false economy here: GO's
    top terms subsume nearly everything, so a corrupted pair that happens to be
    true is not a rare event at the top of the graph, and training the encoder
    to push those apart teaches it the opposite of the relation.
    """

    def __init__(self, dag: Dag, closure: set[tuple[str, str]], seed: int) -> None:
        self.dag = dag
        self.closure = closure
        self.rng = random.Random(seed)
        self.terms = dag.terms

    def prepare_bulk(self) -> None:
        """Pack the closure into a sorted integer array for vectorised lookup.

        The per-pair sampler below is correct and far too slow to feed a large
        batch: at 8,192 pairs and four negatives each it draws 23.6 million
        pairs in Python across a run, and raising the batch to make the graph
        forward pay off would take that to 84 million. The bulk path draws them
        with numpy and rejects true pairs with a single searchsorted.
        """
        idx = self.dag.index
        packed = np.fromiter(
            (idx[a] * len(idx) + idx[b] for a, b in self.closure if a in idx and b in idx),
            dtype=np.int64,
        )
        self._packed = np.sort(packed)
        self._n = len(idx)

    def corrupt_bulk(self, pairs: list[tuple[str, str]], n: int, rng: np.random.Generator
                     ) -> tuple[np.ndarray, np.ndarray]:
        """``n`` false pairs per true one, as index arrays.

        Corrupted draws that turn out to be true are DROPPED rather than
        redrawn. Retrying would put a Python loop back in the hot path, and a
        batch that comes back a fraction short is harmless where a slow one is
        not. It matters that they are dropped and not kept: the top of an
        ontology subsumes nearly everything, so a corrupted pair landing on a
        true one is not rare there, and training the encoder to push those
        apart teaches the opposite of the relation.
        """
        idx = self.dag.index
        up = np.repeat(np.array([idx[a] for a, _ in pairs], dtype=np.int64), n)
        lo = np.repeat(np.array([idx[b] for _, b in pairs], dtype=np.int64), n)
        head = np.tile(np.arange(n) % 2 == 0, len(pairs))
        draw = rng.integers(0, self._n, size=up.shape, dtype=np.int64)
        up = np.where(head, draw, up)
        lo = np.where(head, lo, draw)
        packed = up * self._n + lo
        pos = np.searchsorted(self._packed, packed)
        pos = np.clip(pos, 0, len(self._packed) - 1)
        keep = (self._packed[pos] != packed) & (up != lo)
        return up[keep], lo[keep]

    def corrupt(self, pair: tuple[str, str], n: int) -> list[tuple[str, str]]:
        """``n`` false pairs built from a true one, half by replacing each end."""
        up, lo = pair
        out: list[tuple[str, str]] = []
        for i in range(n):
            for _ in range(8):
                cand = (
                    (self.rng.choice(self.terms), lo)
                    if i % 2 == 0
                    else (up, self.rng.choice(self.terms))
                )
                if cand not in self.closure and cand[0] != cand[1]:
                    out.append(cand)
                    break
        return out


class SemanticNegatives:
    """False pairs drawn near, and pushed apart only as far as they deserve.

    Replaces uniform corruption for two measured reasons. A uniformly drawn
    pair has Resnik 0.00 at the median, meaning the two terms share nothing but
    the root, so separating them is free and teaches almost nothing. And
    sibling pairs, which are the informative negatives, span 0.18 to 13.23 in
    Resnik with an interquartile range of 3.51 nats, so calling them all "hard"
    and giving them one margin discards most of what distinguishes them.

    Each negative carries its own margin: full when the two share nothing,
    approaching zero as they approach being the same thing.
    """

    def __init__(self, dag: Dag, closure: set[tuple[str, str]], sem: Semantics,
                 near: float, seed: int) -> None:
        self.dag = dag
        self.closure = closure
        self.sem = sem
        self.near = near
        self.rng = random.Random(seed)

    def _one(self, up: str, lo: str, i: int) -> tuple[str, str] | None:
        """One false pair from a true one.

        THE OBVIOUS CHOICE IS THE WRONG ONE. Replacing the child by one of its
        siblings looks like the hard negative, and it is a true pair 78 per
        cent of the time: if a term subsumes a child it usually subsumes that
        child's siblings too, because they share a parent. Measured at 21.7 per
        cent surviving the closure filter, so four fifths of the work was
        generating true pairs and discarding them, and the fifth that survived
        was selected in a way nobody had examined.

        The two that work, measured on the same 3,000 pairs: the pair read
        BACKWARDS is false 100 per cent of the time and is the hardest negative
        that exists, since the two terms are as related as a pair can be and
        the answer is still no. And replacing the PARENT by one of its siblings
        is false 94.7 per cent of the time and asks the discriminative
        question, whether some other term at the same level also subsumes this
        child.
        """
        if self.rng.random() < self.near:
            if i % 2:
                return lo, up
            sibs = self.dag.sibling_list(up)
            return (self.rng.choice(sibs), lo) if sibs else None
        if i % 2:
            return up, self.rng.choice(self.dag.terms)
        return self.rng.choice(self.dag.terms), lo

    def draw(self, pairs: list[tuple[str, str]], n: int, full: float
             ) -> tuple[list[tuple[str, str]], np.ndarray]:
        """``n`` negatives per true pair, a fraction ``near`` of them siblings.

        The uniform remainder is kept on purpose. Training only on near
        negatives would leave the encoder never told that two terms from
        different aspects have nothing to do with each other, which is most of
        the ontology and is the part a protein search actually has to reject.
        """
        out: list[tuple[str, str]] = []
        for up, lo in pairs:
            for i in range(n):
                cand = self._one(up, lo, i)
                if cand and cand[0] != cand[1] and cand not in self.closure:
                    out.append(cand)
        return out, self.sem.margins(out, full)


def _batch_ids(dag: Dag, pairs: list[tuple[str, str]]) -> tuple[Tensor, Tensor]:
    idx = dag.index
    return (
        torch.tensor([idx[a] for a, _ in pairs], dtype=torch.long),
        torch.tensor([idx[b] for _, b in pairs], dtype=torch.long),
    )


def fit(
    model: OrderEncoder,
    train_pairs: list[tuple[str, str]],
    closure: set[tuple[str, str]],
    config: TrainConfig,
    *,
    log: Callable[[str], None] | None = None,
    semantic: SemanticNegatives | None = None,
) -> OrderEncoder:
    """Train on the transitive closure of the training edges, not just on them.

    Training on direct edges alone would leave the encoder free to satisfy
    every parent-child inequality while getting grandparents wrong, and
    subsumption is exactly the relation that has to survive composition.
    """
    dag = model.dag
    model = model.to(config.device)
    opt = torch.optim.Adam(model.parameters(), lr=config.lr)
    sampler = NegativeSampler(dag, closure, config.seed)
    sampler.prepare_bulk()
    rng = random.Random(config.seed)
    nrng = np.random.default_rng(config.seed)
    pairs = list(train_pairs)

    for epoch in range(config.epochs):
        rng.shuffle(pairs)
        total = 0.0
        for start in range(0, len(pairs), config.batch):
            chunk = pairs[start : start + config.batch]
            if semantic is not None:
                neg, marg = semantic.draw(chunk, config.negatives, config.margin)
                if not neg:
                    continue
                nu, nl = _batch_ids(dag, neg)
                margin = torch.from_numpy(marg)
            else:
                nu_a, nl_a = sampler.corrupt_bulk(chunk, config.negatives, nrng)
                if not len(nu_a):
                    continue
                nu, nl = torch.from_numpy(nu_a), torch.from_numpy(nl_a)
                margin = torch.full((len(nu),), config.margin)
            pu, pl = _batch_ids(dag, chunk)
            loss = model.penalty(pu, pl).mean() + torch.clamp(
                margin - model.penalty(nu, nl), min=0.0
            ).mean()
            opt.zero_grad()
            loss.backward()
            opt.step()
            total += float(loss.detach()) * len(chunk)
        if log is not None:
            log(f"    epoch {epoch + 1:3d}/{config.epochs}  loss {total / len(pairs):.4f}")
    return model
