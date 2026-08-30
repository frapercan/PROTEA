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
            nu_a, nl_a = sampler.corrupt_bulk(chunk, config.negatives, nrng)
            if not len(nu_a):
                continue
            pu, pl = _batch_ids(dag, chunk)
            nu = torch.from_numpy(nu_a)
            nl = torch.from_numpy(nl_a)
            loss = model.penalty(pu, pl).mean() + torch.clamp(
                config.margin - model.penalty(nu, nl), min=0.0
            ).mean()
            opt.zero_grad()
            loss.backward()
            opt.step()
            total += float(loss.detach()) * len(chunk)
        if log is not None:
            log(f"    epoch {epoch + 1:3d}/{config.epochs}  loss {total / len(pairs):.4f}")
    return model
