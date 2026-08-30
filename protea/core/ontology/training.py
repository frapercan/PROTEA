"""Fitting an order encoder, and the negatives that decide what it learns.

The loss is max-margin over a contrast: a true (ancestor, descendant) pair
should have zero penalty, and a corrupted one should have at least ``margin``.
Everything interesting is in how the corrupted pairs are drawn.
"""

from __future__ import annotations

import random
from collections.abc import Callable

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
    dag: Dag,
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
    model = OrderEncoder(dag, config).to(config.device)
    opt = torch.optim.Adam(model.parameters(), lr=config.lr)
    sampler = NegativeSampler(dag, closure, config.seed)
    rng = random.Random(config.seed)
    pairs = list(train_pairs)

    for epoch in range(config.epochs):
        rng.shuffle(pairs)
        total = 0.0
        for start in range(0, len(pairs), config.batch):
            chunk = pairs[start : start + config.batch]
            neg = [c for p in chunk for c in sampler.corrupt(p, config.negatives)]
            if not neg:
                continue
            pu, pl = _batch_ids(dag, chunk)
            nu, nl = _batch_ids(dag, neg)
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
