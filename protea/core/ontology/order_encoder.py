"""Subsumption as containment: the simplest encoder that can carry the relation.

WHY THIS SHAPE. A term's position has to answer one question: what does it
subsume, and what subsumes it. A plain distance cannot answer it, because
subsumption is not symmetric and distance is. An order embedding can: put every
term in the non-negative orthant and read ``a`` subsumes ``b`` as
``a_i <= b_i`` on every coordinate. The root sits near the origin because it is
below everything; a leaf sits far out because almost nothing is below it. The
violation of that inequality is a penalty that is zero for a true pair and
grows with how wrong a false one is, which is exactly what is needed to rank.

Starting here rather than at boxes or hyperbolic space is deliberate. This has
one hyperparameter that matters, it trains in minutes on CPU, and it gives a
number to beat. Anything more elaborate has to earn its place against it.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import torch
from torch import Tensor, nn

from protea.core.ontology.dag import Dag


@dataclass(frozen=True)
class TrainConfig:
    """Everything the training loop reads. No value here is a default that
    matters silently: the encoder records the config it was trained under."""

    dim: int = 64
    epochs: int = 30
    batch: int = 4096
    lr: float = 0.05
    margin: float = 1.0
    #: Negatives per positive. Both ends are corrupted, so this is per end.
    negatives: int = 4
    seed: int = 0
    #: The laptop has a thermal ceiling and the graphics card belongs to the
    #: other machine. This is small enough that CPU is not the bottleneck.
    device: str = "cpu"


class OrderEncoder(nn.Module):
    """A vector per term in the non-negative orthant, ordered by containment."""

    def __init__(self, dag: Dag, config: TrainConfig) -> None:
        super().__init__()
        self.dag = dag
        self.config = config
        torch.manual_seed(config.seed)
        self.emb = nn.Embedding(len(dag.terms), config.dim)
        nn.init.uniform_(self.emb.weight, 0.0, 0.1)

    def forward(self, ids: Tensor) -> Tensor:
        """Coordinates are held non-negative, which is what makes the order an
        order. Enforced here rather than by clamping the parameters, so the
        gradient stays defined everywhere."""
        return torch.abs(self.emb(ids))

    def penalty(self, upper: Tensor, lower: Tensor) -> Tensor:
        """How badly ``upper`` fails to subsume ``lower``. Zero means it does."""
        return torch.linalg.vector_norm(
            torch.clamp(self(upper) - self(lower), min=0.0), dim=-1
        ) ** 2

    def score_pairs(self, pairs: list[tuple[str, str]]) -> np.ndarray:
        """The penalty for (ancestor, descendant) candidates, lower being better."""
        idx = self.dag.index
        up = torch.tensor([idx[a] for a, _ in pairs], dtype=torch.long)
        lo = torch.tensor([idx[b] for _, b in pairs], dtype=torch.long)
        with torch.no_grad():
            return self.penalty(up, lo).cpu().numpy()

    def rank_parents(self, child: str, candidates: list[str]) -> np.ndarray:
        """Order ``candidates`` by how well each subsumes ``child``."""
        scores = self.score_pairs([(c, child) for c in candidates])
        return np.argsort(scores, kind="stable")
