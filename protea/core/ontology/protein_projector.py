"""A protein placed in the ontology's own space, from its sequence alone.

THE IDEA. In the order space a term ``t`` sits at ``V[t]``, and ``a`` subsumes
``b`` when ``a <= b`` on every coordinate. A protein that has terms T must
therefore sit at a point that every term in T subsumes, and the smallest such
point is the coordinate-wise maximum of their vectors. So a protein is a point
in the same space as the terms, and scoring ANY term for ANY protein is one
penalty:

    pen(t, p) = || max(0, V[t] - p) ||^2

with no parameter belonging to the term. That is what makes it work for the
8,804 GO terms that have no carrier in the bank at all: they have a position
because the ontology gives them one, not because anything was observed about
them.

WHAT IS LEARNED. Only the map from a sequence embedding to that point. The
ontology encoder is frozen, so a term with one example keeps the position the
ontology gave it rather than drifting to fit its single observation.

WHY NEGATIVES DECIDE EVERYTHING. Push a protein's point far enough out and
every term subsumes it, so every prediction is true and the loss on positives
is zero. Nothing in the positive term prevents that. What bounds the point is
the negatives, and the informative ones are structural: the siblings of a term
the protein has. See :meth:`protea.core.ontology.dag.Dag.siblings_of`.
"""

from __future__ import annotations

from dataclasses import dataclass

import torch
from torch import Tensor, nn


@dataclass(frozen=True)
class ProjectorConfig:
    """Everything the projector reads, recorded with the model it produced."""

    in_dim: int
    out_dim: int
    hidden: int = 512
    dropout: float = 0.1
    lr: float = 1e-3
    epochs: int = 20
    batch: int = 256
    #: How far a negative must be pushed past a positive. The scale is set by
    #: the order space the ontology encoder produced, not chosen freely.
    margin: float = 1.0
    seed: int = 0
    device: str = "cpu"


class ProteinProjector(nn.Module):
    """Sequence embedding to a point in the frozen order space."""

    def __init__(self, terms: Tensor, config: ProjectorConfig) -> None:
        super().__init__()
        torch.manual_seed(config.seed)
        self.config = config
        #: The ontology encoder's output. A buffer, not a parameter: it is the
        #: target and it does not move.
        self.register_buffer("terms", terms)
        self.terms: Tensor
        self.net = nn.Sequential(
            nn.Linear(config.in_dim, config.hidden),
            nn.GELU(),
            nn.Dropout(config.dropout),
            nn.Linear(config.hidden, config.hidden // 2),
            nn.GELU(),
            nn.Dropout(config.dropout),
            nn.Linear(config.hidden // 2, config.out_dim),
        )

    def forward(self, x: Tensor) -> Tensor:
        """The protein's point, held in the non-negative orthant like a term."""
        return torch.abs(self.net(x))

    def penalty(self, point: Tensor, term_ids: Tensor) -> Tensor:
        """How badly ``term_ids`` fail to subsume ``point``. Zero means they do.

        ``point`` is (b, d) and ``term_ids`` is (b, k), giving (b, k).
        """
        v = self.terms[term_ids]
        return torch.clamp(v - point.unsqueeze(1), min=0.0).pow(2).sum(-1)

    def score_all(self, x: Tensor) -> Tensor:
        """Every term scored for every protein in ``x``, higher being better.

        The negated penalty, so it reads like a score. This is the whole
        inference step: one matrix operation over the full ontology, with no
        retrieval and no donor.
        """
        point = self(x)
        return -torch.clamp(
            self.terms.unsqueeze(0) - point.unsqueeze(1), min=0.0
        ).pow(2).sum(-1)
