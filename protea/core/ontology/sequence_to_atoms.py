"""What atoms a sequence supplies, which is the only unknown left.

The ontology side is imposed: a term's code is the union of its ancestors' with
its own, so containment among terms is exact and costs no parameters. What has
to be learned is the other half, the map from a sequence representation to the
atoms a protein makes available, such that

    code(t) <= atoms(sequence)

holds for the terms the protein has and fails for the ones it does not.

THE OUTPUT IS NOT A CLASSIFICATION. There is no unit per GO term here, which is
what a CAFA-style multi-label head would have. 40,214 output units means 8,804
of them have no positive example at all in the t0 bank and 11,788 have ten or
fewer. A term's score is instead read off the SAME atom space every other term
uses, so a term with one carrier is scored by atoms that thousands of other
terms trained.
"""

from __future__ import annotations

from dataclasses import dataclass

import torch
from torch import Tensor, nn


@dataclass(frozen=True)
class AtomEncoderConfig:
    in_dim: int
    atoms: int = 1024
    hidden: int = 1024
    dropout: float = 0.1
    lr: float = 1e-3
    epochs: int = 12
    batch: int = 256
    #: Negatives drawn per positive term.
    negatives: int = 24
    #: How far a term the protein does NOT have must fail to be contained.
    margin: float = 1.0
    seed: int = 0
    device: str = "cpu"


class SequenceToAtoms(nn.Module):
    """A sequence representation to the atoms it makes available."""

    def __init__(self, config: AtomEncoderConfig) -> None:
        super().__init__()
        torch.manual_seed(config.seed)
        self.config = config
        self.net = nn.Sequential(
            nn.Linear(config.in_dim, config.hidden),
            nn.GELU(),
            nn.Dropout(config.dropout),
            nn.Linear(config.hidden, config.hidden),
            nn.GELU(),
            nn.Dropout(config.dropout),
            nn.Linear(config.hidden, config.atoms),
        )

    def forward(self, x: Tensor) -> Tensor:
        """Non-negative, like a term's code, because containment is read
        coordinate-wise between the two and a negative coordinate would mean
        the protein supplies less than nothing."""
        return torch.relu(self.net(x))
