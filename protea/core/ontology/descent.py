"""Generating an annotation set by walking down the ontology.

WHAT THIS REPLACES. The method this project has been improving retrieves
candidates and reorders them, and its ceiling is a candidate set that may not
contain the answer. The one previous attempt at generating instead of
retrieving (research/entail_kwta) generated over a FLAT vocabulary of 40,214
terms and reached a candidate recall of 5.7 per cent at top 500 on BP: you
cannot rank into the top what the model never surfaces.

WHAT IS DIFFERENT HERE. The vocabulary is not flat. A GO annotation set is an
ancestor-closed subgraph, so generating one is a DESCENT: start at the roots,
and at each step decide which of the terms immediately below the accepted set
to accept as well. Four measured properties of GO make this the right shape.

  A parent has a median of 2 children, so each decision is small rather than a
  softmax over forty thousand.

  The median edge cuts the carrier set to 7.8 per cent of the parent's, so each
  step is a sharp and informative decision rather than a choice between
  near-equals.

  Half the ontology has ten or fewer examples, and a descent shares its
  parameters across every edge, so a rare term is reached by decisions learned
  on common ones. A per-term classifier has nothing to learn from for half the
  ontology; a descent has the whole of it.

  Every set it can emit is ancestor-closed by construction, so nothing has to
  be repaired afterwards and a child can never be emitted without its parents.

THE FRONTIER IS THE WHOLE IDEA. At any point the model holds an accepted set S,
and the only terms it may consider are those whose parents are all in S. That
is a handful of nodes, not the ontology, which is why one forward pass per step
is cheap and why the walk terminates.
"""

from __future__ import annotations

from dataclasses import dataclass

import torch
from torch import Tensor, nn

from protea.core.ontology.dag import Dag


@dataclass(frozen=True)
class DescentConfig:
    """Everything the descent reads, recorded with the model it produced."""

    context_dim: int
    term_dim: int = 64
    hidden: int = 256
    dropout: float = 0.1
    lr: float = 1e-3
    epochs: int = 20
    batch: int = 512
    seed: int = 0
    device: str = "cpu"
    #: Below this the walk stops descending a branch. Not a post-hoc cut: it
    #: decides what the model emits, so it belongs to the model and is fitted
    #: on a validation window rather than chosen.
    tau: float = 0.5
    #: A DAG can be walked forever if a node's parents keep being accepted in
    #: a cycle the ontology should not contain. This bounds it regardless.
    max_steps: int = 24


def frontier(dag: Dag, accepted: set[str]) -> set[str]:
    """The terms the walk may consider next.

    A term qualifies when every one of its parents is already accepted. ALL
    rather than ANY is what makes the emitted set ancestor-closed: accepting a
    term whose second parent was rejected would assert a subsumption the
    ontology denies.
    """
    out: set[str] = set()
    for term in accepted:
        for child in dag.children_of(term):
            if child not in accepted:
                parents = dag.parents_of(child)
                if parents and all(p in accepted for p in parents):
                    out.add(child)
    return out


def training_boundary(dag: Dag, target: set[str]) -> tuple[list[str], list[str]]:
    """The decisions a walk toward ``target`` would have had to make.

    Positives are every term in the target that has a parent, since reaching it
    meant entering it. Negatives are the terms one step OUTSIDE the target,
    those whose parents are all in it and which are nonetheless absent. They
    are the only negatives that mean anything: a term deep in another branch
    was never offered to the walk and never had to be rejected.
    """
    pos = [t for t in target if dag.parents_of(t)]
    neg = sorted(frontier(dag, target))
    return pos, neg


class DescentModel(nn.Module):
    """Scores one term against one protein's context, for the walk."""

    def __init__(self, dag: Dag, term_vectors: Tensor, config: DescentConfig) -> None:
        super().__init__()
        torch.manual_seed(config.seed)
        self.dag = dag
        self.config = config
        #: Where the ontology encoder is spent. Frozen: a term with one example
        #: keeps the position the ontology gave it rather than drifting to fit
        #: its single observation.
        self.register_buffer("terms", term_vectors)
        self.terms: Tensor
        self.ctx = nn.Sequential(
            nn.Linear(config.context_dim, config.hidden),
            nn.GELU(),
            nn.Dropout(config.dropout),
            nn.Linear(config.hidden, config.term_dim),
        )
        #: The decision reads the protein, the term, and their interaction. The
        #: product is what carries "does this protein look like the things this
        #: term is usually true of".
        self.head = nn.Sequential(
            nn.Linear(3 * config.term_dim + 2, config.hidden),
            nn.GELU(),
            nn.Dropout(config.dropout),
            nn.Linear(config.hidden, 1),
        )

    def forward(self, context: Tensor, term_ids: Tensor, extra: Tensor) -> Tensor:
        """Logits for (context, term) pairs. ``extra`` carries per-decision
        state the term embedding cannot know: how deep the walk already is and
        how much of the term's own parentage was accepted."""
        c = self.ctx(context)
        t = self.terms[term_ids]
        return self.head(torch.cat([c, t, c * t, extra], dim=-1)).squeeze(-1)
