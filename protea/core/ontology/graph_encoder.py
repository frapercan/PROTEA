"""A term's position computed from its neighbourhood, not looked up in a table.

WHY A GRAPH AND NOT A FEATURE VECTOR. Depth, ancestor count and descendant count
describe where a term sits, but they are derived from the very edges the encoder
is asked to reproduce, so feeding them in is circular, and a term that is new
does not have them yet. A graph encoder uses the edges as the COMPUTATION rather
than as an input: a term is whatever its parents and children make it, and the
order loss over the transitive closure remains the target. Nothing is fed in
that the encoder is being asked to predict.

DIRECTION IS KEPT. Subsumption is asymmetric and a symmetric aggregation would
destroy the only thing that matters. Each relation is carried by two weight
matrices, one for messages arriving from parents and one from children, so
"what subsumes me" and "what I subsume" are never mixed.

ALL RELATIONS ARE USED AS STRUCTURE, ONLY TWO AS SUPERVISION. GO has 77,600
edges, of which 69,188 are is_a or part_of and 8,412 are the three regulates
kinds. An annotation does not propagate along a regulates edge, so those cannot
define subsumption, but they do say something true about the term and there is
no reason to throw that away when computing a representation.

DEPTH OF PASSING IS DELIBERATELY SMALL. Three layers see a three-hop
neighbourhood, and GO chains run far deeper than that. The long-range
consistency comes from the order loss over the closure, not from message
passing, which is also what keeps this away from the over-smoothing that eats
deep graph networks on 40,214 nodes with a median of two children.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import torch
from torch import Tensor, nn

from protea.core.ontology.dag import Dag
from protea.core.ontology.order_encoder import TermVectors

#: Relations used to build the computation graph. Order is fixed because the
#: weight matrices are indexed by it.
GRAPH_RELATIONS: tuple[str, ...] = (
    "is_a",
    "part_of",
    "regulates",
    "negatively_regulates",
    "positively_regulates",
)


@dataclass(frozen=True)
class GraphConfig:
    in_dim: int
    out_dim: int = 64
    hidden: int = 256
    layers: int = 3
    dropout: float = 0.1
    seed: int = 0
    relations: tuple[str, ...] = field(default=GRAPH_RELATIONS)


def build_adjacency(
    dag: Dag, typed_edges: dict[str, list[tuple[str, str]]], relations: tuple[str, ...]
) -> list[Tensor]:
    """One row-normalised sparse matrix per (relation, direction).

    Row normalisation rather than the symmetric form: a term with forty
    children should not have its own identity swamped by them, and the DAG's
    degree distribution is very uneven.
    """
    n = len(dag.terms)
    idx = dag.index
    mats: list[Tensor] = []
    for rel in relations:
        pairs = [(p, c) for p, c in typed_edges.get(rel, []) if p in idx and c in idx]
        for up in (True, False):
            rows = [idx[c] if up else idx[p] for p, c in pairs]
            cols = [idx[p] if up else idx[c] for p, c in pairs]
            if not rows:
                mats.append(torch.sparse_coo_tensor(torch.empty(2, 0, dtype=torch.long),
                                                    torch.empty(0), (n, n)).coalesce())
                continue
            deg = torch.zeros(n)
            deg.index_add_(0, torch.tensor(rows), torch.ones(len(rows)))
            vals = 1.0 / deg[torch.tensor(rows)].clamp(min=1.0)
            mats.append(
                torch.sparse_coo_tensor(
                    torch.tensor([rows, cols]), vals, (n, n)
                ).coalesce()
            )
    return mats


class _RelationalLayer(nn.Module):
    """Self transform plus one transform per (relation, direction)."""

    def __init__(self, in_dim: int, out_dim: int, n_mats: int) -> None:
        super().__init__()
        self.self_w = nn.Linear(in_dim, out_dim)
        self.rel_w = nn.ModuleList(
            [nn.Linear(in_dim, out_dim, bias=False) for _ in range(n_mats)]
        )

    def forward(self, h: Tensor, mats: list[Tensor]) -> Tensor:
        out = self.self_w(h)
        for w, a in zip(self.rel_w, mats, strict=True):
            if a._nnz():
                out = out + torch.sparse.mm(a, w(h))
        return out


class GraphOrderEncoder(nn.Module):
    """Node features and a DAG in, a point in the order space per term out."""

    def __init__(self, dag: Dag, features: Tensor, mats: list[Tensor], config: GraphConfig) -> None:
        super().__init__()
        torch.manual_seed(config.seed)
        self.dag = dag
        self.config = config
        self.register_buffer("features", features)
        self.features: Tensor
        self._mats = mats
        dims = [config.in_dim] + [config.hidden] * (config.layers - 1) + [config.out_dim]
        self.layers = nn.ModuleList(
            [_RelationalLayer(dims[i], dims[i + 1], len(mats)) for i in range(config.layers)]
        )
        self.act = nn.GELU()
        self.drop = nn.Dropout(config.dropout)

    def all_terms(self) -> Tensor:
        """Every term's position, in one full-graph pass.

        Full batch rather than sampled: 40,214 nodes and 77,600 edges fit
        comfortably, and sampling a DAG's neighbourhood would change what the
        encoder sees between training and inference.
        """
        h = self.features
        for i, layer in enumerate(self.layers):
            h = layer(h, self._mats)
            if i < len(self.layers) - 1:
                h = self.drop(self.act(h))
        return torch.abs(h)

    def frozen(self) -> TermVectors:
        was = self.training
        self.eval()
        with torch.no_grad():
            out = TermVectors(self.dag, self.all_terms().cpu().numpy())
        self.train(was)
        return out

    def penalty(self, upper: Tensor, lower: Tensor) -> Tensor:
        """How badly ``upper`` fails to subsume ``lower``. Zero means it does."""
        v = self.all_terms()
        return torch.clamp(v[upper] - v[lower], min=0.0).pow(2).sum(-1)
