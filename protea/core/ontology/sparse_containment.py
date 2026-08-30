"""A term is the atoms of its ancestors plus its own, and having it is containing it.

THE ONE IDEA. Give every term a small set of atoms of its own, and define its
full code as the union of that set with the codes of all its ancestors. Then

    parent's code is contained in child's code

holds BY CONSTRUCTION rather than being learned, because the child's union
includes the parent's. And a protein has a term exactly when the term's code is
contained in the protein's:

    has(p, t)  <=>  code(t) <= code(p)   coordinate-wise

Ancestor closure follows for free: if a child's code fits inside the protein's,
so does the parent's, because it is a subset. Nothing is repaired afterwards.

WHY CONTAINMENT AND NOT A DOT PRODUCT. A dot product is symmetric and
subsumption is not, so a joint space read by cosine cannot tell a parent from a
child, which is the only thing about the ontology that matters. Measured on GO
snapshot 36038118: a free order embedding read by containment separates a true
subsumption from the same pair reversed 98.89 per cent of the time. Here that
number is not needed at all, because containment among terms is exact.

WHY IT STOPS US SPENDING CAPACITY ON WHAT WE ALREADY KNOW. The previous encoder
in this module learned the ontology's structure and reached 98.89 per cent on a
relation that is available at 100 per cent. That is a lossy compression of a
known fact with gradient steps attached. Here the structure is imposed and the
parameters are spent only on what is unknown: which atoms a term needs, and
which atoms a sequence provides.

WHY SPARSE. The two learned-code arms of the twelve-arm sweep, which read their
codes with a SYMMETRIC Jaccard, had the highest recall of all twelve (0.447 and
0.439 against 0.399 for the best arm by f_micro_w) and lost on precision. The
sparse code already finds more material than any dense protein language model.
Reading it by containment instead of by similarity is the change this module
makes.

WHY THE UNION AND NOT A POOLED VECTOR. Selection does not commute with pooling.
The residue-level arm of the sweep records this on the sequence side: it takes
the top atoms per residue and then aggregates, because aggregating first and
selecting after is a different and worse object. The same holds here: a set of
terms is the union of the members' selections, not a selection over their mean.
"""

from __future__ import annotations

from dataclasses import dataclass

import torch
from torch import Tensor, nn

from protea.core.ontology.dag import Dag


@dataclass(frozen=True)
class SparseCodeConfig:
    """Everything the codes read, recorded with the model they produced."""

    atoms: int = 1024
    #: How many atoms a term may claim as its own, on top of what it inherits.
    #: Small on purpose: a term's differentia is what distinguishes it from its
    #: parent, and GO's definitions are genus-differentia, so most of a term's
    #: meaning is already in its ancestors.
    own_k: int = 8
    #: Depth of the max-propagation. GO's longest is_a chain is well inside
    #: this; the bound exists so a malformed edge list cannot hang the pass.
    depth: int = 24
    temperature: float = 1.0
    seed: int = 0


def parent_index(dag: Dag) -> tuple[Tensor, Tensor]:
    """(child, parent) index pairs, for propagating a max up into children."""
    child, parent = [], []
    for p, c in dag.edges:
        child.append(dag.index[c])
        parent.append(dag.index[p])
    return (torch.tensor(child, dtype=torch.long),
            torch.tensor(parent, dtype=torch.long))


class SparseTermCodes(nn.Module):
    """Per-term codes whose containment mirrors the ontology exactly."""

    def __init__(self, dag: Dag, config: SparseCodeConfig) -> None:
        super().__init__()
        torch.manual_seed(config.seed)
        self.dag = dag
        self.config = config
        self.own = nn.Parameter(torch.randn(len(dag.terms), config.atoms) * 0.01)
        ch, pa = parent_index(dag)
        self.register_buffer("child_idx", ch)
        self.register_buffer("parent_idx", pa)
        self.child_idx: Tensor
        self.parent_idx: Tensor

    def codes(self) -> Tensor:
        """Every term's full code, by propagating a max down from the roots.

        Iterated rather than closed-form: the closure of GO is 783,477 pairs
        and materialising it against a thousand atoms would be a billion-entry
        tensor for a quantity twenty cheap passes produce exactly.
        """
        h = torch.relu(self.own)
        for _ in range(self.config.depth):
            up = h.index_select(0, self.parent_idx)
            nxt = h.clone().index_reduce(
                0, self.child_idx, up, "amax", include_self=True
            )
            if torch.equal(nxt, h):
                break
            h = nxt
        return h

    def sparsify(self, codes: Tensor) -> Tensor:
        """Keep each term's strongest atoms and zero the rest.

        Applied for inspection and for the sparse readout, NOT inside the
        training pass: a hard top-k has no gradient, and the containment the
        ontology imposes is already exact on the dense values.
        """
        k = min(self.config.own_k * 4, codes.shape[1])
        keep = codes.topk(k, dim=1).indices
        out = torch.zeros_like(codes)
        return out.scatter(1, keep, codes.gather(1, keep))


def containment_violation(term_code: Tensor, protein_code: Tensor) -> Tensor:
    """How much of the term the protein fails to supply. Zero means it has it.

    The asymmetric readout. ``term_code`` is (n, a) and ``protein_code`` is
    (n, a) or (1, a); the result is (n,).
    """
    return torch.clamp(term_code - protein_code, min=0.0).pow(2).sum(-1)
