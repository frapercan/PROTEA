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

WHAT THIS DID NOT ACHIEVE, WHICH YOU MUST READ BEFORE BUILDING ON IT. Everything
above is about the term side, and it holds: containment among terms is exact by
construction and the guard below keeps it that way. The predictor built on top
of it did not work. Trained on the full corpus, 1,889,171 pairs over all three
aspects, it emitted 386 terms per protein at precision 0.0042 and f_micro_w
0.0082, against a frequency prior at 0.0852. It lost to guessing the commonest
terms by an order of magnitude.

Its recall was the highest of anything measured (0.1989, and 0.2969 on an
earlier run), which is the same shape the two learned-code arms of the
twelve-arm sweep showed: the sparse side finds the material and cannot say no.
That is the open problem, and it is on the sequence-to-atoms side, not here.

So this module is kept for the construction and for the two failures it now
refuses to repeat, not as a working method. The rest of the branch it came from
was abandoned. If you are about to train something on it, the thing to fix first
is precision, and nothing in this file addresses precision.
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
    #: The largest share of the atom space the deepest term may demand. Past
    #: this the codes saturate and the order collapses; see the check in
    #: SparseTermCodes.__init__, which refuses rather than degrading quietly.
    max_occupancy: float = 0.5


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
        deepest = max((len(dag.ancestors(t)) for t in dag.terms), default=0) + 1
        needed = deepest * config.own_k
        if needed > config.atoms * config.max_occupancy:
            raise ValueError(
                f"a term with {deepest} ancestors claiming {config.own_k} atoms each "
                f"needs {needed} of {config.atoms}, over the {config.max_occupancy:.0%} "
                "occupancy this construction tolerates. Past it the deepest codes "
                "saturate, a parent and its child both hold every atom, and the "
                "containment becomes symmetric, which is the one thing it exists to "
                "avoid. Raise atoms or lower own_k."
            )
        ch, pa = parent_index(dag)
        self.register_buffer("child_idx", ch)
        self.register_buffer("parent_idx", pa)
        self.child_idx: Tensor
        self.parent_idx: Tensor

    def own_atoms(self) -> Tensor:
        """Exactly ``own_k`` atoms per term, at one, and the rest at zero.

        THE THING THAT WAS MISSING. The first version of this module declared
        own_k and never used it: it left ``own`` as a dense vector of small
        positive numbers, and a near-zero code is contained in ANY protein, so
        the containment violation was near zero for everything. Worse, zero is
        a FIXED POINT that satisfies every positive example, so nothing in the
        loss pushed the codes away from it. Trained that way the model emitted
        386 terms per protein at precision 0.0042, which is what "everything is
        contained" looks like from outside.

        Binary and of fixed size, so the magnitude cannot collapse. A term
        claims k atoms; a descendant claims those of every ancestor plus its
        own, so it demands strictly more of a protein than its parent does.

        The selection is hard in the forward pass and smooth in the backward
        one, so an atom a term does not currently claim can still be pulled in
        by the gradient rather than being dead for the rest of training.
        """
        soft = torch.sigmoid(self.own / self.config.temperature)
        keep = soft.topk(self.config.own_k, dim=1).indices
        hard = torch.zeros_like(soft).scatter(1, keep, 1.0)
        return hard.detach() + soft - soft.detach()

    def codes(self) -> Tensor:
        """Every term's full code, by propagating a max down from the roots.

        Iterated rather than closed-form: the closure of GO is 783,477 pairs
        and materialising it against a thousand atoms would be a billion-entry
        tensor for a quantity twenty cheap passes produce exactly.
        """
        h = self.own_atoms()
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
