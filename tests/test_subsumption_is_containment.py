"""The ontology is imposed on the code, not learned from it.

WHY THIS TEST EXISTS. The previous encoder in this module LEARNED that a parent
subsumes a child and reached 98.89 per cent on a relation that is available at
100 per cent. That is a lossy compression of a known fact with gradient steps
attached, and the 1.11 per cent it gets wrong are wrong for no reason.

Here a term's code is the union of its own atoms with its ancestors', so
containment holds by construction. These tests are what make that a property
rather than a claim: they assert it on an untrained model, at initialisation,
because a structural guarantee that only holds after training is not one.
"""

from __future__ import annotations

import random

import pytest
import torch

from protea.core.ontology.dag import Dag
from protea.core.ontology.sparse_containment import (
    SparseCodeConfig,
    SparseTermCodes,
    containment_violation,
)


def _layered(levels: int, width: int, seed: int) -> Dag:
    """Layers with diamonds, so a term has two lineages and the union of both
    has to be respected rather than one path winning."""
    rng = random.Random(seed)
    return Dag.from_pairs([
        (f"L{i}_{j}", f"L{i+1}_{k}")
        for i in range(levels) for j in range(width)
        for k in rng.sample(range(width), 2)
    ])


@pytest.fixture(scope="module")
def dag() -> Dag:
    return _layered(levels=5, width=20, seed=0)


@pytest.fixture(scope="module")
def codes(dag: Dag) -> torch.Tensor:
    """UNTRAINED on purpose. The guarantee is structural or it is nothing."""
    return SparseTermCodes(dag, SparseCodeConfig(atoms=1024, own_k=4, seed=0)).codes().detach()


class TestContainmentIsExactAtInitialisation:
    def test_every_parent_fits_inside_every_child(self, dag: Dag, codes) -> None:
        idx = dag.index
        bad = [(p, c) for p, c in dag.edges
               if (codes[idx[p]] > codes[idx[c]] + 1e-9).any()]
        assert not bad, f"{len(bad)} of {len(dag.edges)} edges violate containment"

    def test_it_survives_transitivity(self, dag: Dag, codes) -> None:
        """A grandparent has to fit too, or the relation does not compose and
        the whole construction is pointless."""
        idx = dag.index
        pairs = sorted(dag.closure())
        bad = [(a, d) for a, d in pairs if (codes[idx[a]] > codes[idx[d]] + 1e-9).any()]
        assert not bad, f"{len(bad)} of {len(pairs)} closure pairs violate containment"

    def test_no_child_fits_inside_its_parent(self, dag: Dag, codes) -> None:
        """THE ASYMMETRY, which is the only thing about the ontology that
        matters and the thing a dot product cannot express. Without this the
        code could satisfy every test above by making all terms identical."""
        idx = dag.index
        symmetric = [(p, c) for p, c in dag.edges
                     if not (codes[idx[c]] > codes[idx[p]] + 1e-9).any()]
        assert not symmetric, f"{len(symmetric)} edges are symmetric, so the order is lost"

    def test_a_term_is_not_the_same_as_its_parent(self, dag: Dag, codes) -> None:
        """The positive control for the one above: a construction that collapsed
        every term onto its parent would pass containment perfectly."""
        idx = dag.index
        for p, c in list(dag.edges)[:40]:
            assert not torch.equal(codes[idx[p]], codes[idx[c]])


class TestTheReadoutGivesAncestorClosureForFree:
    def test_whatever_holds_a_term_holds_all_its_ancestors(self, dag: Dag, codes) -> None:
        """Nothing propagates this afterwards. Every repair step this project
        has added after the fact has been the site of a defect."""
        idx = dag.index
        deep = [t for t in dag.terms if t.startswith("L4_")][:10]
        for term in deep:
            protein = codes[idx[term]]
            for a in dag.ancestors(term):
                v = containment_violation(
                    codes[idx[a]].unsqueeze(0), protein.unsqueeze(0)
                ).item()
                assert v == pytest.approx(0.0, abs=1e-9), f"{a} did not come with {term}"

    def test_a_protein_that_holds_a_parent_need_not_hold_the_child(
        self, dag: Dag, codes
    ) -> None:
        """THE POSITIVE CONTROL. A readout that returned zero for everything
        would satisfy every assertion above. Holding a parent must leave at
        least one of its children unsupported, or the code says nothing."""
        idx = dag.index
        shallow = [t for t in dag.terms if t.startswith("L1_")][:20]
        unsupported = 0
        for term in shallow:
            protein = codes[idx[term]]
            for ch in dag.children_of(term):
                if containment_violation(
                    codes[idx[ch]].unsqueeze(0), protein.unsqueeze(0)
                ).item() > 1e-9:
                    unsupported += 1
        assert unsupported > 0, "every child was free, so containment is vacuous"


class TestThePropagationTerminates:
    def test_a_chain_longer_than_the_bound_is_still_monotone(self) -> None:
        """Depth bounds the pass; it must not break the order where it stops."""
        chain = Dag.from_pairs([(f"n{i}", f"n{i+1}") for i in range(40)])
        c = SparseTermCodes(chain, SparseCodeConfig(atoms=2048, own_k=2, depth=8, seed=0))
        v = c.codes().detach()
        idx = chain.index
        for i in range(8):
            assert not (v[idx[f"n{i}"]] > v[idx[f"n{i+1}"]] + 1e-9).any()

    def test_sparsify_keeps_the_strongest_and_zeroes_the_rest(self, dag: Dag) -> None:
        m = SparseTermCodes(dag, SparseCodeConfig(atoms=1024, own_k=4, seed=0))
        dense = m.codes().detach()
        sparse = m.sparsify(dense)
        assert (sparse != 0).sum(1).max().item() <= 16
        assert torch.all(sparse.sum(1) <= dense.sum(1) + 1e-6)


class TestTheCodeIsActuallySparse:
    """The first version of this module declared own_k and never used it.

    It left the codes as dense vectors of small positive numbers, and a
    near-zero code is contained in ANY protein, so every containment violation
    was near zero. Zero is a fixed point that satisfies every positive example,
    so nothing in the loss pushed the codes off it, and the trained model
    emitted 386 terms per protein at precision 0.0042. The tests above did not
    catch it because they assert at initialisation and the collapse happens
    during training.
    """

    def test_every_term_claims_exactly_own_k_atoms(self, dag: Dag) -> None:
        m = SparseTermCodes(dag, SparseCodeConfig(atoms=1024, own_k=6, seed=0))
        counts = (m.own_atoms().detach() > 0.5).sum(1)
        assert counts.min().item() == 6
        assert counts.max().item() == 6

    def test_a_descendant_demands_strictly_more_than_its_parent(self, dag: Dag) -> None:
        """The property that makes containment mean anything. If a child asked
        no more of a protein than its parent, holding the parent would hold the
        child and the ontology would be flat."""
        m = SparseTermCodes(dag, SparseCodeConfig(atoms=2048, own_k=4, seed=0))
        codes = m.codes().detach()
        idx = dag.index
        flat = [(p, c) for p, c in dag.edges
                if (codes[idx[c]] > 0.5).sum() <= (codes[idx[p]] > 0.5).sum()]
        assert not flat, f"{len(flat)} of {len(dag.edges)} children demand no more"

    def test_it_survives_a_gradient_step(self, dag: Dag) -> None:
        """THE GAP THAT LET THE COLLAPSE THROUGH. Asserting only at
        initialisation says nothing about the state a run actually reaches."""
        m = SparseTermCodes(dag, SparseCodeConfig(atoms=2048, own_k=4, seed=0))
        opt = torch.optim.Adam(m.parameters(), lr=0.1)
        for _ in range(40):
            opt.zero_grad()
            m.codes().sum().backward()   # pushes every atom DOWN, the collapse
            opt.step()
        counts = (m.own_atoms().detach() > 0.5).sum(1)
        assert counts.min().item() == 4, "the code collapsed under pressure"
        codes = m.codes().detach()
        idx = dag.index
        bad = [(p, c) for p, c in dag.edges
               if (codes[idx[p]] > codes[idx[c]] + 1e-6).any()]
        assert not bad, "containment broke after training"


class TestTheConstructionRefusesToSaturate:
    def test_it_will_not_run_a_configuration_that_collapses_the_order(self) -> None:
        """Found by the asymmetry test, not by reading the code. With too few
        atoms the deepest codes fill the space, a parent and its child both
        hold every atom, and containment becomes symmetric. It refuses instead
        of degrading quietly, because a symmetric containment still passes
        every other assertion here."""
        d = _layered(levels=5, width=20, seed=0)
        with pytest.raises(ValueError, match="saturate"):
            SparseTermCodes(d, SparseCodeConfig(atoms=64, own_k=6, seed=0))
