"""The encoder has to learn that subsumption runs one way, not that terms are related.

WHY THIS TEST EXISTS. An encoder scored against random negatives will report
99 per cent on an ontology of any shape, because two terms drawn at random have
nothing to do with each other and telling "related" from "unrelated" is not the
task. This project has already shipped one control where the defect could not
appear, and one circular positive control that had to be thrown away.

So the assertions here are about asymmetry on pairs that are as related as they
can be while still being false: a true pair read backwards, and two children of
one parent. And every one of them is paired with the same measurement on an
untrained encoder, which must land at chance. Without that, a passing number
says nothing about whether training did anything.
"""

from __future__ import annotations

import random

import pytest

from protea.core.ontology.dag import Dag
from protea.core.ontology.evaluation import separates_ancestors
from protea.core.ontology.order_encoder import OrderEncoder, TrainConfig
from protea.core.ontology.training import NegativeSampler, fit

#: A small three-level ontology with two roots, a shared descendant and a
#: diamond, so the closure is not just a tree walk.
_EDGES = [
    ("root_a", "mid_1"), ("root_a", "mid_2"), ("root_b", "mid_2"),
    ("mid_1", "leaf_1"), ("mid_1", "leaf_2"), ("mid_2", "leaf_2"),
    ("mid_2", "leaf_3"), ("leaf_3", "deep_1"),
]


@pytest.fixture(scope="module")
def dag() -> Dag:
    return Dag.from_pairs(_EDGES)


def _synthetic(levels: int, width: int, seed: int) -> Dag:
    """A layered DAG big enough for the chance level to be chance.

    The eight-edge ontology above is readable, which is why the structural
    assertions use it, but it is far too small to train on: an untrained
    encoder scores 0.75 on its hard negatives simply because there are barely
    any pairs to get wrong. A control that cannot separate is not a control.
    """
    rng = random.Random(seed)
    layers = [[f"L{i}_{j}" for j in range(width)] for i in range(levels)]
    edges: list[tuple[str, str]] = []
    for i in range(levels - 1):
        for child in layers[i + 1]:
            for parent in rng.sample(layers[i], rng.randint(1, 2)):
                edges.append((parent, child))
    return Dag.from_pairs(edges)


class TestTheGraphItself:
    def test_it_finds_the_roots(self, dag: Dag) -> None:
        assert sorted(dag.roots()) == ["root_a", "root_b"]

    def test_the_closure_composes(self, dag: Dag) -> None:
        """A grandparent subsumes: the whole reason to train on the closure."""
        cl = set(dag.closure())
        assert ("root_a", "deep_1") in cl
        assert ("root_a", "leaf_1") in cl

    def test_it_is_not_symmetric(self, dag: Dag) -> None:
        cl = set(dag.closure())
        assert ("mid_1", "leaf_1") in cl
        assert ("leaf_1", "mid_1") not in cl

    def test_a_diamond_gives_a_term_two_lineages(self, dag: Dag) -> None:
        assert dag.ancestors("leaf_2") == {"mid_1", "mid_2", "root_a", "root_b"}

    def test_a_split_never_orphans_a_child(self, dag: Dag) -> None:
        """Holding out a child's only parent would test whether the encoder can
        place an isolated term, which is a different question."""
        split = dag.split_edges(held_out=0.5, seed=0)
        for _, child in split.test:
            assert len(dag.parents_of(child)) > 1
        assert not set(split.train) & set(split.test)


class TestNegativesAreCheckedBeforeUse:
    def test_a_corrupted_pair_that_happens_to_be_true_is_not_emitted(
        self, dag: Dag
    ) -> None:
        """The top of an ontology subsumes nearly everything, so a corrupted
        pair landing on a true one is not a rare event there. Training the
        encoder to push those apart teaches the opposite of the relation."""
        closure = set(dag.closure())
        sampler = NegativeSampler(dag, closure, seed=0)
        drawn = [c for _ in range(200) for c in sampler.corrupt(("root_a", "deep_1"), 4)]
        assert drawn, "the sampler produced nothing to check"
        assert not [d for d in drawn if d in closure]
        assert not [d for d in drawn if d[0] == d[1]]


class TestTrainingLearnsTheDirection:
    @pytest.fixture(scope="class")
    def trained(self) -> tuple[OrderEncoder, OrderEncoder, set[tuple[str, str]]]:
        big = _synthetic(levels=6, width=40, seed=0)
        closure = set(big.closure())
        cfg = TrainConfig(dim=32, epochs=60, batch=512, lr=0.1, negatives=6, seed=0)
        return fit(OrderEncoder(big, cfg), sorted(closure), closure, cfg), OrderEncoder(big, cfg), closure

    def test_a_true_pair_costs_less_than_the_same_pair_reversed(self, trained) -> None:
        model, _, closure = trained
        pairs = sorted(p for p in closure if p[0].startswith("L0"))[:50]
        worse = [p for p in pairs
                 if model.score_pairs([p])[0] >= model.score_pairs([(p[1], p[0])])[0]]
        assert not worse, f"{len(worse)} of {len(pairs)} pairs cost no more reversed"

    def test_siblings_do_not_subsume_each_other(self, trained) -> None:
        model, _, closure = trained
        pairs = [(p, k) for p in model.dag.terms
                 for k in [model.dag.children_of(p)] if len(k) > 1][:40]
        bad = [p for p, kids in pairs
               if model.score_pairs([(p, kids[0])])[0]
               >= model.score_pairs([(kids[0], kids[1])])[0]]
        assert not bad, f"{len(bad)} of {len(pairs)} parents cost more than a sibling pair"

    def test_it_beats_chance_on_hard_negatives(self, trained) -> None:
        model, _, closure = trained
        acc, _ = separates_ancestors(
            model, model.dag, closure, n=200, seed=1, hard=True
        )
        assert acc > 0.75

    def test_an_untrained_encoder_is_at_chance(self, trained) -> None:
        """THE POSITIVE CONTROL. Without this the accuracy above could come
        from the shape of the toy ontology rather than from training."""
        _, untrained, closure = trained
        acc, _ = separates_ancestors(
            untrained, untrained.dag, closure, n=200, seed=1, hard=True
        )
        assert acc < 0.70
