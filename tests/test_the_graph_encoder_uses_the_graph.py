"""The graph has to be doing the work, not the features it is fed.

WHY THIS TEST EXISTS. A relational encoder over an ontology can post a good
subsumption number without the graph contributing anything, because the node
features already carry most of the answer. On GO the parent's name appears
verbatim inside the child's on 23.4 per cent of edges, so a model handed the
text and asked about structure is partly being handed the structure.

The assertions therefore aim at the mechanism rather than at the score.
Direction has to survive aggregation, since a symmetric encoder would destroy
the only property subsumption has. The adjacency has to point the way it claims
to. And the same encoder run on features that say nothing has to be measurably
worse, or the graph is decoration.
"""

from __future__ import annotations

import random

import numpy as np
import pytest
import torch

from protea.core.ontology.dag import Dag
from protea.core.ontology.evaluation import separates_ancestors
from protea.core.ontology.graph_encoder import (
    GRAPH_RELATIONS,
    GraphConfig,
    GraphOrderEncoder,
    build_adjacency,
)
from protea.core.ontology.order_encoder import TrainConfig
from protea.core.ontology.training import fit


def _layered(levels: int, width: int, seed: int) -> tuple[Dag, dict[str, list]]:
    rng = random.Random(seed)
    layers = [[f"L{i}_{j}" for j in range(width)] for i in range(levels)]
    sub, reg = [], []
    for i in range(levels - 1):
        for child in layers[i + 1]:
            for parent in rng.sample(layers[i], rng.randint(1, 2)):
                sub.append((parent, child))
    # A relation annotations do NOT propagate along, present so the encoder has
    # to keep it apart from the two that define subsumption.
    for _ in range(width):
        a, b = rng.choice(layers[1]), rng.choice(layers[2])
        reg.append((a, b))
    return Dag.from_pairs(sub), {"is_a": sub, "regulates": reg}


@pytest.fixture(scope="module")
def graph() -> tuple[Dag, dict[str, list]]:
    return _layered(levels=6, width=40, seed=0)


class TestTheAdjacencyPointsWhereItSays:
    def test_there_are_two_matrices_per_relation(self, graph) -> None:
        """One for messages from parents, one from children. A single symmetric
        matrix would mix "what subsumes me" with "what I subsume"."""
        dag, typed = graph
        mats = build_adjacency(dag, typed, GRAPH_RELATIONS)
        assert len(mats) == 2 * len(GRAPH_RELATIONS)

    def test_a_relation_with_no_edges_is_empty_not_absent(self, graph) -> None:
        """Its weights still exist, so the encoder is the same shape whatever
        an ontology happens to declare."""
        dag, typed = graph
        mats = build_adjacency(dag, typed, GRAPH_RELATIONS)
        assert [m._nnz() for m in mats[GRAPH_RELATIONS.index("part_of") * 2 :][:2]] == [0, 0]

    def test_the_upward_matrix_carries_children_to_parents(self, graph) -> None:
        dag, typed = graph
        mats = build_adjacency(dag, typed, GRAPH_RELATIONS)
        up = mats[0].to_dense()
        parent, child = typed["is_a"][0]
        assert up[dag.index[child], dag.index[parent]] > 0
        # and not the other way, unless that edge exists in its own right
        if (child, parent) not in set(typed["is_a"]):
            assert up[dag.index[parent], dag.index[child]] == 0

    def test_rows_are_normalised(self, graph) -> None:
        """A term with forty children should not have its identity swamped."""
        dag, typed = graph
        rows = mats_rows = build_adjacency(dag, typed, GRAPH_RELATIONS)[0].to_dense().sum(1)
        nonzero = mats_rows[rows > 0]
        assert torch.allclose(nonzero, torch.ones_like(nonzero), atol=1e-5)


class TestTheGraphIsDoingTheWork:
    def _train(self, dag: Dag, typed: dict, features: np.ndarray) -> GraphOrderEncoder:
        closure = set(dag.closure())
        mats = build_adjacency(dag, typed, GRAPH_RELATIONS)
        cfg = GraphConfig(in_dim=features.shape[1], out_dim=16, hidden=32, layers=2, seed=0)
        model = GraphOrderEncoder(dag, torch.tensor(features), mats, cfg)
        return fit(model, sorted(closure),  # type: ignore[arg-type]
                   closure, TrainConfig(epochs=40, batch=4096, lr=3e-3, negatives=6, seed=0))

    def test_it_learns_the_relation_from_uninformative_features(self, graph) -> None:
        """THE POINT OF THE WHOLE THING. Every node is handed the SAME feature
        vector, so the text says nothing and any separation at all has to have
        come through the edges. If this fails the encoder is a lookup table
        wearing a graph's clothes."""
        dag, typed = graph
        flat = np.ones((len(dag.terms), 8), dtype=np.float32)
        model = self._train(dag, typed, flat)
        acc, _ = separates_ancestors(
            model.frozen(), dag, set(dag.closure()), n=400, seed=1, hard=True
        )
        assert acc > 0.70, f"the graph carried nothing: {acc:.2%}"

    def test_an_untrained_graph_encoder_is_near_chance(self, graph) -> None:
        """THE CONTROL. Without it the number above could come from the shape
        of the ontology rather than from anything being learned."""
        dag, typed = graph
        flat = np.ones((len(dag.terms), 8), dtype=np.float32)
        mats = build_adjacency(dag, typed, GRAPH_RELATIONS)
        cfg = GraphConfig(in_dim=8, out_dim=16, hidden=32, layers=2, seed=0)
        untrained = GraphOrderEncoder(dag, torch.tensor(flat), mats, cfg)
        acc, _ = separates_ancestors(
            untrained.frozen(), dag, set(dag.closure()), n=400, seed=1, hard=True
        )
        assert acc < 0.70


class TestTheFrozenViewMatchesTheModel:
    def test_the_cached_vectors_give_the_same_penalty(self, graph) -> None:
        """The evaluation reads a cached matrix rather than re-running the
        graph forty thousand times. If the two disagree, every number this
        module reports is about the cache."""
        dag, typed = graph
        mats = build_adjacency(dag, typed, GRAPH_RELATIONS)
        cfg = GraphConfig(in_dim=8, out_dim=16, hidden=32, layers=2, seed=0)
        model = GraphOrderEncoder(
            dag, torch.tensor(np.ones((len(dag.terms), 8), dtype=np.float32)), mats, cfg
        )
        model.eval()
        pairs = sorted(dag.closure())[:32]
        idx = dag.index
        with torch.no_grad():
            direct = model.penalty(
                torch.tensor([idx[a] for a, _ in pairs]),
                torch.tensor([idx[b] for _, b in pairs]),
            ).numpy()
        assert np.allclose(direct, model.frozen().score_pairs(pairs), atol=1e-5)
