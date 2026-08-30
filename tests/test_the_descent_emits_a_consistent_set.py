"""What the walk emits must be a thing the ontology allows, always.

WHY THIS TEST EXISTS. Every method this project has built emits terms and then
repairs them: propagate the ancestors afterwards, drop what contradicts a
curated NOT afterwards, cut the depth afterwards. Each of those repairs has
been the site of a defect, because a step that runs after the fact is a step
somebody can forget to run. A descent cannot emit an inconsistent set, and
these tests are what makes that a property rather than a hope.

The frontier rule carries it. A term is considered only when EVERY one of its
parents is already accepted, so a child can never appear without its parents,
whatever the scores say and whatever the threshold is.
"""

from __future__ import annotations

import pytest
import torch

from protea.core.ontology.dag import Dag
from protea.core.ontology.descent import (
    DescentConfig,
    DescentModel,
    frontier,
    training_boundary,
)
from protea.core.ontology.descent_walk import walk

#: A diamond, so a term has two parents and the ALL rule can be told from ANY.
_EDGES = [
    ("root", "a"), ("root", "b"),
    ("a", "both"), ("b", "both"),
    ("both", "deep"),
    ("a", "only_a"),
]


@pytest.fixture(scope="module")
def dag() -> Dag:
    return Dag.from_pairs(_EDGES)


class TestTheFrontierIsWhatItClaims:
    def test_a_term_waits_for_all_its_parents(self, dag: Dag) -> None:
        """THE RULE. `both` has parents a and b. With only a accepted it is not
        offered, because accepting it would assert that b subsumes this protein
        when the walk decided b does not."""
        assert "both" not in frontier(dag, {"root", "a"})
        assert "both" in frontier(dag, {"root", "a", "b"})

    def test_a_single_parent_term_is_offered_at_once(self, dag: Dag) -> None:
        assert "only_a" in frontier(dag, {"root", "a"})

    def test_nothing_already_accepted_is_offered_again(self, dag: Dag) -> None:
        assert "a" not in frontier(dag, {"root", "a", "b"})

    def test_a_closed_set_offers_only_its_boundary(self, dag: Dag) -> None:
        assert frontier(dag, {"root", "a", "b", "both"}) == {"only_a", "deep"}


class TestTheBoundaryIsTheOnlyComparisonTheWalkMakes:
    def test_negatives_are_one_step_out_not_the_whole_ontology(self, dag: Dag) -> None:
        """A term deep in another branch was never offered and never had to be
        rejected, so counting it as a negative trains the model on a decision
        it will never face."""
        pos, neg = training_boundary(dag, {"root", "a", "b", "both"})
        assert set(pos) == {"a", "b", "both"}
        assert set(neg) == {"only_a", "deep"}

    def test_the_root_is_not_a_decision(self, dag: Dag) -> None:
        """Nothing subsumes it, so the walk never chose to enter it."""
        pos, _ = training_boundary(dag, {"root", "a"})
        assert "root" not in pos


class TestTheWalkCannotEmitSomethingInconsistent:
    def _model(self, dag: Dag, tau: float, seed: int) -> DescentModel:
        cfg = DescentConfig(context_dim=4, term_dim=8, hidden=16, tau=tau, seed=seed)
        return DescentModel(dag, torch.rand(len(dag.terms), 8), cfg)

    @pytest.mark.parametrize("seed", range(6))
    @pytest.mark.parametrize("tau", [0.0, 0.3, 0.5, 0.9])
    def test_every_emitted_set_is_ancestor_closed(
        self, dag: Dag, tau: float, seed: int
    ) -> None:
        """Untrained and at every threshold, including tau=0 where the walk
        accepts everything it is offered. The property is structural, so a
        random model must not be able to break it."""
        model = self._model(dag, tau, seed)
        got = set(walk(model, torch.randn(4), seeds={"root"}))
        for term in got:
            missing = [p for p in dag.parents_of(term) if p not in got | {"root"}]
            assert not missing, f"{term} emitted without {missing}"

    def test_it_stops_when_nothing_is_accepted(self, dag: Dag) -> None:
        """A threshold above every probability ends the walk at the root
        rather than running to max_steps."""
        model = self._model(dag, tau=1.01, seed=0)
        assert walk(model, torch.randn(4), seeds={"root"}) == {}

    def test_it_can_start_from_what_a_protein_already_has(self, dag: Dag) -> None:
        """A protein with history does not restart at the root: 64.2 per cent
        of new curated CCO annotations are descendants of terms the protein
        already carried, so the walk has to be able to begin there."""
        model = self._model(dag, tau=0.0, seed=0)
        got = walk(model, torch.randn(4), seeds={"root", "a", "b"})
        assert "both" in got
        assert "a" not in got, "a seed is held, not re-emitted as a decision"

    def test_the_walk_terminates_on_a_dag_with_a_long_chain(self) -> None:
        chain = Dag.from_pairs([(f"n{i}", f"n{i+1}") for i in range(60)])
        cfg = DescentConfig(context_dim=4, term_dim=8, hidden=16, tau=0.0, seed=0)
        model = DescentModel(chain, torch.rand(len(chain.terms), 8), cfg)
        got = walk(model, torch.randn(4), seeds={"n0"})
        assert len(got) <= cfg.max_steps, "max_steps did not bound the walk"


class TestItDegradesTheWayItShould:
    def test_a_lower_threshold_emits_a_superset(self, dag: Dag) -> None:
        """The threshold has to be a monotone knob or it cannot be fitted."""
        cfg = DescentConfig(context_dim=4, term_dim=8, hidden=16, seed=3)
        model = DescentModel(dag, torch.rand(len(dag.terms), 8), cfg)
        ctx = torch.randn(4)
        loose = set(walk(model, ctx, tau=0.0, seeds={"root"}))
        tight = set(walk(model, ctx, tau=0.7, seeds={"root"}))
        assert tight <= loose
