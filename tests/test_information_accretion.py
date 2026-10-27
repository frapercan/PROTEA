"""Gates for the Information Accretion artifact pipeline.

``tests/test_ia.py`` already validates the IA ARITHMETIC against an independent
reimplementation of LAFA's ``calc_ia``. It does not validate the PROPAGATION:
both sides of that comparison consume the same propagated protein sets, so a
propagation bug would cancel out and the test would still pass.

The first test here closes that hole with a genuinely different algorithm
(fixpoint relaxation over parent edges, rather than an ancestor-closure walk per
annotation). The rest gate the operation: the evidence regime that identifies
the corpus, and the structural invariants that each have a silent failure mode.
"""

from __future__ import annotations

import random

import pytest

from protea.core.ia import build_ancestors, propagate_annotations
from protea.core.ia_regimes import (
    DEFAULT_REGIME,
    EVIDENCE_REGIMES,
    LAFA_EVIDENCE,
    resolve_regime,
)
from protea.core.operations.compute_information_accretion import (
    ComputeInformationAccretionOperation,
    ComputeInformationAccretionPayload,
    InformationAccretionGateError,
    _Dag,
    ia_key_for,
)


# --------------------------------------------------------------------------
# 1. Independent propagation oracle
# --------------------------------------------------------------------------
def _relaxation_propagate(
    annotations: list[tuple[str, str]],
    parents: dict[str, set[str]],
    terms: list[str],
) -> dict[str, set[str]]:
    """Propagate by fixpoint relaxation instead of per-annotation BFS.

    Seed every term with its direct annotations, then repeatedly push each
    term's protein set into its direct parents until nothing changes. This
    shares no code path with ``propagate_annotations``: it never builds an
    ancestor closure and never walks upward from an annotation.
    """
    universe = set(terms)
    out: dict[str, set[str]] = {t: set() for t in terms}
    for protein, go_id in annotations:
        if go_id in universe:
            out[go_id].add(protein)

    changed = True
    while changed:
        changed = False
        for t in terms:
            src = out[t]
            if not src:
                continue
            for p in parents.get(t, ()):
                if p in out and not src <= out[p]:
                    out[p] |= src
                    changed = True
    return {t: s for t, s in out.items() if s}


def _random_dag(n_terms: int, seed: int) -> tuple[list[str], dict[str, set[str]]]:
    rng = random.Random(seed)
    terms = [f"GO:{i:07d}" for i in range(n_terms)]
    parents: dict[str, set[str]] = {}
    for i in range(1, n_terms):
        n_parents = rng.choice([1, 1, 2, 3])
        parents[terms[i]] = {terms[rng.randint(0, i - 1)] for _ in range(n_parents)}
    return terms, parents


@pytest.mark.parametrize("seed", range(6))
def test_propagation_matches_independent_relaxation_oracle(seed: int) -> None:
    """The gap tests/test_ia.py leaves open: is the propagation itself right?"""
    terms, parents = _random_dag(80, seed)
    rng = random.Random(5000 + seed)
    proteins = [f"prot{j}" for j in range(50)]
    annotations = [
        (rng.choice(proteins), rng.choice(terms)) for _ in range(400)
    ]

    ancestors = build_ancestors(parents)
    for t in terms:
        ancestors.setdefault(t, frozenset({t}))

    ours = propagate_annotations(annotations, ancestors)
    oracle = _relaxation_propagate(annotations, parents, terms)

    assert set(ours) == set(oracle), (
        f"term sets differ: only-ours={set(ours) - set(oracle)} "
        f"only-oracle={set(oracle) - set(ours)}"
    )
    for t in oracle:
        assert ours[t] == oracle[t], f"{t}: {ours[t]} != {oracle[t]}"


@pytest.mark.parametrize("seed", range(4))
def test_propagation_is_monotone_along_every_edge(seed: int) -> None:
    """After propagation a child's proteins are a subset of every parent's.

    This is the invariant ``term_ia`` clamps rather than raises on, so it is
    asserted directly here.
    """
    terms, parents = _random_dag(60, seed)
    rng = random.Random(9000 + seed)
    annotations = [
        (f"p{rng.randint(0, 25)}", rng.choice(terms)) for _ in range(300)
    ]
    ancestors = build_ancestors(parents)
    for t in terms:
        ancestors.setdefault(t, frozenset({t}))
    prop = propagate_annotations(annotations, ancestors)

    for child, ps in parents.items():
        cset = prop.get(child, set())
        for p in ps:
            assert cset <= prop.get(p, set()), f"{child} not subset of parent {p}"


# --------------------------------------------------------------------------
# 2. Evidence regime, the axis that was not expressible before
# --------------------------------------------------------------------------
def test_default_regime_is_board_comparable() -> None:
    assert DEFAULT_REGIME == "lafa"
    assert resolve_regime(DEFAULT_REGIME) == LAFA_EVIDENCE


def test_all_regime_means_no_predicate() -> None:
    assert resolve_regime("all") is None


def test_unknown_regime_raises_rather_than_defaulting() -> None:
    with pytest.raises(ValueError, match="unknown evidence regime"):
        resolve_regime("swissprot-ish")


def test_lafa_regime_is_the_thirteen_scored_codes() -> None:
    assert set(LAFA_EVIDENCE) == {
        "EXP", "IDA", "IPI", "IMP", "IGI", "IEP",
        "HTP", "HDA", "HMP", "HGI", "HEP",
        "IC", "TAS",
    }


def test_regimes_are_immutable() -> None:
    with pytest.raises(TypeError):
        EVIDENCE_REGIMES["all"] = ("EXP",)  # type: ignore[index]


# --------------------------------------------------------------------------
# 3. Payload
# --------------------------------------------------------------------------
def test_payload_defaults_to_lafa_regime() -> None:
    p = ComputeInformationAccretionPayload.model_validate(
        {"ontology_snapshot_id": "s", "annotation_set_id": "a"}
    )
    assert p.evidence_regime == "lafa"
    assert p.force is False


def test_payload_rejects_unknown_regime() -> None:
    with pytest.raises(ValueError):
        ComputeInformationAccretionPayload.model_validate(
            {"ontology_snapshot_id": "s", "annotation_set_id": "a",
             "evidence_regime": "everything"}
        )


def test_payload_rejects_blank_ids() -> None:
    with pytest.raises(ValueError):
        ComputeInformationAccretionPayload.model_validate(
            {"ontology_snapshot_id": "  ", "annotation_set_id": "a"}
        )


def test_artifact_key_is_namespaced_by_set_id() -> None:
    assert ia_key_for("abc-123") == "information_accretion/abc-123/IA.tsv"


# --------------------------------------------------------------------------
# 4. Gates. Each one has a silent failure mode it exists to make loud.
# --------------------------------------------------------------------------
def _gate(**over):
    """Run the gate with a small healthy fixture, overridden per test."""
    op = ComputeInformationAccretionOperation()
    terms = ["R", "A", "B"]
    parents = {"A": {"R"}, "B": {"A"}}
    ancestors = build_ancestors(parents)
    for t in terms:
        ancestors.setdefault(t, frozenset({t}))
    ppt = {"R": {"p1", "p2", "p3"}, "A": {"p1", "p2"}, "B": {"p1"}}
    ia = {"R": 0.0, "A": 0.5, "B": 0.4}
    # The gate now takes the propagated DAG as one object and the corpus
    # accounting separately. Split the overrides the same way so every test
    # below keeps overriding whichever field it cares about, unchanged.
    dag_fields = dict(
        terms=terms, parents=parents, ancestors=ancestors,
        proteins_per_term=ppt, ia=ia,
    )
    counts = dict(raw=100, dropped=0, max_drop_rate_pct=1.0)
    for key, value in over.items():
        if key in dag_fields:
            dag_fields[key] = value
        else:
            counts[key] = value
    return op._gate(_Dag(**dag_fields), **counts)


def test_gate_passes_on_a_healthy_table() -> None:
    stats = _gate()
    assert stats["tpr_violations"] == 0
    assert stats["cycles"] == 0
    assert stats["roots"] == 1
    assert stats["nonzero"] == 2


def test_gate_rejects_empty_corpus() -> None:
    with pytest.raises(InformationAccretionGateError, match="corpus is empty"):
        _gate(raw=0)


def test_gate_rejects_excessive_drop_rate() -> None:
    with pytest.raises(InformationAccretionGateError, match="not congruent"):
        _gate(raw=100, dropped=5)


def test_gate_accepts_drop_rate_under_the_limit() -> None:
    _gate(raw=1000, dropped=5)  # 0.5 percent, under the 1.0 default


def test_gate_accepts_a_cycle_and_counts_it() -> None:
    """This used to refuse, and refusing stopped being right.

    Refusing was correct while every snapshot was acyclic, because a cycle
    could then only mean a broken load. Release 221 has two real ones, so
    refusing there withholds a number instead of protecting one. The cycle is
    condensed and reported.
    """
    parents = {"A": {"B"}, "B": {"A", "X"}}
    ancestors = build_ancestors(parents)
    stats = _gate(
        terms=["A", "B", "X"], parents=parents, ancestors=ancestors,
        proteins_per_term={"A": {"p"}, "B": {"p"}, "X": {"p", "q"}},
        ia={"A": 0.25, "B": 0.25, "X": 0.0},
    )
    assert stats["cycles"] == 1
    assert stats["terms_in_cycles"] == 2


def test_gate_rejects_a_cycle_whose_members_disagree() -> None:
    """What IS still refused, because it means propagation did not close.

    Members of a cycle are ancestors of one another, so reachability
    propagation must give them the same protein set. If it did not, the
    component's IA is measured through one member against the wrong
    population, and nothing downstream would show it.
    """
    parents = {"A": {"B"}, "B": {"A", "X"}}
    ancestors = build_ancestors(parents)
    with pytest.raises(InformationAccretionGateError, match="do not share a protein set"):
        _gate(
            terms=["A", "B", "X"], parents=parents, ancestors=ancestors,
            proteins_per_term={"A": {"p", "q"}, "B": {"p"}, "X": {"p", "q"}},
            ia={"A": 0.0, "B": 0.0, "X": 0.0},
        )


def test_gate_rejects_child_larger_than_parent_intersection() -> None:
    """The violation term_ia silently clamps to 0.0."""
    with pytest.raises(InformationAccretionGateError, match="propagation is broken"):
        _gate(proteins_per_term={"R": {"p1"}, "A": {"p1", "p2", "p3"}, "B": {"p1"}})


def test_gate_rejects_negative_ia() -> None:
    with pytest.raises(InformationAccretionGateError, match="negative IA"):
        _gate(ia={"R": 0.0, "A": -0.1, "B": 0.4})


def test_gate_rejects_nonzero_root() -> None:
    with pytest.raises(InformationAccretionGateError, match="roots have non-zero IA"):
        _gate(ia={"R": 1.5, "A": 0.5, "B": 0.4})


def test_gate_rejects_a_rootless_dag() -> None:
    parents = {"R": {"A"}, "A": {"B"}, "B": {"R"}}
    with pytest.raises(InformationAccretionGateError):
        _gate(parents=parents)


def test_gate_rejects_an_all_zero_table() -> None:
    with pytest.raises(InformationAccretionGateError, match="uniform IC=1"):
        _gate(ia={"R": 0.0, "A": 0.0, "B": 0.0})


class TestANotRowIsNotEvidenceFor:
    """A row that says the protein does NOT have the term must not count as a carrier.

    IA is ``-log2`` of a term's frequency, so counting a refutation as evidence
    inflates that frequency and therefore LOWERS the weight of exactly the terms
    somebody took the trouble to refute. The damage runs the wrong way and it is
    silent: the table still has the right shape and the right term set.

    Five other queries under ``protea/core`` spell this filter the same way
    (`evaluation.py` twice, `build_go_cooccurrence`, `_training_dump_loaders`,
    `analyze_annotation_evolution`). This operation was the only one without it,
    which is what a convention carried by copying eventually costs.
    """

    @staticmethod
    def _sql_for(evidence_codes):
        from unittest.mock import MagicMock

        from protea.core.operations.compute_information_accretion import (
            ComputeInformationAccretionOperation,
        )

        session = MagicMock()
        session.execute.return_value.yield_per.return_value = []
        ComputeInformationAccretionOperation()._propagate(
            session,
            annotation_set_id="a-set",
            evidence_codes=evidence_codes,
            ancestors={},
            emit=lambda *a, **k: None,
        )
        return str(session.execute.call_args[0][0])

    def test_the_filter_is_there_when_no_regime_is_asked_for(self):
        assert "not like '%NOT%'" in self._sql_for(None)

    def test_the_filter_is_there_when_a_regime_is_asked_for(self):
        """The branch that a naive fix misses.

        The statement is built in two steps and the evidence clause is appended
        only when a regime is given. A qualifier filter appended beside it would
        apply to one branch and not the other, and the branch it would miss is
        the one every real run takes.
        """
        assert "not like '%NOT%'" in self._sql_for(("EXP", "IDA"))

    def test_a_null_qualifier_still_counts(self):
        """Most rows carry no qualifier at all; excluding them would empty the corpus."""
        sql = self._sql_for(None)
        assert "pga.qualifier is null or" in sql

    def test_it_matches_the_relation_form_and_not_a_bare_flag(self):
        """Since GAF 2.2 -- release 202 of this series -- the column is a RELATION.

        The real values are ``NOT|enables``, ``enables``, ``involved_in``. An
        equality test against ``'NOT'`` would pass ``NOT|enables`` straight
        through, which is every negated row from 202 onwards.
        """
        sql = self._sql_for(None)
        assert "not like '%NOT%'" in sql
        assert "!= 'NOT'" not in sql and "<> 'NOT'" not in sql
