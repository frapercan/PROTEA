"""LAFA's frame, step by step, on ontologies small enough to read.

Each class pins one step of :mod:`protea.core.lafa_frame` against what LAFA's
own code does there (``democafa_package`` at the commit the module names): the
two renamings and their order, the propagation through ``is_a`` and ``part_of``
inside one aspect, the trim of both ends, the binding rule after the trim and
the classification. One class pins the author's departure: a ``NOT`` row takes
its term, everything under it and the ancestors only they supported out of the
truth. One pins a behaviour of LAFA's that the frame keeps on purpose and the
documentation names as a flaw: a gain made by an edit to the ontology alone.
"""

from __future__ import annotations

import uuid
from collections import Counter

import pytest

from protea.core._binding_rule import CAFA_BINDING_TERMS
from protea.core._lafa_frame_data import EndOntology
from protea.core.lafa_frame import (
    LafaInputs,
    LafaWindow,
    binding_terms,
    end_truth,
    lafa_truth,
    named,
    refuse_unless_lafa,
    terms_of_interest,
    trimmed,
)

F_ROOT, BINDING, PROTEIN_BINDING = "GO:0003674", "GO:0005488", "GO:0005515"
IDENTICAL_PROTEIN_BINDING = "GO:0042802"
CATALYTIC, KINASE, PROTEIN_KINASE = "GO:0003824", "GO:0016301", "GO:0004672"
P_ROOT, PHOSPHORYLATION, PROTEIN_PHOSPHORYLATION = "GO:0008150", "GO:0016310", "GO:0006468"
MODIFICATION = "GO:0036211"
C_ROOT, NUCLEUS, CHROMOSOME = "GO:0005575", "GO:0005634", "GO:0005694"
#: A term only the t1 ontology has, under "protein binding".
NEW_BINDING = "GO:0140999"
#: A term live at t0 and obsolete at t1.
RETIRED = "GO:0000123"

_ASPECT = {
    F_ROOT: "F",
    BINDING: "F",
    PROTEIN_BINDING: "F",
    IDENTICAL_PROTEIN_BINDING: "F",
    CATALYTIC: "F",
    KINASE: "F",
    PROTEIN_KINASE: "F",
    RETIRED: "F",
    NEW_BINDING: "F",
    P_ROOT: "P",
    PHOSPHORYLATION: "P",
    PROTEIN_PHOSPHORYLATION: "P",
    MODIFICATION: "P",
    C_ROOT: "C",
    NUCLEUS: "C",
    CHROMOSOME: "C",
}

_EDGES_T0 = {
    BINDING: {F_ROOT},
    PROTEIN_BINDING: {BINDING},
    IDENTICAL_PROTEIN_BINDING: {PROTEIN_BINDING},
    CATALYTIC: {F_ROOT},
    KINASE: {CATALYTIC},
    PROTEIN_KINASE: {KINASE},
    RETIRED: {CATALYTIC},
    PHOSPHORYLATION: {P_ROOT},
    PROTEIN_PHOSPHORYLATION: {PHOSPHORYLATION},
    MODIFICATION: {P_ROOT},
    NUCLEUS: {C_ROOT},
    # part_of, which LAFA propagates through as it does is_a.
    CHROMOSOME: {NUCLEUS},
}


def _ontology(
    live: set[str], edges: dict[str, set[str]], **renamings: dict[str, str]
) -> EndOntology:
    return EndOntology(
        live={go_id: _ASPECT[go_id] for go_id in live},
        parents={child: frozenset(parents) for child, parents in edges.items() if child in live},
        alt_ids=renamings.get("alt_ids", {}),
        replaced_by=renamings.get("replaced_by", {}),
    )


T0 = _ontology(
    set(_ASPECT) - {NEW_BINDING},
    _EDGES_T0,
    alt_ids={"GO:0099991": PROTEIN_KINASE, "GO:0099992": "GO:0000001"},
    replaced_by={"GO:0000001": KINASE, "GO:0000002": "GO:0000003"},
)
#: At t1: "protein binding" has a new child, one term is retired, and protein
#: phosphorylation has a second parent that was live at t0 already.
T1 = _ontology(
    set(_ASPECT) - {RETIRED},
    {
        **_EDGES_T0,
        NEW_BINDING: {PROTEIN_BINDING},
        PROTEIN_PHOSPHORYLATION: {PHOSPHORYLATION, MODIFICATION},
    },
)


def _truth(ontology: EndOntology, positives: dict, negations: dict | None = None) -> dict:
    return end_truth(ontology, positives, negations or {}, Counter(), "t0")


class TestTheRenamings:
    def test_a_secondary_id_becomes_its_primary(self) -> None:
        counts: Counter[str] = Counter()
        assert named(T0, {"GO:0099991"}, counts, "x") == {PROTEIN_KINASE}
        assert counts == {"x_renamed_alt_id": 1}

    def test_an_obsolete_term_becomes_its_successor(self) -> None:
        counts: Counter[str] = Counter()
        assert named(T0, {"GO:0000001"}, counts, "x") == {KINASE}
        assert counts == {"x_renamed_replaced_by": 1}

    def test_the_two_run_in_lafas_order_one_step_each(self) -> None:
        # A secondary id of an obsolete term reaches its successor...
        assert named(T0, {"GO:0099992"}, Counter(), "x") == {KINASE}
        # ...and a successor that is not live is dropped, never followed.
        counts: Counter[str] = Counter()
        assert named(T0, {"GO:0000002"}, counts, "x") == set()
        assert counts == {"x_renamed_replaced_by": 1, "x_dropped_not_live": 1}

    def test_what_the_end_cannot_name_is_dropped(self) -> None:
        counts: Counter[str] = Counter()
        assert named(T0, {NEW_BINDING, "GO:9999999"}, counts, "x") == set()
        assert counts == {"x_dropped_not_live": 2}


class TestThePropagation:
    def test_ancestors_through_is_a_and_part_of_inside_each_aspect(self) -> None:
        truth = _truth(T0, {"P1": {PROTEIN_KINASE, CHROMOSOME}})
        assert truth == {
            "P1": {
                "F": {PROTEIN_KINASE, KINASE, CATALYTIC, F_ROOT},
                "C": {CHROMOSOME, NUCLEUS, C_ROOT},
            }
        }

    def test_each_end_closes_under_its_own_graph(self) -> None:
        assert _truth(T0, {"P1": {PROTEIN_PHOSPHORYLATION}})["P1"]["P"] == {
            PROTEIN_PHOSPHORYLATION,
            PHOSPHORYLATION,
            P_ROOT,
        }
        assert MODIFICATION in _truth(T1, {"P1": {PROTEIN_PHOSPHORYLATION}})["P1"]["P"]

    def test_a_cycle_ends(self) -> None:
        cyclic = _ontology(
            {C_ROOT, NUCLEUS, CHROMOSOME}, {NUCLEUS: {CHROMOSOME}, CHROMOSOME: {NUCLEUS}}
        )
        assert _truth(cyclic, {"P1": {NUCLEUS}})["P1"]["C"] == {NUCLEUS, CHROMOSOME}


class TestTheNotRowsTakeTruthAway:
    """The author's departure from LAFA, 2026-11-20: LAFA keeps what a NOT contradicts."""

    def test_the_negated_term_everything_under_it_and_the_ancestors_only_they_supported(
        self,
    ) -> None:
        counts: Counter[str] = Counter()
        truth = end_truth(
            T0,
            {"P1": {PROTEIN_KINASE, IDENTICAL_PROTEIN_BINDING}},
            {"P1": {KINASE}},
            counts,
            "t0",
        )
        # Catalytic activity came only from the kinase: it leaves with it. The
        # root stays, because the binding annotation supports it too.
        assert truth == {"P1": {"F": {IDENTICAL_PROTEIN_BINDING, PROTEIN_BINDING, BINDING, F_ROOT}}}
        assert counts["t0_negated_proteins"] == 1
        assert counts["t0_negated_direct_terms"] == 1

    def test_a_protein_whose_every_term_is_negated_leaves(self) -> None:
        assert _truth(T0, {"P1": {KINASE}}, {"P1": {CATALYTIC}}) == {}

    def test_the_not_ids_are_named_by_the_end_that_applies_them(self) -> None:
        assert _truth(T0, {"P1": {KINASE}}, {"P1": {"GO:0000001"}}) == {}

    def test_both_ends_apply_the_not_rows_of_both(self) -> None:
        inputs = LafaInputs(
            T0,
            T1,
            t0_positives={"P1": {CATALYTIC}},
            t1_positives={"P1": {CATALYTIC, PROTEIN_KINASE}},
            negations={"P1": {PROTEIN_KINASE}},
        )
        data, _ = lafa_truth(inputs)
        # The kinase negated at either end is a gain at neither.
        assert data.pk == {}


class TestTheTrimOfBothEnds:
    def test_the_terms_of_interest_are_the_ids_live_at_both_ends(self) -> None:
        assert terms_of_interest(T0, T1) == frozenset(_ASPECT) - {NEW_BINDING, RETIRED}

    def test_trim_drops_terms_then_empty_aspects(self) -> None:
        truth = {"P1": {"F": {RETIRED}, "P": {P_ROOT}}, "P2": {"F": {RETIRED}}}
        assert trimmed(truth, frozenset({P_ROOT})) == {"P1": {"P": {P_ROOT}}}

    def test_the_known_terms_of_pk_are_trimmed_too(self) -> None:
        inputs = LafaInputs(
            T0,
            T1,
            t0_positives={"P1": {RETIRED}},
            t1_positives={"P1": {PROTEIN_KINASE}},
            negations={},
        )
        data, stats = lafa_truth(inputs)
        assert data.pk == {"P1": {PROTEIN_KINASE, KINASE}}
        # RETIRED is not live at t1: LAFA's PK_known never carries it.
        assert data.pk_known == {"P1": {CATALYTIC, F_ROOT}}
        assert stats["lafa_t0_pairs_before_trim"] == 3
        assert stats["lafa_t0_pairs_after_trim"] == 2


class TestTheBindingRule:
    def test_the_set_is_protein_binding_and_its_ancestors_at_t0(self) -> None:
        assert binding_terms(T0) == CAFA_BINDING_TERMS

    def test_a_t0_ontology_without_protein_binding_is_refused(self) -> None:
        with pytest.raises(ValueError, match="no set to test"):
            binding_terms(_ontology({F_ROOT}, {}))

    def test_it_runs_after_the_trim(self) -> None:
        # Annotated at t1 to a term only t1 has, under protein binding: the trim
        # leaves "protein binding" and its ancestors, and LAFA drops the F side.
        inputs = LafaInputs(T0, T1, {}, {"P1": {NEW_BINDING, NUCLEUS}}, {})
        data, _ = lafa_truth(inputs)
        assert data.nk == {"P1": {NUCLEUS, C_ROOT}}
        assert data.binding_dropped == {"old_exp": 0, "new_exp": 1}

    def test_a_descendant_of_protein_binding_is_real_function(self) -> None:
        inputs = LafaInputs(T0, T1, {}, {"P1": {IDENTICAL_PROTEIN_BINDING}}, {})
        data, _ = lafa_truth(inputs)
        assert data.nk == {"P1": {IDENTICAL_PROTEIN_BINDING, PROTEIN_BINDING, BINDING, F_ROOT}}

    def test_t0_is_left_alone(self) -> None:
        inputs = LafaInputs(
            T0, T1, {"P1": {PROTEIN_BINDING}}, {"P1": {PROTEIN_BINDING, CATALYTIC}}, {}
        )
        data, _ = lafa_truth(inputs)
        assert data.pk_known == {"P1": {PROTEIN_BINDING, BINDING, F_ROOT}}
        assert data.pk == {"P1": {CATALYTIC}}


class TestTheClassification:
    def test_nk_lk_and_pk(self) -> None:
        inputs = LafaInputs(
            T0,
            T1,
            t0_positives={"LK": {NUCLEUS}, "PK": {KINASE}},
            t1_positives={"NK": {NUCLEUS}, "LK": {NUCLEUS, KINASE}, "PK": {PROTEIN_KINASE}},
            negations={},
        )
        data, stats = lafa_truth(inputs)
        assert data.nk == {"NK": {NUCLEUS, C_ROOT}}
        assert data.lk == {"LK": {KINASE, CATALYTIC, F_ROOT}}
        assert data.pk == {"PK": {PROTEIN_KINASE}}
        assert data.pk_known == {"PK": {KINASE, CATALYTIC, F_ROOT}}
        assert data.known == {"LK": {NUCLEUS, C_ROOT}, "PK": {KINASE, CATALYTIC, F_ROOT}}
        # PK's gain comes from a new annotation: nothing here is the ontology's.
        assert stats["lafa_pk_pairs_made_by_the_ontology"] == 0
        assert stats["lafa_pk_proteins_made_by_the_ontology"] == 0

    def test_an_edit_to_the_ontology_alone_is_a_gain_as_in_lafa(self) -> None:
        # The same annotation at both ends; t1's graph gave the term a second
        # parent that t0 already had. LAFA counts it as new knowledge (its own
        # code asks "should not be gaining?"), and so does the frame: a known
        # flaw, kept to stay faithful and documented in the frame's page.
        inputs = LafaInputs(
            T0, T1, {"P1": {PROTEIN_PHOSPHORYLATION}}, {"P1": {PROTEIN_PHOSPHORYLATION}}, {}
        )
        data, stats = lafa_truth(inputs)
        assert data.pk == {"P1": {MODIFICATION}}
        # Recorded on every window, so the flaw is a number and not a footnote.
        assert stats["lafa_pk_pairs_made_by_the_ontology"] == 1
        assert stats["lafa_pk_proteins_made_by_the_ontology"] == 1

    def test_the_stats_name_the_reference_and_the_terms_of_interest(self) -> None:
        _, stats = lafa_truth(LafaInputs(T0, T1, {}, {}, {}))
        assert stats["lafa_workflow"].startswith("github.com/anphan0828/CAFA_forever@")
        assert stats["lafa_package"].startswith("github.com/anphan0828/democafa_package@")
        assert stats["terms_of_interest"] == len(_ASPECT) - 2


class TestTheWindowMustAskForLafasRules:
    def _window(self, **over: str) -> LafaWindow:
        rules = {
            "truth_profile": "own_graph",
            "toi_rule": "both_ends_trimmed",
            "not_codes": "experimental",
            "binding_rule": "cafa",
            **over,
        }
        return LafaWindow(*(uuid.uuid4() for _ in range(4)), **rules)

    def test_lafas_rules_pass_with_either_not_rule(self) -> None:
        refuse_unless_lafa(self._window())
        refuse_unless_lafa(self._window(not_codes="any"))

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("truth_profile", "projected"),
            ("toi_rule", "live_in_both_ends"),
            ("binding_rule", "descendants_both_ends"),
        ],
    )
    def test_any_other_rule_is_refused(self, field: str, value: str) -> None:
        with pytest.raises(ValueError, match="LAFA's frame applies"):
            refuse_unless_lafa(self._window(**{field: value}))
