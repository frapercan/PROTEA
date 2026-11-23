"""CAFA's and LAFA's protein-binding rule, the one the benchmark declares.

democafa drops a protein's F side at t1 when its PROPAGATED F set is a subset of
"protein binding" and its two ancestors, and drops nothing at t0; LAFA's
published truth follows it (``tests/test_lafa_profile_acceptance.py`` pins the
contract). PROTEA's own rule, ``descendants_both_ends``, stays for the windows it
built. These pin the second rule, where it runs on each side, and how a window
asks for it.
"""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock

import pytest

from protea.core._binding_rule import (
    BINDING_RULES,
    CAFA_BINDING_TERMS,
    DESCENDANTS_STEP,
    REQUESTABLE_BINDING_RULES,
    BindingStep,
    binding_step,
    drop_cafa_binding_only_f,
)
from protea.core._truth_rules import ReconciledWindow, _sides_of, refuse_unknown_rules
from tests.test_lafa_profile_acceptance import BINDING_FORM, BINDING_TERMS

PROTEIN_BINDING = "GO:0005515"
BINDING = "GO:0005488"
MF_ROOT = "GO:0003674"
IDENTICAL_PROTEIN_BINDING = "GO:0042802"  # a descendant: PROTEA's rule drops it, CAFA's not
KINASE = "GO:0004672"
A_P_TERM = "GO:0006468"

T1 = binding_step("cafa", t1_side=True)


def _window(binding_rule: str) -> ReconciledWindow:
    return ReconciledWindow(*(uuid.uuid4() for _ in range(5)), binding_rule=binding_rule)


class TestTheThreeTerms:
    def test_they_are_the_three_the_lafa_contract_pins(self) -> None:
        assert CAFA_BINDING_TERMS == frozenset(BINDING_TERMS)

    def test_the_contract_reads_them_as_a_subset(self) -> None:
        assert BINDING_FORM == "subset"


class TestTheRuleAtT1:
    def test_a_binding_only_f_side_is_dropped(self) -> None:
        by_aspect = {"P1": {"F": {PROTEIN_BINDING, BINDING, MF_ROOT}, "P": {A_P_TERM}}}
        assert drop_cafa_binding_only_f(by_aspect, T1) == 1
        assert by_aspect == {"P1": {"P": {A_P_TERM}}}

    def test_a_subset_counts_not_only_the_three(self) -> None:
        # Annotated straight to "binding": propagated, two of the three.
        by_aspect = {"P1": {"F": {BINDING, MF_ROOT}, "P": {A_P_TERM}}}
        assert drop_cafa_binding_only_f(by_aspect, T1) == 1
        assert "F" not in by_aspect["P1"]

    def test_any_other_f_term_keeps_the_whole_f_side(self) -> None:
        f_side = {PROTEIN_BINDING, BINDING, MF_ROOT, KINASE}
        by_aspect = {"P1": {"F": set(f_side)}}
        assert drop_cafa_binding_only_f(by_aspect, T1) == 0
        assert by_aspect == {"P1": {"F": f_side}}

    def test_a_descendant_of_protein_binding_is_real_function(self) -> None:
        # The divergence from PROTEA's rule the LAFA contract measures: a protein
        # whose only F term is identical protein binding keeps its F side.
        f_side = {IDENTICAL_PROTEIN_BINDING, PROTEIN_BINDING, BINDING, MF_ROOT}
        by_aspect = {"P1": {"F": set(f_side)}}
        assert drop_cafa_binding_only_f(by_aspect, T1) == 0
        assert by_aspect["P1"]["F"] == f_side

    def test_a_protein_left_with_no_aspect_leaves_the_window(self) -> None:
        by_aspect = {"P1": {"F": {PROTEIN_BINDING, BINDING, MF_ROOT}}, "P2": {"P": {A_P_TERM}}}
        assert drop_cafa_binding_only_f(by_aspect, T1) == 1
        assert by_aspect == {"P2": {"P": {A_P_TERM}}}


class TestWhereEachRuleRuns:
    def test_cafa_runs_at_t1_only(self) -> None:
        assert binding_step("cafa", t1_side=False) == BindingStep(False, False)
        assert binding_step("cafa", t1_side=True) == BindingStep(False, True)

    def test_proteas_rule_runs_at_both_ends_on_direct_terms(self) -> None:
        assert binding_step("descendants_both_ends", t1_side=False) == DESCENDANTS_STEP
        assert binding_step("descendants_both_ends", t1_side=True) == DESCENDANTS_STEP
        assert DESCENDANTS_STEP == BindingStep(on_direct_terms=True, on_propagated_f=False)

    def test_none_runs_nowhere(self) -> None:
        assert binding_step("none", t1_side=True) == BindingStep(False, False)

    def test_the_t0_side_of_cafa_is_left_alone(self) -> None:
        by_aspect = {"P1": {"F": {PROTEIN_BINDING, BINDING, MF_ROOT}}}
        assert drop_cafa_binding_only_f(by_aspect, binding_step("cafa", t1_side=False)) == 0
        assert by_aspect == {"P1": {"F": {PROTEIN_BINDING, BINDING, MF_ROOT}}}

    def test_an_unknown_rule_is_refused(self) -> None:
        with pytest.raises(ValueError, match="unknown binding rule"):
            binding_step("democafa", t1_side=True)

    @pytest.mark.parametrize("rule", REQUESTABLE_BINDING_RULES)
    def test_each_side_of_a_window_carries_its_step(self, rule: str) -> None:
        old, new = _sides_of(MagicMock(), _window(rule), None)
        assert old.binding == binding_step(rule, t1_side=False)
        assert new.binding == binding_step(rule, t1_side=True)


class TestTheVocabulary:
    def test_a_payload_can_ask_for_every_rule_but_the_one_only_a_path_applies(self) -> None:
        assert BINDING_RULES == ("descendants_both_ends", "cafa", "none")
        assert REQUESTABLE_BINDING_RULES == ("descendants_both_ends", "cafa")

    def test_a_reconciled_window_defaults_to_proteas_rule(self) -> None:
        # The default of a window that names none, so no stored window moves.
        assert ReconciledWindow(*(uuid.uuid4() for _ in range(5))).binding_rule == (
            "descendants_both_ends"
        )

    def test_a_reconciled_window_refuses_a_rule_it_cannot_build(self) -> None:
        with pytest.raises(ValueError, match="unknown binding_rule"):
            refuse_unknown_rules(_window("none"))
