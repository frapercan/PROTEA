"""The protein-binding rule: a protein whose only F annotation is GO:0005515.

CAFA and LAFA refuse to score that protein in MFO at all, because "binds a
protein" is nearly content-free and a predictor that always answers it would
score without knowing anything. democafa implements it in
``groundtruth/process_ground_truth.py``; these tests pin PROTEA's copy, including
the one property that makes the difference between a guard and a decoration:
WHERE in the pipeline it runs.
"""

import uuid
from unittest.mock import MagicMock, patch

from protea.core._binding_rule import PROTEIN_BINDING, drop_binding_only_mfo
from protea.core.evaluation import _bfs_closure, _drop_binding_only

BINDING = "GO:0005515"
BINDING_CHILD = "GO:0042802"  # identical protein binding, a real descendant
REAL_F = "GO:0004672"  # protein kinase activity
A_P_TERM = "GO:0006468"
A_C_TERM = "GO:0005634"

ASPECTS = {
    BINDING: "F",
    BINDING_CHILD: "F",
    REAL_F: "F",
    A_P_TERM: "P",
    A_C_TERM: "C",
}


class TestDropBindingOnlyMfo:
    def test_fires_when_the_only_f_term_is_binding(self):
        raw = {"P1": {BINDING, A_P_TERM}}
        dropped = drop_binding_only_mfo(raw, {BINDING}, ASPECTS)
        assert dropped == 1
        assert raw["P1"] == {A_P_TERM}, "the P side must survive untouched"

    def test_fires_on_a_descendant_too(self):
        raw = {"P1": {BINDING_CHILD}}
        dropped = drop_binding_only_mfo(raw, {BINDING, BINDING_CHILD}, ASPECTS)
        assert dropped == 1
        assert raw["P1"] == set()

    def test_spares_a_protein_with_another_f_term(self):
        raw = {"P1": {BINDING, REAL_F}}
        dropped = drop_binding_only_mfo(raw, {BINDING}, ASPECTS)
        assert dropped == 0
        assert raw["P1"] == {BINDING, REAL_F}, "binding rides along when F has content"

    def test_spares_a_protein_with_no_f_terms(self):
        raw = {"P1": {A_P_TERM, A_C_TERM}}
        assert drop_binding_only_mfo(raw, {BINDING}, ASPECTS) == 0
        assert raw["P1"] == {A_P_TERM, A_C_TERM}

    def test_counts_proteins_not_terms(self):
        raw = {"P1": {BINDING}, "P2": {BINDING, BINDING_CHILD}, "P3": {REAL_F}}
        assert drop_binding_only_mfo(raw, {BINDING, BINDING_CHILD}, ASPECTS) == 2

    def test_unknown_aspect_is_not_treated_as_f(self):
        """A go_id absent from the aspect map must not count toward the F side.

        Terms without an aspect are already excluded upstream, so one reaching
        here is a term the pivot does not bucket. Treating it as F would let it
        make an F side look non-binding and quietly spare the protein.
        """
        raw = {"P1": {BINDING, "GO:9999999"}}
        assert drop_binding_only_mfo(raw, {BINDING}, ASPECTS) == 1
        assert raw["P1"] == {"GO:9999999"}


class TestOrderInThePipeline:
    def test_after_propagation_the_rule_could_never_fire(self):
        """Why the call site sits BEFORE ancestor propagation, as an assertion.

        Propagating first pulls in the ancestors of a binding term, and those are
        not descendants of GO:0005515, so "every F term is binding" stops being
        true for anybody. The rule would run, report nothing, and look applied.
        """
        parents = {BINDING: {"GO:0005488"}, "GO:0005488": {"GO:0003674"}}
        aspects = {**ASPECTS, "GO:0005488": "F", "GO:0003674": "F"}

        direct = {"P1": {BINDING}}
        assert drop_binding_only_mfo(dict(direct), {BINDING}, aspects) == 1

        propagated = {"P1": _bfs_closure({BINDING}, parents)}
        assert drop_binding_only_mfo(propagated, {BINDING}, aspects) == 0
        assert BINDING in propagated["P1"], "and the binding term survives, scored"


class TestTheDbHelper:
    def test_resolves_the_subtree_from_the_snapshot_and_applies_it(self):
        """The subtree is read per snapshot, so a descendant counts as binding."""
        raw = {"P1": {BINDING_CHILD, A_P_TERM}, "P2": {REAL_F}}
        with (
            patch(
                "protea.core.evaluation._load_children_by_go_id",
                return_value={BINDING: {BINDING_CHILD}, BINDING_CHILD: set()},
            ),
            patch(
                "protea.core.evaluation._load_pivot_term_universe",
                return_value=(set(ASPECTS), ASPECTS),
            ),
        ):
            dropped = _drop_binding_only(MagicMock(), uuid.uuid4(), raw)
        assert dropped == 1
        assert raw["P1"] == {A_P_TERM}
        assert raw["P2"] == {REAL_F}

    def test_the_constant_is_the_term_cafa_names(self):
        assert PROTEIN_BINDING == "GO:0005515"
