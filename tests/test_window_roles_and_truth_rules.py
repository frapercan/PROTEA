"""What a window declares, without the operation: roles, rules and the frame helper.

ADR-D60 decision 1: frames are windows with a role (``training``,
``validation`` or ``test_spent``) and declared truth rules, stored with each
window. Pinned here, without a database and without the operation:

* THE ROLES. :data:`WINDOW_ROLES`, the model's CHECK and the column width
  agree, so a role the code accepts is a role the schema stores.
* THE RULES. Every value the stored rules can take is a value the CHECK
  allows; a stored set with other keys does not read back; the same-snapshot
  path declares the defaults because its compute takes no rule at all; and the
  closure is read from the graphs against the window's own ends.
* ONE FRAME, ONE SET OF RULES. The helper refuses windows whose stored rules
  differ, and windows whose rules cannot be compared at all (legacy rows and
  explicit closures), because "cannot say" is not agreement.

What the operation stores, and what its payload accepts, is in
``test_the_window_declares_the_rules_it_applied.py``.
"""

from __future__ import annotations

import inspect
import re
import uuid
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from protea.core._binding_rule import BINDING_RULES
from protea.core._truth_rules import GAIN_RULES, NOT_CODE_RULES, TRUTH_PROFILES
from protea.core._window_population import TARGET_POPULATIONS
from protea.core.evaluation import compute_evaluation_data
from protea.core.window_declarations import (
    CLOSURES,
    LAFA_FRAME,
    NAMEABLE_CLOSURES,
    NAMED_TARGET_POPULATIONS,
    NAMED_TRUTH_RULES,
    OCTOBER_HEADLINE_FRAME,
    SAME_SNAPSHOT_RULES,
    WINDOW_ROLES,
    MixedTruthRulesError,
    TruthRules,
    WindowGraphs,
    closing_graphs_by_default,
    closure_applied,
    common_truth_rules,
    common_truth_rules_of,
    stored_truth_rules,
)
from protea.infrastructure.orm.models.annotation import evaluation_set as model
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet

PER_SIDE_DEFAULTS = {
    "profile": "as_closed",
    "toi_rule": "pivot_terms",
    "not_codes": "any",
    "closure": "per_side",
    "binding": "descendants_both_ends",
}


def _quoted(sql: str) -> list[str]:
    return re.findall(r"'([^']*)'", sql)


# --------------------------------------------------------------------- roles


class TestTheRoles:
    def test_the_vocabulary_is_d40s_two_and_d60s_three(self) -> None:
        assert WINDOW_ROLES == ("valid", "test", "training", "validation", "test_spent")

    def test_the_check_lists_the_same_five(self) -> None:
        # The model spells the list out so it imports nothing from core. This
        # is what keeps the two spellings one list.
        values = _quoted(model.WINDOW_ROLE_CHECK)
        assert values == list(WINDOW_ROLES)

    def test_the_column_is_wide_enough_for_the_longest_role(self) -> None:
        width = EvaluationSet.__table__.c.window_role.type.length
        assert width >= max(len(role) for role in WINDOW_ROLES)


# --------------------------------------------------------------------- rules


class TestTheRuleVocabularies:
    def test_the_check_closes_every_key_on_the_python_vocabularies(self) -> None:
        sql = model.TRUTH_RULES_CHECK
        for key, vocabulary in (
            ("profile", TRUTH_PROFILES),
            ("toi_rule", GAIN_RULES),
            ("not_codes", NOT_CODE_RULES),
            ("closure", CLOSURES),
            ("binding", BINDING_RULES),
        ):
            clause = re.search(rf"'{key}', ''\) IN \(([^)]*)\)", sql)
            assert clause is not None, f"no clause for {key}"
            assert _quoted(clause.group(1)) == list(vocabulary)

    def test_a_missing_key_or_a_json_null_cannot_pass_the_check(self) -> None:
        # ``->>`` gives NULL for both, and a CHECK evaluating to NULL passes.
        assert model.TRUTH_RULES_CHECK.count("COALESCE(") == 5
        assert "jsonb_typeof(truth_rules) = 'object'" in model.TRUTH_RULES_CHECK

    def test_the_column_is_nullable_jsonb_and_out_of_the_identity(self) -> None:
        column = EvaluationSet.__table__.c.truth_rules
        assert column.nullable is True
        assert column.type.__class__.__name__ == "JSONB"
        identity = next(
            c
            for c in EvaluationSet.__table__.constraints
            if getattr(c, "name", "") == "uq_evaluation_set_identity"
        )
        assert "truth_rules" not in {c.name for c in identity.columns}

    def test_an_unknown_value_is_refused_at_construction(self) -> None:
        with pytest.raises(ValueError, match="unknown closure"):
            TruthRules("projected", "live_in_both_ends", "experimental", "graph_of_t0", "cafa")
        with pytest.raises(ValueError, match="unknown profile"):
            TruthRules("remapped", "pivot_terms", "any", "per_side", "cafa")
        with pytest.raises(ValueError, match="unknown binding"):
            TruthRules("projected", "live_in_both_ends", "experimental", "t0_graph", "democafa")

    def test_the_stored_shape_round_trips(self) -> None:
        stored = OCTOBER_HEADLINE_FRAME.as_json()
        assert stored == {
            "profile": "projected",
            "toi_rule": "live_in_both_ends",
            "not_codes": "experimental",
            "closure": "t0_graph",
            "binding": "descendants_both_ends",
        }
        assert TruthRules.from_json(stored) == OCTOBER_HEADLINE_FRAME

    @pytest.mark.parametrize(
        "stored",
        [
            {"profile": "projected", "toi_rule": "live_in_both_ends", "not_codes": "any"},
            {**PER_SIDE_DEFAULTS, "population": "superset"},
            ["profile", "toi_rule", "not_codes", "closure", "binding"],
            {key: value for key, value in PER_SIDE_DEFAULTS.items() if key != "binding"},
        ],
    )
    def test_a_stored_set_with_other_keys_is_refused(self, stored: object) -> None:
        # A sixth key was written by code applying a rule this code does not,
        # and reading it as these five would merge two truths under one name. A
        # set without ``binding`` predates the rule; the migration that declared
        # it gave every stored row its own, so none is left to read.
        with pytest.raises(ValueError, match="exactly the keys"):
            TruthRules.from_json(stored)  # type: ignore[arg-type]

    def test_a_null_reads_as_legacy_and_not_as_the_defaults(self) -> None:
        assert stored_truth_rules(SimpleNamespace(id=uuid.uuid4(), truth_rules=None)) is None


class TestTheSameSnapshotPath:
    def test_its_compute_takes_no_rule_argument(self) -> None:
        """What ``SAME_SNAPSHOT_RULES`` stands on. If this path ever takes a
        rule, the rules it applies are no longer the defaults, and this test is
        what says the declaration has to change with it."""
        params = list(inspect.signature(compute_evaluation_data).parameters)
        assert params == [
            "session",
            "old_annotation_set_id",
            "new_annotation_set_id",
            "ontology_snapshot_id",
        ]

    def test_it_declares_the_defaults_no_build_time_closure_and_no_binding_rule(self) -> None:
        # ``compute_evaluation_data`` runs no protein-binding rule at all.
        assert SAME_SNAPSHOT_RULES == TruthRules(
            TRUTH_PROFILES[0], GAIN_RULES[0], NOT_CODE_RULES[0], "direct_terms", "none"
        )


class TestTheClosure:
    T0, T1, OTHER = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()

    @pytest.mark.parametrize(
        ("used", "closure"),
        [
            (("T0", "T0", "T0"), "t0_graph"),
            (("T0", "T1", "T1"), "per_side"),
            (("T1", "T1", "T1"), "t1_graph"),
            # The pivot is part of it: each side under its own graph and scored
            # at t0's is LAFA's closure, not the default construction.
            (("T0", "T1", "T0"), "per_side_t0_pivot"),
            (("T1", "T0", "T0"), "explicit"),
            (("OTHER", "OTHER", "OTHER"), "explicit"),
            (("T0", "T0", "OTHER"), "explicit"),
        ],
    )
    def test_it_is_read_from_the_graphs_against_the_ends(self, used, closure) -> None:
        graphs = WindowGraphs(self.T0, self.T1, *(getattr(self, name) for name in used))
        assert closure_applied(graphs) == closure

    def test_naming_no_rule_set_keeps_each_side_on_its_own_graph(self) -> None:
        assert closing_graphs_by_default("per_side", self.T0, self.T1) == (self.T0, self.T1)

    def test_the_t0_graph_closes_both_sides(self) -> None:
        assert closing_graphs_by_default("t0_graph", self.T0, self.T1) == (self.T0, self.T0)

    @pytest.mark.parametrize("closure", ["explicit", "direct_terms"])
    def test_a_closure_the_ends_cannot_state_cannot_be_asked_for(self, closure) -> None:
        with pytest.raises(ValueError, match="cannot be resolved"):
            closing_graphs_by_default(closure, self.T0, self.T1)


class TestTheNamedRuleSet:
    def test_the_october_headline_frame_is_the_authors_choice(self) -> None:
        # 2026-11-15: the four training windows take the rules of ea0f4334.
        assert NAMED_TRUTH_RULES["october-headline-frame"] == TruthRules(
            profile="projected",
            toi_rule="live_in_both_ends",
            not_codes="experimental",
            closure="t0_graph",
            binding="descendants_both_ends",
        )

    def test_the_lafa_frame_is_lafas_rules_and_scores_lafas_targets(self) -> None:
        # 2026-11-20: the benchmark, faithful to CAFA and LAFA (protea.core.lafa_frame).
        assert NAMED_TRUTH_RULES["lafa-frame"] == LAFA_FRAME
        assert LAFA_FRAME == TruthRules(
            profile="own_graph",
            toi_rule="both_ends_trimmed",
            not_codes="experimental",
            closure="per_side_t0_pivot",
            binding="cafa",
        )
        assert NAMED_TARGET_POPULATIONS == {"lafa-frame": "swissprot_same_sequence"}
        assert set(NAMED_TARGET_POPULATIONS) <= set(NAMED_TRUTH_RULES)
        assert set(NAMED_TARGET_POPULATIONS.values()) <= set(TARGET_POPULATIONS)

    def test_only_a_requestable_binding_rule_can_be_named(self) -> None:
        # ``none`` is what a path applies, never what a payload asks for.
        assert all(rules.binding != "none" for rules in NAMED_TRUTH_RULES.values())

    def test_every_named_closure_can_be_resolved(self) -> None:
        assert all(rules.closure in NAMEABLE_CLOSURES for rules in NAMED_TRUTH_RULES.values())


# ------------------------------------------------------------ the frame helper


def _window(rules: TruthRules | dict | None) -> SimpleNamespace:
    stored = rules.as_json() if isinstance(rules, TruthRules) else rules
    return SimpleNamespace(id=uuid.uuid4(), truth_rules=stored)


class TestOneFrameOneSetOfRules:
    def test_windows_built_under_one_set_return_it(self) -> None:
        windows = [_window(OCTOBER_HEADLINE_FRAME) for _ in range(4)]
        assert common_truth_rules(windows) == OCTOBER_HEADLINE_FRAME

    def test_mixed_rules_are_refused_and_both_sets_are_named(self) -> None:
        october = _window(OCTOBER_HEADLINE_FRAME)
        per_side = _window(TruthRules(**PER_SIDE_DEFAULTS))

        with pytest.raises(MixedTruthRulesError, match="different truth rules") as refused:
            common_truth_rules([october, per_side])

        message = str(refused.value)
        assert "closure=t0_graph" in message and "closure=per_side" in message
        assert str(october.id) in message and str(per_side.id) in message

    def test_one_rule_apart_is_enough(self) -> None:
        other = TruthRules(
            "projected", "live_in_both_ends", "any", "t0_graph", "descendants_both_ends"
        )
        with pytest.raises(MixedTruthRulesError):
            common_truth_rules([_window(OCTOBER_HEADLINE_FRAME), _window(other)])

    def test_the_binding_rule_alone_is_enough(self) -> None:
        with pytest.raises(MixedTruthRulesError):
            common_truth_rules(
                [
                    _window(OCTOBER_HEADLINE_FRAME),
                    _window(replace(OCTOBER_HEADLINE_FRAME, binding="cafa")),
                ]
            )

    def test_a_legacy_window_is_refused_by_id(self) -> None:
        legacy = _window(None)
        with pytest.raises(MixedTruthRulesError, match="legacy") as refused:
            common_truth_rules([_window(OCTOBER_HEADLINE_FRAME), legacy])
        assert str(legacy.id) in str(refused.value)

    def test_legacy_windows_alone_are_not_agreement(self) -> None:
        with pytest.raises(MixedTruthRulesError, match="legacy"):
            common_truth_rules([_window(None), _window(None)])

    def test_explicit_closures_cannot_be_compared_even_when_equal(self) -> None:
        explicit = TruthRules("projected", "live_in_both_ends", "experimental", "explicit", "cafa")
        with pytest.raises(MixedTruthRulesError, match="explicit graphs"):
            common_truth_rules([_window(explicit), _window(explicit)])

    def test_no_windows_are_refused(self) -> None:
        with pytest.raises(MixedTruthRulesError, match="no windows"):
            common_truth_rules([])

    def test_by_id_an_unknown_window_is_refused(self) -> None:
        known = SimpleNamespace(id=uuid.uuid4(), truth_rules=OCTOBER_HEADLINE_FRAME.as_json())
        missing = uuid.uuid4()
        session = MagicMock()
        session.query.return_value.filter.return_value.all.return_value = [known]

        with pytest.raises(MixedTruthRulesError, match=str(missing)):
            common_truth_rules_of(session, [known.id, missing])

    def test_by_id_known_windows_are_compared(self) -> None:
        rows = [
            SimpleNamespace(id=uuid.uuid4(), truth_rules=OCTOBER_HEADLINE_FRAME.as_json())
            for _ in range(2)
        ]
        session = MagicMock()
        session.query.return_value.filter.return_value.all.return_value = rows

        assert common_truth_rules_of(session, [r.id for r in rows]) == OCTOBER_HEADLINE_FRAME
