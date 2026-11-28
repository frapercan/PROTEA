"""The operation stores the truth rules it APPLIED, and the payload names a rule set.

The other half of ADR-D60's frame windows (the vocabularies and the frame
helper are pinned in ``test_window_roles_and_truth_rules.py``):

* THE PAYLOAD takes the five roles and refuses anything else, and it can name
  ``october-headline-frame`` instead of spelling the rules out, refusing a
  field that contradicts the set.
* THE ROW stores what the build applied. On the same-snapshot path the three
  rule columns keep the request, as they always did, while ``truth_rules``
  holds the defaults that path actually applies, and the build says so.
* THE OCTOBER RULES close both sides and the pivot under the graph of t0, and a
  build that cannot apply them is refused before anything is computed.
* THE REUSE says under which rules the row it serves was built, and still
  serves it.
"""

from __future__ import annotations

import uuid
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from protea.core._window_population import SameSequence
from protea.core.evaluation import EvaluationData
from protea.core.operations.generate_evaluation_set import (
    GenerateEvaluationSetOperation,
    GenerateEvaluationSetPayload,
)
from protea.core.window_declarations import (
    LAFA_FRAME,
    OCTOBER_HEADLINE_FRAME,
    SAME_SNAPSHOT_RULES,
    WINDOW_ROLES,
)

_OPERATION = "protea.core.operations.generate_evaluation_set"

PER_SIDE_DEFAULTS = {
    "profile": "as_closed",
    "toi_rule": "pivot_terms",
    "not_codes": "any",
    "closure": "per_side",
    "binding": "descendants_both_ends",
}


def _ids() -> dict[str, str]:
    return {
        "old_annotation_set_id": str(uuid.uuid4()),
        "new_annotation_set_id": str(uuid.uuid4()),
    }


# ------------------------------------------------------------------- payload


class TestThePayloadTakesTheRoles:
    @pytest.mark.parametrize("role", WINDOW_ROLES)
    def test_the_payload_accepts_every_role(self, role: str) -> None:
        assert GenerateEvaluationSetPayload(**_ids(), window_role=role).window_role == role

    @pytest.mark.parametrize("role", ["train", "holdout", "test-spent", "VALIDATION", "", 3])
    def test_the_payload_refuses_anything_else(self, role: object) -> None:
        with pytest.raises(ValueError):
            GenerateEvaluationSetPayload(**_ids(), window_role=role)


class TestThePayloadNamesARuleSet:
    def test_naming_it_fills_the_three_rule_fields(self) -> None:
        p = GenerateEvaluationSetPayload(**_ids(), truth_rule_set="october-headline-frame")
        assert (p.truth_profile, p.toi_rule, p.not_codes) == (
            "projected",
            "live_in_both_ends",
            "experimental",
        )

    def test_a_field_that_agrees_with_it_is_accepted(self) -> None:
        p = GenerateEvaluationSetPayload(
            **_ids(), truth_rule_set="october-headline-frame", not_codes="experimental"
        )
        assert p.not_codes == "experimental"

    @pytest.mark.parametrize(
        ("field", "value"),
        [("truth_profile", "as_closed"), ("toi_rule", "pivot_terms"), ("not_codes", "any")],
    )
    def test_a_field_that_contradicts_it_is_refused(self, field, value) -> None:
        # Even the default spelled out: it asks for two truths at once.
        with pytest.raises(ValueError, match=f"fixes {field}"):
            GenerateEvaluationSetPayload(
                **_ids(), truth_rule_set="october-headline-frame", **{field: value}
            )

    @pytest.mark.parametrize("name", ["october", "", "OCTOBER-HEADLINE-FRAME", 1])
    def test_an_unknown_name_is_refused(self, name: object) -> None:
        with pytest.raises(ValueError, match="unknown truth_rule_set"):
            GenerateEvaluationSetPayload(**_ids(), truth_rule_set=name)

    def test_naming_nothing_leaves_every_default_where_it_was(self) -> None:
        p = GenerateEvaluationSetPayload(**_ids())
        assert p.truth_rule_set is None
        assert (p.truth_profile, p.toi_rule, p.not_codes) == ("as_closed", "pivot_terms", "any")
        assert p.binding_rule == "descendants_both_ends"

    def test_the_lafa_frame_fills_lafas_rules_and_targets(self) -> None:
        p = GenerateEvaluationSetPayload(**_ids(), truth_rule_set="lafa-frame")
        assert (p.truth_profile, p.toi_rule, p.not_codes, p.binding_rule) == (
            "own_graph",
            "both_ends_trimmed",
            "experimental",
            "cafa",
        )
        assert p.target_population == "swissprot_same_sequence"

    @pytest.mark.parametrize(
        ("field", "value"),
        [("binding_rule", "descendants_both_ends"), ("target_population", "superset_at_t0")],
    )
    def test_the_lafa_frame_refuses_what_it_does_not_apply(self, field: str, value: str) -> None:
        with pytest.raises(ValueError, match=f"fixes {field}"):
            GenerateEvaluationSetPayload(**_ids(), truth_rule_set="lafa-frame", **{field: value})

    @pytest.mark.parametrize("rule", ["none", "democafa", ""])
    def test_a_payload_cannot_ask_for_a_binding_rule_it_cannot_request(self, rule: str) -> None:
        # ``none`` is what the same-snapshot path applies, never a request.
        with pytest.raises(ValueError, match="binding_rule"):
            GenerateEvaluationSetPayload(**_ids(), binding_rule=rule)


# ------------------------------------------------------ the operation stores


def _corpus(snapshot_id: uuid.UUID) -> MagicMock:
    """A corpus dated before the board's mark, loaded under ``snapshot_id``."""
    corpus = MagicMock()
    corpus.source_published_at = date(2024, 4, 16)
    corpus.source_version = "220"
    corpus.ontology_snapshot_id = snapshot_id
    return corpus


def _session(*gets: object, existing: object = None) -> tuple[MagicMock, list]:
    session = MagicMock()
    session.get.side_effect = list(gets)
    session.query.return_value.filter_by.return_value.filter.return_value.one_or_none.return_value = existing
    added: list = []

    def _add(row):
        row.id = uuid.uuid4()
        added.append(row)

    session.add.side_effect = _add
    return session, added


def _data() -> EvaluationData:
    return EvaluationData(nk={"P1": {"GO:0000001"}}, lk={}, pk={})


def _run(session: MagicMock, payload: dict) -> tuple[MagicMock, MagicMock, MagicMock]:
    """Execute with both compute paths and the artifact store patched."""
    emit = MagicMock()
    with (
        patch(f"{_OPERATION}.compute_evaluation_data_reconciled", return_value=_data()) as rec,
        patch(f"{_OPERATION}.compute_evaluation_data", return_value=_data()) as same,
        patch.object(GenerateEvaluationSetOperation, "_persist_groundtruth", return_value=None),
    ):
        GenerateEvaluationSetOperation().execute(session, payload, emit=emit)
    return rec, same, emit


def _events(emit: MagicMock) -> dict[str, tuple]:
    return {c.args[0]: c.args for c in emit.call_args_list}


class TestTheRowStoresWhatWasApplied:
    def test_a_default_call_declares_per_side_and_the_three_defaults(self) -> None:
        t0, t1 = uuid.uuid4(), uuid.uuid4()
        session, added = _session(_corpus(t0), _corpus(t1))

        _, _, emit = _run(session, _ids())

        assert added[0].truth_rules == PER_SIDE_DEFAULTS
        assert "generate_evaluation_set.rules_not_applied" not in _events(emit)

    def test_one_snapshot_everywhere_declares_direct_terms(self) -> None:
        snap = uuid.uuid4()
        session, added = _session(_corpus(snap), _corpus(snap))

        _, same, _ = _run(session, _ids())

        assert same.called
        assert added[0].truth_rules == SAME_SNAPSHOT_RULES.as_json()

    def test_the_same_snapshot_path_stores_what_it_did_and_not_what_was_asked(self) -> None:
        """The whole point of the column. The three identity columns keep the
        request, as they always did; ``truth_rules`` says the request did not
        run, and the build says so out loud."""
        snap = uuid.uuid4()
        session, added = _session(_corpus(snap), _corpus(snap))
        payload = {
            **_ids(),
            "truth_profile": "projected",
            "toi_rule": "live_in_both_ends",
            "not_codes": "experimental",
        }

        _, same, emit = _run(session, payload)

        assert same.called
        row = added[0]
        assert row.truth_profile == "projected"
        assert row.truth_rules == SAME_SNAPSHOT_RULES.as_json()
        event = _events(emit)["generate_evaluation_set.rules_not_applied"]
        assert event[3] == "warning"
        assert event[2]["applied"] == SAME_SNAPSHOT_RULES.as_json()

    def test_loose_rules_on_the_reconciled_path_are_applied_and_declared(self) -> None:
        t0, t1 = uuid.uuid4(), uuid.uuid4()
        session, added = _session(_corpus(t0), _corpus(t1))
        payload = {**_ids(), "truth_profile": "projected", "not_codes": "experimental"}

        rec, _, _ = _run(session, payload)

        assert rec.call_args.args[1].truth_profile == "projected"
        assert added[0].truth_rules == {
            **PER_SIDE_DEFAULTS,
            "profile": "projected",
            "not_codes": "experimental",
        }

    def test_a_new_role_reaches_the_row(self) -> None:
        session, added = _session(_corpus(uuid.uuid4()), _corpus(uuid.uuid4()))

        _run(session, {**_ids(), "window_role": "training"})

        assert added[0].window_role == "training"


class TestTheOctoberRulesBuildTheOctoberWindow:
    def test_both_sides_and_the_pivot_close_under_the_graph_of_t0(self) -> None:
        t0, t1 = uuid.uuid4(), uuid.uuid4()
        session, added = _session(_corpus(t0), _corpus(t1))

        rec, same, _ = _run(session, {**_ids(), "truth_rule_set": "october-headline-frame"})

        assert not same.called
        window = rec.call_args.args[1]
        assert (window.old_native_snapshot_id, window.new_native_snapshot_id) == (t0, t0)
        assert window.pivot_snapshot_id == t0
        assert (window.truth_profile, window.toi_rule, window.not_codes) == (
            "projected",
            "live_in_both_ends",
            "experimental",
        )
        row = added[0]
        assert row.truth_rules == OCTOBER_HEADLINE_FRAME.as_json()
        assert (row.old_native_snapshot_id, row.new_native_snapshot_id) == (t0, t0)

    def test_the_lafa_frame_builds_each_side_under_its_own_graph_and_scores_at_t0(self) -> None:
        t0, t1 = uuid.uuid4(), uuid.uuid4()
        session, added = _session(_corpus(t0), _corpus(t1))
        targets = SameSequence("2022_03", "2025_03", 3, frozenset({"P1", "P2"}))
        with (
            patch(
                f"{_OPERATION}.compute_evaluation_data_lafa",
                return_value=(_data(), {"lafa_workflow": "github.com/x@1"}),
            ) as lafa,
            patch(f"{_OPERATION}.same_sequence_at_both_ends", return_value=targets) as population,
        ):
            rec, same, _ = _run(session, {**_ids(), "truth_rule_set": "lafa-frame"})

        assert not same.called and not rec.called
        window = lafa.call_args.args[1]
        assert (window.old_native, window.new_native) == (t0, t1)
        assert population.called
        row = added[0]
        assert (row.old_native_snapshot_id, row.new_native_snapshot_id) == (t0, t1)
        assert row.pivot_snapshot_id == t0
        assert row.truth_rules == LAFA_FRAME.as_json()
        assert row.target_population == "swissprot_same_sequence"
        assert row.stats["lafa_workflow"] == "github.com/x@1"
        assert row.stats["population_swissprot_releases"] == ["2022_03", "2025_03"]

    def test_an_override_that_moves_the_closure_is_refused_before_computing(self) -> None:
        t0, t1 = uuid.uuid4(), uuid.uuid4()
        session, added = _session(_corpus(t0), _corpus(t1), MagicMock())
        payload = {
            **_ids(),
            "truth_rule_set": "october-headline-frame",
            "new_native_snapshot_id": str(t1),
        }

        with pytest.raises(ValueError, match="october-headline-frame"):
            _run(session, payload)
        assert added == []

    def test_corpora_under_one_snapshot_cannot_apply_it_and_are_refused(self) -> None:
        # The same-snapshot path would run and apply none of the rules.
        snap = uuid.uuid4()
        session, added = _session(_corpus(snap), _corpus(snap))

        with pytest.raises(ValueError, match="closure=direct_terms"):
            _run(session, {**_ids(), "truth_rule_set": "october-headline-frame"})
        assert added == []


class TestTheReuseSaysUnderWhichRules:
    SHA = "637b3517e525a982694fa5ca4b4b6ed6d5b1ea81"

    def _served(self, truth_rules: object) -> MagicMock:
        row = MagicMock()
        row.id = uuid.uuid4()
        row.stats = {"delta_proteins": 1}
        row.groundtruth_uri = "file://gt.parquet"
        row.window_role = None
        row.code_revision = self.SHA
        row.truth_rules = truth_rules
        return row

    def _reuse(self, served: MagicMock, payload: dict) -> tuple:
        session, added = _session(_corpus(uuid.uuid4()), _corpus(uuid.uuid4()), existing=served)
        with patch(f"{_OPERATION}.code_revision", return_value=self.SHA):
            rec, _, emit = _run(session, payload)
        assert not rec.called and added == []
        return _events(emit)["generate_evaluation_set.idempotent_reuse"]

    def test_matching_rules_leave_the_revision_verdict_alone(self) -> None:
        event = self._reuse(
            self._served(OCTOBER_HEADLINE_FRAME.as_json()),
            {**_ids(), "truth_rule_set": "october-headline-frame"},
        )
        assert event[3] == "info"
        assert event[2]["served_truth_rules"] == OCTOBER_HEADLINE_FRAME.as_json()
        assert event[2]["truth_rules_applied_now"] == OCTOBER_HEADLINE_FRAME.as_json()

    def test_a_legacy_row_served_for_a_named_set_is_a_warning(self) -> None:
        event = self._reuse(
            self._served(None), {**_ids(), "truth_rule_set": "october-headline-frame"}
        )
        assert event[3] == "warning"
        assert "predates declared truth rules" in event[2]["note"]

    def test_a_legacy_row_served_for_a_plain_call_keeps_todays_level(self) -> None:
        event = self._reuse(self._served(None), _ids())
        assert event[3] == "info"
        assert event[2]["served_truth_rules"] is None

    def test_a_row_declaring_other_rules_is_a_warning(self) -> None:
        event = self._reuse(self._served({**PER_SIDE_DEFAULTS, "closure": "explicit"}), _ids())
        assert event[3] == "warning"
        assert "this revision applies" in event[2]["note"]

    def test_a_role_still_rebinds_on_reuse(self) -> None:
        served = self._served(None)
        self._reuse(served, {**_ids(), "window_role": "validation"})
        assert served.window_role == "validation"
