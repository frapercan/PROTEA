"""Which NOT annotations may take truth away, and that the answer travels.

A NOT row removes its term and everything under it from the truth. Until now
the loader never asked what evidence the NOT carried, so a ``NOT|IBA`` -- an
inferred denial -- took a term away from a truth whose positives are
experimental only. The lab's frame denies with experiment alone, and
Francisco declared that rule for the long validation window on 2026-10-27.

Measured on 211 -> 227 with everything else held fixed, the two rules differ
by three proteins of the delta (two of them LK) and nine projected entries,
which is the whole distance between reproducing the lab's truth and missing
it. 9.262 of the 17.178 NOT rows across the two ends are non-experimental, so
the effect is small not because the rows are few but because few of them land
on a delta protein.

Half of this file is seams rather than rule, deliberately. The remapping was
written, unit-tested clause by clause, and never wired: ``_projection_request``
built its request without the remap and every rule test still passed. What
caught it was an acceptance number nine entries off. So each test below names
the join it exercises, and each one fails if that single assignment is removed.
"""

from __future__ import annotations

import uuid
from datetime import date
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

import protea.core.evaluation as ev
from protea.core._truth_rules import NOT_CODE_RULES, ReconciledWindow
from protea.core.operations.generate_evaluation_set import (
    GenerateEvaluationSetPayload,
)


class _RecordingSession:
    """A session that remembers the SQL it was handed and answers nothing.

    The query under test uses ``evidence_code = ANY(:exp_codes)``, which is
    the idiom the module already uses twice and is Postgres-only, so the
    filter cannot be exercised against sqlite. What CAN be checked without a
    server is that the clause and its parameter are there, which is exactly
    what the wiring decides.
    """

    def __init__(self) -> None:
        self.statements: list[str] = []
        self.params: list[dict[str, Any]] = []

    def execute(self, statement: Any, params: dict[str, Any] | None = None) -> Any:
        self.statements.append(str(statement))
        self.params.append(params or {})
        result = MagicMock()
        result.fetchall.return_value = []
        return result

    def get(self, *_args: Any, **_kwargs: Any) -> Any:  # pragma: no cover - unused
        return None


class TestTheVocabularyIsClosed:
    """Two rules, named, and nothing else accepted anywhere."""

    def test_the_two_rules(self) -> None:
        assert NOT_CODE_RULES == ("any", "experimental")

    def test_any_is_the_default_so_no_stored_window_moves(self) -> None:
        assert NOT_CODE_RULES[0] == "any"
        assert ReconciledWindow(
            uuid.uuid4(), uuid.uuid4(), uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
        ).not_codes == "any"
        assert GenerateEvaluationSetPayload(
            old_annotation_set_id=str(uuid.uuid4()),
            new_annotation_set_id=str(uuid.uuid4()),
        ).not_codes == "any"

    @pytest.mark.parametrize("rule", NOT_CODE_RULES)
    def test_the_payload_accepts_both(self, rule: str) -> None:
        payload = GenerateEvaluationSetPayload(
            old_annotation_set_id=str(uuid.uuid4()),
            new_annotation_set_id=str(uuid.uuid4()),
            not_codes=rule,
        )
        assert payload.not_codes == rule

    def test_the_payload_refuses_an_unknown_rule(self) -> None:
        with pytest.raises(ValueError, match="unknown not_codes"):
            GenerateEvaluationSetPayload(
                old_annotation_set_id=str(uuid.uuid4()),
                new_annotation_set_id=str(uuid.uuid4()),
                not_codes="experimental_and_curated",
            )

    def test_the_builder_refuses_an_unknown_rule(self) -> None:
        # Not only the payload: a window handed to the builder by any other
        # route is checked there too, beside the other two rules.
        window = ReconciledWindow(
            uuid.uuid4(),
            uuid.uuid4(),
            uuid.uuid4(),
            uuid.uuid4(),
            uuid.uuid4(),
            not_codes="only_ida",
        )
        with pytest.raises(ValueError, match="unknown not_codes"):
            ev.compute_evaluation_data_reconciled(MagicMock(), window)


class TestTheQueryAsksForWhatItWasTold:
    """The clause is in the SQL when it should be, and absent when not."""

    def test_experimental_only_adds_the_evidence_clause(self) -> None:
        session = _RecordingSession()
        ev._load_not_raw_go_ids(session, uuid.uuid4(), experimental_only=True)
        assert "evidence_code = ANY(:exp_codes)" in session.statements[0]
        assert session.params[0]["exp_codes"] == ev._EXP_CODES

    def test_the_default_asks_for_every_not_row(self) -> None:
        session = _RecordingSession()
        ev._load_not_raw_go_ids(session, uuid.uuid4())
        assert "evidence_code" not in session.statements[0]
        assert "exp_codes" not in session.params[0]

    def test_the_not_qualifier_is_still_required_either_way(self) -> None:
        # The evidence filter narrows the NOT rows; it must not become the
        # only filter, or every experimental annotation would deny itself.
        for experimental_only in (False, True):
            session = _RecordingSession()
            ev._load_not_raw_go_ids(session, uuid.uuid4(), experimental_only)
            assert "qualifier LIKE '%NOT%'" in session.statements[0]

    def test_the_codes_are_the_same_list_the_positives_use(self) -> None:
        # One list, so a truth and its denial cannot drift apart over which
        # codes count as experiment. It is 26 and not 13: the thirteen GO
        # codes plus the ECO id each one is written as in the releases that
        # use ECO, which is why the positives read it through this name
        # instead of a literal list of their own.
        from protea.core.evidence_codes import EXPERIMENTAL

        assert EXPERIMENTAL <= set(ev._EXP_CODES)
        assert len(EXPERIMENTAL) == 13
        assert all(c.startswith("ECO:") for c in set(ev._EXP_CODES) - EXPERIMENTAL)
        for inferred in ("IEA", "IBA", "ISS", "ND"):
            assert inferred not in ev._EXP_CODES


class TestTheSeams:
    """The joins. Each of these fails if one assignment is removed."""

    def test_the_window_rule_reaches_the_query(self) -> None:
        # ``_union_of_negatives`` reads ``window.not_codes`` and passes a
        # boolean down two levels. Remove that read and the clause vanishes
        # while every test above still passes.
        for rule, expected in (("experimental", True), ("any", False)):
            session = _RecordingSession()
            window = ReconciledWindow(
                uuid.uuid4(),
                uuid.uuid4(),
                uuid.uuid4(),
                uuid.uuid4(),
                uuid.uuid4(),
                not_codes=rule,
            )
            with patch.object(ev, "_load_children_by_go_id", return_value={}):
                ev._union_of_negatives(session, window, set(), {}, None)
            clauses = [s for s in session.statements if "evidence_code" in s]
            assert bool(clauses) is expected, rule

    def test_both_ends_are_asked_under_the_same_rule(self) -> None:
        # One rule for the window, not one per side: a NOT at t0 and a NOT at
        # t1 that deny the same term must be weighed the same way.
        session = _RecordingSession()
        window = ReconciledWindow(
            uuid.uuid4(),
            uuid.uuid4(),
            uuid.uuid4(),
            uuid.uuid4(),
            uuid.uuid4(),
            not_codes="experimental",
        )
        with patch.object(ev, "_load_children_by_go_id", return_value={}):
            ev._union_of_negatives(session, window, set(), {}, None)
        not_queries = [s for s in session.statements if "qualifier LIKE" in s]
        assert len(not_queries) == 2
        assert all("evidence_code = ANY(:exp_codes)" in s for s in not_queries)

    def test_the_payload_rule_reaches_the_window(self) -> None:
        # The operation builds the ReconciledWindow. This is the assignment
        # that was missing for the remap.
        from protea.core.operations.generate_evaluation_set import (
            GenerateEvaluationSetOperation,
        )

        captured: dict[str, Any] = {}

        def _capture(_session: Any, window: ReconciledWindow) -> Any:
            captured["window"] = window
            data = MagicMock()
            data.stats.return_value = {"delta_proteins": 1}
            data.nk_proteins = 1
            data.lk_proteins = 0
            data.pk_proteins = 0
            return data

        op = GenerateEvaluationSetOperation()
        session = MagicMock()
        old_set, new_set = MagicMock(), MagicMock()
        # Dated, and dated before the board's mark: a MagicMock answers every
        # attribute with another MagicMock, and the holdout guard refuses a
        # corpus whose publication date it cannot read rather than guess.
        for side, version in ((old_set, "211"), (new_set, "220")):
            side.source_published_at = date(2024, 4, 16)
            side.source_version = version
            side.ontology_snapshot_id = uuid.uuid4()
        session.get.side_effect = [old_set, new_set, MagicMock()]
        session.query.return_value.filter_by.return_value.one_or_none.return_value = None
        payload = {
            "old_annotation_set_id": str(uuid.uuid4()),
            "new_annotation_set_id": str(uuid.uuid4()),
            "not_codes": "experimental",
        }
        with patch.object(ev, "compute_evaluation_data_reconciled", _capture):
            with patch(
                "protea.core.operations.generate_evaluation_set"
                ".compute_evaluation_data_reconciled",
                _capture,
            ):
                with patch.object(
                    GenerateEvaluationSetOperation, "_persist_groundtruth", return_value=None
                ):
                    op.execute(session, payload, emit=lambda *a, **k: None)
        assert captured["window"].not_codes == "experimental"

    def test_the_rule_lands_in_the_stats_the_row_publishes(self) -> None:
        # A number published without the rule that produced it is a number
        # two runs can disagree on with nothing to tell them apart.
        import inspect

        source = inspect.getsource(
            __import__(
                "protea.core.operations.generate_evaluation_set", fromlist=["x"]
            ).GenerateEvaluationSetOperation._compute_delta
        )
        assert 'stats["not_codes"]' in source


class TestTheRowKeepsThemApart:
    """Two windows differing only in this rule are two windows."""

    def test_the_identity_holds_the_rule(self) -> None:
        from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet

        constraint = next(
            c
            for c in EvaluationSet.__table_args__
            if getattr(c, "name", None) == "uq_evaluation_set_identity"
        )
        assert "not_codes" in [c.name for c in constraint.columns]

    def test_the_column_defaults_to_any_at_the_database(self) -> None:
        from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet

        column = EvaluationSet.__table__.c.not_codes
        assert column.nullable is False
        assert column.server_default.arg == "any"
