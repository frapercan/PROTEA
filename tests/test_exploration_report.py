"""The report is generated from rows, and its verdict and grade follow written rules.

The verdict and the grade are first versions, written in
``protea.services.exploration_report`` so the review can change them on
purpose; these pin them as written. The service's ``report`` move and the
fingerprint the decision fixes run on SQLite with the report itself stubbed;
the report built from real rows runs on Postgres.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from typing import Any

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import protea.infrastructure.orm.models  # noqa: F401  (registers every table)
from protea.infrastructure.orm.base import Base
from protea.services import exploration_report as report
from protea.services import explorations_service as svc


def _comparison(**panels: dict[str, Any]) -> list[dict[str, Any]]:
    return [{"job_id": "j", "method_axis": [], "panels": panels, "tally": {}}]


A_WINS = {"status": "ok", "verdict": "a_greater"}
B_WINS = {"status": "ok", "verdict": "b_greater"}
NULL_WITH_POWER = {"status": "null_with_power", "verdict": "not_resolved"}
UNDERPOWERED = {"status": "underpowered", "verdict": "not_resolved"}


class TestTheVerdict:
    def test_anything_that_blocks_blocks(self) -> None:
        assert (
            report.verdict(["the results are crossed"], _comparison(**{"NK:BPO": A_WINS}), None)
            == "blocked"
        )

    def test_nothing_compared_is_no_verdict(self) -> None:
        assert report.verdict([], [], None) is None

    def test_the_base_winning_any_deciding_panel_is_against(self) -> None:
        panels = {"NK:BPO": A_WINS, "LK:MFO": B_WINS}
        assert report.verdict([], _comparison(**panels), None) == "against"

    def test_the_candidate_winning_every_deciding_panel_is_for(self) -> None:
        panels = {"NK:BPO": A_WINS, "LK:MFO": B_WINS}
        assert report.verdict([], _comparison(**panels), ["NK:BPO"]) == "for"

    def test_nulls_with_power_everywhere_are_equal(self) -> None:
        panels = {"NK:BPO": NULL_WITH_POWER, "NK:MFO": NULL_WITH_POWER}
        assert report.verdict([], _comparison(**panels), None) == "equal"

    def test_anything_else_is_underpowered(self) -> None:
        panels = {"NK:BPO": A_WINS, "NK:MFO": UNDERPOWERED}
        assert report.verdict([], _comparison(**panels), None) == "underpowered"


class TestAnInstrumentReadsItsPredicate:
    """Identical runs make a degenerate bootstrap; that is the instrument holding."""

    IDENTICAL = {"status": "unresolvable", "verdict": "not_resolved", "delta": 0.0}

    def test_identical_runs_hold_the_instrument(self) -> None:
        panels = {"NK:BPO": self.IDENTICAL, "PK:MFO": self.IDENTICAL}
        assert report.verdict([], _comparison(**panels), None, instrument_tolerance=0.0) == "for"

    def test_any_delta_beyond_the_tolerance_breaks_it(self) -> None:
        panels = {"NK:BPO": self.IDENTICAL, "PK:MFO": {**self.IDENTICAL, "delta": 0.0004}}
        assert (
            report.verdict([], _comparison(**panels), None, instrument_tolerance=0.0) == "against"
        )
        assert report.verdict([], _comparison(**panels), None, instrument_tolerance=0.001) == "for"

    def test_a_panel_without_a_delta_cannot_hold_it(self) -> None:
        panels = {"NK:BPO": {"status": "refused", "verdict": "not_computed"}}
        assert (
            report.verdict([], _comparison(**panels), None, instrument_tolerance=0.0)
            == "underpowered"
        )

    def test_read_for_power_the_same_panels_would_say_the_opposite(self) -> None:
        panels = {"NK:BPO": self.IDENTICAL}
        assert report.verdict([], _comparison(**panels), None) == "underpowered"


def _exploration(**over: Any) -> Any:
    return SimpleNamespace(**{"kind": "one_axis", "post_hoc": False, **over})


class TestTheGrade:
    @pytest.mark.parametrize(
        ("role", "expected"), [("test", "thesis"), ("valid", "development"), (None, "exploratory")]
    )
    def test_the_window_decides_where_the_figure_may_be_used(
        self, role: str | None, expected: str
    ) -> None:
        assert report.grade(_exploration(), role, failed_blocking=False) == expected

    def test_post_hoc_rules_or_a_failed_check_are_exploratory(self) -> None:
        assert (
            report.grade(_exploration(post_hoc=True), "test", failed_blocking=False)
            == "exploratory"
        )
        assert report.grade(_exploration(), "test", failed_blocking=True) == "exploratory"

    def test_an_instrument_is_a_check(self) -> None:
        assert (
            report.grade(_exploration(kind="instrument"), "test", failed_blocking=False) == "check"
        )


def test_a_layer_not_recorded_is_answered_not_recorded_and_does_not_fail() -> None:
    assert report._layer_answer({"status": "not recorded"}, ("one frame",)) == report.NOT_RECORDED
    assert report._layer_answer({"status": "crossed"}, ("one frame",)) == report.FAIL
    assert report._layer_answer({"status": "one frame"}, ("one frame",)) == report.PASS


# ---------------------------------------------------------------------------
# The service's report move, on SQLite with the report stubbed
# ---------------------------------------------------------------------------

TABLES = ("external_reference", "exploration", "exploration_link", "exploration_event")
LAB = svc.Actor(name="key:lab", type="api_key")
SERVER = svc.Actor(name="key:server", type="api_key")
AUTHOR = svc.Actor(name="author@example.org", type="user")
DOCUMENT = {"verdict": "for", "grade": "development", "checklist": {"version": "1"}}


@pytest.fixture()
def session(monkeypatch: pytest.MonkeyPatch) -> Any:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine, tables=[Base.metadata.tables[t] for t in TABLES])
    monkeypatch.setattr(svc, "_had_results_already", lambda _s, _e: False)
    monkeypatch.setattr(report, "build", lambda _s, _e: (DOCUMENT, "d" * 64))
    with Session(engine) as s:
        yield s


def _running(session: Session) -> Any:
    draft = svc.Draft(
        title="t",
        question="q",
        hypothesis="h",
        kind="one_axis",
        evaluation_set_id=uuid.uuid4(),
        rules={},
    )
    row = svc.create(session, draft, LAB)
    svc.launch(session, row, SERVER)
    return row


def test_the_report_moves_it_to_reported_with_the_whole_report_in_the_log(session: Session) -> None:
    row = _running(session)
    svc.report(session, row, SERVER)
    assert (row.state, row.verdict, row.grade, row.checklist_version) == (
        "reported",
        "for",
        "development",
        "1",
    )
    event = svc.describe(session, row)["events"][-1]
    assert event["event"] == "report"
    assert event["body"] == {"sha256": "d" * 64, "report": DOCUMENT}


def test_a_draft_is_not_reported(session: Session) -> None:
    draft = svc.Draft(
        title="t",
        question="q",
        hypothesis="h",
        kind="one_axis",
        evaluation_set_id=uuid.uuid4(),
        rules={},
    )
    with pytest.raises(svc.ExplorationConflict):
        svc.report(session, svc.create(session, draft, LAB), SERVER)


def test_accepting_fixes_the_fingerprint_of_the_report_it_accepted(session: Session) -> None:
    row = _running(session)
    svc.report(session, row, SERVER)
    svc.review(session, row, SERVER)
    svc.decide(session, row, "accepted", "the measure and its reading hold", AUTHOR)
    assert row.report_sha256 == "d" * 64


def test_rejecting_fixes_no_fingerprint(session: Session) -> None:
    row = _running(session)
    svc.report(session, row, SERVER)
    svc.review(session, row, SERVER)
    svc.decide(session, row, "rejected", "the frame moved", AUTHOR)
    assert row.report_sha256 is None
