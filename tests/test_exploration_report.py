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
EMPTY_PANEL = {"status": "empty", "verdict": "not_resolved"}


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


class TestARefusedPanelIsNotAnUncomputedOne:
    """Three states, because either pair of them alone gets a real case wrong.

    ``PK:BPO`` cannot be written at all while the weighted and unweighted
    branches score two populations out of one row per protein, so blocking on
    absence alone would block that exploration for ever. And
    :func:`verdict` reads its deciding panels through a comprehension that
    skips the ones that never arrived, so ignoring absence publishes a verdict
    over part of its declared evidence without saying which part.
    """

    PK_BPO = SimpleNamespace(
        setting="PK", namespace="biological_process", code="variant_populations_differ"
    )
    NK_MFO = SimpleNamespace(
        setting="NK", namespace="molecular_function", code="unmappable_row_numbering"
    )

    def _coverage(
        self,
        panels: dict[str, Any],
        deciding: list[str] | None = None,
        refusals: tuple[SimpleNamespace, ...] = (),
    ) -> tuple[dict[str, Any], list[str]]:
        session = SimpleNamespace(execute=lambda *_a, **_k: list(refusals))
        return report._panel_coverage(
            session, SimpleNamespace(id=uuid.uuid4()), _comparison(**panels), deciding
        )

    def test_a_panel_the_comparison_read_is_computed(self) -> None:
        coverage, blockers = self._coverage({"NK:BPO": A_WINS})
        assert coverage["panels"] == {"NK:BPO": {"state": "computed"}}
        assert blockers == []

    def test_a_refusal_carries_the_producers_own_code_and_does_not_block(self) -> None:
        coverage, blockers = self._coverage(
            {"PK:BPO": EMPTY_PANEL}, refusals=(self.PK_BPO,)
        )
        assert coverage["panels"]["PK:BPO"] == {
            "state": "refused",
            "code": "variant_populations_differ",
        }
        assert blockers == []

    def test_an_absence_with_no_recorded_reason_blocks(self) -> None:
        coverage, blockers = self._coverage({"PK:BPO": EMPTY_PANEL})
        assert coverage["panels"]["PK:BPO"] == {"state": "uncomputed"}
        assert blockers == ["the deciding panel PK:BPO was never computed"]

    def test_a_declared_panel_the_comparison_never_mentions_blocks(self) -> None:
        """The defect this closes: the verdict's comprehension just skips it."""
        panels = {"LK:CCO": A_WINS}
        assert report.verdict([], _comparison(**panels), ["LK:CCO", "NK:MFO"]) == "for"
        _, blockers = self._coverage(panels, deciding=["LK:CCO", "NK:MFO"])
        assert blockers == ["the deciding panel NK:MFO was never computed"]
        assert report.verdict(blockers, _comparison(**panels), ["LK:CCO", "NK:MFO"]) == "blocked"

    def test_a_refusal_for_another_cell_does_not_excuse_this_one(self) -> None:
        _, blockers = self._coverage({"PK:BPO": EMPTY_PANEL}, refusals=(self.NK_MFO,))
        assert blockers == ["the deciding panel PK:BPO was never computed"]

    def test_a_deciding_panel_no_aspect_answers_to_blocks(self) -> None:
        coverage, blockers = self._coverage({}, deciding=["NK:XXO"], refusals=(self.PK_BPO,))
        assert coverage["panels"]["NK:XXO"] == {"state": "uncomputed"}
        assert blockers == ["the deciding panel NK:XXO was never computed"]

    def test_with_nothing_declared_the_asked_for_set_stands_in(self) -> None:
        """Including the empty ones, so the record says why nine became six."""
        panels = {"LK:CCO": A_WINS, "NK:MFO": EMPTY_PANEL, "PK:BPO": EMPTY_PANEL}
        coverage, blockers = self._coverage(panels, refusals=(self.PK_BPO, self.NK_MFO))
        assert coverage["panels"] == {
            "LK:CCO": {"state": "computed"},
            "NK:MFO": {"state": "refused", "code": "unmappable_row_numbering"},
            "PK:BPO": {"state": "refused", "code": "variant_populations_differ"},
        }
        assert blockers == []


class TestAnInstrumentReadsItsPredicate:
    """A repeatability test asserts identity: the rows decide, the panels corroborate."""

    IDENTICAL_PANEL = {"status": "unresolvable", "verdict": "not_resolved", "delta": 0.0}
    SAME = {"identical": True, "rows": {"a": 10, "b": 10}, "first_difference": None}
    DIFFERENT = {
        "identical": False,
        "rows": {"a": 10, "b": 10},
        "first_difference": {"protein_accession": "P1", "go_term_id": 7, "a": "x", "b": "y"},
    }

    def _verdict(
        self, identity: dict[str, Any], panels: dict[str, Any], tolerance: float = 0.0
    ) -> Any:
        return report.verdict(
            [],
            _comparison(**panels) if panels else [],
            None,
            instrument={"identity": identity, "panel_tolerance": tolerance},
        )

    def test_identical_rows_hold_the_instrument_even_without_panels(self) -> None:
        assert self._verdict(self.SAME, {}) == "for"

    def test_identical_rows_and_identical_panels_hold_it(self) -> None:
        assert self._verdict(self.SAME, {"NK:BPO": self.IDENTICAL_PANEL}) == "for"

    def test_rows_that_differ_break_it_whatever_the_panels_say(self) -> None:
        # The panels are rounded to four decimals: a zero delta there is an
        # upper bound on equality, and it cannot outvote a row that differs.
        assert self._verdict(self.DIFFERENT, {"NK:BPO": self.IDENTICAL_PANEL}) == "against"

    def test_a_panel_beyond_its_tolerance_breaks_it(self) -> None:
        moved = {"NK:BPO": {**self.IDENTICAL_PANEL, "delta": 0.0004}}
        assert self._verdict(self.SAME, moved) == "against"
        assert self._verdict(self.SAME, moved, tolerance=0.001) == "for"

    def test_without_two_runs_there_is_no_verdict(self) -> None:
        assert self._verdict({"identical": None}, {}) is None

    def test_read_for_power_the_same_panels_would_say_the_opposite(self) -> None:
        assert (
            report.verdict([], _comparison(**{"NK:BPO": self.IDENTICAL_PANEL}), None)
            == "underpowered"
        )


class TestTheBanksCameFromOnePipeline:
    """A cold load runs the pool query and its ORDER BY; a warm one runs neither."""

    def _loads(self, a: list[str], b: list[str]) -> dict[str, dict[str, Any]]:
        return {
            "first": {"sources": a, "hosts": ["node"]},
            "second": {"sources": b, "hosts": ["node"]},
        }

    def test_two_cold_loads_are_one_pipeline(self) -> None:
        assert report.bank_load_problems(self._loads(["database"], ["database"]), "database") == []

    def test_a_cold_run_against_a_warm_one_is_two_pipelines(self) -> None:
        problems = report.bank_load_problems(self._loads(["database"], ["disk_cache"]), None)
        assert problems == [
            "the runs loaded their banks from different sources: first=database; second=disk_cache"
        ]

    def test_two_warm_loads_break_a_rule_that_asks_for_cold_ones(self) -> None:
        problems = report.bank_load_problems(
            self._loads(["disk_cache"], ["disk_cache"]), "database"
        )
        assert len(problems) == 2
        assert all("the rules require database" in p for p in problems)

    def test_an_unrecorded_or_mixed_load_cannot_be_vouched_for(self) -> None:
        problems = report.bank_load_problems(self._loads([], ["database", "disk_cache"]), None)
        assert "first: no bank load recorded" in problems
        assert "second: its bank came from database and disk_cache" in problems


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


def test_rejecting_fixes_the_fingerprint_of_the_report_it_rejected(session: Session) -> None:
    """A rejection is evidence too, and evidence names what it was about.

    The report is rebuilt from rows that keep moving, so the next build is a
    different document; a rejection with no fingerprint cannot be checked
    against the one it was taken on.
    """
    row = _running(session)
    svc.report(session, row, SERVER)
    svc.review(session, row, SERVER)
    svc.decide(session, row, "rejected", "the frame moved", AUTHOR)
    assert row.report_sha256 == "d" * 64
