"""Tests for the evaluation census.

The operation exists because the frame is what makes a number comparable, and
the schema does not require one. These pin the two things that matter about it:
that it is registered so it can be dispatched rather than run as a script, and
that its arithmetic does not invent recomputable rows.
"""

from __future__ import annotations

from typing import Any

from protea.core.operation_catalog import build_operation_registry
from protea.core.operations.audit_evaluation_frames import AuditEvaluationFramesOperation


def test_the_census_is_registered_and_dispatchable() -> None:
    """A procedure outside the platform is a capability that dies with the disk."""
    registry = build_operation_registry()
    op = registry.get("audit_evaluation_frames")
    assert isinstance(op, AuditEvaluationFramesOperation)


def test_it_declares_itself_read_only_in_its_description() -> None:
    """The description is what a dispatcher reads before running it against a
    shared database this project has wiped five times."""
    op = AuditEvaluationFramesOperation()
    assert "Writes nothing" in op.description


class _FakeResult:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def mappings(self) -> _FakeResult:
        return self

    def one(self) -> dict[str, Any]:
        return self._rows[0]

    def keys(self) -> Any:  # the operation reads column names off the mapping
        return self._rows[0].keys()

    def all(self) -> list[dict[str, Any]]:
        return self._rows


class _FakeSession:
    """Answers the four queries in the order the operation issues them."""

    def __init__(
        self, totals: dict[str, Any], reachable: int, combos: list[dict[str, Any]]
    ) -> None:
        self._answers = [
            _FakeResult([totals]),
            _FakeResult([{"recomputable": reachable}]),
            _FakeResult(combos),
            _FakeResult([{"results_without_job": 0}]),
        ]

    def execute(self, *_: Any, **__: Any) -> _FakeResult:
        return self._answers.pop(0)


def _run(totals: dict[str, Any], reachable: int, combos: list[dict[str, Any]]) -> dict[str, Any]:
    events: list[tuple[str, Any]] = []
    result = AuditEvaluationFramesOperation().execute(
        _FakeSession(totals, reachable, combos),  # type: ignore[arg-type]
        {},
        emit=lambda name, msg, fields, level: events.append((name, fields)),
    )
    return result.result


def test_a_row_whose_parents_are_gone_counts_as_deletable_only() -> None:
    """It can be described but not reproduced, so it can only be removed."""
    out = _run(
        {
            "n_rows": 100,
            "with_frame": 40,
            "with_window": 0,
            "with_role": 0,
            "with_arms": 0,
            "without_job": 0,
        },
        reachable=70,
        combos=[],
    )
    assert out["deletable_only"] == 30
    assert out["re_framable_by_recompute"] == 30


def test_more_framed_rows_than_recomputable_never_goes_negative() -> None:
    """Legacy rows can carry a frame label while their parents are gone. The
    verdict must not report a negative amount of work."""
    out = _run(
        {
            "n_rows": 10,
            "with_frame": 9,
            "with_window": 0,
            "with_role": 0,
            "with_arms": 0,
            "without_job": 0,
        },
        reachable=2,
        combos=[],
    )
    assert out["re_framable_by_recompute"] == 0
    assert out["deletable_only"] == 8


def test_combinations_are_reported_verbatim() -> None:
    """How many frames exist in fact, not how many we imagine."""
    out = _run(
        {
            "n_rows": 3,
            "with_frame": 3,
            "with_window": 3,
            "with_role": 3,
            "with_arms": 3,
            "without_job": 0,
        },
        reachable=3,
        combos=[
            {
                "frame": "lafa",
                "temporal_window": "FINAL_227_230",
                "leakage_role": "test",
                "has_arms": True,
                "n": 2,
            },
            {
                "frame": "internal",
                "temporal_window": None,
                "leakage_role": None,
                "has_arms": False,
                "n": 1,
            },
        ],
    )
    assert [c["n"] for c in out["combinations"]] == [2, 1]
    assert out["combinations"][1]["frame"] == "internal"
