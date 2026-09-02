"""Tests for the evaluation census.

The operation exists because the frame is what makes a number comparable, and
the schema does not require one. These pin the three things that matter about
it: that it is registered so it can be dispatched rather than run as a script,
that the verdict counts rows rather than subtracting two totals that describe
different populations, and that it calls the missing frame a declaration
instead of promising that a recomputation would produce one.
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
    """Answers the three queries in the order the operation issues them.

    ``reachable`` and ``reachable_unframed`` arrive as two columns of one row
    because that is how the database returns them: they are counted over the
    same join, in the same statement, so a fixture cannot express the
    impossible combination of an unframed-reachable count that exceeds the
    reachable count.
    """

    def __init__(
        self,
        totals: dict[str, Any],
        reachable: int,
        reachable_unframed: int,
        combos: list[dict[str, Any]],
    ) -> None:
        assert reachable_unframed <= reachable
        self._answers = [
            _FakeResult([totals]),
            _FakeResult(
                [{"recomputable": reachable, "needs_frame_declaration": reachable_unframed}]
            ),
            _FakeResult(combos),
        ]

    def execute(self, *_: Any, **__: Any) -> _FakeResult:
        return self._answers.pop(0)


def _run(
    totals: dict[str, Any],
    reachable: int,
    reachable_unframed: int,
    combos: list[dict[str, Any]],
) -> tuple[dict[str, Any], list[tuple[str, str, Any]]]:
    events: list[tuple[str, str, Any]] = []
    result = AuditEvaluationFramesOperation().execute(
        _FakeSession(totals, reachable, reachable_unframed, combos),  # type: ignore[arg-type]
        {},
        emit=lambda name, msg, fields, level: events.append((name, msg, fields)),
    )
    return result.result, events


def _totals(n_rows: int, with_frame: int) -> dict[str, Any]:
    return {
        "n_rows": n_rows,
        "with_frame": with_frame,
        "with_window": 0,
        "with_role": 0,
        "with_arms": 0,
        "without_job": 0,
    }


def test_the_verdict_counts_rows_that_are_both_unframed_and_reachable() -> None:
    """The two totals describe different populations, so their difference
    describes neither.

    Here 100 rows carry 40 frames and 70 rows still have both parents, but the
    frames are concentrated on the rows that are already unreachable: only 10
    of the 70 survivors carry one, so 60 need a declaration. Subtracting the
    totals gives 70 - 40 = 30, which is the smallest answer the two numbers
    permit rather than the true one -- it is right only when every framed row
    happens to be reachable.
    """
    out, _ = _run(_totals(100, 40), reachable=70, reachable_unframed=60, combos=[])
    assert out["needs_frame_declaration"] == 60
    assert out["deletable_only"] == 30


def test_framed_rows_that_lost_their_parents_do_not_hide_the_survivors() -> None:
    """Legacy rows can carry a frame label while their parents are gone.

    Nine of these ten rows are framed and only two are reachable, so the
    subtraction clamps to zero and reports that there is nothing to do. One of
    the two survivors is in fact unframed, and it is exactly the row the census
    exists to find.
    """
    out, _ = _run(_totals(10, 9), reachable=2, reachable_unframed=1, combos=[])
    assert out["needs_frame_declaration"] == 1
    assert out["deletable_only"] == 8


def test_the_verdict_asks_for_a_declaration_not_a_recomputation() -> None:
    """Nothing computes ``frame``.

    It is a payload field that run_cafa_evaluation stamps as the caller passed
    it and batch_rescore_evaluation forwards unchanged, so re-running an
    unframed row yields another unframed row unless the dispatcher declares a
    frame. A verdict phrased as "re-framed by recomputing" sends a reader
    looking for a pipeline step that does not exist.
    """
    _, events = _run(_totals(5, 0), reachable=5, reachable_unframed=5, combos=[])
    _name, message, fields = next(e for e in events if e[0] == "audit.verdict")
    assert "needs_frame_declaration" in fields
    assert "re_framable_by_recompute" not in fields
    assert "recomput" not in message.lower()


def test_combinations_are_reported_verbatim() -> None:
    """How many frames exist in fact, not how many we imagine."""
    out, _ = _run(
        _totals(3, 3),
        reachable=3,
        reachable_unframed=0,
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
