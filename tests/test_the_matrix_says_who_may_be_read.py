"""Every cell of the matrix says whether that arm may be read on that window.

``leakage_role`` travels on every result and is a LABEL: whoever dispatched
the job put it in the payload. It says what the window was meant to be, not
whether this arm is entitled to it, and the difference is the whole point of
the research screens: a method appears only on windows whose t0 is at or
after its knowledge cutoff.

The rule that decides that already existed and already had the boundary
right. What it did not have was a caller on the READ side, so these pin the
four verdicts as the matrix computes them, and that the guard and the badge
are one function rather than two implementations of one rule.
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest

from protea.api.routers._leak_badge import frames_by_eval_set, stamp_leak_verdicts
from protea.core.leakage_guard import Frame

WINDOW = str(uuid.uuid4())
PRETRAINED = str(uuid.uuid4())
FITTED_CLEAN = str(uuid.uuid4())
FITTED_INSIDE = str(uuid.uuid4())
FITTED_MUTE = str(uuid.uuid4())

#: A 220 -> 230 window, as _load_eval_set_metadata hands it over.
EVAL_SETS = [
    {"id": WINDOW, "old_source_version": "220", "new_source_version": "230"},
]


class _Session:
    """Answers the one query the badge makes: the cut of every config."""

    def __init__(self, rows: list[tuple[Any, str, Any]]) -> None:
        self._rows = rows

    def execute(self, _stmt: Any) -> _Session:
        return self

    def all(self) -> list[tuple[Any, str, Any]]:
        return self._rows


def _session() -> _Session:
    return _Session(
        [
            (PRETRAINED, "ankh", None),
            # An ingested method fitted at the window's start: entitled to it.
            (FITTED_CLEAN, "ingested-method", "220"),
            # Fitted one release in: the terms gained after 220 are what the
            # window scores.
            (FITTED_INSIDE, "ingested-method", "221"),
            # Fitted and silent about it.
            (FITTED_MUTE, "learned-code", None),
        ]
    )


def _row(config_id: str, window: str = WINDOW) -> dict[str, Any]:
    return {"embedding_config_id": config_id, "evaluation_set_id": window}


class TestTheFourVerdicts:
    @pytest.mark.parametrize(
        ("config_id", "expected"),
        [
            (PRETRAINED, "not_fitted"),
            (FITTED_CLEAN, "clean"),
            (FITTED_INSIDE, "contaminated"),
            (FITTED_MUTE, "undeclared"),
        ],
    )
    def test_each_arm_gets_its_own(self, config_id: str, expected: str) -> None:
        rows = [_row(config_id)]
        stamp_leak_verdicts(_session(), rows, EVAL_SETS)
        assert rows[0]["leak"] == expected

    def test_the_numbers_behind_the_verdict_travel_with_it(self) -> None:
        # So the page can explain the badge instead of asserting it.
        rows = [_row(FITTED_INSIDE)]
        stamp_leak_verdicts(_session(), rows, EVAL_SETS)
        assert rows[0]["training_release"] == 221
        assert rows[0]["window_releases"] == [220, 230]

    def test_fitted_comes_from_the_backend_and_not_from_the_cut(self) -> None:
        # The config with no cut is 'undeclared' only because its backend
        # says it was fitted. Reading NULL as "not fitted" is the case the
        # guard exists to catch.
        rows = [_row(FITTED_MUTE), _row(PRETRAINED)]
        stamp_leak_verdicts(_session(), rows, EVAL_SETS)
        assert [r["leak"] for r in rows] == ["undeclared", "not_fitted"]


class TestWhatCannotBeDerivedIsNotCalledClean:
    def test_a_window_whose_ends_are_not_releases_has_no_verdict(self) -> None:
        # A source whose version is not an ordinal has no inside for a cut to
        # fall into, and "clean" would be a claim nobody can check.
        rows = [_row(FITTED_INSIDE)]
        stamp_leak_verdicts(_session(), rows, [{"id": WINDOW, "old_source_version": "2026_01"}])
        assert rows[0]["leak"] is None
        assert rows[0]["window_releases"] is None

    def test_an_unknown_config_has_no_verdict(self) -> None:
        rows = [_row(str(uuid.uuid4()))]
        stamp_leak_verdicts(_session(), rows, EVAL_SETS)
        assert rows[0]["leak"] is None

    def test_a_window_absent_from_the_payload_has_no_verdict(self) -> None:
        rows = [_row(FITTED_CLEAN, window=str(uuid.uuid4()))]
        stamp_leak_verdicts(_session(), rows, EVAL_SETS)
        assert rows[0]["leak"] is None

    def test_the_frame_map_skips_what_it_cannot_order(self) -> None:
        frames = frames_by_eval_set(
            [
                {"id": "a", "old_source_version": "220", "new_source_version": "230"},
                {"id": "b", "old_source_version": None, "new_source_version": "230"},
                {"id": "c", "old_source_version": "2026_01", "new_source_version": "2026_03"},
            ]
        )
        assert frames == {"a": Frame(220, 230)}


class TestItIsOneRuleAndNotTwo:
    def test_the_badge_calls_the_function_the_guard_refuses_on(self) -> None:
        # Two implementations of one rule drift the first time the boundary
        # is argued about again, and the argument is a year old already.
        import inspect

        from protea.api.routers import _leak_badge

        assert "training_cut_verdict" in inspect.getsource(_leak_badge)

    def test_the_matrix_stamps_before_it_builds_the_response(self) -> None:
        # The leaderboard is derived from the stamped rows, so a stamp after
        # the build would badge the rows and not the winner.
        import inspect

        from protea.api.routers import benchmark

        source = inspect.getsource(benchmark.get_benchmark_matrix)
        assert source.index("stamp_leak_verdicts") < source.index("_build_matrix_response")

    def test_the_winner_carries_its_verdict(self) -> None:
        from protea.api.routers.benchmark import _make_leaderboard

        row = {
            "category": "nk",
            "aspect": "F",
            "primary": 0.5,
            "primary_metric": "f_micro_w",
            "fmax": 0.5,
            "precision": 0.5,
            "recall": 0.5,
            "coverage": 0.5,
            "embedding_config_id": FITTED_INSIDE,
            "k": 3,
            "stage": "baseline",
            "evaluation_result_id": "r",
            "evaluation_set_id": WINDOW,
            "leak": "contaminated",
            "training_release": 221,
        }
        board = _make_leaderboard([row], ("nk",), ("F",))
        assert board[0]["leak"] == "contaminated"
        assert board[0]["training_release"] == 221
