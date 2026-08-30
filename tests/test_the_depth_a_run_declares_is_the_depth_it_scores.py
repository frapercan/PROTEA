"""A depth counted in sequences reaches the query, and the guard can see it.

WHY THIS TEST EXISTS. Two independent failures lined up, and each one alone
would have been caught by the other.

First, neither place that builds a ``WritePredictionsContext`` passed
``max_sequence_rank`` through. The payload accepted it, the unit guard
validated it, the driver's own context carried it, and the two constructions
that matter dropped it. The SELECT's ``sequence_rank <= n`` clause was present
and unreachable. A third call site, ``batch_rescore_evaluation``, did pass it,
which is the shape that lets this kind of defect survive review: it works
everywhere someone checked.

Second, ``assert_depth_was_applied`` returned as soon as ``max_k_position`` was
None. A run counting depth in sequences leaves that null by construction, so
the guard written to catch a depth accepted and not applied was blind to the
unit this campaign chose.

The consequence, measured on 2026-08-30: five evaluations of prediction set
9995651a at sequence depths 2, 5, 10, 20 and 30 returned identical numbers to
four decimals in all nine panels, though the depth-2 frame holds 247,482 rows
and the depth-30 frame holds 2,441,584. Four of the five were labelled with a
depth they had not scored.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from protea.core.operations._depth_guard import (
    DepthNotApplied,
    _declared_depth,
    assert_depth_was_applied,
)


@dataclass
class _Ctx:
    """Only the fields the guard reads."""

    max_k_position: int | None = None
    max_sequence_rank: int | None = None


def _counter(by_k: int, by_seq: int, unrestricted: int) -> Any:
    """A count that answers differently per unit, as the real one does."""

    def count(_session: Any, ctx: _Ctx) -> int:
        if ctx.max_k_position is not None:
            return by_k
        if ctx.max_sequence_rank is not None:
            return by_seq
        return unrestricted

    return count


def test_the_sequence_unit_is_a_declared_depth() -> None:
    """The predicate the guard used to answer with an unconditional None."""
    assert _declared_depth(_Ctx()) is None
    assert "max_k_position=10" == _declared_depth(_Ctx(max_k_position=10))
    assert "max_sequence_rank=2" == _declared_depth(_Ctx(max_sequence_rank=2))


def test_a_sequence_depth_that_was_not_applied_is_refused() -> None:
    """The case that produced five identical numbers and no complaint."""
    ctx = _Ctx(max_sequence_rank=2)
    # The cut admits 247,482 rows; the frame handed over holds all 2,441,584,
    # which is what an unapplied filter looks like.
    with pytest.raises(DepthNotApplied) as caught:
        assert_depth_was_applied(
            None, ctx, range(2_441_584), _counter(0, 247_482, 2_441_584)
        )
    message = str(caught.value)
    assert "max_sequence_rank=2" in message
    assert "247482" in message and "2441584" in message


def test_a_sequence_depth_that_was_applied_passes_silently() -> None:
    ctx = _Ctx(max_sequence_rank=2)
    assert_depth_was_applied(
        None, ctx, range(247_482), _counter(0, 247_482, 2_441_584)
    )


def test_a_cut_that_admits_everything_is_not_a_failure() -> None:
    """Depth 30 of 30 restricts nothing, and must not read as unapplied."""
    ctx = _Ctx(max_sequence_rank=30)
    assert_depth_was_applied(
        None, ctx, range(2_441_584), _counter(0, 2_441_584, 2_441_584)
    )


def test_the_protein_unit_still_works() -> None:
    """The unit that was covered must not be lost to the one that was not."""
    ctx = _Ctx(max_k_position=10)
    with pytest.raises(DepthNotApplied):
        assert_depth_was_applied(None, ctx, range(900), _counter(400, 0, 900))
    assert_depth_was_applied(None, ctx, range(400), _counter(400, 0, 900))


def test_no_depth_at_all_is_not_checked() -> None:
    """Both null is the whole neighbourhood, which cannot be scored wrongly."""
    assert_depth_was_applied(None, _Ctx(), range(999), _counter(0, 0, 999))


def test_both_call_sites_pass_the_sequence_depth() -> None:
    """The other half, asserted on the source rather than through a run.

    Building the real context needs a session, a delta-protein set and a
    staged directory. Reading the two constructions is the cheap check that
    they name the field at all, which is exactly what neither did.
    """
    from pathlib import Path

    root = Path(__file__).resolve().parents[1] / "protea/core/operations"
    for name in ("run_cafa_evaluation.py", "_run_cafa_eval_driver.py"):
        text = (root / name).read_text()
        at = text.index("WritePredictionsContext(")
        block = text[at : text.index(")", text.index("path=", at))]
        assert "max_sequence_rank=" in block, (
            f"{name} builds a WritePredictionsContext without the sequence "
            "depth, so a run counting depth in sequences scores every row"
        )
