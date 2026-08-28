"""The check that a run scored the depth it declared.

Separated from the artifact writers because it is not one of them: they turn a
frame into a file, and this refuses to let a frame become a file at all. Keeping
it here also holds ``_run_cafa_artifacts`` under its size budget, which is a
lesser reason and a real one.
"""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover - typing only
    from sqlalchemy.orm import Session


class DepthNotApplied(RuntimeError):
    """A run declared a neighbourhood depth and scored something else."""


def assert_depth_was_applied(session: Session, ctx: Any, base: Any, count_rows: Any) -> None:
    """Refuse to write a frame that does not match the depth it is labelled with.

    A depth sweep ran across two machines whose deployed code differed. The
    machine without the field accepted the payload, dropped the unknown key
    without a word, scored the full unrestricted frame and returned success.
    Sixteen of fifty-two cells came back carrying another arm's measurement
    under this arm's label, and nothing anywhere said so: not an event, not a
    status, not a log line. A wrong number that announces itself is a bug; a
    wrong number wearing the right label is the thing this campaign exists to
    avoid producing.

    So the run checks its own work rather than trusting that the filter bit.
    The comparison is against a fresh COUNT over the same filter, which is the
    one quantity a stale worker cannot fake: it either has the depth clause or
    it does not, and if it does not, it never reaches this function at all.

    This cannot protect against a worker too old to contain it. That defence is
    post hoc and belongs to whoever dispatched the sweep.
    """
    if ctx.max_k_position is None:
        return
    expected = count_rows(session, ctx)
    unrestricted = count_rows(session, replace(ctx, max_k_position=None))
    if expected == unrestricted:
        return  # the cut admits everything, so there is nothing to distinguish
    if len(base) > expected:
        raise DepthNotApplied(
            f"declared max_k_position={ctx.max_k_position} admits {expected} rows "
            f"but the frame holds {len(base)}; the unrestricted frame is "
            f"{unrestricted} rows, so this arm scored a depth it was not asked for"
        )
