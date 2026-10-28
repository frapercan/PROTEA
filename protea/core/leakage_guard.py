"""Refuse an encoding whose training cut lies inside the frame it is scored on.

An encoder fitted on release 227 has seen the terms proteins gained
between 220 and 227. Scoring it on a 220-to-230 frame therefore measures,
in part, its memory of the answer. That is not a subtle bias: it is the
evaluation asking a question the encoder was trained on.

This is the check the other machine's own leakage would have failed, and
it found that leakage by reading its own preparation script rather than by
being stopped. The point of putting it here is that the next one does not
need somebody to remember.

Three states, and the middle one is the reason this module exists:

    fitted, cut at or before t0     allowed
    fitted, cut after t0            refused
    fitted, cut not declared        refused, and NOT because it is
                                    suspected. Because it cannot be
                                    checked, and an artifact that cannot
                                    be certified either way is worse than
                                    one known to be dirty: the dirty one
                                    can be excluded.

A pretrained backbone used as it ships declares no cut and is not fitted,
which is a fourth state and is allowed. The column distinguishes them:
NULL means not fitted, and `fitted` is what the caller asserts.

WHY "OUTSIDE THE FRAME" IS NOT THE RULE

The table above said "cut outside the frame" until 2026-10-28 and the
comparison read ``frame.start < cut <= frame.end``, which treats the frame
as an interval a cut can leave through either side. It has one. A cut
LATER than t1 is outside that interval and is the worst case there is: an
encoder fitted on release 230 has seen every term the 211-to-227 frame
scores and then some, and the condition returned "clean" for it.

Unreachable while the only fitted cuts in the campaign were 211 and 227,
because 227 falls inside the 211-to-227 frame and was caught. It became
reachable the moment the second cut moved to 230, which is the cut of the
arm the 230-to-235 panel is built on: point that same arm at the
211-to-227 panel and the guard waved it through.

So t1 does not enter the comparison at all. What makes a cut dirty is that
it is after t0.
"""

from __future__ import annotations

from dataclasses import dataclass


class LeakageRefusal(ValueError):
    """An arm was dispatched whose encoding may have seen the answer."""


@dataclass(frozen=True)
class Frame:
    """The temporal window an evaluation scores, by release ordinal."""

    start: int
    end: int


#: The four states, as a read surface names them. ``check_training_cut``
#: refuses the middle two and allows the outer two, and it decides which by
#: calling :func:`training_cut_verdict`, so the refusal and the badge cannot
#: disagree. That was the point of writing this: a screen that computed the
#: same rule a second time would be a second rule.
VERDICTS: tuple[str, ...] = ("not_fitted", "undeclared", "contaminated", "clean")


def training_cut_verdict(
    *, fitted: bool, training_release: int | None, frame: Frame
) -> str:
    """Which of the four states this arm is in on this frame.

    Written for a surface that has to SHOW the answer for every arm of every
    window at once, where raising is not an option: a matrix of cells cannot
    stop at the first contaminated one. The guard below then refuses on this
    verdict instead of repeating the comparison.

    The boundary is deliberate and lives here, once. A cut AT the frame start
    is clean: the encoder saw the state of the world at t0, which is what
    every arm is entitled to. ANY later cut is not, and it does not matter
    whether it falls inside the window or past its end: a cut inside has seen
    part of what the frame scores, a cut past t1 has seen all of it.
    """
    if not fitted:
        return "not_fitted"
    if training_release is None:
        return "undeclared"
    if training_release > frame.start:
        return "contaminated"
    return "clean"


def check_training_cut(
    *,
    fitted: bool,
    training_release: int | None,
    frame: Frame,
    name: str = "encoding",
) -> None:
    """Raise unless this encoding may be scored on this frame.

    ``training_release`` is the release ordinal the encoding was fitted
    against, or None if it declares none.
    """
    verdict = training_cut_verdict(
        fitted=fitted, training_release=training_release, frame=frame
    )
    if verdict == "not_fitted":
        # A pretrained backbone saw none of our annotations. It has no cut
        # to declare and declaring one would be false.
        return
    if verdict == "undeclared":
        raise LeakageRefusal(
            f"{name} is fitted but declares no training release, so it cannot be "
            f"certified clean or contaminated for the {frame.start} to {frame.end} "
            "frame. Record the annotation set it was fitted against, or refit it."
        )
    if verdict == "contaminated":
        # Says "later than t0" and not "inside the frame", because a cut past
        # t1 is also refused and naming it the wrong way was the defect.
        raise LeakageRefusal(
            f"{name} was fitted on release {training_release}, later than the "
            f"{frame.start} where the frame it would be scored on starts. That frame "
            f"scores the terms gained between {frame.start} and {frame.end}, and the "
            f"encoder had already read release {training_release}, so the evaluation "
            "would measure its memory of them."
        )


__all__ = [
    "VERDICTS",
    "Frame",
    "LeakageRefusal",
    "check_training_cut",
    "training_cut_verdict",
]
