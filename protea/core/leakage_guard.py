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

    fitted, cut outside the frame   allowed
    fitted, cut inside the frame    refused
    fitted, cut not declared        refused, and NOT because it is
                                    suspected. Because it cannot be
                                    checked, and an artifact that cannot
                                    be certified either way is worse than
                                    one known to be dirty: the dirty one
                                    can be excluded.

A pretrained backbone used as it ships declares no cut and is not fitted,
which is a fourth state and is allowed. The column distinguishes them:
NULL means not fitted, and `fitted` is what the caller asserts.
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
    if not fitted:
        # A pretrained backbone saw none of our annotations. It has no cut
        # to declare and declaring one would be false.
        return

    if training_release is None:
        raise LeakageRefusal(
            f"{name} is fitted but declares no training release, so it cannot be "
            f"certified clean or contaminated for the {frame.start} to {frame.end} "
            "frame. Record the annotation set it was fitted against, or refit it."
        )

    # The boundary is deliberate. A cut AT the frame start is allowed: the
    # encoder saw the state of the world at t0, which is what every arm
    # is entitled to. A cut one release later is not, because the terms
    # gained in between are part of what the frame scores.
    if frame.start < training_release <= frame.end:
        raise LeakageRefusal(
            f"{name} was fitted on release {training_release}, which lies inside the "
            f"{frame.start} to {frame.end} frame. The terms gained between "
            f"{frame.start} and {training_release} are part of what this frame "
            "scores, so the evaluation would measure the encoder's memory of them."
        )


__all__ = ["Frame", "LeakageRefusal", "check_training_cut"]
