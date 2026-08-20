"""The check that would have caught the 227 encoder before it was proposed.

The other machine fitted three maps on release 227 closures and offered
them as arms on a 220-to-230 frame. 227 sits inside that window, so the
terms gained between 220 and 227 are part of what the frame scores. It
found that itself, by reading its own preparation script. Nothing stopped
it, and the next one should not need somebody to remember.
"""

from __future__ import annotations

import pytest

from protea.core.leakage_guard import Frame, LeakageRefusal, check_training_cut

WINDOW = Frame(start=220, end=230)


class TestTheCaseThisWasWrittenFor:
    def test_a_cut_inside_the_frame_is_refused(self):
        with pytest.raises(LeakageRefusal, match="227"):
            check_training_cut(fitted=True, training_release=227, frame=WINDOW)

    def test_the_message_says_why_rather_than_that(self):
        # A refusal nobody can act on gets overridden. This one names the
        # release, the frame, and what the evaluation would be measuring.
        with pytest.raises(LeakageRefusal) as exc:
            check_training_cut(fitted=True, training_release=227, frame=WINDOW)
        assert "memory" in str(exc.value)


class TestTheBoundary:
    def test_a_cut_at_the_frame_start_is_allowed(self):
        # Seeing the world as it stood at t0 is what every arm is entitled
        # to. Refusing it would refuse every legitimate fitted encoder.
        check_training_cut(fitted=True, training_release=220, frame=WINDOW)

    def test_one_release_past_the_start_is_not(self):
        with pytest.raises(LeakageRefusal):
            check_training_cut(fitted=True, training_release=221, frame=WINDOW)

    def test_a_cut_before_the_frame_is_allowed(self):
        check_training_cut(fitted=True, training_release=214, frame=WINDOW)

    def test_a_cut_at_the_frame_end_is_refused(self):
        # The end IS the target. An encoder fitted there has seen every
        # answer the frame asks for.
        with pytest.raises(LeakageRefusal):
            check_training_cut(fitted=True, training_release=230, frame=WINDOW)

    def test_a_cut_after_the_frame_is_allowed(self):
        # Later than the target is a different frame's problem, not this
        # one's, and refusing it here would be a guess rather than a check.
        check_training_cut(fitted=True, training_release=231, frame=WINDOW)


class TestTheUndeclaredCase:
    def test_a_fitted_encoding_with_no_declared_cut_is_refused(self):
        # Not because it is suspected. Because it cannot be checked, and
        # an artifact that can be certified neither way is worse than one
        # known to be dirty: the dirty one can be excluded.
        with pytest.raises(LeakageRefusal, match="certified"):
            check_training_cut(fitted=True, training_release=None, frame=WINDOW)

    def test_a_pretrained_backbone_declares_nothing_and_passes(self):
        # It saw none of our annotations. It has no cut, and inventing one
        # would be false. This is the fourth state and the column's NULL
        # means exactly it.
        check_training_cut(fitted=False, training_release=None, frame=WINDOW)

    def test_not_fitted_wins_even_if_a_release_is_present(self):
        # A stale value on an unfitted config must not manufacture a
        # refusal, or the guard becomes something people route around.
        check_training_cut(fitted=False, training_release=227, frame=WINDOW)


def test_the_name_travels_into_the_message():
    with pytest.raises(LeakageRefusal, match="sparse pooled"):
        check_training_cut(
            fitted=True, training_release=227, frame=WINDOW, name="sparse pooled"
        )
