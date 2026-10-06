"""The holdout is protected by a guard, not by nobody having asked yet.

Until 2026-09-02 the temporal design was declared and unenforced.
``split_registry`` said the validation split may inform nothing, and
``assert_may_inform`` had zero call sites in the whole tree. The reserve was
intact by luck.

It had not always been. The reason recorded with the deletion of the campaign's
stored results on 2026-08-27 is that 594 of 1,296 results had been evaluated on
220->230, the union of the experimental window and the competitive one: depth,
preset and representation had all been selected on a window containing the
holdout, and no preservation could undo it. Nothing objected, because nothing
was asking.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pytest

from protea.core.operations._holdout_guard import (
    refuse_if_the_set_reads_the_holdout,
)
from protea.core.split_registry import (
    BOARD_MARK,
    HOLDOUT_WAIVER,
    HoldoutTouchedError,
    SplitLeakError,
    assert_window_may_inform,
    release,
    window_reads_past_the_mark,
)

MARK = release(BOARD_MARK).published


class TestTheRuleIsAboutTimeAndNotAboutMembership:
    """Membership in the validation windows is the wrong test, and wrongly.

    Those windows are consecutive pairs, so 227->230 spans three of them and
    belongs to none. A membership check would wave through the exact window the
    holdout exists to protect.
    """

    def test_a_window_ending_at_the_mark_may_inform(self) -> None:
        """226->227 is the tune window: it ends AT the mark and is admissible."""
        assert not window_reads_past_the_mark(MARK)
        assert_window_may_inform(MARK)

    def test_a_window_ending_before_the_mark_may_inform(self) -> None:
        assert_window_may_inform(date(2025, 5, 3))

    def test_the_window_that_spans_three_validation_windows_is_refused(self) -> None:
        """227->230, which no membership test would catch."""
        with pytest.raises(HoldoutTouchedError):
            assert_window_may_inform(date(2026, 3, 4))

    def test_a_single_validation_window_is_refused_too(self) -> None:
        with pytest.raises(HoldoutTouchedError):
            assert_window_may_inform(date(2025, 11, 10))

    def test_a_release_the_registry_has_never_heard_of_is_still_judged(self) -> None:
        """GOA 220 is not in the release table at all.

        A rule keyed on identifiers could not place it; a rule keyed on dates
        can, and every annotation set records when its corpus was published.
        """
        assert_window_may_inform(date(2024, 4, 16))


class TestTheWaiverIsClaimedAndNotInferred:
    def test_the_exact_sentence_permits_the_single_pass(self) -> None:
        assert_window_may_inform(date(2026, 3, 4), waiver=HOLDOUT_WAIVER)

    @pytest.mark.parametrize("near", ["true", "1", "yes", HOLDOUT_WAIVER.upper(), HOLDOUT_WAIVER[:-1]])
    def test_nothing_close_enough_to_be_typed_by_reflex_works(self, near: str) -> None:
        """A flag that can be set by reflex is a flag that gets set by reflex."""
        with pytest.raises(HoldoutTouchedError):
            assert_window_may_inform(date(2026, 3, 4), waiver=near)


class TestTheRefusalIsUsable:
    def test_it_is_a_leak_error_so_a_general_catch_still_sees_it(self) -> None:
        assert issubclass(HoldoutTouchedError, SplitLeakError)

    def test_the_message_says_what_would_settle_it(self) -> None:
        """An error nobody can act on becomes an error everybody routes around."""
        with pytest.raises(HoldoutTouchedError) as excinfo:
            assert_window_may_inform(date(2026, 3, 4), context="scoring at 230")
        message = str(excinfo.value)
        assert HOLDOUT_WAIVER in message
        assert "scoring at 230" in message
        assert BOARD_MARK in message


class TestAGuardThatCannotDecideSaysSo:
    """Passing for lack of a date is not the same as passing.

    The original justification for the silent pass was that
    ``refresh_goa_release_dates`` "has run for every set this platform holds".
    True when it was written, false during a rebuild: phase 2 of the clean
    campaign creates 75 annotation sets and that job has not run on any of them,
    so this branch becomes the COMMON case and the guard goes inert exactly when
    it is needed.
    """

    def _set_without_a_date(self):
        s = MagicMock()
        s.source_published_at = None
        s.source_version = "v231"
        return s

    def test_it_still_passes(self):
        """Refusing every undated window would take down the tune windows to
        protect the holdout from a case that cannot be decided either way."""
        refuse_if_the_set_reads_the_holdout(
            self._set_without_a_date(), waiver=None, context="scoring at", emit=MagicMock()
        )

    def test_but_it_emits_a_warning_naming_the_remedy(self):
        emit = MagicMock()
        refuse_if_the_set_reads_the_holdout(
            self._set_without_a_date(), waiver=None, context="scoring at", emit=emit
        )
        assert emit.call_count == 1
        event, _msg, fields, level = emit.call_args[0]
        assert event == "holdout_guard.undecidable"
        assert level == "warning"
        assert fields["annotation_set"] == "v231"
        assert "refresh_goa_release_dates" in fields["remedy"]

    def test_a_caller_with_no_emit_still_works(self):
        """``emit`` is optional so the guard stays callable from a context that
        has none, and the absence of a logger must not become an exception."""
        refuse_if_the_set_reads_the_holdout(
            self._set_without_a_date(), waiver=None, context="scoring at"
        )

    def test_a_dated_set_emits_nothing(self):
        """The warning is about the guard being unable to decide, not about the
        verdict, so a decidable window must stay quiet."""
        emit = MagicMock()
        s = MagicMock()
        s.source_published_at = date(2024, 4, 16)
        s.source_version = "v220"
        refuse_if_the_set_reads_the_holdout(s, waiver=None, context="scoring at", emit=emit)
        emit.assert_not_called()
