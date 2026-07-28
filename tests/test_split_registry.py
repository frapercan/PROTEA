"""The temporal design, pinned where it can fail loudly.

Every property here has already gone wrong in this project or is one step away
from doing so. The registry states the design; these tests are what stop the
design from drifting away from the code that claims to implement it.
"""

from __future__ import annotations

import pytest

from protea.core.split_registry import (
    BOARD_MARK,
    COMPARABLE_WINDOW,
    RELEASES,
    ExclusionBasis,
    ReleaseWindow,
    SplitLeakError,
    SplitName,
    SplitUndecidedError,
    UnknownReleaseError,
    UnknownSplitError,
    adjustment_candidates,
    assert_may_inform,
    consecutive_windows,
    exclusion_basis,
    ground_truth_requires_history,
    menu_is_sufficient,
    release,
    releases_before,
    resolve_split,
    windows_for,
)


class TestResolutionRaisesRatherThanGuessing:
    def test_an_unknown_split_raises(self) -> None:
        with pytest.raises(UnknownSplitError, match="known splits"):
            resolve_split("test")

    def test_the_error_names_what_does_exist(self) -> None:
        """A resolver that only says no makes the caller guess twice."""
        with pytest.raises(UnknownSplitError, match="train, adjustment, validation"):
            resolve_split("holdout")

    def test_an_unknown_release_raises(self) -> None:
        with pytest.raises(UnknownReleaseError):
            release("v999")

    @pytest.mark.parametrize("name", ["train", "adjustment", "validation"])
    def test_every_declared_split_resolves(self, name: str) -> None:
        assert resolve_split(name).name.value == name

    def test_the_enum_and_the_string_resolve_alike(self) -> None:
        assert resolve_split(SplitName.VALIDATION) is resolve_split("validation")


class TestAnUndecidedSplitHasNoDefault:
    """A placeholder here would propagate into results that look decided."""

    @pytest.mark.parametrize("name", ["train", "adjustment"])
    def test_it_raises_instead_of_returning_something(self, name: str) -> None:
        with pytest.raises(SplitUndecidedError):
            windows_for(name)

    @pytest.mark.parametrize("name", ["train", "adjustment"])
    def test_the_error_says_what_would_settle_it(self, name: str) -> None:
        """An error nobody can act on becomes an error everybody routes around."""
        with pytest.raises(SplitUndecidedError) as excinfo:
            windows_for(name)
        assert len(str(excinfo.value)) > 100

    def test_a_split_may_not_be_both_decided_and_undecided(self) -> None:
        from protea.core.split_registry import Split

        with pytest.raises(ValueError, match="either have windows or say why"):
            Split(
                name=SplitName.TRAIN,
                scored_by="ours",
                may_inform=frozenset(),
                balanced=True,
                windows=(ReleaseWindow("v226", "v227"),),
                undecided_because="both at once",
            )


class TestTheValidationSplitIsNotOursToOptimise:
    def test_it_may_inform_nothing(self) -> None:
        assert resolve_split("validation").may_inform == frozenset()

    @pytest.mark.parametrize(
        "decision", ["champion_choice", "thresholds", "hyperparameters", "design", "model_fitting"]
    )
    def test_every_kind_of_decision_is_refused(self, decision: str) -> None:
        with pytest.raises(SplitLeakError):
            assert_may_inform("validation", decision)

    def test_the_adjustment_split_is_what_may_select_a_champion(self) -> None:
        assert_may_inform("adjustment", "champion_choice")

    def test_the_train_split_may_only_fit(self) -> None:
        assert_may_inform("train", "model_fitting")
        with pytest.raises(SplitLeakError):
            assert_may_inform("train", "champion_choice")

    def test_it_is_never_balanced(self) -> None:
        """Reweighting a real population makes the number incomparable."""
        assert resolve_split("validation").balanced is False

    def test_the_fitting_splits_are_balanced(self) -> None:
        assert resolve_split("train").balanced is True
        assert resolve_split("adjustment").balanced is True

    def test_it_is_scored_by_the_board_and_the_others_are_not(self) -> None:
        assert resolve_split("validation").scored_by == "the board"
        assert resolve_split("adjustment").scored_by == "ours"


class TestTheValidationSeriesCoversTheBoardWindowForward:
    def test_it_starts_at_the_window_the_board_scored(self) -> None:
        assert windows_for("validation")[0] == COMPARABLE_WINDOW

    def test_the_comparable_window_ends_at_the_board_mark(self) -> None:
        """The board scored one window; that window is the only comparable point."""
        assert COMPARABLE_WINDOW.end == BOARD_MARK

    def test_it_runs_through_the_newest_release(self) -> None:
        assert windows_for("validation")[-1].end == RELEASES[-1].identifier

    def test_it_is_an_unbroken_chain(self) -> None:
        windows = windows_for("validation")
        for earlier, later in zip(windows, windows[1:], strict=False):
            assert earlier.end == later.start

    def test_it_crosses_the_contraction_rather_than_stopping_short(self) -> None:
        """A curve that shows the discontinuity beats a number that hides it."""
        assert len(windows_for("validation")) > 1


class TestTheAdjustmentSetCannotTouchTheBoardWindow:
    """Selecting on the window you then validate against destroys the validation."""

    def test_no_candidate_overlaps_the_validation_series(self) -> None:
        validation = set(windows_for("validation"))
        assert not set(adjustment_candidates()) & validation

    def test_no_candidate_ends_after_the_comparable_window_starts(self) -> None:
        cutoff = release(COMPARABLE_WINDOW.start).published
        assert all(release(w.end).published <= cutoff for w in adjustment_candidates())

    def test_the_menu_is_reported_insufficient_while_it_is(self) -> None:
        """The finding, pinned: the current table cannot express this split.

        The releases preceding the comparable window's start are not in the
        table, so there is nothing earlier to select on. When they are
        ingested this test is what tells the author the split became
        expressible, rather than the split silently starting to work.
        """
        assert menu_is_sufficient() == (len(adjustment_candidates()) >= 2)

    def test_a_single_window_menu_is_not_sufficient(self) -> None:
        """One window means the contraction, which the campaign rules out."""
        assert not menu_is_sufficient() or len(adjustment_candidates()) >= 2


class TestWindowsAreOrderedPairs:
    def test_a_reversed_window_is_refused(self) -> None:
        with pytest.raises(ValueError, match="not ordered"):
            ReleaseWindow("v227", "v226")

    def test_a_degenerate_window_is_refused(self) -> None:
        with pytest.raises(ValueError, match="not ordered"):
            ReleaseWindow("v227", "v227")

    def test_a_window_over_an_unknown_release_is_refused(self) -> None:
        with pytest.raises(UnknownReleaseError):
            ReleaseWindow("v226", "v999")

    def test_elapsed_days_is_the_real_gap(self) -> None:
        """Gaps range from about two weeks to four months, so rates need it."""
        assert ReleaseWindow("v232", "v233").elapsed_days == 33
        assert ReleaseWindow("v226", "v227").elapsed_days == 124


class TestOrderingComesFromDatesNotIdentifiers:
    def test_the_table_is_sorted_by_publication(self) -> None:
        assert list(RELEASES) == sorted(RELEASES)

    def test_releases_before_is_strict(self) -> None:
        earlier = releases_before(BOARD_MARK)
        assert all(r.published < release(BOARD_MARK).published for r in earlier)
        assert release(BOARD_MARK) not in earlier

    def test_consecutive_windows_spans_without_gaps(self) -> None:
        chain = consecutive_windows(RELEASES[0].identifier, RELEASES[-1].identifier)
        assert len(chain) == len(RELEASES) - 1

    def test_consecutive_windows_refuses_a_reversed_span(self) -> None:
        with pytest.raises(ValueError):
            consecutive_windows(RELEASES[-1].identifier, RELEASES[0].identifier)


class TestTheGroundTruthRule:
    def test_endpoints_alone_are_not_enough(self) -> None:
        """First appearance needs the history, not the two ends of the window."""
        assert ground_truth_requires_history() is True


class TestTheExclusionRule:
    def test_withholding_applies_to_prior_knowledge_only(self) -> None:
        """Omitting it there overstated every prior-knowledge result once."""
        assert exclusion_basis("PK") is ExclusionBasis.KNOWN_AT_START

    @pytest.mark.parametrize("category", ["NK", "LK"])
    def test_nothing_is_withheld_elsewhere(self, category: str) -> None:
        assert exclusion_basis(category) is ExclusionBasis.NONE

    def test_the_category_is_read_case_insensitively(self) -> None:
        assert exclusion_basis("pk") is ExclusionBasis.KNOWN_AT_START
