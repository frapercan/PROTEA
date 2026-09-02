"""The temporal design, pinned where it can fail loudly.

Every property here has already gone wrong in this project or is one step away
from doing so. The registry states the design; these tests are what stop the
design from drifting away from the code that claims to implement it.
"""

from __future__ import annotations

import pytest

from protea.core.split_registry import (
    BOARD_MARK,
    SplitUndecidedError,
    comparable_window,
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

    @pytest.mark.parametrize("name", ["train"])
    def test_it_raises_instead_of_returning_something(self, name: str) -> None:
        with pytest.raises(SplitUndecidedError):
            windows_for(name)

    @pytest.mark.parametrize("name", ["train"])
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


class TestTheTuneWindowIsDecided:
    """It was undecided until the author fixed it on 2026-07-27.

    E2E-CANONICAL-RUN.md section 3: "TUNE window: 226 -> 227. Every parameter,
    threshold and design decision is selected here. Nothing after 227 informs a
    choice." The registry refused to name it for five weeks after that, so a
    caller asking it for the adjustment windows was told the decision had not
    been taken when it had.
    """

    def test_it_names_the_window_the_author_fixed(self) -> None:
        assert [str(w) for w in windows_for("adjustment")] == ["v226->v227"]

    def test_it_may_inform_the_decisions_it_was_fixed_for(self) -> None:
        for decision in ("hyperparameters", "thresholds", "design", "champion_choice"):
            assert_may_inform("adjustment", decision)

    def test_the_validation_split_still_may_inform_nothing(self) -> None:
        with pytest.raises(SplitLeakError):
            assert_may_inform("validation", "champion_choice")


class TestTheValidationSeriesCoversTheBoardWindowForward:
    def test_it_starts_AT_the_mark_and_not_one_window_earlier(self) -> None:
        """The boundary the whole temporal design rests on.

        It used to start at the window ENDING at the mark, which is 226->227 --
        the window the author fixed as TUNE. That put the selection set inside
        the holdout split, so asking whether it could inform a hyperparameter
        raised a leak error about the very window chosen for that purpose.
        """
        assert windows_for("validation")[0].start == BOARD_MARK

    def test_the_tune_window_is_not_in_the_validation_series(self) -> None:
        """The two halves of the design, kept apart, pinned."""
        tune = windows_for("adjustment")[0]
        assert tune.end == BOARD_MARK
        assert tune not in windows_for("validation")

    def test_the_comparable_point_refuses_rather_than_guesses(self) -> None:
        """One point must be designated and nothing in the record designates it.

        The board's own frame is recorded elsewhere as 227->230, which spans
        three consecutive releases and is therefore not a member of this series
        at all. Returning the first point instead would publish a
        characterisation point as the competitive claim.
        """
        with pytest.raises(SplitUndecidedError, match="not decided"):
            comparable_window()

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

    def test_no_candidate_ends_after_the_mark(self) -> None:
        """The author's rule verbatim: nothing after 227 informs a choice.

        A window ending AT the mark satisfies it. A window starting at the mark
        does not, and those are the validation series.
        """
        cutoff = release(BOARD_MARK).published
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
        """One window means the tune window alone, with nothing to check it against."""
        assert not menu_is_sufficient() or len(adjustment_candidates()) >= 2

    def test_the_menu_holds_the_tune_window_and_nothing_earlier(self) -> None:
        """What the fixed frame costs, pinned so it is visible rather than found.

        The release table begins at v226, so the only window the leak rule
        admits is the tune window itself. A decision selected on it cannot
        currently be shown to hold anywhere else, and the tune window is one of
        the two roughly thirty percent contractions. Ingesting a release before
        v226 is what changes this, and this test is what says so.
        """
        assert adjustment_candidates() == tuple(windows_for("adjustment"))
        assert not menu_is_sufficient()


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
