"""A band beyond the spent holdout, and the straddle that is not one.

The board's mark says where selection stops. It does not say that everything
after it is one single band, and the platform behaved as though it did: the
only way past the guard was ``HOLDOUT_WAIVER``, a sentence that says of itself
that it is the single declared pass and that is already written on the payload
of the job that built `cb46395a`, the 227 -> 230 window. Claiming it again
would not have been a second pass on the holdout. It would have been a false
record of the first one.

What the waiver protects is looking at the same held-back data twice -- look,
tune, look again. A band that does not overlap anything already scored cannot
be that, and refusing it would mean the platform can never report on data
newer than the one band it already spent.

So there is a second sentence, and the one thing it checks is where the band
BEGINS. The dangerous case is the straddle: a window that starts before the
spent band ends and finishes after it is the union of a window used for
selection and the one held back, which is the exact shape of the 220 -> 230
windows whose 594 results were deleted on 2026-08-27.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pytest

from protea.core.operations._holdout_guard import Claim, refuse_if_the_set_reads_the_holdout
from protea.core.split_registry import (
    BOARD_MARK,
    HOLDOUT_BAND_END,
    HOLDOUT_WAIVER,
    PROSPECTIVE_DECLARATION,
    HoldoutTouchedError,
    assert_window_may_inform,
    release,
)

MARK = release(BOARD_MARK).published
BAND_END = release(HOLDOUT_BAND_END).published
#: goa 235, the first corpus beyond the spent band. Its date is the one on the
#: annotation set this platform holds, not a cadence guess.
V235 = date(2026, 7, 9)


class TestTheTwoSentencesAreDifferent:
    def test_they_are_not_the_same_string(self) -> None:
        assert HOLDOUT_WAIVER != PROSPECTIVE_DECLARATION

    def test_each_names_what_it_asserts(self) -> None:
        # Both are sentences rather than flags, and each one says the thing it
        # would be wrong to claim: the single pass, or beginning after it.
        assert "single declared pass" in HOLDOUT_WAIVER
        assert "begins after the holdout" in PROSPECTIVE_DECLARATION

    def test_the_band_end_is_where_the_spent_window_ended(self) -> None:
        # cb46395a is 227 -> 230; the band that was read ends at 230.
        assert HOLDOUT_BAND_END == "v230"
        assert BAND_END > MARK


class TestWhatTheDeclarationLetsThrough:
    def test_a_band_beginning_at_the_spent_band_s_end_passes(self) -> None:
        assert_window_may_inform(
            V235, start_published=BAND_END, declaration=PROSPECTIVE_DECLARATION
        )

    def test_a_band_beginning_later_still_passes(self) -> None:
        assert_window_may_inform(
            V235, start_published=date(2026, 4, 10), declaration=PROSPECTIVE_DECLARATION
        )

    def test_nothing_changes_for_a_window_that_ends_at_the_mark(self) -> None:
        # The 211 -> 227 window needs no sentence at all, and still does not.
        assert_window_may_inform(MARK, start_published=date(2022, 7, 1))


class TestTheStraddleIsRefusedByName:
    def test_a_window_spanning_the_spent_band_is_refused(self) -> None:
        with pytest.raises(HoldoutTouchedError, match="STRADDLES"):
            assert_window_may_inform(
                V235, start_published=MARK, declaration=PROSPECTIVE_DECLARATION
            )

    def test_the_refusal_names_the_deletion_it_is_protecting_against(self) -> None:
        with pytest.raises(HoldoutTouchedError) as err:
            assert_window_may_inform(
                V235, start_published=MARK, declaration=PROSPECTIVE_DECLARATION
            )
        assert "220 -> 230" in str(err.value)
        assert "2026-08-27" in str(err.value)

    def test_one_day_before_the_band_ends_is_still_a_straddle(self) -> None:
        # The boundary is the band's end and not a nearby date: the band is
        # read, so a window reaching one day into it has read it.
        with pytest.raises(HoldoutTouchedError, match="STRADDLES"):
            assert_window_may_inform(
                V235,
                start_published=BAND_END.replace(day=BAND_END.day - 1),
                declaration=PROSPECTIVE_DECLARATION,
            )

    def test_the_declaration_without_a_start_is_refused(self) -> None:
        # The start is the whole of what makes the declaration safe, so a
        # declaration that cannot be checked is refused rather than trusted.
        with pytest.raises(HoldoutTouchedError, match="without the date the window BEGINS"):
            assert_window_may_inform(V235, declaration=PROSPECTIVE_DECLARATION)

    def test_a_near_miss_of_the_sentence_is_not_the_sentence(self) -> None:
        for near in ("this band begins after the holdout", PROSPECTIVE_DECLARATION.upper(), ""):
            with pytest.raises(HoldoutTouchedError):
                assert_window_may_inform(
                    V235, start_published=BAND_END, declaration=near
                )


class TestClaimingBothIsRefused:
    """The guard checks the waiver first, so a caller passing both would have
    its declaration ignored -- and a straddle would pass under a sentence
    nothing verified."""

    def test_both_sentences_at_once_raise(self) -> None:
        with pytest.raises(ValueError, match="both the holdout waiver and the prospective"):
            Claim(HOLDOUT_WAIVER, PROSPECTIVE_DECLARATION).validated()

    def test_either_one_alone_is_fine(self) -> None:
        assert Claim(HOLDOUT_WAIVER).validated().waiver == HOLDOUT_WAIVER
        assert Claim(None, PROSPECTIVE_DECLARATION).validated().declaration == (
            PROSPECTIVE_DECLARATION
        )
        assert Claim().validated() == Claim()

    def test_the_guard_applies_the_refusal(self) -> None:
        # Not only the NamedTuple: the guard calls validated() itself, so a
        # caller cannot bypass it by building the Claim and never validating.
        new_set = MagicMock(source_published_at=V235, source_version="235")
        old_set = MagicMock(source_published_at=MARK, source_version="227")
        with pytest.raises(ValueError, match="both the holdout waiver"):
            refuse_if_the_set_reads_the_holdout(
                new_set,
                claim=Claim(HOLDOUT_WAIVER, PROSPECTIVE_DECLARATION),
                old_set=old_set,
                context="prueba",
            )


class TestTheSeamsIntoTheOperations:
    """The declaration has to reach the guard from both payloads, and the
    window's START has to reach it too. A rule wired on one side only is the
    defect this campaign keeps paying for."""

    def test_the_window_operation_passes_both_ends(self) -> None:
        import inspect

        from protea.core.operations.generate_evaluation_set import (
            GenerateEvaluationSetOperation,
        )

        source = inspect.getsource(GenerateEvaluationSetOperation.execute)
        assert "Claim(p.holdout_waiver, p.prospective_declaration)" in source
        assert "old_set=old_set" in source

    def test_the_scoring_operation_passes_both_ends(self) -> None:
        import inspect

        from protea.core.operations.run_cafa_evaluation import (
            RunCafaEvaluationOperation,
        )

        # ``_load_evaluation_inputs`` and not ``execute``: the guard fires
        # during input resolution, before anything is staged, which is where
        # it belongs and not where a reader would first look for it.
        source = inspect.getsource(RunCafaEvaluationOperation._load_evaluation_inputs)
        assert "Claim(p.holdout_waiver, p.prospective_declaration)" in source
        assert "old_annotation_set_id=eval_set.old_annotation_set_id" in source

    def test_both_payloads_carry_the_field(self) -> None:
        from protea.core.operations._run_cafa_payload import RunCafaEvaluationPayload
        from protea.core.operations.generate_evaluation_set import (
            GenerateEvaluationSetPayload,
        )

        for model in (GenerateEvaluationSetPayload, RunCafaEvaluationPayload):
            assert "prospective_declaration" in model.model_fields

    def test_the_guard_drops_the_start_when_the_old_set_has_no_date(self) -> None:
        # Undecidable rather than assumed: a corpus with no date cannot place
        # the band, so the declaration is refused for want of the check.
        new_set = MagicMock(source_published_at=V235, source_version="235")
        old_set = MagicMock(source_published_at=None, source_version="230")
        with pytest.raises(HoldoutTouchedError, match="without the date the window BEGINS"):
            refuse_if_the_set_reads_the_holdout(
                new_set,
                claim=Claim(None, PROSPECTIVE_DECLARATION),
                old_set=old_set,
                context="prueba",
            )


class TestV235IsInTheOneTable:
    def test_the_release_resolves(self) -> None:
        assert release("v235").published == V235

    def test_it_is_later_than_every_other_release(self) -> None:
        from protea.core.split_registry import RELEASES

        assert RELEASES[-1].identifier == "v235"
