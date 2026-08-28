"""A cut recounts its donors instead of inheriting a wider neighbourhood.

The defect these pin: an arm cut to depth 2 carrying a consensus measured
over depth 30 and reporting it as its own. Nothing fails when that
happens, which is why it needs tests rather than monitoring.
"""

from __future__ import annotations

from typing import Any

import pytest

from protea.core.operations._donor_recount import (
    DepthCut,
    recount_at_depth,
)

#: Four donors. Two share a sequence, so protein depth and sequence depth
#: disagree from the second donor onwards.
_ROW: dict[str, Any] = {
    "donor_accessions": ["R1", "R2", "R3", "R4"],
    "donor_k_positions": [1, 2, 3, 4],
    "donor_sequence_ranks": [1, 1, 2, 3],
    "donor_distances": [0.10, 0.20, 0.30, 0.40],
    "donor_count": 4,
}


class TestACutMustSayWhatItCounts:
    def test_naming_neither_unit_is_refused(self) -> None:
        with pytest.raises(ValueError, match="counted either in proteins"):
            DepthCut()

    def test_naming_both_units_is_refused(self) -> None:
        """Two depths in one cut has no reading, so it is not a default."""
        with pytest.raises(ValueError, match="counted either in proteins"):
            DepthCut(max_k_position=2, max_sequence_rank=2)


class TestTheRecountUsesOnlyTheSurvivingDonors:
    def test_depth_two_in_proteins_keeps_two(self) -> None:
        got = recount_at_depth(_ROW, DepthCut(max_k_position=2))
        assert got is not None
        assert got.donor_count == 2
        assert got.mean_distance == pytest.approx(0.15)
        assert got.min_distance == pytest.approx(0.10)

    def test_depth_one_in_sequences_keeps_two_because_they_share_one(self) -> None:
        """The whole point of counting in sequences rather than proteins."""
        got = recount_at_depth(_ROW, DepthCut(max_sequence_rank=1))
        assert got is not None
        assert got.donor_count == 2
        assert got.mean_distance == pytest.approx(0.15)

    def test_the_two_units_disagree_at_the_same_number(self) -> None:
        by_protein = recount_at_depth(_ROW, DepthCut(max_k_position=3))
        by_sequence = recount_at_depth(_ROW, DepthCut(max_sequence_rank=3))
        assert by_protein is not None and by_sequence is not None
        assert by_protein.donor_count == 3
        assert by_sequence.donor_count == 4

    def test_a_depth_past_the_end_keeps_everything(self) -> None:
        got = recount_at_depth(_ROW, DepthCut(max_k_position=99))
        assert got is not None
        assert got.donor_count == 4
        assert got.mean_distance == pytest.approx(0.25)

    def test_the_deviation_of_a_single_survivor_is_zero_not_undefined(self) -> None:
        got = recount_at_depth(_ROW, DepthCut(max_k_position=1))
        assert got is not None
        assert got.donor_count == 1
        assert got.distance_std == 0.0


#: Donors only at protein positions 1 and 4, so the fraction is genuinely
#: below 1 and a numerator counted in the wrong unit shows up as a number
#: rather than as a coincidence.
_SPARSE: dict[str, Any] = {
    "donor_accessions": ["R1", "R4"],
    "donor_k_positions": [1, 4],
    "donor_sequence_ranks": [1, 3],
    "donor_distances": [0.10, 0.40],
    "donor_count": 2,
}


class TestTheFractionIsAFraction:
    def test_it_cannot_exceed_one_counting_proteins(self) -> None:
        """The stored column of this name reaches 4.9 on 104,627 rows."""
        for depth in (1, 2, 3, 4, 30):
            got = recount_at_depth(_ROW, DepthCut(max_k_position=depth))
            assert got is not None
            assert 0.0 <= (got.vote_fraction() or 0.0) <= 1.0

    def test_it_cannot_exceed_one_counting_sequences_either(self) -> None:
        """Two proteins can share one sequence, so donors over a sequence
        depth is not a fraction: at depth 1 it would give 2.0."""
        got = recount_at_depth(_ROW, DepthCut(max_sequence_rank=1))
        assert got is not None
        assert got.donor_count == 2
        assert got.sequence_count == 1
        assert got.vote_fraction() == pytest.approx(1.0)

    def test_the_numerator_is_counted_in_the_unit_of_the_denominator(self) -> None:
        got = recount_at_depth(_SPARSE, DepthCut(max_sequence_rank=3))
        assert got is not None
        assert got.donor_count == 2
        assert got.sequence_count == 2
        assert got.vote_fraction() == pytest.approx(2 / 3)

    def test_a_sparse_neighbourhood_scores_below_one(self) -> None:
        got = recount_at_depth(_SPARSE, DepthCut(max_k_position=10))
        assert got is not None
        assert got.vote_fraction() == pytest.approx(0.2)

    def test_a_depth_of_zero_has_no_fraction_rather_than_a_division(self) -> None:
        got = recount_at_depth(_ROW, DepthCut(max_k_position=0))
        assert got is not None
        assert got.vote_fraction() is None


class TestARowWithoutALedger:
    def test_it_returns_nothing_rather_than_the_stored_aggregate(self) -> None:
        """Keeping the stored value is the defect this module exists to end."""
        old_row = {"vote_count": 9, "neighbor_vote_fraction": 4.9}
        assert recount_at_depth(old_row, DepthCut(max_k_position=2)) is None

    def test_a_run_that_never_counted_sequences_returns_nothing_for_that_unit(
        self,
    ) -> None:
        row = dict(_ROW, donor_sequence_ranks=None)
        assert recount_at_depth(row, DepthCut(max_sequence_rank=2)) is None
        assert recount_at_depth(row, DepthCut(max_k_position=2)) is not None

    def test_arrays_of_different_lengths_are_refused_rather_than_zipped(self) -> None:
        """A short array would silently drop donors off the end."""
        row = dict(_ROW, donor_distances=[0.1, 0.2])
        assert recount_at_depth(row, DepthCut(max_k_position=4)) is None

    def test_a_ledger_with_nobody_within_the_depth_counts_zero(self) -> None:
        row = dict(_ROW, donor_k_positions=[7, 8, 9, 10])
        got = recount_at_depth(row, DepthCut(max_k_position=2))
        assert got is not None
        assert got.donor_count == 0
        assert got.mean_distance is None
