"""The base frame's aggregates describe its own cut.

The frame IS the candidate set after the cut, so an arm cut to depth 2
carrying depth 30's consensus is not a labelling problem, it is a wrong
number. These pin that the correction happens where the frame is built,
which is the one place every consumer passes through.
"""

from __future__ import annotations

import pandas as pd
import pytest

from protea.core.operations._base_frame_recount import (
    DONOR_LEDGER_COLS,
    cut_of,
    recount_frame_aggregates,
)
from protea.core.operations._depth_unit_guard import why_the_depth_cannot_be_cut
from protea.core.operations._donor_recount import DepthCut


class _Ctx:
    def __init__(self, k: int | None = None, seq: int | None = None) -> None:
        self.max_k_position = k
        self.max_sequence_rank = seq


def _frame() -> pd.DataFrame:
    """One row, four donors, two of which share a sequence.

    The stored aggregates are deliberately the wrong ones for any cut:
    they are what a depth-4 retrieval wrote, and every cut below that must
    move away from them.
    """
    return pd.DataFrame([{
        "protein_accession": "Q1",
        "go_id": "GO:0000007",
        "distance": 0.10,
        "vote_count": 9,
        "donor_count": 4,
        "neighbor_vote_fraction": 4.9,
        "neighbor_mean_distance": 0.25,
        "neighbor_distance_std": 0.11,
        "donor_k_positions": [1, 2, 3, 4],
        "donor_sequence_ranks": [1, 1, 2, 3],
        "donor_distances": [0.10, 0.20, 0.30, 0.40],
    }])


class TestTheCutIsReadOffTheContext:
    def test_no_depth_is_no_cut(self) -> None:
        assert cut_of(_Ctx()) is None

    def test_either_depth_becomes_a_cut_in_its_own_unit(self) -> None:
        by_protein = cut_of(_Ctx(k=3))
        by_sequence = cut_of(_Ctx(seq=3))
        assert by_protein is not None and by_protein.max_k_position == 3
        assert by_sequence is not None and by_sequence.max_sequence_rank == 3


class TestTheRecountReplacesTheStoredAggregates:
    def test_a_protein_cut_recounts_to_its_own_survivors(self) -> None:
        out = recount_frame_aggregates(_frame(), DepthCut(max_k_position=2))
        row = out.iloc[0]
        assert row["donor_count"] == 2
        assert row["neighbor_vote_fraction"] == pytest.approx(1.0)
        assert row["neighbor_mean_distance"] == pytest.approx(0.15)

    def test_a_sequence_cut_counts_sequences_in_the_numerator(self) -> None:
        """Two donors at sequence depth 1 is one sequence, so the fraction
        is 1.0 and not 2.0."""
        out = recount_frame_aggregates(_frame(), DepthCut(max_sequence_rank=1))
        row = out.iloc[0]
        assert row["donor_count"] == 2
        assert row["neighbor_vote_fraction"] == pytest.approx(1.0)

    def test_a_cut_never_redefines_vote_count_under_its_readers(self) -> None:
        """It counts annotation rows and a recount counts donors. One column
        meaning voters in a cut arm and paperwork in an uncut one is the
        defect this campaign keeps finding."""
        out = recount_frame_aggregates(_frame(), DepthCut(max_k_position=2))
        assert out.iloc[0]["vote_count"] == 9
        assert out.iloc[0]["donor_count"] == 2

    def test_the_stored_fraction_above_one_does_not_survive_a_cut(self) -> None:
        """5.69% of stored rows carry a fraction above 1; a cut must not."""
        before = _frame().iloc[0]["neighbor_vote_fraction"]
        after = recount_frame_aggregates(_frame(), DepthCut(max_k_position=4)).iloc[0]
        assert before > 1.0
        assert after["neighbor_vote_fraction"] <= 1.0

    def test_no_cut_leaves_the_stored_aggregates_alone(self) -> None:
        """With no cut the stored values already describe the whole
        neighbourhood, so rewriting them would be inventing a change."""
        out = recount_frame_aggregates(_frame(), None)
        assert out.iloc[0]["neighbor_vote_fraction"] == 4.9
        assert out.iloc[0]["vote_count"] == 9


class TestTheLedgerDoesNotReachTheCache:
    def test_the_arrays_are_dropped_after_the_recount(self) -> None:
        """They are the widest columns in the table and nothing downstream
        reads them, so carrying them into the parquet buys nothing."""
        out = recount_frame_aggregates(_frame(), DepthCut(max_k_position=2))
        assert not [c for c in DONOR_LEDGER_COLS if c in out.columns]

    def test_they_are_dropped_without_a_cut_too(self) -> None:
        out = recount_frame_aggregates(_frame(), None)
        assert not [c for c in DONOR_LEDGER_COLS if c in out.columns]

    def test_an_empty_frame_survives_both_ways(self) -> None:
        empty = pd.DataFrame(columns=list(_frame().columns))
        assert recount_frame_aggregates(empty, None).empty
        assert recount_frame_aggregates(empty, DepthCut(max_k_position=2)).empty


class TestARowWithoutALedgerCarriesTheAbsence:
    def test_it_is_emptied_rather_than_left_at_its_stored_value(self) -> None:
        """The run guard refuses such a set up front. This is the second
        line: if one ever gets here, it must not look scored."""
        df = _frame()
        for col in DONOR_LEDGER_COLS:
            df[col] = None
        out = recount_frame_aggregates(df, DepthCut(max_k_position=2))
        row = out.iloc[0]
        assert row["neighbor_vote_fraction"] is None
        assert row["donor_count"] is None


class TestTheRunRefusesBeforeItGetsHere:
    def test_any_cut_without_a_ledger_stops_the_run(self) -> None:
        said = why_the_depth_cannot_be_cut(3, 0, 2, "proteins", "S")
        assert said is not None
        assert "0 of its 3" in said
        assert "report it as the cut's own" in said

    def test_a_partial_ledger_stops_it_as_firmly(self) -> None:
        """Recounting some rows and inheriting others gives a score that is
        not comparable with itself."""
        said = why_the_depth_cannot_be_cut(3, 2, 2, "proteins", "S")
        assert said is not None
        assert "2 of its 3" in said

    def test_a_complete_ledger_has_no_complaint(self) -> None:
        assert why_the_depth_cannot_be_cut(3, 3, 2, "proteins", "S") is None

    def test_the_complaint_says_what_to_do(self) -> None:
        said = why_the_depth_cannot_be_cut(3, 0, 2, "sequences", "S")
        assert said is not None
        assert "Re-retrieve the set, or score it whole" in said


