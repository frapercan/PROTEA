"""Per-protein scores must be keyed, aligned, and refuse to guess.

This is the layer that turns cafaeval's per-protein vectors into something
joinable to sequence length, donor identity and taxonomic relation. Three ways
it could quietly produce a wrong table, and a test for each:

1. reading the wrong threshold column, which gives every protein a neighbouring
   threshold's score;
2. naming rows by the wrong protein, which is the positional misalignment that
   cost this project a fortnight in its neighbour search;
3. writing rows for a record it cannot key, which produces a table that looks
   complete and is not.
"""

from __future__ import annotations

import numpy as np
import pytest

from protea.core.operations._run_cafa_per_protein import (
    PerProteinShapeError,
    _column_for_tau,
    rows_from_sink,
)


class _Sink:
    def __init__(self, records):
        self.records = records


def _record(*, tp, pred, n_gt, ids=None, row_index=None, ns="biological_process",
            variant="weighted"):
    return {
        "tp_at_tau": np.asarray(tp, dtype=float),
        "pred_at_tau": np.asarray(pred, dtype=float),
        "n_gt": np.asarray(n_gt, dtype=float),
        "ids": ids,
        "row_index": None if row_index is None else np.asarray(row_index),
        "ns": ns,
        "variant": variant,
    }


class TestTheThresholdColumnIsFoundNotGuessed:
    def test_it_locates_the_reported_tau(self) -> None:
        # th_step 0.25 gives thresholds 0.25, 0.50, 0.75
        assert _column_for_tau(0.25, 3, 0.50) == 1

    def test_a_width_mismatch_raises_rather_than_picking_a_column(self) -> None:
        """A silent off-by-one would score every protein at the wrong threshold."""
        with pytest.raises(PerProteinShapeError, match="refusing to guess"):
            _column_for_tau(0.25, 7, 0.50)


class TestRowsAreKeyedByTheRightProtein:
    def test_row_index_maps_arrays_back_to_accessions(self) -> None:
        """The PK kernel is handed a subset, so array row 0 is not protein 0."""
        rec = _record(
            tp=[[2.0]], pred=[[4.0]], n_gt=[4.0],
            ids={"P00001": 0, "P00002": 1, "P00003": 2},
            row_index=[2],  # the array holds only the third protein
        )
        rows = rows_from_sink(_Sink([rec]), th_step=0.5, tau_by_ns={"biological_process": 0.5})
        assert [r["protein_accession"] for r in rows] == ["P00003"]

    def test_a_record_without_identity_is_dropped_not_invented(self) -> None:
        rec = _record(tp=[[1.0]], pred=[[2.0]], n_gt=[2.0], ids=None, row_index=None)
        assert rows_from_sink(_Sink([rec]), th_step=0.5, tau_by_ns={"biological_process": 0.5}) == []

    def test_a_row_index_of_the_wrong_length_is_dropped(self) -> None:
        """Better no rows than rows whose keys do not line up with their scores."""
        rec = _record(tp=[[1.0], [1.0]], pred=[[2.0], [2.0]], n_gt=[2.0, 2.0],
                      ids={"P1": 0, "P2": 1}, row_index=[0])
        assert rows_from_sink(_Sink([rec]), th_step=0.5, tau_by_ns={"biological_process": 0.5}) == []


class TestOnlyTheMetricWeReportIsKept:
    def test_the_unweighted_variant_is_skipped(self) -> None:
        """Every published cell in this project reports f_micro_w."""
        rec = _record(tp=[[1.0]], pred=[[2.0]], n_gt=[2.0], ids={"P1": 0},
                      row_index=[0], variant="unweighted")
        assert rows_from_sink(_Sink([rec]), th_step=0.5, tau_by_ns={"biological_process": 0.5}) == []

    def test_a_namespace_with_no_reported_tau_is_skipped(self) -> None:
        rec = _record(tp=[[1.0]], pred=[[2.0]], n_gt=[2.0], ids={"P1": 0}, row_index=[0])
        assert rows_from_sink(_Sink([rec]), th_step=0.5, tau_by_ns={}) == []


class TestTheArithmeticIsTheOneTheAggregateUses:
    def test_precision_recall_and_f_are_the_standard_definitions(self) -> None:
        rec = _record(tp=[[3.0]], pred=[[4.0]], n_gt=[6.0], ids={"P1": 0}, row_index=[0])
        row = rows_from_sink(_Sink([rec]), th_step=0.5, tau_by_ns={"biological_process": 0.5})[0]
        assert row["precision_w"] == pytest.approx(0.75)
        assert row["recall_w"] == pytest.approx(0.5)
        assert row["f_w"] == pytest.approx(2 * 0.75 * 0.5 / 1.25)

    def test_a_protein_that_predicted_nothing_scores_zero_not_nan(self) -> None:
        """NaN would propagate into every stratum mean that contains it."""
        rec = _record(tp=[[0.0]], pred=[[0.0]], n_gt=[3.0], ids={"P1": 0}, row_index=[0])
        row = rows_from_sink(_Sink([rec]), th_step=0.5, tau_by_ns={"biological_process": 0.5})[0]
        assert row["precision_w"] == 0.0
        assert row["f_w"] == 0.0

    def test_a_protein_with_no_ground_truth_scores_zero_not_nan(self) -> None:
        rec = _record(tp=[[0.0]], pred=[[2.0]], n_gt=[0.0], ids={"P1": 0}, row_index=[0])
        row = rows_from_sink(_Sink([rec]), th_step=0.5, tau_by_ns={"biological_process": 0.5})[0]
        assert row["recall_w"] == 0.0
        assert row["f_w"] == 0.0


def test_the_column_sums_reproduce_what_the_aggregate_would_report() -> None:
    """The property that makes a stratified table trustworthy.

    If the per-protein rows do not sum to the totals the aggregate is built
    from, the strata and the headline are two different measurements wearing one
    name. Here the sum of tp and pred across proteins must equal the column the
    kernel reduced.
    """
    # th_step 0.4 gives thresholds 0.4 and 0.8, so two columns. Picking a
    # th_step whose threshold count does not match the array width is what the
    # shape guard exists to catch, and it caught an earlier draft of this test.
    tp = np.array([[1.0, 2.0], [3.0, 4.0]])
    pred = np.array([[2.0, 4.0], [4.0, 8.0]])
    rec = _record(tp=tp, pred=pred, n_gt=[2.0, 4.0], ids={"P1": 0, "P2": 1}, row_index=[0, 1])
    rows = rows_from_sink(_Sink([rec]), th_step=0.4, tau_by_ns={"biological_process": 0.4})
    assert sum(r["tp_w"] for r in rows) == tp[:, 0].sum()
    assert sum(r["pred_w"] for r in rows) == pred[:, 0].sum()
