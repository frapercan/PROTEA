"""The guard named in the producer's own docstring, which did not exist.

``cafaeval.evaluation.PerProteinSink`` says of the arrays it hands over:

    The arrays handed over are the ones the aggregate was computed from, not a
    recomputation, so a caller can verify that their column sums reproduce the
    published totals. ``test_per_protein_sink`` does exactly that, because two
    numbers describing the same quantity by different routes will drift unless
    something checks.

Nothing checked, and the two numbers drifted. This is that check.

The contract has one sentence: **the per-protein rows must recompose the
published cell**. Micro precision is the tp column over the pred column, micro
recall is the tp column over the truth column, and the reported f is their
harmonic mean. If that does not hold, every interval, every stratified read and
every paired comparison built on those rows is describing a different quantity
from the one the thesis reports.
"""

from __future__ import annotations

import numpy as np
import pytest

from protea.core.operations._run_cafa_per_protein import rows_from_sink

TH_STEP = 0.01
WIDTH = 99  # the tau grid cafaeval builds from th_step (verified: _tau_array(0.01))


def _recompose(rows: list[dict], namespace: str) -> float:
    """Rebuild the micro-weighted f the way every published cell defines it."""
    sel = [r for r in rows if r["namespace"] == namespace]
    tp = sum(r["tp_w"] for r in sel)
    pred = sum(r["pred_w"] for r in sel)
    gt = sum(r["n_gt_w"] for r in sel)
    if pred <= 0 or gt <= 0:
        return 0.0
    precision, recall = tp / pred, tp / gt
    if precision + recall <= 0:
        return 0.0
    return 2 * precision * recall / (precision + recall)


def _sink(tp: np.ndarray, pred: np.ndarray, n_gt: np.ndarray, ns: str = "molecular_function"):
    """A sink shaped exactly as the kernel hands one over: [protein, tau]."""

    class _S:
        records = [
            {
                "variant": "weighted",
                "ns": ns,
                "tp_at_tau": tp,
                "pred_at_tau": pred,
                "n_gt": n_gt,
                "ids": {f"P{i}": i for i in range(tp.shape[0])},
                "row_index": list(range(tp.shape[0])),
            }
        ]

    return _S()


def _grid(weighted_peak: int, unweighted_peak: int) -> tuple[np.ndarray, ...]:
    """Two proteins whose weighted and unweighted optima fall in different columns.

    The heavy protein carries most of the information accretion, so the micro
    weighted score peaks where IT peaks; the light one is more numerous in the
    unweighted count and drags the plain optimum elsewhere. That divergence is
    not contrived: it is why the two optima exist as separate columns at all.
    """
    tp = np.zeros((2, WIDTH))
    pred = np.zeros((2, WIDTH))
    tp[0, : weighted_peak + 1] = 40.0
    pred[0, : weighted_peak + 1] = 45.0
    tp[1, : unweighted_peak + 1] = 2.0
    pred[1, : unweighted_peak + 1] = 12.0
    return tp, pred, np.array([44.0, 3.0])


def test_rows_recompose_the_published_cell():
    """The rows must rebuild the cell they were cut from.

    Cut at the column the weighted optimum names, the two routes agree. This is
    the contract holding when the driver hands over the right tau.
    """
    weighted_col = 60
    tp, pred, n_gt = _grid(weighted_peak=weighted_col, unweighted_peak=20)
    rows = rows_from_sink(
        _sink(tp, pred, n_gt), th_step=TH_STEP, tau_by_ns={"molecular_function": weighted_col * TH_STEP}
    )
    published_tp = tp[:, weighted_col].sum()
    published_pred = pred[:, weighted_col].sum()
    published_gt = n_gt.sum()
    precision, recall = published_tp / published_pred, published_tp / published_gt
    published = 2 * precision * recall / (precision + recall)

    assert _recompose(rows, "molecular_function") == pytest.approx(published, abs=1e-12)


@pytest.mark.xfail(
    strict=True,
    reason=(
        "The per-protein artefact is cut at the unweighted Fmax optimum while the "
        "cell it is supposed to recompose reports the IA-weighted one, and it holds "
        "a single threshold, so a resample that re-selects the operating point has "
        "nothing to select from. Both are defects of the producer, not of the test: "
        "the test states what a correct producer owes. It is strict so the day a "
        "grid producer lands this fails as XPASS and the mark has to come off, "
        "rather than quietly going green with nobody noticing the contract was met."
    ),
)
def test_rows_recompose_the_cell_at_the_tau_the_driver_actually_passes():
    """The same contract, at the tau the driver really hands over.

    ``_run_cafa_eval_driver._persist_per_protein`` builds ``tau_by_ns`` from
    ``parse_results(dfs_best)``, which reads the optimum of the UNWEIGHTED
    frame, while the cell every table publishes is ``f_micro_w``, whose optimum
    is a different column. ``_EXTRA_METRIC_SPECS`` extracts four fields from the
    weighted frame and ``tau`` is not among them, so the weighted threshold is
    discarded before anything can pass it.

    Cutting a weighted array at an unweighted threshold produces rows that are
    internally consistent and describe a cell nobody published.
    """
    weighted_col, unweighted_col = 60, 20
    tp, pred, n_gt = _grid(weighted_peak=weighted_col, unweighted_peak=unweighted_col)

    published_tp = tp[:, weighted_col].sum()
    published_pred = pred[:, weighted_col].sum()
    precision, recall = published_tp / published_pred, published_tp / n_gt.sum()
    published = 2 * precision * recall / (precision + recall)

    rows = rows_from_sink(
        _sink(tp, pred, n_gt),
        th_step=TH_STEP,
        tau_by_ns={"molecular_function": unweighted_col * TH_STEP},
    )

    assert _recompose(rows, "molecular_function") == pytest.approx(published, abs=1e-12), (
        "the per-protein rows do not recompose the published cell: they were cut "
        "at the unweighted optimum while the cell reports the weighted one"
    )


@pytest.mark.xfail(
    strict=True,
    reason=(
        "The per-protein artefact is cut at the unweighted Fmax optimum while the "
        "cell it is supposed to recompose reports the IA-weighted one, and it holds "
        "a single threshold, so a resample that re-selects the operating point has "
        "nothing to select from. Both are defects of the producer, not of the test: "
        "the test states what a correct producer owes. It is strict so the day a "
        "grid producer lands this fails as XPASS and the mark has to come off, "
        "rather than quietly going green with nobody noticing the contract was met."
    ),
)
def test_a_single_column_cannot_answer_a_resampled_question():
    """Why fixing the threshold is not the fix.

    A paired bootstrap re-selects each system's operating point inside every
    resample, so it needs the whole grid. The sink is handed the whole grid, in
    two dimensions, and the writer keeps one column of it. Fixing WHICH column
    leaves the artefact self-consistent and still unable to carry an interval,
    which is the property it exists for.
    """
    tp, pred, n_gt = _grid(weighted_peak=60, unweighted_peak=20)
    assert tp.ndim == 2 and tp.shape[1] == WIDTH, "the kernel hands over a grid, not a point"

    rows = rows_from_sink(_sink(tp, pred, n_gt), th_step=TH_STEP, tau_by_ns={"molecular_function": 0.6})
    taus = {r["tau"] for r in rows}
    assert len(taus) > 1, (
        f"the written rows carry a single threshold ({taus}); a resample that "
        "re-selects the operating point has nothing to select from"
    )
