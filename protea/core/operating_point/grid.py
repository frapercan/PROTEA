"""The grid a reading is evaluated to, by the evaluator's own code, and what is read off it.

**One path to the grid.** ``run_cafa_evaluation`` scores a stored prediction set
and a fit (the readings of the literature ladder, regime calibration, the
calibrated maximum, the champion, the reranker) scores its out-of-fold scores,
which are in memory and never stored. Both reach the per-protein threshold grid
through this module: :func:`evaluate_setting` calls ``cafaeval`` with LAFA's
options and the per-protein sink, and :func:`evaluate_to_grid` writes in-memory
scores the way the evaluator's prediction file is written and evaluates them.
The evaluation driver calls :func:`evaluate_setting` too, so the two paths cannot
drift; a fixture holds them byte-identical (``tests/test_operating_point_grid.py``).

**The order of a written score.** A fit's scores pass, in this order, through
its fitted map, the hierarchy restoration of
:mod:`protea.core.operating_point.hierarchy` (when the caller gives the window's
ancestry), the evaluator's rendering (``'%.4f' % score``, the file cafaeval
reads, :data:`SCORE_RENDERING`) and then the grid. Rendering is monotone, so it
keeps every order the restoration made; it has to happen before the grid,
because a threshold declared on unrendered scores can sit one grid step away
from the one the evaluator would read.

**What is read off a grid.** :func:`metric_curves` computes, from the
per-protein rows of one cell in one weighting, every metric cafaeval reports at
every threshold of the grid (its own formulas: ``cafa`` normalisation, the
macro precision over the proteins predicting anything, the micro ratios of
pooled sums, S from the remaining uncertainty and the misinformation), so a
reading at a declared threshold and a reading at the evaluator's own optimum
are the same arithmetic. :func:`pooled_threshold` pools the rows of several
windows of one arm, each (protein, window) one unit, into the declared
threshold of a cell.
"""

from __future__ import annotations

import signal
import threading
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pcsv

from protea.core.operations._paired_panels_bootstrap import PanelArrays, micro_curve
from protea.core.operations._run_cafa_grid_artifact import GridArtifact, grid_rows_from_sink

if TYPE_CHECKING:  # pragma: no cover - typing only
    from protea.core.operating_point.hierarchy import Ancestry

#: The frame literals the evaluator is called with, named once: arguments to the
#: cafaeval call AND facts the grid artefact stamps, so the two cannot disagree.
CAFAEVAL_PROP = "fill"
CAFAEVAL_NORM = "cafa"
CAFAEVAL_NO_ORPHANS = True

#: How a score is written in the prediction file cafaeval reads.
SCORE_RENDERING = "%.4f"

#: The evaluator's three settings, one per knowledge category, in report order.
SETTINGS: tuple[str, ...] = ("NK", "LK", "PK")

#: The columns of a table of scores, as a method stores them.
SCORE_COLUMNS: tuple[str, str, str] = ("protein_accession", "go_id", "score")


def render_scores(table: pa.Table) -> pa.Table:
    """``(protein_accession, go_id, score)`` ordered by protein and term, scores as text.

    The one rendering of a set's scores: ``'%.4f' % score``, the file cafaeval
    reads and the CAFA export of the API. ``table`` carries the three columns of
    :data:`SCORE_COLUMNS` with a float score; a null score is refused by the
    caller, which knows which set it came from.
    """
    order = pc.sort_indices(
        table, sort_keys=[("protein_accession", "ascending"), ("go_id", "ascending")]
    )
    rows = table.take(order)
    scores = np.char.mod(SCORE_RENDERING, rows["score"].to_numpy(zero_copy_only=False))
    return pa.table(
        {
            "protein_accession": rows["protein_accession"],
            "go_id": rows["go_id"],
            "score": pa.array(scores.tolist(), type=pa.string()),
        }
    )


def write_prediction_file(table: pa.Table, path: str | Path) -> int:
    """Write a rendered table as cafaeval's ``protein\\tgo_id\\tscore``; how many rows went out."""
    if table.num_rows == 0:
        Path(path).write_text("")
        return 0
    pcsv.write_csv(
        table,
        str(path),
        write_options=pcsv.WriteOptions(include_header=False, delimiter="\t", quoting_style="none"),
    )
    return int(table.num_rows)


@dataclass(frozen=True)
class EvaluatorInputs:
    """The staged files one window is scored against, and the grid it is scored on.

    ``ground_truth`` maps each setting to its truth file; ``pk_known_path`` is
    the known terms the PK setting excludes. The other fields are cafaeval's
    arguments, passed as the evaluation driver passes them.
    """

    obo_path: str
    ground_truth: Mapping[str, str]
    pk_known_path: str
    toi_path: str
    ia_path: str | None
    th_step: float = 0.01
    max_terms: int | None = None

    def known_for(self, setting: str) -> str | None:
        """The exclusion file of a setting: PK excludes the known terms, the others nothing."""
        return self.pk_known_path if setting == "PK" else None


def new_per_protein_sink() -> Any:
    """The sink, failing loudly if the installed cafaeval cannot make one.

    Imported through a function because the driver calls the evaluation inside a
    broad ``except``: an ImportError raised there would be reported as "this
    setting produced no results", and a wrong dependency version would look
    exactly like an evaluation that legitimately scored nothing.
    """
    try:
        from cafaeval.evaluation import PerProteinSink
    except ImportError as exc:  # pragma: no cover - depends on the installed pin
        raise RuntimeError(
            "the installed cafaeval cannot emit per-protein scores; the pin in "
            "pyproject expects a build with PerProteinSink. Reinstall the "
            "dependency rather than reading this run's empty settings as a result."
        ) from exc
    return PerProteinSink()


@contextmanager
def default_signals_where_possible() -> Iterator[None]:
    """Restore default SIGTERM/SIGINT for the block, on the main thread only.

    The worker's handler only sets a flag, so forked pool children would ignore
    SIGTERM; resetting to ``SIG_DFL`` for the call avoids that. Off the main
    thread the reset is skipped, and has to be: CPython refuses ``signal.signal``
    anywhere else, and the job pump runs every operation in a per-job thread.
    Skipping it is safe, because the sink forces cafaeval's sparse kernels, which
    fan nothing out.
    """
    if threading.current_thread() is not threading.main_thread():
        yield
        return
    old_sigterm = signal.signal(signal.SIGTERM, signal.SIG_DFL)
    old_sigint = signal.signal(signal.SIGINT, signal.SIG_DFL)
    try:
        yield
    finally:
        signal.signal(signal.SIGTERM, old_sigterm)
        signal.signal(signal.SIGINT, old_sigint)


def invoke_cafaeval(
    inputs: EvaluatorInputs, *, pred_dir: str, gt_file: str, known_file: str | None
) -> tuple[Any, Any, Any]:
    """``(curve, best, sink)`` of one cafaeval call with LAFA's options and the sink.

    The curve is the frame cafaeval writes as ``evaluation_all.tsv`` (every
    threshold that kept a prediction), the best frames its ``evaluation_best_*``
    tables, and the sink the per-protein arrays both were computed from.
    """
    from cafaeval.evaluation import cafa_eval

    sink = new_per_protein_sink()
    with default_signals_where_possible():
        curve, best = cafa_eval(
            inputs.obo_path,
            pred_dir,
            gt_file,
            ia=inputs.ia_path,
            exclude=known_file,
            prop=CAFAEVAL_PROP,
            norm=CAFAEVAL_NORM,
            no_orphans=CAFAEVAL_NO_ORPHANS,
            toi_file=inputs.toi_path,
            max_terms=inputs.max_terms,
            th_step=inputs.th_step,
            n_cpu=1,
            per_protein_sink=sink,
        )
    return curve, best, sink


@dataclass(frozen=True)
class SettingEvaluation:
    """What one setting's evaluation gave: the curve, the best frames, the metrics, the sink."""

    setting: str
    curve: Any
    best: Any
    metrics: dict[str, Any]
    sink: Any


def evaluate_setting(inputs: EvaluatorInputs, setting: str, pred_dir: str) -> SettingEvaluation:
    """Score one setting and read its metrics: the cafaeval call every path shares.

    The metrics are the per-aspect cells ``run_cafa_evaluation`` persists, with
    S's minimum in both weightings and its threshold read from the curve
    (:func:`protea.core.operations._run_cafa_artifacts.merge_smin`).
    """
    from protea.core.operations import _run_cafa_artifacts as artifacts

    curve, best, sink = invoke_cafaeval(
        inputs,
        pred_dir=pred_dir,
        gt_file=inputs.ground_truth[setting],
        known_file=inputs.known_for(setting),
    )
    metrics = artifacts.parse_results(best)
    artifacts.merge_smin(curve, metrics)
    return SettingEvaluation(setting, curve, best, metrics, sink)


@dataclass(frozen=True)
class GridEvaluation:
    """Each setting's evaluation and the per-protein grid flattened from its sink."""

    settings: dict[str, SettingEvaluation]
    grids: dict[str, GridArtifact]

    def refuse_dropped(self) -> None:
        """Refuse a grid that left a namespace out: a fit needs every cell it scored."""
        dropped = {
            setting: [entry["namespace"] for entry in grid.dropped]
            for setting, grid in self.grids.items()
            if grid.dropped
        }
        if dropped:
            raise ValueError(
                f"the grid left namespaces out ({dropped}); a cell without its rows cannot "
                "declare a threshold or carry a reading"
            )


def evaluate_to_grid(
    scores: pa.Table,
    inputs: EvaluatorInputs,
    workdir: str | Path,
    *,
    ancestry: Ancestry | None = None,
    settings: Sequence[str] = SETTINGS,
) -> GridEvaluation:
    """Score in-memory scores as the evaluator scores a stored set, to the per-protein grid.

    ``scores`` holds :data:`SCORE_COLUMNS` with a float score, after the
    caller's fitted map. With ``ancestry`` the scores are restored
    (:func:`protea.core.operating_point.hierarchy.restore`) before they are
    rendered; then they are rendered (:func:`render_scores`), written where
    cafaeval reads them, and each setting is evaluated
    (:func:`evaluate_setting`) and flattened to its grid
    (:func:`~protea.core.operations._run_cafa_grid_artifact.grid_rows_from_sink`).
    """
    if ancestry is not None:
        from protea.core.operating_point.hierarchy import restore

        scores = restore(scores, ancestry)
    pred_dir = Path(workdir) / "predictions"
    pred_dir.mkdir(parents=True, exist_ok=True)
    write_prediction_file(render_scores(scores), pred_dir / "predictions.tsv")
    evaluated = {s: evaluate_setting(inputs, s, str(pred_dir)) for s in settings}
    grids = {s: grid_rows_from_sink(evaluated[s].sink) for s in settings}
    return GridEvaluation(evaluated, grids)


def score_table(
    proteins: Sequence[str], go_ids: Sequence[str], scores: Sequence[float] | np.ndarray
) -> pa.Table:
    """A table of scores in :data:`SCORE_COLUMNS`, from three aligned columns."""
    return pa.table(
        {
            "protein_accession": pa.array(list(proteins), pa.string()),
            "go_id": pa.array(list(go_ids), pa.string()),
            "score": pa.array(np.asarray(scores, dtype=np.float64), pa.float64()),
        }
    )


# ---------------------------------------------------------------------------
# What a grid reads at a threshold
# ---------------------------------------------------------------------------

#: The metrics :func:`metric_curves` returns, by cafaeval's own column names.
CURVE_METRICS: tuple[str, ...] = (
    "n",
    "cov",
    "pr",
    "rc",
    "f",
    "ru",
    "mi",
    "s",
    "pr_micro",
    "rc_micro",
    "f_micro",
)


def _ratio(numer: np.ndarray, denom: np.ndarray) -> np.ndarray:
    out = np.zeros(np.broadcast(numer, denom).shape, dtype=np.float64)
    np.divide(numer, denom, out=out, where=denom > 0)
    return out


def _harmonic(pr: np.ndarray, rc: np.ndarray) -> np.ndarray:
    return _ratio(2.0 * pr * rc, pr + rc)


def metric_curves(arrays: PanelArrays) -> dict[str, np.ndarray]:
    """Every metric cafaeval reports, at every threshold, from one cell's per-protein rows.

    ``arrays`` holds one weighting's population (the rows the evaluator
    normalised by): ``tp`` and ``pred`` are ``(n, thresholds)``, ``n_gt`` is
    ``(n,)``. cafaeval's ``cafa`` normalisation: the macro precision averages
    over the proteins predicting anything at the threshold (``n``), every other
    column over the population; ``f`` is the harmonic mean of the macro pair,
    ``s`` the norm of the mean remaining uncertainty and misinformation, and the
    micro pair the ratios of pooled sums.
    """
    size = arrays.n
    tp = np.asarray(arrays.tp, dtype=np.float64)
    pred = np.asarray(arrays.pred, dtype=np.float64)
    gt = np.asarray(arrays.n_gt, dtype=np.float64)
    predicting = (pred > 0.0).sum(axis=0).astype(np.float64)
    precision_sum = _ratio(tp, pred).sum(axis=0)
    recall_sum = _ratio(tp, gt[:, None]).sum(axis=0)
    population = np.full(tp.shape[1], float(size))
    pr = _ratio(precision_sum, predicting)
    rc = _ratio(recall_sum, population)
    ru = _ratio(gt.sum() - tp.sum(axis=0), population)
    mi = _ratio(pred.sum(axis=0) - tp.sum(axis=0), population)
    tp_total, pred_total = tp.sum(axis=0), pred.sum(axis=0)
    pr_micro = _ratio(tp_total, pred_total)
    rc_micro = _ratio(tp_total, np.full(tp.shape[1], gt.sum()))
    return {
        "n": predicting,
        "cov": _ratio(predicting, population),
        "pr": pr,
        "rc": rc,
        "f": _harmonic(pr, rc),
        "ru": ru,
        "mi": mi,
        "s": np.sqrt(ru**2 + mi**2),
        "pr_micro": pr_micro,
        "rc_micro": rc_micro,
        "f_micro": _harmonic(pr_micro, rc_micro),
    }


def metrics_at(
    curves: Mapping[str, np.ndarray], index: int, *, suffix: str = ""
) -> dict[str, float]:
    """The metrics of :func:`metric_curves` at one grid column, named with ``suffix``."""
    return {f"{name}{suffix}": float(curves[name][index]) for name in CURVE_METRICS}


@dataclass(frozen=True)
class Extremum:
    """Where a curve peaks (or bottoms) over the thresholds that kept a prediction.

    ``edge`` is ``"bottom"`` at the lowest such threshold, ``"top"`` at the
    highest and None inside, as ``run_cafa_evaluation`` records its maxima.
    """

    value: float
    tau: float
    index: int
    edge: str | None


def extremum(
    curve: np.ndarray, covered: np.ndarray, tau_grid: np.ndarray, *, lowest: bool = False
) -> Extremum | None:
    """The curve's maximum (or minimum) over the covered thresholds, ties to the lowest tau.

    cafaeval drops the thresholds at which nothing is predicted before it
    chooses, so ``covered`` (the unweighted coverage above zero) is the domain;
    None when nothing is covered.
    """
    domain = np.flatnonzero(covered)
    if domain.size == 0:
        return None
    values = curve[domain]
    position = int(np.argmin(values)) if lowest else int(np.argmax(values))
    index = int(domain[position])
    edge = "bottom" if index == domain[0] else "top" if index == domain[-1] else None
    return Extremum(float(curve[index]), float(tau_grid[index]), index, edge)


@dataclass(frozen=True)
class PooledThreshold:
    """A cell's declared threshold: the grid argmax of the pooled ``f_micro_w``, lowest tau on ties.

    ``units`` counts the (protein, window) units pooled; ``window_taus`` each
    window's own argmax and its value, by window; ``at_grid_edge`` whether the
    threshold is the grid's first or last element.
    """

    tau: float
    index: int
    value: float
    units: int
    curve: tuple[float, ...]
    window_taus: dict[str, tuple[float, float]]
    at_grid_edge: bool

    @property
    def half_value(self) -> float:
        """``F*_fit / 2``, where a calibrated reading's optimal threshold is expected."""
        return self.value / 2.0


def pooled_threshold(
    windows: Mapping[str, PanelArrays], tau_grid: Sequence[float]
) -> PooledThreshold:
    """Pool one cell's rows over several windows, each (protein, window) a unit, and take the argmax.

    ``f_micro_w(tau) = 2 sum tp / (sum pred + sum n_gt)`` over every unit of
    every window; ``np.argmax`` takes the first maximum, which is the lowest
    threshold on an ascending grid.
    """
    if not windows:
        raise ValueError("a threshold is pooled over at least one window")
    grid = np.asarray(tau_grid, dtype=np.float64)
    tp = np.zeros(grid.size)
    pred = np.zeros(grid.size)
    gt = 0.0
    own: dict[str, tuple[float, float]] = {}
    units = 0
    for window, arrays in sorted(windows.items()):
        if arrays.n_tau != grid.size:
            raise ValueError(
                f"window {window} carries {arrays.n_tau} thresholds and the grid {grid.size}"
            )
        curve = micro_curve(arrays.tp.sum(axis=0), arrays.pred.sum(axis=0), arrays.n_gt.sum())
        best = int(np.argmax(curve))
        own[window] = (float(grid[best]), float(curve[best]))
        tp += arrays.tp.sum(axis=0)
        pred += arrays.pred.sum(axis=0)
        gt += float(arrays.n_gt.sum())
        units += arrays.n
    pooled = micro_curve(tp, pred, gt)
    index = int(np.argmax(pooled))
    return PooledThreshold(
        tau=float(grid[index]),
        index=index,
        value=float(pooled[index]),
        units=units,
        curve=tuple(float(v) for v in pooled),
        window_taus=own,
        at_grid_edge=index in (0, grid.size - 1),
    )


def tau_grid_of(th_step: float) -> np.ndarray:
    """cafaeval's grid at ``th_step``, ``np.arange(th_step, 1, th_step)``, float accumulation kept."""
    return np.arange(th_step, 1, th_step)


__all__ = [
    "CAFAEVAL_NORM",
    "CAFAEVAL_NO_ORPHANS",
    "CAFAEVAL_PROP",
    "CURVE_METRICS",
    "SCORE_COLUMNS",
    "SCORE_RENDERING",
    "SETTINGS",
    "EvaluatorInputs",
    "Extremum",
    "GridEvaluation",
    "PooledThreshold",
    "SettingEvaluation",
    "default_signals_where_possible",
    "evaluate_setting",
    "evaluate_to_grid",
    "extremum",
    "invoke_cafaeval",
    "metric_curves",
    "metrics_at",
    "new_per_protein_sink",
    "pooled_threshold",
    "render_scores",
    "score_table",
    "tau_grid_of",
    "write_prediction_file",
]
