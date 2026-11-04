"""The scale of the score, read before cafaeval thresholds it (ADR-D55, section 6).

Without a re-ranker or a scoring preset a neighbour prediction is scored
``1 - cosine_distance / 2``, so its floor is set by how anisotropic the bank
is. Measured on 2026-11-04 over 4,000 random pairs per bank (seed 20261104),
two unrelated proteins already score 0.975 in the full length ankh bank
``f3a7fe16``, against 0.569 in the learned sparse code ``76792dc2``. On a grid
of step 0.01 the first bank has two steps of useful range. On the panel of no
prior knowledge in biological process, the F curve of both neighbour arms rises
to the last point of the grid, so the maximum each reports is an edge and not a
maximum, and a difference between the two compares one edge with another.

Two things live here:

* the percentile rank reading, which replaces each written score by its
  percentile rank among the rows its file scores, so the uniform grid falls on
  the arm's own quantiles instead of on a scale the bank compressed;
* the interior check, which says for each namespace and each F variant whether
  the curve's maximum is interior or sits at an edge of the thresholds that
  kept any prediction. ADR-D55 blocks a verdict on a maximum at an edge.

The reading is monotone, so it keeps each arm's ordering, and with it every
comparison between two of its predictions. What it moves is where the grid
falls, and that has one consequence a reader must carry: a metric at a FIXED
threshold becomes relative to the arm. Precision at 0.5 is precision at the
arm's median score, not at a similarity of 0.5.
"""

from __future__ import annotations

import os
from typing import Any

import numpy as np

from protea.core.contracts.operation import EmitFn
from protea.core.operations._run_cafa_artifacts import _NS_LABELS
from protea.core.operations._run_cafa_grid_artifact import tau_grid_for

#: How a file's scores can be read before cafaeval thresholds them. ``raw`` is
#: every run before ADR-D55 and the value the frame seal treats as historical.
SCORE_READINGS: tuple[str, ...] = ("raw", "percentile_rank")

#: The tie method of the percentile reading, fixed and written down because
#: another one is another reading. Tied scores take the AVERAGE of the ranks
#: they span, so they read alike and a threshold keeps or drops a tie group
#: whole, exactly as it did on the raw scale.
TIE_METHOD = "average"

#: The F variants cafaeval maximises over the grid; each curve is checked.
F_VARIANTS: tuple[str, ...] = ("f", "f_w", "f_micro", "f_micro_w")


def percentile_rank(scores: np.ndarray) -> np.ndarray:
    """Each score's mid rank percentile within ``scores``, in (0, 1].

    The average rank of the score's tie group over the count: the highest
    unique score reads 1.0, tied scores read alike, and a higher score never
    reads lower.
    """
    n = int(scores.size)
    if n == 0:
        return np.zeros(0, dtype=np.float64)
    _, inverse, counts = np.unique(scores, return_inverse=True, return_counts=True)
    upper = np.cumsum(counts)
    mid = upper - (counts - 1) / 2.0
    return np.asarray(mid[inverse] / n, dtype=np.float64)


def read_prediction_file(path: str, reading: str, th_step: float) -> dict[str, Any]:
    """Apply ``reading`` in place to a ``protein, go_id, score`` file; return its scale.

    The share of predictions in the grid's top bin, at or above its last
    threshold, is reported before and after the reading, so a raw scale that
    was censored stays visible once the percentile reading has resolved it.
    Percentiles are written with six decimals: the reading has its own
    resolution and the grid is a hundred times coarser.
    """
    if reading not in SCORE_READINGS:
        raise ValueError(f"score_reading must be one of {SCORE_READINGS}, not {reading!r}")
    top = tau_grid_for(th_step)[-1]
    scale: dict[str, Any] = {"reading": reading, "ties": TIE_METHOD, "top_tau": top}
    if os.path.getsize(path) == 0:
        return {**scale, "n": 0, "top_bin_share_raw": None, "top_bin_share_read": None}

    import pandas as pd

    frame = pd.read_csv(
        path,
        sep="\t",
        header=None,
        names=["protein", "go_id", "score"],
        dtype={"protein": str, "go_id": str, "score": np.float64},
    )
    raw = frame["score"].to_numpy(dtype=np.float64)
    read = raw if reading == "raw" else percentile_rank(raw)
    if reading != "raw":
        frame["score"] = read
        frame.to_csv(path, sep="\t", header=False, index=False, float_format="%.6f")
    return {
        **scale,
        "n": int(raw.size),
        "top_bin_share_raw": round(float(np.mean(raw >= top)), 6),
        "top_bin_share_read": round(float(np.mean(read >= top)), 6),
    }


def read_and_report(
    path: str, *, reading: str, th_step: float, label: str, emit: EmitFn
) -> dict[str, Any]:
    """:func:`read_prediction_file`, with the scale recorded on the job."""
    scale = read_prediction_file(path, reading, th_step)
    emit("run_cafa_evaluation.score_scale", None, {"file": label, **scale}, "info")
    return scale


def annotate_maxima(curve: Any, ns_results: dict[str, Any]) -> dict[str, list[str]]:
    """Record where each F curve peaks, and return the variants peaking at an edge.

    Adds ``maxima`` to every namespace already in ``ns_results``: per F
    variant, the threshold of the maximum and its ``edge``, which is ``"top"``
    at the highest threshold that kept any prediction, ``"bottom"`` at the
    lowest, and None when the maximum is interior. cafaeval drops thresholds
    with no coverage before it chooses, so these are the edges of the curve it
    chose on. Ties go to the lowest threshold, as cafaeval's own choice does.
    """
    censored: dict[str, list[str]] = {}
    if curve is None or curve.empty:
        return censored
    flat = curve.reset_index()
    for ns_long, group in flat.groupby("ns", sort=True):
        ns = _NS_LABELS.get(str(ns_long))
        if ns is None or ns not in ns_results:
            continue
        if "filename" in group.columns and group["filename"].nunique() > 1:
            raise ValueError(
                f"namespace {ns}: the curve holds {group['filename'].nunique()} prediction "
                "files, and one evaluation reads one file per setting"
            )
        taus = group["tau"].to_numpy(dtype=np.float64)
        maxima: dict[str, Any] = {}
        for variant in F_VARIANTS:
            if variant not in group.columns:
                continue
            values = group[variant].to_numpy(dtype=np.float64)
            if not np.isfinite(values).any():
                continue
            tau = float(taus[int(np.nanargmax(values))])
            edge = "top" if tau == taus.max() else "bottom" if tau == taus.min() else None
            maxima[variant] = {"tau": round(tau, 4), "edge": edge}
            if edge is not None:
                censored.setdefault(ns, []).append(variant)
        ns_results[ns]["maxima"] = maxima
    return censored
