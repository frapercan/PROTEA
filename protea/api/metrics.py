"""Shared IA-weighted primary-metric helpers for the benchmark + showcase routers.

The IA-weighted micro-averaged F (``f_micro_w``) is the LAFA / CAFA headline
metric and the only number comparable to the external leaderboards
(FIX-METRIC-IA). Rows evaluated before a real IA file was wired carry no
``f_micro_w``; for those we fall back to the unweighted ``fmax`` so historical
cells still rank, flagged via ``primary_metric`` so the front-end never silently
mixes a non-IA-weighted number into the IA-weighted headline.

Both routers consume the same definition here so the home spotlight and the
benchmark matrix can never disagree on which number is the headline.
"""

from __future__ import annotations

import math
from typing import Any

# Primary ranking metric (IA-weighted, LAFA-comparable) and the legacy
# unweighted fallback for cells that predate real-IA evaluation.
PRIMARY_METRIC = "f_micro_w"
FALLBACK_METRIC = "fmax"


def primary_cell_score(cell: dict[str, Any]) -> tuple[float, str]:
    """Return ``(score, metric_name)`` for one ``(category, aspect)`` cell.

    Prefers the IA-weighted ``f_micro_w``; falls back to the unweighted
    ``fmax`` when the cell predates real-IA evaluation. The metric name lets
    the caller label which number is driving the ranking so the front-end can
    flag legacy cells as "not IA-weighted".
    """
    fw = cell.get(PRIMARY_METRIC)
    if fw is not None:
        return float(fw), PRIMARY_METRIC
    fmax = cell.get(FALLBACK_METRIC)
    if fmax is not None:
        return float(fmax), FALLBACK_METRIC
    return 0.0, FALLBACK_METRIC


def per_task_aggregate(values: list[float]) -> dict[str, float | int] | None:
    """Mean + normal-approx 95% CI half-width over a list of primary scores.

    Returns ``None`` for an empty list. The CI half-width is
    ``1.96 * sd / sqrt(n)`` (sample sd, ``n - 1`` denominator); ``0.0`` when a
    single value is present. This is the honest central-tendency the
    dashboards headline instead of the best-cell maximum (winner's-curse,
    FIX-METRIC-IA).
    """
    if not values:
        return None
    n = len(values)
    mean = sum(values) / n
    if n > 1:
        var = sum((v - mean) ** 2 for v in values) / (n - 1)
        ci = 1.96 * math.sqrt(var) / math.sqrt(n)
    else:
        ci = 0.0
    return {
        "mean": round(mean, 4),
        "ci95": round(ci, 4),
        "max": round(max(values), 4),
        "min": round(min(values), 4),
        "n": n,
    }
