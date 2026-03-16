"""CAFA-style precision-recall metrics for GO term prediction evaluation.

Takes scored GOPrediction rows and EvaluationData (ground truth) and computes
Fmax, AUC-PR, and the full precision-recall curve following the CAFA protocol.

CAFA protocol summary
---------------------
- Evaluate only on proteins present in the ground truth (NK or LK).
- At each score threshold t:
    precision(t) = mean over proteins-with-predictions of |pred ∩ true| / |pred|
    recall(t)    = mean over ALL ground-truth proteins of |pred ∩ true| / |true|
- Fmax = max_t(2 * P(t) * R(t) / (P(t) + R(t)))
- AUC-PR via trapezoidal integration of the PR curve.

Note: This implementation uses exact GO term matching (no DAG propagation).
Ancestor propagation is intentionally left for a future iteration.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

import numpy as np

from protea.core.evaluation import EvaluationData

_N_THRESHOLDS = 101  # sweep [0.0, 0.01, …, 1.0]


@dataclass
class PRPoint:
    threshold: float
    precision: float
    recall: float
    f1: float


@dataclass
class CAFAMetrics:
    """CAFA evaluation results for one (PredictionSet, ScoringConfig, category) triple."""

    category: str  # "nk" or "lk"
    fmax: float
    threshold_at_fmax: float
    auc_pr: float
    n_ground_truth_proteins: int  # proteins in the chosen NK/LK category
    n_predicted_proteins: int  # proteins that received at least 1 prediction
    n_predictions: int  # total scored predictions passed in
    curve: list[PRPoint] = field(default_factory=list)

    def summary(self) -> dict[str, Any]:
        return {
            "category": self.category,
            "fmax": self.fmax,
            "threshold_at_fmax": self.threshold_at_fmax,
            "auc_pr": self.auc_pr,
            "n_ground_truth_proteins": self.n_ground_truth_proteins,
            "n_predicted_proteins": self.n_predicted_proteins,
            "n_predictions": self.n_predictions,
        }


def compute_cafa_metrics(
    scored_predictions: list[dict[str, Any]],
    evaluation_data: EvaluationData,
    category: str = "nk",
) -> CAFAMetrics:
    """Compute CAFA Fmax and PR curve.

    Parameters
    ----------
    scored_predictions:
        List of dicts, each must have:
          - ``protein_accession`` (str)
          - ``go_id`` (str, e.g. "GO:0005488")
          - ``score`` (float in [0, 1])
    evaluation_data:
        Ground truth from ``compute_evaluation_data()``.
    category:
        ``"nk"`` (no-knowledge) or ``"lk"`` (limited-knowledge).

    Returns
    -------
    CAFAMetrics
    """
    if category not in ("nk", "lk"):
        raise ValueError(f"category must be 'nk' or 'lk', got {category!r}")

    ground_truth: dict[str, set[str]] = (
        evaluation_data.nk if category == "nk" else evaluation_data.lk
    )

    # Group predictions by protein, keep only proteins in ground truth
    preds_by_protein: dict[str, list[tuple[float, str]]] = defaultdict(list)
    for p in scored_predictions:
        acc = p["protein_accession"]
        if acc in ground_truth:
            preds_by_protein[acc].append((float(p["score"]), str(p["go_id"])))

    n_gt = len(ground_truth)
    n_predicted = len(preds_by_protein)

    thresholds = np.linspace(0.0, 1.0, _N_THRESHOLDS)
    curve: list[PRPoint] = []
    best_f = 0.0
    best_t = 0.0

    for t in thresholds:
        t = float(t)
        tp_sum = 0
        pred_sum = 0
        rc_num = 0
        n_with_preds = 0

        for acc, true_terms in ground_truth.items():
            predicted = {go for score, go in preds_by_protein.get(acc, []) if score >= t}
            tp = len(predicted & true_terms)
            rc_num += tp
            if predicted:
                n_with_preds += 1
                tp_sum += tp
                pred_sum += len(predicted)

        pr = (tp_sum / pred_sum) if pred_sum > 0 else 0.0
        rc = (rc_num / sum(len(v) for v in ground_truth.values())) if n_gt > 0 else 0.0
        f1 = (2 * pr * rc / (pr + rc)) if (pr + rc) > 0 else 0.0

        curve.append(
            PRPoint(
                threshold=round(t, 4), precision=round(pr, 6), recall=round(rc, 6), f1=round(f1, 6)
            )
        )

        if f1 > best_f:
            best_f = f1
            best_t = t

    # AUC-PR: trapezoidal integration (recall on x-axis, precision on y-axis)
    recalls = [p.recall for p in curve]
    precisions = [p.precision for p in curve]
    auc = float(abs(np.trapezoid(precisions, recalls)))

    return CAFAMetrics(
        category=category,
        fmax=round(best_f, 4),
        threshold_at_fmax=round(best_t, 4),
        auc_pr=round(auc, 4),
        n_ground_truth_proteins=n_gt,
        n_predicted_proteins=n_predicted,
        n_predictions=len(scored_predictions),
        curve=curve,
    )
