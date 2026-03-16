"""Unit tests for protea.core.metrics — pure-Python, no DB."""
from __future__ import annotations

import pytest

from protea.core.evaluation import EvaluationData
from protea.core.metrics import CAFAMetrics, PRPoint, compute_cafa_metrics


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_eval(nk=None, lk=None):
    return EvaluationData(
        nk=nk or {},
        lk=lk or {},
        pk={},
    )


def _pred(acc, go_id, score):
    return {"protein_accession": acc, "go_id": go_id, "score": score}


# ---------------------------------------------------------------------------
# PRPoint / CAFAMetrics dataclasses
# ---------------------------------------------------------------------------

class TestDataclasses:
    def test_prpoint_fields(self):
        p = PRPoint(threshold=0.5, precision=0.8, recall=0.6, f1=0.686)
        assert p.threshold == 0.5
        assert p.precision == 0.8
        assert p.recall == 0.6
        assert p.f1 == 0.686

    def test_cafa_metrics_summary_keys(self):
        m = CAFAMetrics(
            category="nk",
            fmax=0.75,
            threshold_at_fmax=0.3,
            auc_pr=0.5,
            n_ground_truth_proteins=10,
            n_predicted_proteins=8,
            n_predictions=50,
        )
        s = m.summary()
        assert set(s.keys()) == {
            "category", "fmax", "threshold_at_fmax", "auc_pr",
            "n_ground_truth_proteins", "n_predicted_proteins", "n_predictions",
        }
        assert s["fmax"] == 0.75


# ---------------------------------------------------------------------------
# compute_cafa_metrics — validation
# ---------------------------------------------------------------------------

class TestComputeCafaMetricsValidation:
    def test_invalid_category_raises(self):
        with pytest.raises(ValueError, match="category"):
            compute_cafa_metrics([], _make_eval(), category="pk")

    def test_valid_nk_category(self):
        result = compute_cafa_metrics([], _make_eval(nk={"P1": {"GO:0001"}}), category="nk")
        assert result.category == "nk"

    def test_valid_lk_category(self):
        result = compute_cafa_metrics([], _make_eval(lk={"P1": {"GO:0001"}}), category="lk")
        assert result.category == "lk"


# ---------------------------------------------------------------------------
# compute_cafa_metrics — empty / no predictions
# ---------------------------------------------------------------------------

class TestComputeCafaMetricsEmpty:
    def test_empty_ground_truth_and_preds(self):
        result = compute_cafa_metrics([], _make_eval())
        assert result.fmax == 0.0
        assert result.auc_pr == 0.0
        assert result.n_ground_truth_proteins == 0
        assert result.n_predictions == 0

    def test_no_predictions_fmax_zero(self):
        eval_data = _make_eval(nk={"P1": {"GO:0001"}, "P2": {"GO:0002"}})
        result = compute_cafa_metrics([], eval_data)
        assert result.fmax == 0.0
        assert result.n_predicted_proteins == 0
        assert result.n_ground_truth_proteins == 2

    def test_curve_has_101_points(self):
        result = compute_cafa_metrics([], _make_eval(nk={"P1": {"GO:0001"}}))
        assert len(result.curve) == 101


# ---------------------------------------------------------------------------
# compute_cafa_metrics — correctness
# ---------------------------------------------------------------------------

class TestComputeCafaMetricsCorrectness:
    def test_perfect_prediction_fmax_one(self):
        """Protein with one GO term predicted at score 1.0 → Fmax == 1.0."""
        eval_data = _make_eval(nk={"P1": {"GO:0001"}})
        preds = [_pred("P1", "GO:0001", 1.0)]
        result = compute_cafa_metrics(preds, eval_data)
        assert result.fmax == 1.0

    def test_wrong_term_fmax_zero(self):
        eval_data = _make_eval(nk={"P1": {"GO:0001"}})
        preds = [_pred("P1", "GO:9999", 1.0)]
        result = compute_cafa_metrics(preds, eval_data)
        assert result.fmax == 0.0

    def test_protein_outside_ground_truth_ignored(self):
        eval_data = _make_eval(nk={"P1": {"GO:0001"}})
        preds = [_pred("OTHER", "GO:0001", 1.0)]
        result = compute_cafa_metrics(preds, eval_data)
        # OTHER is not in ground truth — should be ignored
        assert result.n_predicted_proteins == 0

    def test_partial_prediction(self):
        """P1 has 2 true terms; predict only one → F1 < 1."""
        eval_data = _make_eval(nk={"P1": {"GO:0001", "GO:0002"}})
        preds = [_pred("P1", "GO:0001", 0.9)]
        result = compute_cafa_metrics(preds, eval_data)
        assert 0 < result.fmax < 1.0

    def test_multiple_proteins(self):
        eval_data = _make_eval(nk={
            "P1": {"GO:0001"},
            "P2": {"GO:0002"},
        })
        preds = [_pred("P1", "GO:0001", 0.9), _pred("P2", "GO:0002", 0.9)]
        result = compute_cafa_metrics(preds, eval_data)
        assert result.fmax > 0.5

    def test_n_predictions_counts_all(self):
        eval_data = _make_eval(nk={"P1": {"GO:0001"}})
        preds = [_pred("P1", "GO:0001", 0.9), _pred("OTHER", "GO:0002", 0.5)]
        result = compute_cafa_metrics(preds, eval_data)
        assert result.n_predictions == 2

    def test_lk_uses_lk_ground_truth(self):
        eval_data = _make_eval(
            nk={"P1": {"GO:0001"}},
            lk={"P2": {"GO:0002"}},
        )
        preds = [_pred("P2", "GO:0002", 1.0)]
        result = compute_cafa_metrics(preds, eval_data, category="lk")
        assert result.fmax == 1.0
        assert result.category == "lk"
