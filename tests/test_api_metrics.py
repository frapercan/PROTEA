"""Unit tests for the shared IA-weighted primary-metric helpers.

These back the FIX-METRIC-IA web de-bias: the benchmark matrix and the home
showcase both rank by ``f_micro_w`` (fmax fallback) and headline a per-task
mean + 95% CI instead of the winner's-curse maximum.
"""

from __future__ import annotations

import math

import pytest

from protea.api.metrics import (
    FALLBACK_METRIC,
    PRIMARY_METRIC,
    per_task_aggregate,
    primary_cell_score,
)


class TestPrimaryCellScore:
    def test_prefers_ia_weighted(self):
        score, metric = primary_cell_score({"fmax": 0.4, "f_micro_w": 0.7})
        assert score == 0.7
        assert metric == PRIMARY_METRIC

    def test_falls_back_to_fmax(self):
        score, metric = primary_cell_score({"fmax": 0.55})
        assert score == 0.55
        assert metric == FALLBACK_METRIC

    def test_empty_cell_is_zero_fmax(self):
        score, metric = primary_cell_score({})
        assert score == 0.0
        assert metric == FALLBACK_METRIC


class TestPerTaskAggregate:
    def test_empty_list_is_none(self):
        assert per_task_aggregate([]) is None

    def test_single_value_zero_ci(self):
        agg = per_task_aggregate([0.6])
        assert agg == {"mean": 0.6, "ci95": 0.0, "max": 0.6, "min": 0.6, "n": 1}

    def test_mean_and_ci_match_normal_approx(self):
        vals = [0.4, 0.6, 0.8]
        agg = per_task_aggregate(vals)
        assert agg is not None
        assert agg["mean"] == pytest.approx(0.6)
        assert agg["n"] == 3
        assert agg["max"] == 0.8
        assert agg["min"] == 0.4
        # 1.96 * sd / sqrt(n), sample sd (n-1 denominator).
        mean = sum(vals) / 3
        sd = math.sqrt(sum((v - mean) ** 2 for v in vals) / 2)
        assert agg["ci95"] == pytest.approx(round(1.96 * sd / math.sqrt(3), 4))

    def test_max_exceeds_mean_documents_winners_curse(self):
        # A spread of model scores: the best-cell maximum is higher than the
        # honest per-task mean -- the gap the dashboard must not headline.
        agg = per_task_aggregate([0.50, 0.52, 0.70])
        assert agg is not None
        assert agg["max"] > agg["mean"]
