"""Unit tests for ``scripts/bootstrap_fmax_ci.py``.

Cover the pure functions only (per-protein Fmax compute + the two
bootstrap helpers); the CLI itself is a thin orchestrator that hits
MinIO and is exercised at integration time.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import numpy as np
import pytest

# The module lives under scripts/, not the package, so import by path.
_SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "bootstrap_fmax_ci.py"
_spec = importlib.util.spec_from_file_location("bootstrap_fmax_ci", _SCRIPT_PATH)
assert _spec is not None and _spec.loader is not None
boot = importlib.util.module_from_spec(_spec)
sys.modules["bootstrap_fmax_ci"] = boot
_spec.loader.exec_module(boot)


def test_per_protein_fmax_perfect_match():
    preds = [("P1", "GO:1", 1.0), ("P1", "GO:2", 1.0)]
    gt = [("P1", "GO:1"), ("P1", "GO:2")]
    out = boot.per_protein_fmax(preds, gt, n_thresholds=10)
    assert out == {"P1": 1.0}


def test_per_protein_fmax_no_overlap():
    preds = [("P1", "GO:1", 1.0)]
    gt = [("P1", "GO:9")]
    out = boot.per_protein_fmax(preds, gt, n_thresholds=10)
    assert out == {"P1": 0.0}


def test_per_protein_fmax_partial_threshold_picks_best():
    # P1: 1 of 2 GT terms predicted at score 1.0; nothing at 0.5.
    # The best F1 across thresholds is at the high tau (precision=1, recall=0.5).
    preds = [("P1", "GO:1", 1.0)]
    gt = [("P1", "GO:1"), ("P1", "GO:2")]
    out = boot.per_protein_fmax(preds, gt, n_thresholds=10)
    # F1 = 2 * 1 * 0.5 / 1.5 = 0.6667
    assert out["P1"] == pytest.approx(2 / 3, abs=1e-4)


def test_per_protein_fmax_protein_with_gt_no_preds_zero():
    preds = [("P1", "GO:1", 1.0)]
    gt = [("P2", "GO:1")]
    out = boot.per_protein_fmax(preds, gt, n_thresholds=10)
    assert out == {"P2": 0.0}


def test_bootstrap_ci_zero_variance():
    arr = np.full(50, 0.5, dtype=float)
    mean, lo, hi = boot.bootstrap_ci(arr, n_resamples=200, seed=1)
    assert mean == 0.5
    assert lo == 0.5 and hi == 0.5


def test_bootstrap_ci_brackets_truth():
    rng = np.random.default_rng(0)
    arr = rng.normal(loc=0.4, scale=0.05, size=500)
    _, lo, hi = boot.bootstrap_ci(arr, n_resamples=300, seed=42)
    assert lo < 0.4 < hi
    assert hi - lo < 0.05  # tight for a normal sample of 500


def test_paired_bootstrap_delta_zero_when_equal():
    rng = np.random.default_rng(0)
    arr = rng.uniform(0.2, 0.8, size=200)
    point, lo, hi = boot.paired_bootstrap_delta(arr, arr, n_resamples=200, seed=7)
    assert point == 0.0
    assert lo == 0.0 and hi == 0.0


def test_paired_bootstrap_delta_positive_when_a_better():
    rng = np.random.default_rng(0)
    n = 300
    base = rng.uniform(0.0, 0.5, size=n)
    boost = base + 0.05  # paired uplift on every protein
    point, lo, hi = boot.paired_bootstrap_delta(boost, base, n_resamples=300, seed=11)
    assert point == pytest.approx(0.05, abs=1e-9)
    assert lo == pytest.approx(0.05, abs=1e-9) and hi == pytest.approx(0.05, abs=1e-9)


def test_paired_bootstrap_delta_length_mismatch_raises():
    a = np.array([0.1, 0.2])
    b = np.array([0.1])
    with pytest.raises(ValueError, match="paired bootstrap"):
        boot.paired_bootstrap_delta(a, b)
