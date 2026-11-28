"""The nine panels as one family: one stream, balanced percentile bounds, joint coverage.

The literature ladder's adoption rule reads the nine cells together, so their
intervals have to hold together. Pinned here: the bounds are order statistics
and their Monte Carlo error is read from the order statistics around them (on a
known distribution, against the asymptotic error of a sample quantile); one per
panel level lies between Bonferroni's and the per-panel one, reaches the joint
coverage, and moves with the dependence between panels; a protein carries one
multiplicity into every panel; the detectable effect is the bootstrap's own
formula on the simultaneous bounds; ``bca`` is refused with the family; and a
comparison read at each arm's declared threshold reads each arm at its own
column, a difference checked by hand.
"""

from __future__ import annotations

import contextlib
import math
from pathlib import Path
from typing import Any

import numpy as np
import pytest
from pydantic import ValidationError
from scipy.stats import norm

from protea.core.operating_point import DeclaredThreshold
from protea.core.operations import _paired_panels_bootstrap as boot
from protea.core.operations import compare_paired_panels as operation
from protea.core.operations._paired_panels_panel import align, load_side
from protea.core.operations._paired_panels_simultaneous import (
    balanced_level,
    bound_error,
    joint_coverage,
    order_statistic,
    shared_draws,
)
from protea.core.operations.compare_paired_panels import ComparePairedPanelsPayload
from tests.helpers.paired_panels import write_panel
from tests.test_compare_paired_panels import (
    _A_ID,
    _B_ID,
    _GUARD_N,
    MFO,
    TAUS,
    TH_STEP,
    _reselection_arrays,
    _run,
    _write_reselection_panel,
)

ALPHA = 0.05


class TestTheBoundsAreOrderStatistics:
    def test_the_rank_is_the_ceiling_of_q_times_b(self) -> None:
        ordered = np.arange(10.0)
        assert order_statistic(ordered, 0.25) == 2.0  # rank ceil(2.5) = 3
        assert order_statistic(ordered, 0.975) == 9.0
        assert order_statistic(ordered, 0.0001) == 0.0  # never below rank 1

    def test_the_monte_carlo_error_of_a_bound_on_a_known_distribution(self) -> None:
        """Half the spread of the order statistics one binomial deviation around the rank.

        For a sample quantile the error is asymptotically
        ``sqrt(q (1 - q) / B) / f(x_q)``. Over forty samples of 20,000 normal
        draws, the mean of the estimate is that within ten per cent, and it is
        the spread of the bound itself across the samples within a quarter.
        """
        rng = np.random.default_rng(7)
        for q in (0.025, 0.003, 0.5):
            errors, bounds = [], []
            for _ in range(40):
                ordered = np.sort(rng.standard_normal(20_000))
                errors.append(bound_error(ordered, q))
                bounds.append(order_statistic(ordered, q))
            expected = math.sqrt(q * (1 - q) / 20_000) / norm.pdf(norm.ppf(q))
            assert float(np.mean(errors)) == pytest.approx(expected, rel=0.10), q
            assert float(np.mean(errors)) == pytest.approx(float(np.std(bounds)), rel=0.25), q

    def test_more_resamples_shrink_the_error(self) -> None:
        rng = np.random.default_rng(3)
        small = bound_error(np.sort(rng.standard_normal(2_000)), 0.0028)
        large = bound_error(np.sort(rng.standard_normal(32_000)), 0.0028)
        assert large < small


def _panels(rho: float, k: int = 9, size: int = 4_000, seed: int = 11) -> dict[str, np.ndarray]:
    """``k`` panels of differences sharing a common factor of correlation ``rho``."""
    rng = np.random.default_rng(seed)
    common = rng.standard_normal(size)
    return {
        f"panel-{i}": math.sqrt(rho) * common + math.sqrt(1 - rho) * rng.standard_normal(size)
        for i in range(k)
    }


class TestTheBalancedLevel:
    def test_identical_panels_need_no_more_than_the_per_panel_level(self) -> None:
        one = _panels(1.0)["panel-0"]
        level, coverage = balanced_level({f"p{i}": one for i in range(9)}, 1 - ALPHA)
        assert level == pytest.approx(ALPHA) and coverage >= 1 - ALPHA

    def test_dependent_panels_sit_between_bonferroni_and_the_per_panel_level(self) -> None:
        independent = balanced_level(_panels(0.0), 1 - ALPHA)
        correlated = balanced_level(_panels(0.7), 1 - ALPHA)
        for level, coverage in (independent, correlated):
            assert ALPHA / 9 <= level < ALPHA - 1e-6
            assert coverage >= 1 - ALPHA
        # Correlated panels miss together, so each needs less widening.
        assert correlated[0] > independent[0]

    def test_the_level_is_the_largest_that_covers(self) -> None:
        deltas = _panels(0.3)
        ordered = {k: np.sort(v) for k, v in deltas.items()}
        level, coverage = balanced_level(deltas, 1 - ALPHA)
        assert joint_coverage(deltas, ordered, level) == coverage >= 1 - ALPHA
        assert joint_coverage(deltas, ordered, level * 1.01) < 1 - ALPHA

    def test_a_single_panel_is_its_own_family(self) -> None:
        level, coverage = balanced_level({"only": _panels(0.0, k=1)["panel-0"]}, 1 - ALPHA)
        assert level == pytest.approx(ALPHA)
        assert coverage == pytest.approx(1 - ALPHA, abs=0.002)


def _arrays(accessions: list[str], seed: int, n_tau: int = 2) -> boot.PanelArrays:
    rng = np.random.default_rng(seed)
    pred = rng.uniform(1.0, 4.0, size=(len(accessions), n_tau))
    pred = np.sort(pred, axis=1)[:, ::-1]
    tp = pred * rng.uniform(0.2, 0.9, size=(len(accessions), 1))
    return boot.PanelArrays(tuple(accessions), tp, pred, tp[:, 0] + 1.0)


class TestOneStream:
    def test_a_protein_carries_one_multiplicity_into_every_panel(self) -> None:
        accs = [f"P{i:03d}" for i in range(40)]
        a, b = _arrays(accs, 1), _arrays(accs, 2)
        draws = shared_draws(
            {"NK:MFO": (a, b, (0, 0)), "LK:BPO": (a, b, (0, 0))}, n_resamples=300, seed=5
        )
        assert np.array_equal(draws["NK:MFO"].deltas, draws["LK:BPO"].deltas)

    def test_the_stream_depends_on_the_proteins_and_the_seed_not_the_order(self) -> None:
        a1, b1 = _arrays([f"P{i}" for i in range(30)], 3), _arrays([f"P{i}" for i in range(30)], 4)
        a2, b2 = _arrays([f"Q{i}" for i in range(20)], 5), _arrays([f"Q{i}" for i in range(20)], 6)
        one = shared_draws({"x": (a1, b1, None), "y": (a2, b2, None)}, n_resamples=500, seed=9)
        two = shared_draws({"y": (a2, b2, None), "x": (a1, b1, None)}, n_resamples=500, seed=9)
        assert np.array_equal(one["x"].deltas, two["x"].deltas)
        assert np.array_equal(one["y"].deltas, two["y"].deltas)
        other = shared_draws({"x": (a1, b1, None), "y": (a2, b2, None)}, n_resamples=500, seed=10)
        assert not np.array_equal(one["x"].deltas, other["x"].deltas)


class TestThePayload:
    BASE: dict[str, Any] = {
        "evaluation_result_id": _A_ID,
        "baseline_evaluation_result_id": _B_ID,
    }

    def test_bca_is_refused_under_the_simultaneous_family(self) -> None:
        with pytest.raises(ValidationError, match="interval_method='percentile'"):
            ComparePairedPanelsPayload.model_validate(
                {**self.BASE, "interval_family": "simultaneous"}
            )
        ComparePairedPanelsPayload.model_validate(
            {**self.BASE, "interval_family": "simultaneous", "interval_method": "percentile"}
        )

    def test_declared_threshold_ids_come_together_and_only_at_a_fixed_point(self) -> None:
        weights = "4b8f0b48-1f0e-4f45-9c43-5d3e21b2a001"
        with pytest.raises(ValidationError, match="together"):
            ComparePairedPanelsPayload.model_validate(
                {**self.BASE, "operating_point": "fixed", "declared_threshold_id": weights}
            )
        with pytest.raises(ValidationError, match="re-selects the threshold"):
            ComparePairedPanelsPayload.model_validate(
                {
                    **self.BASE,
                    "declared_threshold_id": weights,
                    "baseline_declared_threshold_id": weights,
                }
            )
        with pytest.raises(ValidationError, match="read at one of them"):
            ComparePairedPanelsPayload.model_validate(
                {
                    **self.BASE,
                    "operating_point": "fixed",
                    "declared_tau": 0.4,
                    "declared_threshold_id": weights,
                    "baseline_declared_threshold_id": weights,
                }
            )
        ok = ComparePairedPanelsPayload.model_validate(
            {
                **self.BASE,
                "operating_point": "fixed",
                "declared_threshold_id": weights,
                "baseline_declared_threshold_id": weights,
            }
        )
        assert ok.declared_tau is None


def _two_panels(tmp_path: Path) -> None:
    """The reselection panel in NK and a second, shifted one in LK, for both arms."""
    spec = _reselection_arrays()
    for side, root in (("a", tmp_path / "a"), ("b", tmp_path / "b")):
        tp, pred, n_gt = spec[side]
        for setting, scale in (("NK", 1.0), ("LK", 0.9)):
            write_panel(
                root,
                setting,
                MFO,
                accessions=spec["accessions"],
                tp=tp * scale,
                pred=pred,
                n_gt=n_gt,
                th_step=TH_STEP,
                variants=("weighted",),
            )


class TestTheFamilyThroughTheOperation:
    def test_a_per_panel_comparison_is_what_it_was(self, tmp_path: Path) -> None:
        _write_reselection_panel(tmp_path)
        result = _run(tmp_path, min_population=_GUARD_N, interval_method="percentile")
        for key in ("interval_family", "simultaneous_level", "joint_coverage"):
            assert key not in result
        assert "monte_carlo_error" not in result["panels"]["NK:MFO"]
        assert "declared_threshold_id" not in result

    def test_the_family_records_its_level_coverage_and_errors(self, tmp_path: Path) -> None:
        _two_panels(tmp_path)
        result = _run(
            tmp_path,
            panels=["NK:MFO", "LK:MFO"],
            min_population=_GUARD_N,
            interval_method="percentile",
            interval_family="simultaneous",
            operating_point="fixed",
            declared_tau=TAUS[0],
            effect_of_interest=0.01,
        )
        assert result["interval_family"] == "simultaneous"
        assert result["family_panels"] == ["LK:MFO", "NK:MFO"]
        assert ALPHA / 2 <= result["simultaneous_level"] <= ALPHA + 1e-12
        assert result["joint_coverage"] >= 0.95
        errors = [
            value
            for panel in result["panels"].values()
            for value in panel["monte_carlo_error"].values()
        ]
        assert result["monte_carlo_error"] == max(errors)

    def test_the_detectable_effect_is_the_codes_formula_on_the_simultaneous_bounds(
        self, tmp_path: Path
    ) -> None:
        _two_panels(tmp_path)
        result = _run(
            tmp_path,
            panels=["NK:MFO", "LK:MFO"],
            min_population=_GUARD_N,
            interval_method="percentile",
            interval_family="simultaneous",
            operating_point="fixed",
            declared_tau=TAUS[0],
        )
        # The draws the job made, rebuilt from the same files, seed and columns.
        with contextlib.ExitStack() as stack:
            sides = [
                load_side(r, str(tmp_path / s), {}, stack, (("NK", "LK"), "weighted"))
                for r, s in ((_A_ID, "a"), (_B_ID, "b"))
            ]
            pairs = {
                f"{setting}:MFO": (
                    *align(
                        sides[0].grids[setting].panels[MFO], sides[1].grids[setting].panels[MFO]
                    ),
                    (0, 0),
                )
                for setting in ("NK", "LK")
            }
        draws = shared_draws(pairs, n_resamples=3000, seed=0)
        level = result["simultaneous_level"]
        for key, panel in result["panels"].items():
            ordered = np.sort(draws[key].deltas)
            assert panel["ci_low"] == order_statistic(ordered, level / 2)
            assert panel["ci_high"] == order_statistic(ordered, 1 - level / 2)
            interval = boot.Interval(
                panel["ci_low"], panel["ci_high"], boot.PERCENTILE, None, None, None
            )
            expected = boot.minimum_detectable_effect(
                draws[key].deltas, panel["delta"], interval, power=0.80
            )
            assert panel["minimum_detectable_effect"] == pytest.approx(expected.mde, abs=1e-12)
            assert panel["monte_carlo_error"] == {
                "low": bound_error(ordered, level / 2),
                "high": bound_error(ordered, 1 - level / 2),
            }


class TestEachArmAtItsDeclaredThreshold:
    def test_each_arm_is_read_at_its_own_column_and_the_delta_is_the_hand_one(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        spec = _write_reselection_panel(tmp_path)
        arm, base = "4b8f0b48-1f0e-4f45-9c43-5d3e21b2a001", "4b8f0b48-1f0e-4f45-9c43-5d3e21b2a002"
        declared = {
            arm: DeclaredThreshold(
                arm, "operating_threshold", TH_STEP, {"NK:MFO": TAUS[0]}, {"NK:MFO": True}
            ),
            base: DeclaredThreshold(
                base, "operating_threshold", TH_STEP, {"NK:MFO": TAUS[1]}, {"NK:MFO": True}
            ),
        }
        monkeypatch.setattr(operation, "_threshold_loader", lambda _s: lambda wid: declared[wid])
        result = _run(
            tmp_path,
            min_population=_GUARD_N,
            interval_method="percentile",
            operating_point="fixed",
            declared_threshold_id=arm,
            baseline_declared_threshold_id=base,
        )
        panel = result["panels"]["NK:MFO"]

        def micro(side: str, column: int) -> float:
            tp, pred, n_gt = spec[side]
            return float(2 * tp[:, column].sum() / (pred[:, column].sum() + n_gt.sum()))

        assert panel["a"]["tau"] == TAUS[0] and panel["b"]["tau"] == TAUS[1]
        assert panel["delta"] == pytest.approx(micro("a", 0) - micro("b", 1), abs=1e-6)
        assert result["declared_threshold_id"] == arm
        assert result["baseline_declared_threshold_id"] == base
        assert result["operating_point"] == "fixed"
        assert panel["diagnostics"]["tau_a_switched_fraction"] == 0.0
