"""A difference carries its interval, computed on the estimator the campaign reports.

The discriminating test in this file is
``TestTheCampaignMetricAndTheOracleMean``. It is built on a panel with
heterogeneous ground-truth masses, where the campaign's accretion-weighted ratio
of sums, the mean of per-protein F at a shared threshold, the mean of
per-protein oracle Fmax and the unweighted ratio of sums all give different
answers and three of them name the opposite winner, and on a second panel where
a fixed-threshold bootstrap resolves a difference the correct re-selecting one
cannot. It is the guard that stops the old ``scripts/bootstrap_fmax_ci.py``
being promoted later, so its failure messages name the statistic that was
computed rather than printing two floats. Every fixture that says something
about weighting writes the two variants with DIFFERENT numbers: written with one
set under both names, no test could tell a consumer reading the wrong columns
from one reading the right ones.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

import numpy as np
import pytest
from pydantic import ValidationError

from protea.core.operation_catalog import build_operation_registry
from protea.core.operations import _paired_panels_bootstrap as boot
from protea.core.operations._paired_panels_artifact import (
    GridInvariantError,
    PanelComparabilityError,
    ThresholdGridUnavailableError,
)
from protea.core.operations.compare_paired_panels import (
    ALL_PANELS,
    ComparePairedPanelsOperation,
    ComparePairedPanelsPayload,
)
from tests.helpers.paired_panels import (
    tau_grid_for,
    write_grid_parquet,
    write_legacy_parquet,
    write_panel,
)

# A two-point grid, which is cafaeval's own arange at this step. Two thresholds
# are enough to show everything the operation exists for and few enough that
# every number below can be checked by hand.
TH_STEP = 0.4
TAUS = tau_grid_for(TH_STEP)
MFO = "molecular_function"

_A_ID = "aaaaaaaa-0000-0000-0000-000000000001"
_B_ID = "bbbbbbbb-0000-0000-0000-000000000002"


class _StubResult:
    def __init__(self, row: dict[str, Any] | None) -> None:
        self._row = row

    def mappings(self) -> _StubResult:
        return self

    def first(self) -> dict[str, Any] | None:
        return self._row


class _StubSession:
    """The two ``evaluation_result`` rows the provenance gate reads, and nothing else."""

    def __init__(self, rows: dict[str, dict[str, Any]]) -> None:
        self._rows = rows

    def execute(self, statement: Any, params: dict[str, Any] | None = None) -> _StubResult:
        return _StubResult(self._rows.get((params or {}).get("id", "")))


def _row(result_id: str, **overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "id": result_id,
        "evaluation_set_id": "eval-set-1",
        "prediction_set_id": f"pred-{result_id[:4]}",
        "scoring_config_id": None,
        "reranker_model_id": None,
        "frame": "lafa",
        "temporal_window": "220->230",
        "leakage_role": "test",
        "results": {},
    }
    row.update(overrides)
    return row


def _session(**overrides: Any) -> _StubSession:
    return _StubSession(
        {
            _A_ID: _row(_A_ID, **overrides.get("a", {})),
            _B_ID: _row(_B_ID, **overrides.get("b", {})),
        }
    )


def _events() -> tuple[list[tuple], Any]:
    seen: list[tuple] = []

    def emit(event: str, message: str | None, fields: dict[str, Any], level: str) -> None:
        seen.append((event, message, fields, level))

    return seen, emit


def _run(tmp_path: Path, session: Any = None, **payload: Any) -> dict[str, Any]:
    seen, emit = _events()
    body = {
        "evaluation_result_id": _A_ID,
        "baseline_evaluation_result_id": _B_ID,
        "artifacts_root": str(tmp_path / "a"),
        "baseline_artifacts_root": str(tmp_path / "b"),
        "panels": ["NK:MFO"],
        "n_resamples": 3000,
        "seed": 0,
        "min_population": 1,
        **payload,
    }
    result = ComparePairedPanelsOperation().execute(
        cast("Session", session or _session()), body, emit=emit
    ).result
    result["_events"] = seen
    return result


# ---------------------------------------------------------------------------
# The two panels the guards are built on
# ---------------------------------------------------------------------------

#: THE ESTIMATOR PANEL. Twenty proteins in two groups of ten, and the four
#: statistics that could be computed from them name different winners:
#:
#:   information-accretion weighted micro F   A wins by 0.1335   <- the campaign's
#:   mean of per-protein F at the shared tau  B wins by 0.0534
#:   mean of per-protein ORACLE Fmax          B wins by 0.0558
#:   UNWEIGHTED micro F                       B wins by 0.2059
#:
#: The heterogeneity is what does it. Group H carries ten times group L's
#: weighted ground-truth mass and a tenth of its unweighted term count, so the
#: ratio of sums is decided by H (where A is better), the mean over proteins
#: weights H and L equally (B is better on L), and the unweighted ratio is
#: decided by L. A fixture with one constant ``pred`` and one constant ``n_gt``,
#: which is what this file used to carry, makes the mean of per-protein F equal
#: to the ratio of sums identically, so it cannot see that difference at all.
_GUARD_N = 20
_GUARD_HALF = 10


def _estimator_arrays() -> dict[str, Any]:
    """Weighted and unweighted components for both arms, deliberately unequal."""

    def fill(high: tuple[list[float], list[float]], low: tuple[list[float], list[float]]):
        tp = np.zeros((_GUARD_N, 2))
        pred = np.zeros((_GUARD_N, 2))
        for i in range(_GUARD_N):
            tp[i], pred[i] = high if i < _GUARD_HALF else low
        return tp, pred

    n_gt_w = np.array([40.0] * _GUARD_HALF + [4.0] * _GUARD_HALF)
    n_gt_u = np.array([2.0] * _GUARD_HALF + [20.0] * _GUARD_HALF)
    tp_a_w, pred_a_w = fill(([24.0, 6.0], [48.0, 12.0]), ([1.2, 0.3], [6.0, 1.5]))
    tp_b_w, pred_b_w = fill(([16.0, 11.5], [48.0, 24.0]), ([2.4, 2.4], [6.0, 5.0]))
    tp_a_u, pred_a_u = fill(([1.2, 0.3], [2.4, 0.6]), ([6.0, 1.5], [30.0, 7.5]))
    tp_b_u, pred_b_u = fill(([0.8, 0.2], [2.4, 0.6]), ([12.0, 12.0], [30.0, 30.0]))
    return {
        "accessions": [f"P{i:05d}" for i in range(_GUARD_N)],
        "a": (tp_a_w, pred_a_w, n_gt_w),
        "b": (tp_b_w, pred_b_w, n_gt_w),
        "a_unweighted": (tp_a_u, pred_a_u, n_gt_u),
        "b_unweighted": (tp_b_u, pred_b_u, n_gt_u),
    }


def _write_estimator_panel(tmp_path: Path) -> dict[str, Any]:
    spec = _estimator_arrays()
    for side, root in (("a", tmp_path / "a"), ("b", tmp_path / "b")):
        tp, pred, n_gt = spec[side]
        write_panel(
            root,
            "NK",
            MFO,
            accessions=spec["accessions"],
            tp=tp,
            pred=pred,
            n_gt=n_gt,
            unweighted=spec[f"{side}_unweighted"],
            th_step=TH_STEP,
            variants=("weighted", "unweighted"),
        )
    return spec


#: THE RE-SELECTION PANEL. Twenty proteins, all carrying the same ground-truth
#: mass, so nothing here is about weighting. Group H is where system A does its
#: work; group L is where system B has a second operating point that only pays
#: off when the resample happens to be rich in group L. The full-sample optimum
#: for both arms is the lower threshold, and B's moves in about a quarter of the
#: resamples, which is what makes freezing it wrong.
def _reselection_arrays() -> dict[str, Any]:
    tp_a = np.zeros((_GUARD_N, 2))
    pred_a = np.zeros((_GUARD_N, 2))
    tp_b = np.zeros((_GUARD_N, 2))
    pred_b = np.zeros((_GUARD_N, 2))
    n_gt = np.full(_GUARD_N, 10.0)
    for i in range(_GUARD_N):
        high = i < _GUARD_HALF
        tp_a[i] = [6.0 if high else 4.0, 1.0]
        pred_a[i] = [14.0, 3.0]
        tp_b[i] = [4.0, 0.0 if high else 4.0]
        pred_b[i] = [12.0, 0.0 if high else 4.0]
    return {
        "accessions": [f"P{i:05d}" for i in range(_GUARD_N)],
        "a": (tp_a, pred_a, n_gt),
        "b": (tp_b, pred_b, n_gt),
    }


def _write_reselection_panel(tmp_path: Path) -> dict[str, Any]:
    spec = _reselection_arrays()
    for side, root in (("a", tmp_path / "a"), ("b", tmp_path / "b")):
        tp, pred, n_gt = spec[side]
        write_panel(
            root,
            "NK",
            MFO,
            accessions=spec["accessions"],
            tp=tp,
            pred=pred,
            n_gt=n_gt,
            th_step=TH_STEP,
        )
    return spec


def _micro_curve(tp: np.ndarray, pred: np.ndarray, n_gt: np.ndarray) -> np.ndarray:
    """The campaign's estimator: the ratio of POOLED sums, per threshold."""
    return 2.0 * tp.sum(axis=0) / (pred.sum(axis=0) + n_gt.sum())


def _shared_threshold_macro(tp: np.ndarray, pred: np.ndarray, n_gt: np.ndarray) -> float:
    """The mean of per-protein F at one shared threshold, that threshold chosen
    to maximise the mean. The likeliest half-fix of the old script: it removes
    the per-protein oracle without touching the aggregation."""
    return float((2.0 * tp / (pred + n_gt[:, None])).mean(axis=0).max())


def _per_protein_oracle_mean(tp: np.ndarray, pred: np.ndarray, n_gt: np.ndarray) -> float:
    """What ``scripts/bootstrap_fmax_ci.py`` computes: every protein its own threshold."""
    return float((2.0 * tp / (pred + n_gt[:, None])).max(axis=1).mean())


def _name_the_statistic(observed: float, alternatives: dict[str, float]) -> str:
    """Say which wrong statistic the observed number is, when it is one of them.

    The point of the guard is not that a number changed, it is WHICH statistic
    produced it. Two floats in an assertion message send the next reader to a
    debugger; this sends them to the estimator.
    """
    hits = [f"{name} ({value:.6f})" for name, value in alternatives.items()
            if abs(observed - value) < 1e-4]
    if len(hits) == 1:
        return f"it is {hits[0]}"
    if hits:
        return "it is " + " or ".join(hits) + ", which coincide on this arm"
    listed = ", ".join(f"{name} {value:.6f}" for name, value in alternatives.items())
    return f"it matches none of the known wrong statistics either ({listed})"


def _reference_bootstrap(spec: dict[str, Any], *, reselect: bool, seed: int = 11) -> tuple:
    """A paired bootstrap written out longhand, both ways, for the guard to compare against."""
    rng = np.random.default_rng(seed)
    n = _GUARD_N
    counts = rng.multinomial(n, np.full(n, 1.0 / n), size=4000).astype(float)
    curves = {}
    for side in ("a", "b"):
        tp, pred, n_gt = spec[side]
        curves[side] = 2.0 * (counts @ tp) / ((counts @ pred) + (counts @ n_gt)[:, None])
    if reselect:
        deltas = curves["a"].max(axis=1) - curves["b"].max(axis=1)
    else:
        full = {s: _micro_curve(*spec[s]) for s in "ab"}
        deltas = (
            curves["a"][:, int(np.argmax(full["a"]))]
            - curves["b"][:, int(np.argmax(full["b"]))]
        )
    return tuple(float(q) for q in np.quantile(deltas, [0.025, 0.975]))


class TestRegistration:
    def test_it_is_registered_and_dispatchable(self) -> None:
        """A procedure outside the platform is a capability that dies with the disk."""
        op = build_operation_registry().get("compare_paired_panels")
        assert isinstance(op, ComparePairedPanelsOperation)

    def test_it_declares_itself_read_only(self) -> None:
        assert "Writes nothing" in ComparePairedPanelsOperation().description

    def test_it_summarises_what_distinguishes_two_runs(self) -> None:
        line = ComparePairedPanelsOperation().summarize_payload(
            {"evaluation_result_id": _A_ID, "baseline_evaluation_result_id": _B_ID, "seed": 7}
        )
        assert "seed 7" in line


class TestPayload:
    def test_an_aggregate_over_the_nine_is_refused_by_name(self) -> None:
        with pytest.raises(ValidationError) as exc:
            ComparePairedPanelsPayload.model_validate(
                {
                    "evaluation_result_id": "a",
                    "baseline_evaluation_result_id": "b",
                    "panels": ["ALL"],
                }
            )
        assert "nine populations" in str(exc.value)

    def test_a_result_against_itself_is_refused(self) -> None:
        with pytest.raises(ValidationError) as exc:
            ComparePairedPanelsPayload.model_validate(
                {"evaluation_result_id": "a", "baseline_evaluation_result_id": "a"}
            )
        assert "zero-width interval" in str(exc.value)

    def test_one_local_root_without_the_other_is_refused(self) -> None:
        with pytest.raises(ValidationError):
            ComparePairedPanelsPayload.model_validate(
                {
                    "evaluation_result_id": "a",
                    "baseline_evaluation_result_id": "b",
                    "artifacts_root": "/tmp/x",
                }
            )

    def test_there_is_no_auto_weighting(self) -> None:
        """A weighted name over unweighted components is the defect this prevents."""
        with pytest.raises(ValidationError):
            ComparePairedPanelsPayload.model_validate(
                {
                    "evaluation_result_id": "a",
                    "baseline_evaluation_result_id": "b",
                    "weighting": "auto",
                }
            )

    def test_too_few_resamples_is_refused(self) -> None:
        with pytest.raises(ValidationError):
            ComparePairedPanelsPayload.model_validate(
                {
                    "evaluation_result_id": "a",
                    "baseline_evaluation_result_id": "b",
                    "n_resamples": 200,
                }
            )

    def test_the_estimator_is_named_after_what_it_weights(self) -> None:
        weighted = ComparePairedPanelsPayload.model_validate(
            {"evaluation_result_id": "a", "baseline_evaluation_result_id": "b"}
        )
        plain = ComparePairedPanelsPayload.model_validate(
            {
                "evaluation_result_id": "a",
                "baseline_evaluation_result_id": "b",
                "weighting": "unweighted",
            }
        )
        assert weighted.estimator == "f_micro_w"
        assert plain.estimator == "f_micro"

    def test_all_nine_panels_are_the_default(self) -> None:
        p = ComparePairedPanelsPayload.model_validate(
            {"evaluation_result_id": "a", "baseline_evaluation_result_id": "b"}
        )
        assert tuple(p.panels) == ALL_PANELS


class TestTheCampaignMetricAndTheOracleMean:
    """The guard. One panel, four statistics, three different winners.

    Each of the three ways the old ``scripts/bootstrap_fmax_ci.py`` differs from
    the campaign's estimator moves this panel on its own, and each moves it far
    enough to change the sign of the answer:

    * the per-protein ORACLE threshold: B by 0.0558
    * the mean over proteins rather than the RATIO OF SUMS: B by 0.0534
    * the UNWEIGHTED components rather than the accretion-weighted ones: B by 0.2059

    against the campaign's A by 0.1335. Every assertion below names the
    statistic it is protecting, and the failure message says which of the three
    the observed number actually is.
    """

    def test_it_reports_the_shared_threshold_micro_not_the_per_protein_oracle_mean(
        self, tmp_path: Path
    ) -> None:
        spec = _write_estimator_panel(tmp_path)
        wrong = {
            side: {
                "the mean of per-protein ORACLE Fmax": _per_protein_oracle_mean(*spec[side]),
                "the mean of per-protein F at the shared threshold": _shared_threshold_macro(
                    *spec[side]
                ),
                "the UNWEIGHTED micro F": float(
                    _micro_curve(*spec[f"{side}_unweighted"]).max()
                ),
            }
            for side in ("a", "b")
        }

        # The estimator itself, before any file or job is involved, so a
        # mutation inside the resampler fails HERE with a message that names the
        # statistic rather than tripping a downstream control on two floats.
        for side, expected in (("a", 0.5142857), ("b", 0.3808219)):
            arrays = boot.PanelArrays(tuple(spec["accessions"]), *spec[side])
            value = boot.select_operating_point(boot.panel_curve(arrays)).value
            assert value == pytest.approx(expected, abs=1e-6), (
                f"panel_curve on system {side.upper()} is not the accretion-weighted ratio of "
                f"pooled sums: expected {expected:.6f}, got {value!r}. "
                + _name_the_statistic(value, wrong[side])
            )

        panel = _run(tmp_path, min_population=_GUARD_N)["panels"]["NK:MFO"]

        assert panel["a"]["estimate"] == pytest.approx(0.5142857, abs=1e-5), (
            "system A's panel value is the accretion-weighted ratio of pooled sums at one "
            f"shared threshold, 2T/(P+G) = 0.514286. Got {panel['a']['estimate']!r}: "
            + _name_the_statistic(panel["a"]["estimate"], wrong["a"])
        )
        assert panel["b"]["estimate"] == pytest.approx(0.3808219, abs=1e-5), (
            "system B's panel value is the accretion-weighted ratio of pooled sums at one "
            f"shared threshold, 2T/(P+G) = 0.380822. Got {panel['b']['estimate']!r}: "
            + _name_the_statistic(panel["b"]["estimate"], wrong["b"])
        )
        assert panel["delta"] > 0, (
            "under the campaign's own metric system A leads by 0.1335. A negative delta here "
            "means one of the three known wrong statistics was computed: the per-protein "
            f"oracle mean gives B by {wrong['a']['the mean of per-protein ORACLE Fmax'] - wrong['b']['the mean of per-protein ORACLE Fmax']:.4f}, "
            "the mean over proteins at a shared threshold gives B by "
            f"{wrong['a']['the mean of per-protein F at the shared threshold'] - wrong['b']['the mean of per-protein F at the shared threshold']:.4f}, "
            "and the unweighted components give B by "
            f"{wrong['a']['the UNWEIGHTED micro F'] - wrong['b']['the UNWEIGHTED micro F']:.4f}."
        )
        assert panel["delta"] == pytest.approx(0.1334638, abs=1e-5)

    def test_the_two_variants_are_different_numbers_under_different_names(
        self, tmp_path: Path
    ) -> None:
        """``f_micro_w`` never labels a number computed without weights.

        The file carries both variants with different components, which is the
        only way this can be tested: written with one set of numbers under both
        names, a consumer reading the wrong columns is indistinguishable from
        one reading the right ones.
        """
        _write_estimator_panel(tmp_path)
        weighted = _run(tmp_path, min_population=_GUARD_N)
        unweighted = _run(tmp_path, min_population=_GUARD_N, weighting="unweighted")

        assert weighted["estimator"] == "f_micro_w"
        assert unweighted["estimator"] == "f_micro"
        assert weighted["panels"]["NK:MFO"]["delta"] == pytest.approx(0.1334638, abs=1e-5)
        assert unweighted["panels"]["NK:MFO"]["delta"] == pytest.approx(-0.2058823, abs=1e-5), (
            "the unweighted request returned the WEIGHTED number. The two variants score "
            "different term sets (toi against toi_ia), and on this panel they name opposite "
            "winners: weighted says A by 0.1335, unweighted says B by 0.2059. Reading the "
            "weighted columns under the unweighted name is the defect this operation's "
            "closed weighting vocabulary exists to prevent."
        )

    def test_reselecting_the_operating_point_widens_the_interval_and_flips_the_verdict(
        self, tmp_path: Path
    ) -> None:
        spec = _write_reselection_panel(tmp_path)
        frozen_lo, frozen_hi = _reference_bootstrap(spec, reselect=False)
        reselected_lo, reselected_hi = _reference_bootstrap(spec, reselect=True)
        panel = _run(
            tmp_path, min_population=_GUARD_N, interval_method="percentile"
        )["panels"]["NK:MFO"]

        width = panel["ci_high"] - panel["ci_low"]
        frozen_width = frozen_hi - frozen_lo
        assert frozen_lo > 0, "the fixture is wrong: the frozen bootstrap should resolve"
        assert width > 1.5 * frozen_width, (
            "the interval is no wider than a bootstrap that holds the threshold at the "
            f"full-sample optimum ({width:.4f} against {frozen_width:.4f}). The published "
            "quantity is a MAXIMUM over an estimated surface, so the threshold is itself "
            "estimated and must be re-selected inside every resample. Freezing it "
            "resamples a different functional and understates the variance, and it does "
            "not understate it equally for two systems whose score surfaces differ in "
            "flatness, so it is not conservative either."
        )
        assert panel["ci_low"] == pytest.approx(reselected_lo, abs=0.01)
        assert panel["ci_low"] < 0 < panel["ci_high"], (
            f"the interval [{panel['ci_low']:.4f}, {panel['ci_high']:.4f}] excludes zero. "
            f"The fixed-threshold bootstrap on this panel reports "
            f"[{frozen_lo:.4f}, {frozen_hi:.4f}] and calls the difference resolved; the "
            "correct procedure cannot, because system B's optimum threshold moves in "
            "about a quarter of the resamples."
        )
        assert panel["verdict"] == "not_resolved"
        assert panel["resolves"] is False

    def test_the_switch_fraction_is_reported_so_the_claim_is_measured(
        self, tmp_path: Path
    ) -> None:
        _write_reselection_panel(tmp_path)
        diag = _run(tmp_path, min_population=_GUARD_N)["panels"]["NK:MFO"]["diagnostics"]
        assert diag["tau_a_switched_fraction"] == 0.0
        # B's optimum moves when the resample holds at least 12 of the 20 draws
        # in group L, which is P(Bin(20, 0.5) >= 12) = 0.2517. The band sits
        # inside the neighbouring integer boundaries, P(>=13) = 0.1316 and
        # P(>=11) = 0.4119, so it pins the right count and not just a number.
        assert 0.15 < diag["tau_b_switched_fraction"] < 0.40

    def test_a_null_arrives_with_the_effect_it_could_have_found(self, tmp_path: Path) -> None:
        _write_reselection_panel(tmp_path)
        result = _run(tmp_path, min_population=_GUARD_N)
        panel = result["panels"]["NK:MFO"]
        assert panel["minimum_detectable_effect"] is not None
        assert panel["minimum_detectable_effect"] > 0
        assert panel["diagnostics"]["mde_basis"] == "recentred_percentile"
        assert panel["status"] == "null_unread"
        assert any(e[0] == "compare_paired_panels.null_unread" for e in result["_events"])


class TestANullIsReadAgainstTheEffectOfInterest:
    """Requirement 5, which the arithmetic used to make unreachable.

    The MDE is built from the same bootstrap distribution as the interval, at
    power 0.80, so it is about 1.43 times the half-width; an interval covering
    zero forces the observed difference to be at most the half-width. Comparing
    the two therefore never fires, and every null in the campaign was stamped
    "could not have resolved anything". A null is read against the effect the
    caller came to detect, which is a payload field.
    """

    @staticmethod
    def _panel(tmp_path: Path, side: str) -> None:
        """Two arms that genuinely tie: the same values on a permuted population.

        The pooled sums are identical, so the difference is exactly zero, while
        the two arms disagree protein by protein, so the resample distribution is
        not degenerate and the interval has a real width to report.
        """
        step = 1 if side == "a" else 3
        rows = [
            {
                "accession": f"P{i:03d}",
                "namespace": MFO,
                "tp": [4.0 + ((i * step) % 5) * 0.4, 1.0],
                "pred": [8.0, 2.0],
                "n_gt": 10.0,
            }
            for i in range(60)
        ]
        write_grid_parquet(
            tmp_path / side / "NK" / "per_protein_grid.parquet",
            rows,
            th_step=TH_STEP,
            setting="NK",
        )

    def test_without_a_declared_effect_the_null_is_reported_as_unread(
        self, tmp_path: Path
    ) -> None:
        self._panel(tmp_path, "a")
        self._panel(tmp_path, "b")
        result = _run(tmp_path, min_population=10)
        panel = result["panels"]["NK:MFO"]
        assert panel["excludes_zero"] is False
        assert panel["status"] == "null_unread"
        assert result["verdict"]["null_unread"] == 1
        assert result["verdict"]["underpowered"] == 0
        assert any(e[0] == "compare_paired_panels.null_unread" for e in result["_events"])

    def test_a_null_the_comparison_could_have_resolved_is_a_null_with_power(
        self, tmp_path: Path
    ) -> None:
        self._panel(tmp_path, "a")
        self._panel(tmp_path, "b")
        result = _run(tmp_path, min_population=10, effect_of_interest=0.5)
        panel = result["panels"]["NK:MFO"]
        mde = panel["minimum_detectable_effect"]
        assert mde is not None and mde <= 0.5
        assert panel["status"] == "null_with_power", (
            "an interval covering zero from a comparison that could have resolved "
            f"{mde:.4f}, against a declared effect of interest of 0.5, is evidence of "
            "sameness down to that size. Reporting it as underpowered is the misreading "
            "requirement 5 exists to prevent, and it is what comparing the MDE against the "
            "observed difference always produced."
        )
        assert result["verdict"]["null_with_power"] == 1

    def test_a_null_below_the_effect_of_interest_is_still_underpowered(
        self, tmp_path: Path
    ) -> None:
        self._panel(tmp_path, "a")
        self._panel(tmp_path, "b")
        result = _run(tmp_path, min_population=10, effect_of_interest=0.0005)
        panel = result["panels"]["NK:MFO"]
        assert panel["minimum_detectable_effect"] > 0.0005
        assert panel["status"] == "underpowered"
        assert result["verdict"]["underpowered"] == 1
        assert any(e[0] == "compare_paired_panels.underpowered" for e in result["_events"])


class TestTheLegacyArtefactIsRefused:
    def test_the_single_threshold_table_is_named_along_with_its_producer(
        self, tmp_path: Path
    ) -> None:
        write_legacy_parquet(tmp_path / "a" / "NK" / "per_protein.parquet")
        write_legacy_parquet(tmp_path / "b" / "NK" / "per_protein.parquet")
        with pytest.raises(ThresholdGridUnavailableError) as exc:
            _run(tmp_path)
        message = str(exc.value)
        assert "per_protein.parquet" in message
        assert "grid_rows_from_sink" in message
        assert "re-select the operating point inside every resample" in message
        assert "0.31" in message

    def test_a_file_without_the_schema_version_is_refused(self, tmp_path: Path) -> None:
        for side in ("a", "b"):
            write_grid_parquet(
                tmp_path / side / "NK" / "per_protein_grid.parquet",
                [{"accession": "P1", "namespace": MFO, "tp": [1.0, 0.0], "pred": [2.0, 0.0],
                  "n_gt": 2.0}],
                th_step=TH_STEP,
                setting="NK",
                version=None,
            )
        with pytest.raises(ThresholdGridUnavailableError, match="without the contract"):
            _run(tmp_path)

    def test_asking_for_weights_that_do_not_exist_is_refused_not_downgraded(
        self, tmp_path: Path
    ) -> None:
        for side in ("a", "b"):
            write_grid_parquet(
                tmp_path / side / "NK" / "per_protein_grid.parquet",
                [{"accession": "P1", "namespace": MFO, "tp": [1.0, 0.0], "pred": [2.0, 0.0],
                  "n_gt": 2.0}],
                th_step=TH_STEP,
                setting="NK",
                variants=("unweighted",),
            )
        with pytest.raises(ThresholdGridUnavailableError) as exc:
            _run(tmp_path)
        assert "does not compute an unweighted number and label it f_micro_w" in str(exc.value)

    def test_a_grid_file_carrying_a_per_row_tau_is_the_old_table_renamed(
        self, tmp_path: Path
    ) -> None:
        for side in ("a", "b"):
            write_grid_parquet(
                tmp_path / side / "NK" / "per_protein_grid.parquet",
                [{"accession": "P1", "namespace": MFO, "tp": [1.0, 0.0], "pred": [2.0, 0.0],
                  "n_gt": 2.0}],
                th_step=TH_STEP,
                setting="NK",
                extra_columns={"tau": [0.31]},
            )
        with pytest.raises(ThresholdGridUnavailableError, match="single-threshold"):
            _run(tmp_path)


class TestStructuralInvariants:
    @staticmethod
    def _write(tmp_path: Path, rows: list[dict[str, Any]], **kwargs: Any) -> None:
        for side in ("a", "b"):
            write_grid_parquet(
                tmp_path / side / "NK" / "per_protein_grid.parquet",
                rows,
                th_step=TH_STEP,
                setting="NK",
                **kwargs,
            )

    def test_a_grid_written_in_the_other_order_is_caught(self, tmp_path: Path) -> None:
        """tp is a reverse-cumulative sum, so it can only fall with the threshold."""
        self._write(
            tmp_path,
            [{"accession": "P1", "namespace": MFO, "tp": [1.0, 3.0], "pred": [4.0, 4.0],
              "n_gt": 4.0}],
        )
        with pytest.raises(GridInvariantError, match="other order"):
            _run(tmp_path)

    def test_true_positives_above_predicted_mass_is_caught(self, tmp_path: Path) -> None:
        self._write(
            tmp_path,
            [{"accession": "P1", "namespace": MFO, "tp": [5.0, 0.0], "pred": [2.0, 0.0],
              "n_gt": 9.0}],
        )
        with pytest.raises(GridInvariantError, match="exceeds predicted"):
            _run(tmp_path)

    def test_true_positives_above_ground_truth_mass_is_caught(self, tmp_path: Path) -> None:
        self._write(
            tmp_path,
            [{"accession": "P1", "namespace": MFO, "tp": [5.0, 0.0], "pred": [9.0, 0.0],
              "n_gt": 2.0}],
        )
        with pytest.raises(GridInvariantError, match="exceeds ground-truth"):
            _run(tmp_path)

    def test_a_repeated_protein_is_caught_before_it_doubles_its_weight(
        self, tmp_path: Path
    ) -> None:
        row = {"accession": "P1", "namespace": MFO, "tp": [1.0, 0.0], "pred": [2.0, 0.0],
               "n_gt": 2.0}
        self._write(tmp_path, [row, dict(row)])
        with pytest.raises(GridInvariantError, match="not unique"):
            _run(tmp_path)

    def test_a_grid_that_disagrees_with_its_own_step_is_caught(self, tmp_path: Path) -> None:
        self._write(
            tmp_path,
            [{"accession": "P1", "namespace": MFO, "tp": [1.0, 0.0], "pred": [2.0, 0.0],
              "n_gt": 2.0}],
            tau_grid=[0.1, 0.9],
        )
        with pytest.raises(GridInvariantError, match="disagree"):
            _run(tmp_path)


class TestDegenerateCases:
    """Each of these is required behaviour, not a suggestion."""

    @staticmethod
    def _panel(tmp_path: Path, side: str, rows: list[dict[str, Any]]) -> None:
        write_grid_parquet(
            tmp_path / side / "NK" / "per_protein_grid.parquet",
            rows,
            th_step=TH_STEP,
            setting="NK",
        )

    @staticmethod
    def _rows(accs: list[str], tp: list[float], pred: list[float], n_gt: float = 10.0) -> list:
        return [
            {"accession": a, "namespace": MFO, "tp": list(tp), "pred": list(pred), "n_gt": n_gt}
            for a in accs
        ]

    def test_two_disjoint_populations_are_refused_and_never_a_quiet_empty(
        self, tmp_path: Path
    ) -> None:
        """Zero overlap is the LOUDEST population state, not the quietest.

        It used to be the one case that did not refuse: the jaccard gate was
        guarded on there being a shared protein, so a completely disjoint pair
        skipped it and landed in an empty panel beside a genuinely missing file.
        Two systems evaluated on one frame do not have disjoint populations, so
        it is an identifier-space bug and the strongest evidence of one.
        """
        self._panel(tmp_path, "a", self._rows(["P1", "P2"], [4.0, 1.0], [8.0, 2.0]))
        self._panel(tmp_path, "b", self._rows(["Q1", "Q2"], [4.0, 1.0], [8.0, 2.0]))
        result = _run(tmp_path)
        panel = result["panels"]["NK:MFO"]
        assert panel["status"] == "refused"
        assert panel["reason"] == "population_disjoint"
        assert panel["delta"] is None
        assert panel["ci_low"] is None
        assert panel["minimum_detectable_effect"] is None
        assert panel["reportable"] is False
        assert result["refused"] == ["NK:MFO"]
        assert result["verdict"]["refused"] == 1
        assert any(
            e[0] == "compare_paired_panels.panel_refused" and e[3] == "error"
            for e in result["_events"]
        )

    def test_one_protein_is_below_the_bootstrap_floor(self, tmp_path: Path) -> None:
        self._panel(tmp_path, "a", self._rows(["P1"], [4.0, 1.0], [8.0, 2.0]))
        self._panel(tmp_path, "b", self._rows(["P1"], [3.0, 1.0], [8.0, 2.0]))
        result = _run(tmp_path)
        panel = result["panels"]["NK:MFO"]
        assert panel["status"] == "unresolvable"
        # It was computed and it could not be resolved, which is not the same
        # fact as a panel whose artefact was never read.
        assert panel["verdict"] == "not_resolved"
        assert panel["reason"] == "population_below_bootstrap_floor"
        assert panel["delta"] is not None
        assert panel["ci_low"] is None
        assert result["verdict"]["not_computed"] == 0
        assert result["verdict"]["underpowered"] == 1

    def test_a_degenerate_bootstrap_reports_a_null_mde_and_never_a_win(
        self, tmp_path: Path
    ) -> None:
        """Identical proteins make the ratio of sums invariant to the resample.

        The arithmetic would return a zero MDE beside a zero-width interval that
        excludes zero, which reads as an infinitely powerful comparison. That
        output is forbidden.
        """
        self._panel(tmp_path, "a", self._rows(["P1", "P2", "P3", "P4"], [4.0, 1.0], [8.0, 2.0]))
        self._panel(tmp_path, "b", self._rows(["P1", "P2", "P3", "P4"], [2.0, 1.0], [8.0, 2.0]))
        panel = _run(tmp_path)["panels"]["NK:MFO"]
        assert panel["interval_method"] == "degenerate"
        assert panel["ci_low"] == panel["ci_high"] == pytest.approx(panel["delta"])
        assert panel["minimum_detectable_effect"] is None
        assert panel["diagnostics"]["mde_reason"] == "bootstrap_distribution_degenerate"
        assert panel["status"] == "unresolvable"
        assert panel["verdict"] == "not_resolved"
        assert panel["delta"] > 0

    def test_a_silent_arm_scores_zero_and_is_named_rather_than_dropped(
        self, tmp_path: Path
    ) -> None:
        accs = [f"P{i}" for i in range(6)]
        self._panel(tmp_path, "a", self._rows(accs, [4.0, 1.0], [8.0, 2.0]))
        self._panel(tmp_path, "b", self._rows(accs, [0.0, 0.0], [0.0, 0.0]))
        result = _run(tmp_path)
        panel = result["panels"]["NK:MFO"]
        assert panel["arm_silent"] == "B"
        assert panel["b"]["estimate"] == 0.0
        assert panel["b"]["tau"] == pytest.approx(TAUS[0])
        assert panel["delta"] > 0
        assert any(e[0] == "compare_paired_panels.arm_silent" for e in result["_events"])

    def test_a_tie_at_the_optimum_takes_the_smallest_tau_and_says_so(
        self, tmp_path: Path
    ) -> None:
        # 2T/(P+G) is identical at both thresholds while the masses themselves
        # fall: (4, 8) gives 24/54 and (3, 3.5) gives 18/40.5, both 0.4444 over
        # three proteins with G = 10 each. The masses have to move, because a
        # curve that is constant across the whole grid is the single-threshold
        # table broadcast across the columns and is refused as such.
        self._panel(tmp_path, "a", self._rows(["P1", "P2", "P3"], [4.0, 3.0], [8.0, 3.5]))
        self._panel(tmp_path, "b", self._rows(["P1", "P2", "P3"], [3.0, 1.0], [8.0, 2.0]))
        panel = _run(tmp_path)["panels"]["NK:MFO"]
        assert panel["a"]["tau_star_tied"] is True
        assert panel["a"]["n_tau_at_max"] == 2
        assert panel["a"]["tau"] == pytest.approx(TAUS[0])

    def test_a_panel_below_the_floor_is_computed_flagged_and_not_resolved(
        self, tmp_path: Path
    ) -> None:
        # Heterogeneous on purpose: identical proteins make the ratio of sums
        # invariant to the resample, which is a different degenerate case.
        rows_a = [
            {"accession": f"P{i}", "namespace": MFO, "tp": [6.0 - i * 0.5, 1.0],
             "pred": [8.0, 2.0], "n_gt": 10.0}
            for i in range(6)
        ]
        rows_b = [
            {"accession": f"P{i}", "namespace": MFO, "tp": [1.0 + i * 0.3, 0.0],
             "pred": [8.0, 0.0], "n_gt": 10.0}
            for i in range(6)
        ]
        self._panel(tmp_path, "a", rows_a)
        self._panel(tmp_path, "b", rows_b)
        result = _run(tmp_path, min_population=30)
        panel = result["panels"]["NK:MFO"]
        assert panel["reportable"] is False
        assert panel["delta"] is not None and panel["ci_low"] is not None
        assert panel["status"] == "underpowered"
        assert panel["verdict"] == "not_resolved"
        assert result["withheld"] == ["NK:MFO"]
        assert any(e[0] == "compare_paired_panels.withheld" for e in result["_events"])

    def test_a_panel_whose_artefact_is_absent_is_empty_and_the_job_continues(
        self, tmp_path: Path
    ) -> None:
        accs = [f"P{i}" for i in range(6)]
        self._panel(tmp_path, "a", self._rows(accs, [4.0, 1.0], [8.0, 2.0]))
        self._panel(tmp_path, "b", self._rows(accs, [3.0, 1.0], [8.0, 2.0]))
        result = _run(tmp_path, panels=["NK:MFO", "NK:BPO"])
        assert result["panels"]["NK:BPO"]["status"] == "empty"
        assert result["panels"]["NK:BPO"]["reason"] == "artefact_absent_for_panel"
        assert result["panels"]["NK:MFO"]["status"] in {"ok", "underpowered", "unresolvable"}


class TestPopulationAndProvenance:
    @staticmethod
    def _write(tmp_path: Path, side: str, accs: list[str], tp: list[float]) -> None:
        write_grid_parquet(
            tmp_path / side / "NK" / "per_protein_grid.parquet",
            [
                {"accession": a, "namespace": MFO, "tp": tp, "pred": [8.0, 2.0], "n_gt": 10.0}
                for a in accs
            ],
            th_step=TH_STEP,
            setting="NK",
        )

    def test_the_shift_between_the_paired_and_the_own_population_is_visible(
        self, tmp_path: Path
    ) -> None:
        self._write(tmp_path, "a", [f"P{i:03d}" for i in range(40)], [4.0, 1.0])
        self._write(tmp_path, "b", [f"P{i:03d}" for i in range(1, 41)], [3.0, 1.0])
        panel = _run(tmp_path)["panels"]["NK:MFO"]
        assert panel["n_paired"] == 39
        assert panel["n_only_a"] == 1
        assert panel["n_only_b"] == 1
        assert panel["a"]["n_own"] == 40
        assert "estimate_own_population" in panel["a"]
        assert "population_shift" in panel["b"]

    def test_require_identical_refuses_the_panel_whose_population_moved(
        self, tmp_path: Path
    ) -> None:
        self._write(tmp_path, "a", [f"P{i:03d}" for i in range(40)], [4.0, 1.0])
        self._write(tmp_path, "b", [f"P{i:03d}" for i in range(1, 41)], [3.0, 1.0])
        panel = _run(tmp_path, population_rule="require_identical")["panels"]["NK:MFO"]
        assert panel["status"] == "refused"
        assert panel["reason"] == "population_not_identical"
        assert panel["delta"] is None

    def test_a_badly_overlapping_panel_is_refused_and_the_others_are_not_lost(
        self, tmp_path: Path
    ) -> None:
        """A population shift belongs to ONE panel.

        Raising out of the operation discarded the eight panels that were
        perfectly paired, and the only remedy was to loosen min_jaccard for all
        nine, which is worse than degrading the one that has the problem.
        """
        for setting, accs_a, accs_b in (
            ("NK", [f"P{i}" for i in range(10)], [f"P{i}" for i in range(10)]),
            ("LK", [f"P{i}" for i in range(10)], [f"P{i}" for i in range(5, 15)]),
        ):
            for side, accs, tp in (("a", accs_a, [4.0, 1.0]), ("b", accs_b, [3.0, 1.0])):
                write_grid_parquet(
                    tmp_path / side / setting / "per_protein_grid.parquet",
                    [
                        {"accession": a, "namespace": MFO, "tp": tp, "pred": [8.0, 2.0],
                         "n_gt": 10.0}
                        for a in accs
                    ],
                    th_step=TH_STEP,
                    setting=setting,
                )
        result = _run(tmp_path, panels=["NK:MFO", "LK:MFO"])
        assert result["panels"]["LK:MFO"]["status"] == "refused"
        assert result["panels"]["LK:MFO"]["reason"] == "population_overlap_below_floor"
        assert "population difference" in result["panels"]["LK:MFO"]["message"]
        assert result["panels"]["NK:MFO"]["delta"] is not None
        assert result["refused"] == ["LK:MFO"]

    def test_a_frame_mismatch_is_refused_by_default(self, tmp_path: Path) -> None:
        self._write(tmp_path, "a", ["P1", "P2", "P3"], [4.0, 1.0])
        self._write(tmp_path, "b", ["P1", "P2", "P3"], [3.0, 1.0])
        session = _session(b={"leakage_role": "select"})
        with pytest.raises(PanelComparabilityError, match="leakage_role"):
            _run(tmp_path, session=session)

    def test_two_unstamped_rows_are_refused_rather_than_declared_to_agree(
        self, tmp_path: Path
    ) -> None:
        """An absent marker is not a matching one, and this is the common case.

        The three frame columns are nullable with no server default, so a pair of
        rows that declare nothing is what production mostly holds. Comparing them
        with equality alone makes both sides absent and therefore equal, and the
        operation would report that the frames agree. The campaign this operation
        serves was invalidated by precisely that: numbers compared across frames
        because nothing recorded which frame each was measured in. So the gate
        checks presence first and equality second, and the refusal is not waivable.
        """
        self._write(tmp_path, "a", ["P1", "P2", "P3"], [4.0, 1.0])
        self._write(tmp_path, "b", ["P1", "P2", "P3"], [3.0, 1.0])
        blank = {"frame": None, "temporal_window": None, "leakage_role": None}
        session = _session(a=blank, b=blank)
        with pytest.raises(PanelComparabilityError, match="do not both declare"):
            _run(tmp_path, session=session)

    def test_an_absence_is_not_waived_by_allow_frame_mismatch(self, tmp_path: Path) -> None:
        """The flag waives a DISAGREEMENT between two declarations, not a silence."""
        self._write(tmp_path, "a", ["P1", "P2", "P3"], [4.0, 1.0])
        self._write(tmp_path, "b", ["P1", "P2", "P3"], [3.0, 1.0])
        blank = {"frame": None, "temporal_window": None, "leakage_role": None}
        with pytest.raises(PanelComparabilityError, match="not waivable"):
            _run(tmp_path, session=_session(a=blank, b=blank), allow_frame_mismatch=True)

    def test_a_permitted_frame_mismatch_is_recorded_in_the_result(self, tmp_path: Path) -> None:
        self._write(tmp_path, "a", ["P1", "P2", "P3"], [4.0, 1.0])
        self._write(tmp_path, "b", ["P1", "P2", "P3"], [3.0, 1.0])
        result = _run(
            tmp_path, session=_session(b={"leakage_role": "select"}), allow_frame_mismatch=True
        )
        assert result["frame_mismatch"] == ["leakage_role"]

    def test_a_missing_evaluation_result_row_is_refused(self, tmp_path: Path) -> None:
        self._write(tmp_path, "a", ["P1"], [4.0, 1.0])
        self._write(tmp_path, "b", ["P1"], [3.0, 1.0])
        with pytest.raises(ValueError, match="not in evaluation_result"):
            _run(tmp_path, session=_StubSession({}))

    def test_different_ground_truth_mass_for_the_same_protein_is_refused(
        self, tmp_path: Path
    ) -> None:
        self._write(tmp_path, "a", ["P1", "P2", "P3"], [4.0, 1.0])
        write_grid_parquet(
            tmp_path / "b" / "NK" / "per_protein_grid.parquet",
            [
                {"accession": a, "namespace": MFO, "tp": [3.0, 1.0], "pred": [8.0, 2.0],
                 "n_gt": 12.0}
                for a in ["P1", "P2", "P3"]
            ],
            th_step=TH_STEP,
            setting="NK",
        )
        with pytest.raises(PanelComparabilityError, match="different ground-truth mass"):
            _run(tmp_path)

    def test_a_published_cell_that_does_not_reproduce_is_refused(self, tmp_path: Path) -> None:
        """The guard whose absence let a wrong-tau slice sit unnoticed."""
        self._write(tmp_path, "a", ["P1", "P2", "P3"], [4.0, 1.0])
        self._write(tmp_path, "b", ["P1", "P2", "P3"], [3.0, 1.0])
        session = _session(a={"results": {"NK": {"MFO": {"f_micro_w": 0.9999}}}})
        with pytest.raises(PanelComparabilityError, match="the evaluation result stores"):
            _run(tmp_path, session=session)

    def test_a_published_cell_that_does_reproduce_is_recorded_as_checked(
        self, tmp_path: Path
    ) -> None:
        self._write(tmp_path, "a", ["P1", "P2", "P3"], [4.0, 1.0])
        self._write(tmp_path, "b", ["P1", "P2", "P3"], [3.0, 1.0])
        expected = round(2 * 12.0 / (24.0 + 30.0), 4)
        session = _session(a={"results": {"NK": {"MFO": {"f_micro_w": expected}}}})
        panel = _run(tmp_path, session=session)["panels"]["NK:MFO"]
        assert panel["a"]["estimator_parity_checked"] is True

    def test_the_result_carries_what_makes_the_number_traceable(self, tmp_path: Path) -> None:
        self._write(tmp_path, "a", [f"P{i}" for i in range(6)], [4.0, 1.0])
        self._write(tmp_path, "b", [f"P{i}" for i in range(6)], [3.0, 1.0])
        result = _run(tmp_path)
        assert result["estimator"] == "f_micro_w"
        assert result["operating_point"] == "reselected_per_resample"
        assert result["sampling_unit"] == "protein"
        assert result["tau_grid"]["n_thresholds"] == 2
        assert result["systems"]["a"]["evaluation_result_id"] == _A_ID
        assert result["systems"]["b"]["ontology_snapshot_id"] is not None
        assert set(result["verdict"]) == {
            "resolved",
            "null_with_power",
            "null_unread",
            "underpowered",
            "refused",
            "not_computed",
        }
        assert result["interval_method_requested"] == "bca"
        assert result["effect_of_interest"] is None
        assert result["panels"]["NK:MFO"]["a"]["coverage_at_tau"] == 1.0


class TestTheResamplerItself:
    """Unit tests on the arithmetic, where a ten-protein panel can be checked by hand."""

    def test_the_closed_form_matches_precision_recall_composition(self) -> None:
        tp, pred, gt = 37.0, 91.0, 64.0
        precision, recall = tp / pred, tp / gt
        assert boot.micro_curve(np.array([tp]), np.array([pred]), gt)[0] == pytest.approx(
            2 * precision * recall / (precision + recall), abs=1e-15
        )

    def test_an_empty_denominator_scores_zero_rather_than_raising(self) -> None:
        assert boot.micro_curve(np.zeros(1), np.zeros(1), 0.0)[0] == 0.0

    def test_the_tie_rule_takes_the_smallest_tau(self) -> None:
        point = boot.select_operating_point(np.array([0.2, 0.5, 0.5, 0.1]))
        assert (point.tau_index, point.n_tau_at_max, point.tied) == (1, 2, True)

    def test_the_pairing_is_one_draw_used_twice(self) -> None:
        """Two arms differing by a constant on every protein have a constant delta.

        That is only true if the same resample indices are applied to both arms.
        Two independent draws would leave visible variance here, which is the
        whole reason the paired form is far tighter than two marginal ones.
        """
        n, taus = 8, 3
        rng = np.random.default_rng(4)
        tp = np.sort(rng.uniform(0, 5, size=(n, taus)), axis=1)[:, ::-1]
        pred = tp + 3.0
        gt = np.full(n, 6.0)
        a = boot.PanelArrays(tuple(f"P{i}" for i in range(n)), tp, pred, gt)
        b = boot.PanelArrays(a.accessions, tp, pred, gt)
        draws = boot.paired_bootstrap(a, b, n_resamples=1000, seed=0)
        assert float(np.abs(draws.deltas).max()) == pytest.approx(0.0, abs=1e-12)

    def test_the_jackknife_is_the_exact_leave_one_out(self) -> None:
        n, taus = 7, 3
        rng = np.random.default_rng(5)
        tp = np.sort(rng.uniform(0, 4, size=(n, taus)), axis=1)[:, ::-1]
        pred = tp + 2.0
        gt = np.full(n, 5.0)
        a = boot.PanelArrays(tuple(f"P{i}" for i in range(n)), tp, pred, gt)
        b = boot.PanelArrays(a.accessions, tp * 0.5, pred, gt)
        fast = boot.jackknife_deltas(a, b)
        for i in range(n):
            keep = np.array([j for j in range(n) if j != i])
            slow = boot.panel_curve(a.take(keep)).max() - boot.panel_curve(b.take(keep)).max()
            assert fast[i] == pytest.approx(slow, abs=1e-12)

    def test_the_mde_reproduces_normal_theory_where_normal_theory_applies(self) -> None:
        """It departs from ``2.80 * sigma`` only where the distribution departs from normal."""
        rng = np.random.default_rng(0)
        se = 0.05
        deltas = rng.normal(0.0, se, size=40000)
        interval = boot.Interval(
            float(np.quantile(deltas, 0.025)),
            float(np.quantile(deltas, 0.975)),
            boot.PERCENTILE,
            None,
            None,
            None,
        )
        mde = boot.minimum_detectable_effect(deltas, 0.0, interval, power=0.80)
        assert mde.mde == pytest.approx(1.9600 * se + 0.8416 * se, abs=0.002)
        assert mde.positive == pytest.approx(mde.negative, abs=0.005)

    def test_a_degenerate_distribution_yields_no_interval_and_no_mde(self) -> None:
        deltas = np.full(500, 0.25)
        interval = boot.build_interval(deltas, 0.25, np.full(5, 0.25), alpha=0.05)
        assert interval.method == boot.DEGENERATE
        assert interval.fallback_reason == "bootstrap_distribution_degenerate"
        mde = boot.minimum_detectable_effect(deltas, 0.25, interval, power=0.8)
        assert mde.mde is None and mde.reason == "bootstrap_distribution_degenerate"

    def test_percentile_can_be_forced_and_says_it_was(self) -> None:
        rng = np.random.default_rng(1)
        deltas = rng.normal(0.1, 0.02, size=2000)
        jack = rng.normal(0.1, 0.02, size=50)
        interval = boot.build_interval(
            deltas, 0.1, jack, alpha=0.05, force_percentile=True
        )
        assert interval.method == boot.PERCENTILE
        # Not a fallback: the caller asked for it. What the caller asked for is
        # reported once, in the result, as interval_method_requested, so that
        # filtering on a fallback still means the acceleration failed.
        assert interval.fallback_reason is None

    def test_a_degenerate_jackknife_falls_back_by_name(self) -> None:
        rng = np.random.default_rng(2)
        deltas = rng.normal(0.1, 0.02, size=2000)
        interval = boot.build_interval(deltas, 0.1, np.full(40, 0.1), alpha=0.05)
        assert interval.method == boot.PERCENTILE
        assert interval.fallback_reason == "jackknife_degenerate"
        assert interval.acceleration is None

    def test_bca_is_used_and_recorded_when_the_acceleration_exists(self) -> None:
        rng = np.random.default_rng(3)
        deltas = rng.normal(0.1, 0.02, size=4000)
        jack = rng.normal(0.1, 0.02, size=200)
        interval = boot.build_interval(deltas, 0.1, jack, alpha=0.05)
        assert interval.method == boot.BCA
        assert interval.fallback_reason is None
        assert interval.z0 is not None and interval.acceleration is not None


class TestTheOtherWaysAProducerCanGetItWrong:
    def test_a_scalar_mass_column_is_refused_as_a_widened_row(self, tmp_path: Path) -> None:
        """A producer that widened the rows instead of the columns."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        from tests.helpers.paired_panels import DEFAULT_METADATA

        meta = {
            **DEFAULT_METADATA,
            "version": "1",
            "tau_grid": "[0.4, 0.8]",
            "th_step": "0.4",
            "variants": '["weighted"]',
            "setting": "NK",
        }
        table = pa.table(
            {
                "protein_accession": pa.array(["P1"]),
                "namespace": pa.array([MFO]),
                "tp_w": pa.array([1.0], pa.float64()),
                "pred_w": pa.array([2.0], pa.float64()),
                "n_gt_w": pa.array([2.0], pa.float64()),
                "n_gt": pa.array([2.0], pa.float64()),
            }
        )
        table = table.replace_schema_metadata(
            {f"protea.per_protein_grid.{k}".encode(): str(v).encode() for k, v in meta.items()}
        )
        for side in ("a", "b"):
            target = tmp_path / side / "NK" / "per_protein_grid.parquet"
            target.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(table, target)
        with pytest.raises(ThresholdGridUnavailableError, match="widened the rows"):
            _run(tmp_path)


    def test_a_ragged_list_column_is_refused_rather_than_reshaped(self, tmp_path: Path) -> None:
        """The failure the whole contract exists to prevent, one column wide.

        ``pa.list_`` is pyarrow's default and fixes no width, so rows of 3, 5, 4
        and 4 values on a declared 4-tau grid reach the consumer. They divide
        exactly by the row count, so a reshape on the total accepts them and
        every protein after the short row carries a neighbouring threshold's
        score.
        """
        rows = [
            {"accession": "P1", "namespace": MFO, "tp": [8.0, 7.0, 6.0], "pred": [9.0, 8.0, 7.0],
             "n_gt": 10.0},
            {"accession": "P2", "namespace": MFO, "tp": [5.0, 4.0, 4.0, 3.0, 2.0],
             "pred": [6.0, 5.0, 5.0, 4.0, 3.0], "n_gt": 10.0},
            {"accession": "P3", "namespace": MFO, "tp": [9.0, 8.0, 7.0, 6.0],
             "pred": [9.0, 8.0, 7.0, 6.0], "n_gt": 10.0},
            {"accession": "P4", "namespace": MFO, "tp": [4.0, 3.0, 2.0, 1.0],
             "pred": [5.0, 4.0, 3.0, 2.0], "n_gt": 10.0},
        ]
        for side in ("a", "b"):
            write_grid_parquet(
                tmp_path / side / "NK" / "per_protein_grid.parquet",
                rows,
                th_step=0.2,
                setting="NK",
                list_type="variable",
            )
        with pytest.raises(GridInvariantError, match="rows of length"):
            _run(tmp_path)

    def test_a_row_without_ground_truth_is_refused(self, tmp_path: Path) -> None:
        """cafaeval scores only ground-truth-bearing proteins.

        A row without ground truth adds its predicted mass to P and nothing to
        G, so the panel moves by more than the effects this campaign resolves
        while every structural gate stays green.
        """
        rows = [
            {"accession": f"P{i}", "namespace": MFO, "tp": [4.0, 1.0], "pred": [8.0, 2.0],
             "n_gt": 10.0}
            for i in range(6)
        ]
        rows.append(
            {"accession": "P99", "namespace": MFO, "tp": [0.0, 0.0], "pred": [14.0, 3.0],
             "n_gt": 0.0}
        )
        for side in ("a", "b"):
            write_grid_parquet(
                tmp_path / side / "NK" / "per_protein_grid.parquet",
                rows,
                th_step=TH_STEP,
                setting="NK",
            )
        with pytest.raises(GridInvariantError, match="no ground truth"):
            _run(tmp_path)

    def test_a_float32_grid_at_a_realistic_bpo_mass_is_accepted(self, tmp_path: Path) -> None:
        """The tolerance is relative, because the contract permits float32.

        A producer computing in float64 and storing the grid as float32 gets a
        true-positive mass up to half a float32 ulp above its own float64
        ground truth. At a weighted BPO closure of 300.7 that excess is 1.2e-5,
        which an absolute 1e-5 tolerance reads as a producer bug.
        """
        rows = [
            {"accession": f"P{i}", "namespace": MFO, "tp": [300.7, 120.0],
             "pred": [300.7, 130.0], "n_gt": 300.7}
            for i in range(6)
        ]
        for side in ("a", "b"):
            write_grid_parquet(
                tmp_path / side / "NK" / "per_protein_grid.parquet",
                rows if side == "a" else [{**r, "tp": [280.0, 110.0]} for r in rows],
                th_step=TH_STEP,
                setting="NK",
            )
        panel = _run(tmp_path)["panels"]["NK:MFO"]
        assert panel["delta"] is not None
        assert panel["a"]["estimate"] > panel["b"]["estimate"]

    def test_a_single_threshold_slice_broadcast_across_the_grid_is_refused(
        self, tmp_path: Path
    ) -> None:
        """The legacy artefact renamed, which the filename check cannot see.

        Widening the old table by repeating its one tau column across the grid
        passes the filename check, the width check, monotonicity (a constant row
        never rises) and uniqueness, and then reports the fixed-threshold
        interval this operation exists to refuse, as a resolved win with no
        warning.
        """
        rows = [
            {"accession": f"P{i:03d}", "namespace": MFO, "tp": [4.0, 4.0], "pred": [8.0, 8.0],
             "n_gt": 10.0}
            for i in range(40)
        ]
        for side in ("a", "b"):
            write_grid_parquet(
                tmp_path / side / "NK" / "per_protein_grid.parquet",
                rows if side == "a" else [{**r, "tp": [3.0, 3.0]} for r in rows],
                th_step=TH_STEP,
                setting="NK",
            )
        with pytest.raises(ThresholdGridUnavailableError, match="written out many times"):
            _run(tmp_path)

    def test_a_variant_the_footer_declares_but_the_file_lacks_is_named(
        self, tmp_path: Path
    ) -> None:
        """The refusal has to run BEFORE the read, or pyarrow raises first.

        Read first, the operator gets an arrow field-ref dump naming
        ``__fragment_index`` instead of the contract's message, and the gate
        written to catch a lying footer never executes.
        """
        import pyarrow as pa
        import pyarrow.parquet as pq

        from tests.helpers.paired_panels import DEFAULT_METADATA

        meta = {
            **DEFAULT_METADATA,
            "version": "1",
            "tau_grid": "[0.4, 0.8]",
            "th_step": "0.4",
            "variants": '["weighted", "unweighted"]',
            "setting": "NK",
        }
        flat = pa.array([4.0, 1.0], pa.float32())
        table = pa.table(
            {
                "protein_accession": pa.array(["P1"]),
                "namespace": pa.array([MFO]),
                "tp_w": pa.FixedSizeListArray.from_arrays(flat, 2),
                "pred_w": pa.FixedSizeListArray.from_arrays(pa.array([8.0, 2.0], pa.float32()), 2),
                "n_gt_w": pa.array([10.0], pa.float64()),
                "n_gt": pa.array([4.0], pa.float64()),
            }
        )
        table = table.replace_schema_metadata(
            {f"protea.per_protein_grid.{k}".encode(): str(v).encode() for k, v in meta.items()}
        )
        for side in ("a", "b"):
            target = tmp_path / side / "NK" / "per_protein_grid.parquet"
            target.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(table, target)
        with pytest.raises(GridInvariantError, match="does not carry"):
            _run(tmp_path, weighting="unweighted")

    def test_a_comparability_key_absent_from_both_files_is_refused(
        self, tmp_path: Path
    ) -> None:
        """The gate must not be satisfiable by absence.

        Two absent values compare equal, so a producer that never learned to
        stamp its information-accretion set would sail through the check that
        refuses a producer careful enough to declare a different one.
        """
        rows = [
            {"accession": f"P{i}", "namespace": MFO, "tp": [4.0, 1.0], "pred": [8.0, 2.0],
             "n_gt": 10.0}
            for i in range(6)
        ]
        for side in ("a", "b"):
            write_grid_parquet(
                tmp_path / side / "NK" / "per_protein_grid.parquet",
                rows,
                th_step=TH_STEP,
                setting="NK",
                metadata={"information_accretion_set_id": ""},
            )
        with pytest.raises(ThresholdGridUnavailableError, match="does not stamp"):
            _run(tmp_path)

    def test_an_ontology_mismatch_is_refused_even_when_a_frame_mismatch_is_allowed(
        self, tmp_path: Path
    ) -> None:
        """One flag cannot waive both a label and the ontology snapshot.

        ``allow_frame_mismatch`` exists for a differing temporal-window label on
        otherwise identical runs. Letting it also unlock the ontology snapshot
        and the information-accretion set means two systems scored against
        different references are published as a method difference.
        """
        rows = [
            {"accession": f"P{i}", "namespace": MFO, "tp": [4.0, 1.0], "pred": [8.0, 2.0],
             "n_gt": 10.0}
            for i in range(6)
        ]
        for side, snapshot in (("a", "11111111-1111-1111-1111-111111111111"), ("b", "d" * 32)):
            write_grid_parquet(
                tmp_path / side / "NK" / "per_protein_grid.parquet",
                rows,
                th_step=TH_STEP,
                setting="NK",
                metadata={"ontology_snapshot_id": snapshot},
            )
        with pytest.raises(PanelComparabilityError, match="does not waive these"):
            _run(tmp_path, allow_frame_mismatch=True)

    def test_a_permitted_artifact_mismatch_is_recorded_in_the_result(
        self, tmp_path: Path
    ) -> None:
        """A returned mismatch that nobody reads is a check nobody performed."""
        rows = [
            {"accession": f"P{i}", "namespace": MFO, "tp": [4.0, 1.0], "pred": [8.0, 2.0],
             "n_gt": 10.0}
            for i in range(6)
        ]
        for side, variants in (("a", ("weighted",)), ("b", ("weighted", "unweighted"))):
            write_grid_parquet(
                tmp_path / side / "NK" / "per_protein_grid.parquet",
                rows,
                th_step=TH_STEP,
                setting="NK",
                variants=variants,
            )
        result = _run(tmp_path, allow_frame_mismatch=True)
        assert result["artifact_mismatch"] == {"NK": ["variants"]}
        assert any(
            e[0] == "compare_paired_panels.artifact_mismatch" and e[3] == "warning"
            for e in result["_events"]
        )

    def test_two_settings_of_one_result_written_under_different_frames_are_refused(
        self, tmp_path: Path
    ) -> None:
        """The result block reports one frame, so the side must have one."""
        rows = [
            {"accession": f"P{i}", "namespace": MFO, "tp": [4.0, 1.0], "pred": [8.0, 2.0],
             "n_gt": 10.0}
            for i in range(6)
        ]
        for side in ("a", "b"):
            for setting, snapshot in (
                ("NK", "11111111-1111-1111-1111-111111111111"),
                ("LK", "11111111-1111-1111-1111-111111111111" if side == "b" else "e" * 32),
            ):
                write_grid_parquet(
                    tmp_path / side / setting / "per_protein_grid.parquet",
                    rows,
                    th_step=TH_STEP,
                    setting=setting,
                    metadata={"ontology_snapshot_id": snapshot},
                )
        with pytest.raises(PanelComparabilityError, match="two frames inside one result"):
            _run(tmp_path, panels=["NK:MFO", "LK:MFO"])


class TestAMissingArtefactIsLoudNotGreen:
    """The quiet-success shape: a job that succeeds having read nothing."""

    def test_a_root_holding_no_grid_file_is_refused_rather_than_reported_as_nulls(
        self, tmp_path: Path
    ) -> None:
        (tmp_path / "a").mkdir()
        (tmp_path / "b").mkdir()
        with pytest.raises(ThresholdGridUnavailableError, match="Nothing was read"):
            _run(tmp_path, panels=list(ALL_PANELS))

    def test_a_root_that_does_not_exist_is_refused(self, tmp_path: Path) -> None:
        with pytest.raises(ThresholdGridUnavailableError, match="is not a directory"):
            _run(tmp_path)

    def test_a_mistyped_payload_key_is_refused(self, tmp_path: Path) -> None:
        """``artifact_root`` is a plausible typo of ``artifacts_root``.

        Accepted, it validates, resolves to no local root, sends the operation
        to the object store and returns a successful job full of nulls.
        """
        with pytest.raises(ValidationError, match="artifact_root"):
            ComparePairedPanelsPayload.model_validate(
                {
                    "evaluation_result_id": "a",
                    "baseline_evaluation_result_id": "b",
                    "artifact_root": "/data/run",
                }
            )

    def test_duplicate_panels_are_refused(self) -> None:
        with pytest.raises(ValidationError, match="more than once"):
            ComparePairedPanelsPayload.model_validate(
                {
                    "evaluation_result_id": "a",
                    "baseline_evaluation_result_id": "b",
                    "panels": ["NK:MFO", "NK:MFO"],
                }
            )

    def test_an_absent_panel_is_a_warning_and_is_listed_apart_from_the_withheld(
        self, tmp_path: Path
    ) -> None:
        """"The artefact was absent" and "below the population floor" are not one list."""
        rows = [
            {"accession": f"P{i}", "namespace": MFO, "tp": [4.0, 1.0], "pred": [8.0, 2.0],
             "n_gt": 10.0}
            for i in range(6)
        ]
        for side in ("a", "b"):
            write_grid_parquet(
                tmp_path / side / "NK" / "per_protein_grid.parquet",
                rows if side == "a" else [{**r, "tp": [3.0, 1.0]} for r in rows],
                th_step=TH_STEP,
                setting="NK",
            )
        result = _run(tmp_path, panels=["NK:MFO", "NK:BPO"], min_population=30)
        assert result["absent"] == ["NK:BPO"]
        assert result["withheld"] == ["NK:MFO"]
        assert any(
            e[0] == "compare_paired_panels.panel_absent" and e[3] == "warning"
            for e in result["_events"]
        )


class TestTheAdvertisedInvariants:
    """Properties the build claims, each with a guard rather than a paragraph."""

    @staticmethod
    def _write(tmp_path: Path, setting: str) -> None:
        for side in ("a", "b"):
            write_grid_parquet(
                tmp_path / side / setting / "per_protein_grid.parquet",
                [
                    {"accession": f"P{i:03d}", "namespace": MFO,
                     "tp": [4.0 - (i % 7) * 0.3, 1.0], "pred": [8.0, 2.0], "n_gt": 10.0}
                    if side == "a"
                    else {"accession": f"P{i:03d}", "namespace": MFO,
                          "tp": [3.0 + (i % 5) * 0.2, 1.0], "pred": [8.0, 2.0], "n_gt": 10.0}
                    for i in range(40)
                ],
                th_step=TH_STEP,
                setting=setting,
            )

    def test_the_seed_reproduces_and_the_panel_set_does_not_change_a_panel(
        self, tmp_path: Path
    ) -> None:
        """Seeds are spawned from the panel's canonical index, not taken in sequence.

        Three panels and nine give the same numbers for the panels they share.
        That is a designed property, and ``SeedSequence(spawn_key=...)`` is
        exactly the line someone simplifies away.
        """
        self._write(tmp_path, "NK")
        self._write(tmp_path, "LK")
        both = _run(tmp_path, panels=["NK:MFO", "LK:MFO"], min_population=10)
        alone = _run(tmp_path, panels=["LK:MFO"], min_population=10)
        again = _run(tmp_path, panels=["NK:MFO", "LK:MFO"], min_population=10)
        other_seed = _run(tmp_path, panels=["LK:MFO"], min_population=10, seed=7)

        assert both["panels"]["LK:MFO"]["ci_low"] == alone["panels"]["LK:MFO"]["ci_low"]
        assert both["panels"]["NK:MFO"]["ci_low"] == again["panels"]["NK:MFO"]["ci_low"]
        assert alone["panels"]["LK:MFO"]["ci_low"] != other_seed["panels"]["LK:MFO"]["ci_low"]

    def test_the_run_leaves_both_artefact_roots_byte_identical(self, tmp_path: Path) -> None:
        """Read-only, asserted on the bytes rather than on the description string."""
        import hashlib

        self._write(tmp_path, "NK")

        def fingerprint() -> list[tuple[str, str]]:
            return sorted(
                (str(path.relative_to(tmp_path)), hashlib.sha256(path.read_bytes()).hexdigest())
                for path in tmp_path.rglob("*")
                if path.is_file()
            )

        before = fingerprint()
        _run(tmp_path, min_population=10)
        assert fingerprint() == before

    def test_a_degenerate_interval_never_claims_to_exclude_zero(self) -> None:
        """A zero-width interval around a nonzero point is not a decisive win.

        The verdict already refuses to call it one; the boolean is what a
        thesis table script reads, and it used to say ``True`` beside
        ``ci_low == ci_high`` and a null MDE.
        """
        interval = boot.build_interval(
            np.full(500, 0.05), 0.05, np.full(5, 0.05), alpha=0.05
        )
        assert interval.method == boot.DEGENERATE
        assert interval.low == interval.high == pytest.approx(0.05)
        assert interval.excludes_zero() is False

    def test_the_same_grid_written_with_a_different_float_repr_is_still_one_grid(
        self, tmp_path: Path
    ) -> None:
        """Comparability compares the parsed grid, not the JSON text.

        ``0.28`` and ``0.28000000000000003`` are one threshold, and refusing the
        pair as incomparable frames would be a refusal about formatting.
        """
        rows = [
            {"accession": f"P{i}", "namespace": MFO, "tp": [4.0, 1.0], "pred": [8.0, 2.0],
             "n_gt": 10.0}
            for i in range(6)
        ]
        for side, grid in (("a", [0.4, 0.8]), ("b", [0.4, 0.8000000000000001])):
            write_grid_parquet(
                tmp_path / side / "NK" / "per_protein_grid.parquet",
                rows if side == "a" else [{**r, "tp": [3.0, 1.0]} for r in rows],
                th_step=TH_STEP,
                setting="NK",
                tau_grid=grid,
            )
        result = _run(tmp_path)
        assert result["artifact_mismatch"] == {}
        assert result["panels"]["NK:MFO"]["delta"] is not None

    def test_one_flat_namespace_beside_a_moving_one_is_still_a_grid(
        self, tmp_path: Path
    ) -> None:
        """The broadcast detection is judged over the file, not over one panel.

        A small panel whose every predicted term scores above the top threshold
        is genuinely flat, and refusing it per namespace would take the whole
        nine-panel run down for a legitimate file. The broadcast bug is not
        selective: it flattens every namespace at once.
        """
        flat = [
            {"accession": f"C{i}", "namespace": "cellular_component", "tp": [4.0, 4.0],
             "pred": [8.0, 8.0], "n_gt": 10.0}
            for i in range(3)
        ]
        moving = [
            {"accession": f"P{i}", "namespace": MFO, "tp": [4.0, 1.0], "pred": [8.0, 2.0],
             "n_gt": 10.0}
            for i in range(6)
        ]
        for side in ("a", "b"):
            write_grid_parquet(
                tmp_path / side / "NK" / "per_protein_grid.parquet",
                flat + (moving if side == "a" else [{**r, "tp": [3.0, 1.0]} for r in moving]),
                th_step=TH_STEP,
                setting="NK",
            )
        result = _run(tmp_path, panels=["NK:MFO", "NK:CCO"])
        assert result["panels"]["NK:MFO"]["delta"] is not None
        assert result["panels"]["NK:CCO"]["delta"] == pytest.approx(0.0, abs=1e-12)

    def test_a_ninety_nine_column_broadcast_of_a_real_slice_is_refused(
        self, tmp_path: Path
    ) -> None:
        """The producer's likeliest one-line widening, at the real grid width."""
        rng = np.random.default_rng(3)
        rows = []
        for i in range(120):
            tp = float(rng.uniform(1.0, 8.0))
            rows.append(
                {
                    "accession": f"P{i:04d}",
                    "namespace": MFO,
                    "tp": [tp] * 99,
                    "pred": [tp + 2.0] * 99,
                    "n_gt": 10.0,
                }
            )
        for side in ("a", "b"):
            write_grid_parquet(
                tmp_path / side / "NK" / "per_protein_grid.parquet",
                rows
                if side == "a"
                else [{**r, "tp": [v * 0.9 for v in cast("list[float]", r["tp"])]} for r in rows],
                th_step=0.01,
                setting="NK",
            )
        with pytest.raises(ThresholdGridUnavailableError, match="written out many times"):
            _run(tmp_path)

    def test_two_results_evaluated_on_different_settings_are_refused(
        self, tmp_path: Path
    ) -> None:
        """Both sides loaded a file, and no requested panel exists on both."""
        rows = [
            {"accession": f"P{i}", "namespace": MFO, "tp": [4.0, 1.0], "pred": [8.0, 2.0],
             "n_gt": 10.0}
            for i in range(6)
        ]
        for side, setting in (("a", "NK"), ("b", "LK")):
            write_grid_parquet(
                tmp_path / side / setting / "per_protein_grid.parquet",
                rows,
                th_step=TH_STEP,
                setting=setting,
            )
        with pytest.raises(ThresholdGridUnavailableError, match="nothing was compared"):
            _run(tmp_path, panels=["NK:MFO", "LK:MFO"])
