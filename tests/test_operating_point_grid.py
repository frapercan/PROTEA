"""One path to the per-protein grid, and what a grid reads at a threshold.

The evaluator scores a stored set (``run_cafa_evaluation``: the scores are read
out of the database, rendered and written, and each setting is evaluated) and a
fit scores in-memory scores (``evaluate_to_grid``). The fixture here holds the
two byte-identical, the rendering included, on a window cafaeval scores for
real; with the order of a written score (restoration, then rendering, then the
grid) as part of it. Then the arithmetic of a reading: every metric read off a
grid at a threshold equals cafaeval's own curve at that threshold, in both
weightings, S's minimum included; and the pooled threshold is the grid argmax of
the pooled curve, lowest tau on ties.
"""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pytest

from protea.core.operating_point import grid
from protea.core.operating_point.hierarchy import ancestry_from_obo, restore
from protea.core.operations import _method_score_predictions as method_scores
from protea.core.operations import _run_cafa_artifacts as artifacts
from protea.core.operations._paired_panels_artifact import load_setting_grid, read_grid_metadata
from protea.core.operations._paired_panels_bootstrap import PanelArrays
from protea.core.operations._run_cafa_eval_driver import (
    CafaEvalRunContext,
    _grid_frame,
    evaluate_all_settings,
)
from protea.core.operations._run_cafa_grid_artifact import (
    GRID_FILENAME,
    GridArtifact,
    write_grid_parquet,
)
from tests.helpers.operating_point import raw_scores, write_frame

pytest.importorskip("cafaeval")


def _context(frame: grid.EvaluatorInputs, root: Path, pred_dir: Path) -> CafaEvalRunContext:
    return CafaEvalRunContext(
        artifacts_root=root,
        obo_path=frame.obo_path,
        nk_path=frame.ground_truth["NK"],
        lk_path=frame.ground_truth["LK"],
        pk_path=frame.ground_truth["PK"],
        pk_known_path=frame.pk_known_path,
        ia_path=frame.ia_path,
        toi_path=frame.toi_path,
        shared_pred_dir=str(pred_dir),
        ontology_snapshot_id="11111111-1111-1111-1111-111111111111",
        evaluation_set_id="22222222-2222-2222-2222-222222222222",
        information_accretion_frame="33333333-3333-3333-3333-333333333333",
    )


def _stored_path(
    tmp_path: Path, frame: grid.EvaluatorInputs, stored: pa.Table, monkeypatch: Any
) -> dict[str, bytes]:
    """What run_cafa_evaluation writes for a stored set: its grid files, by setting."""
    rows = pa.table(
        {
            "protein_accession": stored["protein_accession"],
            "go_id": stored["go_id"],
            "method_score": stored["score"],
        }
    )
    monkeypatch.setattr(method_scores, "method_rows", lambda _session, _ctx: rows)
    root = tmp_path / "evaluator"
    pred_dir = root / "predictions"
    pred_dir.mkdir(parents=True)
    artifacts.write_predictions(
        None,  # type: ignore[arg-type]
        artifacts.WritePredictionsContext(
            pred_set_id=uuid.uuid4(),
            delta_proteins=set(rows["protein_accession"].to_pylist()),
            path=str(pred_dir / "predictions.tsv"),
        ),
    )
    ctx = _context(frame, root, pred_dir)
    evaluate_all_settings(ctx=ctx, emit=lambda *a, **k: None)
    return {s: (root / s / GRID_FILENAME).read_bytes() for s in grid.SETTINGS}


def _fit_path(
    tmp_path: Path, frame: grid.EvaluatorInputs, scores: pa.Table, **kwargs: Any
) -> dict[str, bytes]:
    """What evaluate_to_grid gives for in-memory scores, written with the evaluator's footer."""
    evaluation = grid.evaluate_to_grid(scores, frame, tmp_path / "fit", **kwargs)
    ctx = _context(frame, tmp_path / "fit", tmp_path / "fit" / "predictions")
    out: dict[str, bytes] = {}
    for setting in grid.SETTINGS:
        artifact = evaluation.grids[setting]
        path = tmp_path / "fit-grids" / setting / GRID_FILENAME
        write_grid_parquet(path, artifact, _grid_frame(ctx, setting).stamp(artifact.variants))
        out[setting] = path.read_bytes()
    return out


class TestOnePathToTheGrid:
    def test_a_stored_set_and_in_memory_scores_give_byte_identical_grids(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        frame = write_frame(tmp_path / "frame")
        stored = _stored_path(tmp_path, frame, raw_scores(), monkeypatch)
        fitted = _fit_path(tmp_path, frame, raw_scores())
        assert stored == fitted

    def test_the_order_restoration_then_rendering_then_grid_is_the_stored_restored_set(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        frame = write_frame(tmp_path / "frame")
        ancestry = ancestry_from_obo(frame.obo_path)
        stored = _stored_path(tmp_path, frame, restore(raw_scores(), ancestry), monkeypatch)
        fitted = _fit_path(tmp_path, frame, raw_scores(), ancestry=ancestry)
        assert stored == fitted
        # The fixture's set is not consistent, so restoration has something to do:
        # without it the grid the evaluator reads is another one.
        unrestored = _fit_path(tmp_path / "raw", frame, raw_scores())
        assert unrestored != fitted

    def test_the_rendering_is_four_decimals_ordered_by_protein_and_term(self) -> None:
        rendered = grid.render_scores(
            grid.score_table(["P2", "P1", "P1"], ["GO:2", "GO:9", "GO:1"], [0.123456, 0.5, 1e-5])
        )
        assert rendered.to_pydict() == {
            "protein_accession": ["P1", "P1", "P2"],
            "go_id": ["GO:1", "GO:9", "GO:2"],
            "score": ["0.0000", "0.5000", "0.1235"],
        }

    def test_a_namespace_left_out_of_a_grid_is_refused_for_a_fit(self) -> None:
        evaluation = grid.GridEvaluation(
            settings={},
            grids={
                "NK": GridArtifact(
                    [], ("unweighted",), 3, [{"namespace": "biological_process", "code": "x"}]
                )
            },
        )
        with pytest.raises(ValueError, match="left namespaces out"):
            evaluation.refuse_dropped()


def _loaded(path: Path, variant: str) -> dict[str, PanelArrays]:
    meta = read_grid_metadata(path, path.parent.name, result_id="fixture")
    return load_setting_grid(path, meta, variant=variant, result_id="fixture").panels


class TestTheGridReadsWhatTheEvaluatorReports:
    """Every metric at every threshold, from the stored grid, against cafaeval's own curve."""

    @pytest.fixture()
    def evaluated(self, tmp_path: Path) -> tuple[grid.GridEvaluation, Path]:
        frame = write_frame(tmp_path / "frame")
        evaluation = grid.evaluate_to_grid(raw_scores(), frame, tmp_path / "fit")
        ctx = _context(frame, tmp_path / "fit", tmp_path / "fit" / "predictions")
        for setting in grid.SETTINGS:
            artifact = evaluation.grids[setting]
            write_grid_parquet(
                tmp_path / "grids" / setting / GRID_FILENAME,
                artifact,
                _grid_frame(ctx, setting).stamp(artifact.variants),
            )
        return evaluation, tmp_path / "grids"

    @pytest.mark.parametrize("setting", ["NK", "LK", "PK"])
    def test_every_metric_at_every_threshold_equals_the_curve(
        self, evaluated: tuple[grid.GridEvaluation, Path], setting: str
    ) -> None:
        evaluation, root = evaluated
        curve = evaluation.settings[setting].curve.reset_index()
        taus = grid.tau_grid_of(0.01)
        checked = 0
        for variant, suffix in (("unweighted", ""), ("weighted", "_w")):
            for namespace, arrays in _loaded(root / setting / GRID_FILENAME, variant).items():
                curves = grid.metric_curves(arrays)
                rows = curve[curve["ns"] == namespace]
                for _, row in rows.iterrows():
                    index = int(np.flatnonzero(np.isclose(taus, row["tau"]))[0])
                    for name in grid.CURVE_METRICS:
                        assert curves[name][index] == pytest.approx(
                            float(row[f"{name}{suffix}"]), rel=1e-5, abs=1e-6
                        ), (setting, namespace, variant, name, row["tau"])
                        checked += 1
        assert checked > 0

    @pytest.mark.parametrize("setting", ["NK", "LK", "PK"])
    def test_smin_in_both_weightings_is_the_curves_minimum(
        self, evaluated: tuple[grid.GridEvaluation, Path], setting: str
    ) -> None:
        evaluation, root = evaluated
        metrics = evaluation.settings[setting].metrics
        taus = grid.tau_grid_of(0.01)
        unweighted = _loaded(root / setting / GRID_FILENAME, "unweighted")
        weighted = _loaded(root / setting / GRID_FILENAME, "weighted")
        from protea.core.operations._run_cafa_helpers import _NS_LABELS

        assert metrics, "the fixture scores every setting"
        for namespace, arrays in unweighted.items():
            cell = metrics[_NS_LABELS[namespace]]
            covered = grid.metric_curves(arrays)["n"] > 0
            low = grid.extremum(grid.metric_curves(arrays)["s"], covered, taus, lowest=True)
            low_w = grid.extremum(
                grid.metric_curves(weighted[namespace])["s"], covered, taus, lowest=True
            )
            assert low is not None and low_w is not None
            assert (cell["smin"], cell["tau_smin"]) == pytest.approx((low.value, low.tau), abs=1e-4)
            assert (cell["smin_w"], cell["tau_smin_w"]) == pytest.approx(
                (low_w.value, low_w.tau), abs=1e-4
            )

    def test_smin_is_the_published_best_s(
        self, evaluated: tuple[grid.GridEvaluation, Path]
    ) -> None:
        """The unweighted Smin is cafaeval's own evaluation_best_s, threshold and all."""
        evaluation, _ = evaluated
        for setting in grid.SETTINGS:
            best = evaluation.settings[setting].best["s"].reset_index()
            from protea.core.operations._run_cafa_helpers import _NS_LABELS

            for _, row in best.iterrows():
                cell = evaluation.settings[setting].metrics[_NS_LABELS[row["ns"]]]
                assert cell["smin"] == round(float(row["s"]), 4)
                assert cell["tau_smin"] == round(float(row["tau"]), 4)


class TestMergeSmin:
    def test_a_curve_without_rows_adds_nothing(self) -> None:
        import pandas as pd

        cells: dict[str, Any] = {"BPO": {}}
        artifacts.merge_smin(pd.DataFrame(), cells)
        artifacts.merge_smin(None, cells)
        assert cells == {"BPO": {}}

    def test_ties_go_to_the_lowest_threshold_and_the_weighted_pair_needs_its_column(self) -> None:
        import pandas as pd

        curve = pd.DataFrame(
            {
                "filename": ["f"] * 3,
                "ns": ["biological_process"] * 3,
                "tau": [0.1, 0.2, 0.3],
                "s": [0.5, 0.2, 0.2],
            }
        ).set_index(["filename", "ns", "tau"])
        cells: dict[str, Any] = {"BPO": {}}
        artifacts.merge_smin(curve, cells)
        assert cells == {"BPO": {"smin": 0.2, "tau_smin": 0.2}}


def _arrays(tp: list[list[float]], pred: list[list[float]], gt: list[float]) -> PanelArrays:
    return PanelArrays(
        tuple(f"P{i}" for i in range(len(gt))),
        np.asarray(tp, dtype=np.float64),
        np.asarray(pred, dtype=np.float64),
        np.asarray(gt, dtype=np.float64),
    )


class TestThePooledThreshold:
    TAUS = (0.25, 0.5, 0.75)

    def test_the_units_of_every_window_are_pooled_before_the_argmax(self) -> None:
        # Window A alone peaks at 0.25 and window B alone at 0.75; pooled, the
        # sums decide, not the mean of the two curves.
        a = _arrays([[4.0, 3.0, 0.0]], [[10.0, 4.0, 0.0]], [5.0])
        b = _arrays(
            [[1.0, 1.0, 1.0], [2.0, 2.0, 2.0]], [[9.0, 6.0, 3.0], [9.0, 6.0, 3.0]], [4.0, 4.0]
        )
        found = grid.pooled_threshold({"a": a, "b": b}, self.TAUS)
        tp, pred, gt = np.array([7.0, 6.0, 3.0]), np.array([28.0, 16.0, 6.0]), 13.0
        curve = 2 * tp / (pred + gt)
        assert found.curve == pytest.approx(tuple(curve))
        assert found.index == int(np.argmax(curve)) and found.tau == self.TAUS[found.index]
        assert found.value == pytest.approx(curve.max())
        assert found.half_value == pytest.approx(curve.max() / 2)
        assert found.units == 3
        assert found.window_taus["a"][0] == 0.5
        assert found.window_taus["b"][0] == 0.75

    def test_ties_go_to_the_lowest_threshold_and_an_edge_is_flagged(self) -> None:
        flat = _arrays([[1.0, 1.0, 1.0]], [[2.0, 2.0, 2.0]], [2.0])
        found = grid.pooled_threshold({"only": flat}, self.TAUS)
        assert found.index == 0 and found.tau == 0.25
        assert found.at_grid_edge is True

    def test_a_window_on_another_grid_is_refused(self) -> None:
        with pytest.raises(ValueError, match="carries 2 thresholds"):
            grid.pooled_threshold({"w": _arrays([[1.0, 1.0]], [[1.0, 1.0]], [1.0])}, self.TAUS)


class TestTheMetricsOfACell:
    def test_a_hand_computed_cell(self) -> None:
        arrays = _arrays([[2.0, 0.0], [1.0, 1.0]], [[4.0, 0.0], [2.0, 2.0]], [4.0, 2.0])
        curves = grid.metric_curves(arrays)
        at = grid.metrics_at(curves, 0, suffix="_w")
        # Macro precision over the predicting proteins, recall over all, cafa style.
        assert at["pr_w"] == pytest.approx((0.5 + 0.5) / 2)
        assert at["rc_w"] == pytest.approx((0.5 + 0.5) / 2)
        assert at["f_micro_w"] == pytest.approx(2 * 3 / (6 + 6))
        assert at["ru_w"] == pytest.approx((2 + 1) / 2) and at["mi_w"] == pytest.approx(3 / 2)
        assert at["s_w"] == pytest.approx(np.hypot(1.5, 1.5))
        at1 = grid.metrics_at(curves, 1)
        assert at1["n"] == 1 and at1["cov"] == 0.5
        assert at1["pr"] == pytest.approx(0.5)
        assert at1["rc"] == pytest.approx((0.0 + 0.5) / 2)

    def test_the_extremum_is_taken_over_the_covered_thresholds(self) -> None:
        curve = np.array([0.9, 0.2, 0.5, 0.3])
        covered = np.array([False, True, True, True])
        top = grid.extremum(curve, covered, np.array([0.1, 0.2, 0.3, 0.4]))
        assert top is not None and (top.tau, top.edge) == (0.3, None)
        low = grid.extremum(curve, covered, np.array([0.1, 0.2, 0.3, 0.4]), lowest=True)
        assert low is not None and (low.tau, low.edge) == (0.2, "bottom")
        assert grid.extremum(curve, np.zeros(4, dtype=bool), np.zeros(4)) is None
