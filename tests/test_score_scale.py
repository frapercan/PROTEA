"""The scale of the score: the percentile rank reading and the interior check.

ADR-D55, section 6. Measured on 2026-11-04: in the full length ankh bank two
unrelated proteins already score 0.975, 99.7 % of a neighbour arm's predictions
sit at 0.98 or above, and the F curve of the panel of no prior knowledge in
biological process rises to the last point of the 0.01 grid. These pin the
reading that moves the grid onto each arm's own quantiles, the tie method it is
defined with, and the check that says whether a reported maximum is one.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from protea.core.operations._run_cafa_payload import RunCafaEvaluationPayload
from protea.core.operations._run_cafa_scale import (
    SCORE_READINGS,
    TIE_METHOD,
    annotate_maxima,
    percentile_rank,
    read_prediction_file,
)


class TestThePercentileRank:
    def test_distinct_scores_read_as_their_rank_over_the_count(self) -> None:
        assert percentile_rank(np.array([0.3, 0.1, 0.2, 0.4])).tolist() == [0.75, 0.25, 0.5, 1.0]

    def test_tied_scores_read_alike_at_their_average_rank(self) -> None:
        # Ranks 2 and 3 are tied, so both read (2 + 3) / 2 over 4.
        read = percentile_rank(np.array([0.1, 0.5, 0.5, 0.9]))
        assert read.tolist() == [0.25, 0.625, 0.625, 1.0]
        assert TIE_METHOD == "average"

    def test_a_constant_file_reads_at_the_middle(self) -> None:
        n = 5
        assert percentile_rank(np.full(n, 0.975)).tolist() == [(n + 1) / (2 * n)] * n

    def test_the_reading_is_monotone_and_lands_in_the_unit_interval(self) -> None:
        rng = np.random.default_rng(20261104)
        raw = np.round(rng.beta(40, 1, size=5000), 4)  # an anisotropic bank: mass near 1
        read = percentile_rank(raw)
        order = np.argsort(raw, kind="stable")
        assert np.all(np.diff(read[order]) >= 0)
        assert np.all((read > 0) & (read <= 1))
        # Equal raw scores read equal; different raw scores never swap.
        same = raw[:, None] == raw[None, :500]
        assert np.array_equal(same, read[:, None] == read[None, :500])

    def test_an_empty_file_has_nothing_to_read(self) -> None:
        assert percentile_rank(np.array([])).size == 0


def _write(path: Path, scores: list[float]) -> None:
    rows = [f"P{i:05d}\tGO:{i:07d}\t{s:.4f}" for i, s in enumerate(scores)]
    path.write_text("\n".join(rows) + "\n")


def _scores(path: Path) -> np.ndarray:
    return pd.read_csv(path, sep="\t", header=None)[2].to_numpy()


class TestTheFileReading:
    def test_raw_leaves_the_file_as_written_and_still_reports_its_scale(
        self, tmp_path: Path
    ) -> None:
        path = tmp_path / "predictions.tsv"
        _write(path, [0.9750] * 7 + [0.9950] * 3)
        before = path.read_bytes()
        scale = read_prediction_file(str(path), "raw", 0.01)
        assert path.read_bytes() == before
        assert scale["n"] == 10
        assert scale["top_tau"] == 0.99
        assert scale["top_bin_share_raw"] == scale["top_bin_share_read"] == 0.3

    def test_the_percentile_reading_moves_the_grid_onto_the_arm(self, tmp_path: Path) -> None:
        # The measured shape: almost everything at or above 0.98 raw.
        rng = np.random.default_rng(7)
        raw = np.round(0.98 + 0.02 * rng.random(2000), 4).tolist()
        path = tmp_path / "predictions.tsv"
        _write(path, raw)
        scale = read_prediction_file(str(path), "percentile_rank", 0.01)
        assert scale["top_bin_share_raw"] > 0.4
        assert scale["top_bin_share_read"] < 0.02
        read = _scores(path)
        # The top raw scores tie at four decimals, so they read at their
        # average rank, just under 1.
        assert 0.99 < read.max() <= 1.0
        # Same rows, same order, and the ordering of the arm kept.
        assert np.all(np.diff(read[np.argsort(raw, kind="stable")]) >= 0)

    def test_the_rows_keep_their_proteins_and_terms(self, tmp_path: Path) -> None:
        path = tmp_path / "predictions.tsv"
        _write(path, [0.5, 0.7, 0.6])
        read_prediction_file(str(path), "percentile_rank", 0.01)
        frame = pd.read_csv(path, sep="\t", header=None)
        assert frame[0].tolist() == ["P00000", "P00001", "P00002"]
        assert frame[1].tolist() == ["GO:0000000", "GO:0000001", "GO:0000002"]

    def test_an_empty_file_reports_no_scale(self, tmp_path: Path) -> None:
        path = tmp_path / "predictions.tsv"
        path.write_text("")
        scale = read_prediction_file(str(path), "percentile_rank", 0.01)
        assert scale["n"] == 0
        assert scale["top_bin_share_raw"] is None

    def test_an_unknown_reading_is_refused(self, tmp_path: Path) -> None:
        path = tmp_path / "predictions.tsv"
        _write(path, [0.5])
        with pytest.raises(ValueError, match="score_reading"):
            read_prediction_file(str(path), "quantile", 0.01)


_REQUEST = {"evaluation_set_id": "e", "prediction_set_id": "p"}


class TestThePayload:
    def test_raw_is_the_default(self) -> None:
        assert RunCafaEvaluationPayload.model_validate(_REQUEST).score_reading == "raw"

    @pytest.mark.parametrize("reading", SCORE_READINGS)
    def test_each_reading_is_accepted(self, reading: str) -> None:
        payload = RunCafaEvaluationPayload.model_validate({**_REQUEST, "score_reading": reading})
        assert payload.score_reading == reading

    def test_another_name_is_refused(self) -> None:
        with pytest.raises(ValueError, match="score_reading"):
            RunCafaEvaluationPayload.model_validate({**_REQUEST, "score_reading": "rank"})


def _curve(ns: str, taus: list[float], f: list[float], f_micro_w: list[float]) -> pd.DataFrame:
    frame = pd.DataFrame(
        {
            "filename": "predictions.tsv",
            "ns": ns,
            "tau": taus,
            "f": f,
            "f_micro_w": f_micro_w,
            "cov": 1.0,
        }
    )
    return frame.set_index(["filename", "ns", "tau"])


class TestTheInteriorCheck:
    def test_an_interior_maximum_has_no_edge(self) -> None:
        curve = _curve("biological_process", [0.1, 0.2, 0.3], [0.2, 0.4, 0.3], [0.1, 0.5, 0.2])
        results = {"BPO": {"fmax": 0.4}}
        assert annotate_maxima(curve, results) == {}
        assert results["BPO"]["maxima"]["f"] == {"tau": 0.2, "edge": None}
        assert results["BPO"]["maxima"]["f_micro_w"] == {"tau": 0.2, "edge": None}

    def test_a_curve_that_rises_to_the_last_threshold_is_censored(self) -> None:
        # The measured case: the curve still climbing at 0.99.
        curve = _curve("biological_process", [0.97, 0.98, 0.99], [0.1, 0.2, 0.3], [0.2, 0.3, 0.4])
        results = {"BPO": {}}
        assert annotate_maxima(curve, results) == {"BPO": ["f", "f_micro_w"]}
        assert results["BPO"]["maxima"]["f_micro_w"] == {"tau": 0.99, "edge": "top"}

    def test_the_edge_is_the_last_threshold_that_kept_a_prediction(self) -> None:
        # cafaeval drops thresholds with no coverage, so a curve that ends at
        # 0.7 peaks at its top when it peaks at 0.7, whatever the grid's end.
        curve = _curve("molecular_function", [0.5, 0.6, 0.7], [0.1, 0.2, 0.3], [0.3, 0.2, 0.1])
        results = {"MFO": {}}
        assert annotate_maxima(curve, results) == {"MFO": ["f", "f_micro_w"]}
        assert results["MFO"]["maxima"]["f"]["edge"] == "top"
        assert results["MFO"]["maxima"]["f_micro_w"]["edge"] == "bottom"

    def test_ties_go_to_the_lowest_threshold_as_cafaeval_chooses(self) -> None:
        curve = _curve(
            "cellular_component", [0.1, 0.2, 0.3, 0.4], [0.1, 0.5, 0.5, 0.2], [0.1, 0.5, 0.5, 0.2]
        )
        results = {"CCO": {}}
        annotate_maxima(curve, results)
        assert results["CCO"]["maxima"]["f"] == {"tau": 0.2, "edge": None}

    def test_a_variant_cafaeval_did_not_compute_is_not_invented(self) -> None:
        curve = _curve("biological_process", [0.1, 0.2, 0.3], [0.2, 0.4, 0.3], [0.1, 0.5, 0.2])
        results = {"BPO": {}}
        annotate_maxima(curve, results)
        assert set(results["BPO"]["maxima"]) == {"f", "f_micro_w"}

    def test_a_namespace_without_a_result_is_left_alone(self) -> None:
        curve = _curve("biological_process", [0.1, 0.2], [0.2, 0.4], [0.1, 0.5])
        results: dict[str, dict[str, object]] = {}
        assert annotate_maxima(curve, results) == {}
        assert results == {}

    def test_two_prediction_files_in_one_curve_are_refused(self) -> None:
        one = _curve("biological_process", [0.1, 0.2], [0.2, 0.4], [0.1, 0.5]).reset_index()
        two = one.assign(filename="other.tsv")
        curve = pd.concat([one, two]).set_index(["filename", "ns", "tau"])
        with pytest.raises(ValueError, match="one file per setting"):
            annotate_maxima(curve, {"BPO": {}})

    def test_no_curve_records_nothing(self) -> None:
        results = {"BPO": {"fmax": 0.4}}
        assert annotate_maxima(None, results) == {}
        assert "maxima" not in results["BPO"]
