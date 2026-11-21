"""Each variant of the per-protein grid has its own population, and every reader keeps it.

The weighted branch scores the terms of interest that carry information
accretion, so a protein whose ground truth carries none is scored by the
unweighted branch alone. Its row is in the file, with zeros in the weighted
columns. The loader, the stratification and the stratum route each keep the
rows with positive ground truth in the variant they read, so a weighted coverage,
resample or stratum count is over the population cafaeval normalised by. Such
proteins depend on the truth alone, so every method evaluated on a window has the
same ones.
"""

from __future__ import annotations

import io
from pathlib import Path

import numpy as np
import pytest

from protea.core.operations._paired_panels_artifact import (
    GRID_FILENAME,
    GridInvariantError,
    in_variant_population,
    load_setting_grid,
    read_grid_metadata,
)
from tests.helpers.paired_panels import write_grid_parquet

BP = "biological_process"
TH_STEP = 0.25  # cafaeval's grid at this step: 0.25, 0.5, 0.75


def _rows(*, outside_pred: float = 0.0) -> list[dict]:
    """P1 in both populations; P2 in the unweighted one only, zeros in its weighted columns."""
    return [
        {
            "accession": "P1",
            "namespace": BP,
            "tp": [2.0, 1.0, 0.5],
            "pred": [3.0, 2.0, 1.0],
            "n_gt": 4.0,
            "tp_u": [1.0, 1.0, 0.0],
            "pred_u": [2.0, 1.0, 1.0],
            "n_gt_u": 2.0,
        },
        {
            "accession": "P2",
            "namespace": BP,
            "tp": [0.0, 0.0, 0.0],
            "pred": [outside_pred, 0.0, 0.0],
            "n_gt": 0.0,
            "tp_u": [1.0, 0.0, 0.0],
            "pred_u": [1.0, 1.0, 0.0],
            "n_gt_u": 1.0,
        },
    ]


def _grid(tmp_path: Path, **kwargs: float) -> Path:
    return write_grid_parquet(
        tmp_path / "PK" / GRID_FILENAME,
        _rows(**kwargs),
        th_step=TH_STEP,
        setting="PK",
        variants=("weighted", "unweighted"),
    )


def test_the_rule_is_positive_ground_truth_in_the_variant() -> None:
    assert in_variant_population(np.array([4.0, 0.0, 1e-12])).tolist() == [True, False, True]


class TestTheLoader:
    def test_the_weighted_variant_reads_its_own_population(self, tmp_path: Path) -> None:
        path = _grid(tmp_path)
        meta = read_grid_metadata(path, "PK", result_id="r")
        weighted = load_setting_grid(path, meta, variant="weighted", result_id="r")
        assert weighted.panels[BP].accessions == ("P1",)

    def test_the_unweighted_variant_reads_every_row(self, tmp_path: Path) -> None:
        path = _grid(tmp_path)
        meta = read_grid_metadata(path, "PK", result_id="r")
        unweighted = load_setting_grid(path, meta, variant="unweighted", result_id="r")
        assert unweighted.panels[BP].accessions == ("P1", "P2")

    def test_mass_outside_the_population_is_refused(self, tmp_path: Path) -> None:
        path = _grid(tmp_path, outside_pred=0.5)
        meta = read_grid_metadata(path, "PK", result_id="r")
        with pytest.raises(GridInvariantError, match="outside the variant's population"):
            load_setting_grid(path, meta, variant="weighted", result_id="r")


def test_the_stratification_counts_the_weighted_population(tmp_path: Path) -> None:
    from protea.core.operations.stratify_evaluation import _rows_at_published_tau

    path = _grid(tmp_path)
    meta = read_grid_metadata(path, "PK", result_id="r")
    taus = {BP: 0.5}
    weighted = _rows_at_published_tau(path, meta, "weighted", taus)
    unweighted = _rows_at_published_tau(path, meta, "unweighted", taus)
    assert [r["protein_accession"] for r in weighted] == ["P1"]
    assert [r["protein_accession"] for r in unweighted] == ["P1", "P2"]


def test_the_stratum_route_lists_the_weighted_population(tmp_path: Path) -> None:
    from protea.api.routers.stratum_proteins import _sliced_at_the_published_tau

    best = f"filename\tns\ttau\tf\npredictions.tsv\t{BP}\t0.5\t0.5\n"
    grid = io.BytesIO(_grid(tmp_path).read_bytes())
    frame = _sliced_at_the_published_tau(grid, lambda _v: best, BP, result_id="r", setting="PK")
    assert frame["protein_accession"].tolist() == ["P1"]
    assert set(frame["variant"]) == {"weighted"}
