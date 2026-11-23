"""Each cell of a result records its size in each variant, counted from its grid.

``n_proteins`` is cafaeval's ``n``, the proteins with a prediction at the reported
threshold, so it moves with the threshold and is not the cell's size. The size is
the population cafaeval normalised by: every row of the cell's per-protein grid for
the unweighted variant, and the rows with positive weighted ground truth for the
weighted one.
"""

from __future__ import annotations

import numpy as np

from protea.core.operations._run_cafa_eval_driver import _record_cell_populations
from protea.core.operations._run_cafa_grid_artifact import (
    GridArtifact,
    cell_populations,
    grid_rows_from_sink,
)
from tests.test_per_protein_grid_producer import BP, _pk_sink_split_populations


def _row(namespace: str, n_gt: float, n_gt_w: float | None = None) -> dict:
    row = {"protein_accession": "P", "namespace": namespace, "n_gt": n_gt}
    if n_gt_w is not None:
        row["n_gt_w"] = n_gt_w
    return row


def test_the_weighted_population_leaves_out_the_proteins_it_did_not_score() -> None:
    # P00003's ground truth carries no information accretion: the unweighted
    # branch scores it and the weighted one does not.
    artifact = grid_rows_from_sink(_pk_sink_split_populations())
    assert cell_populations(artifact) == {BP: {"n_targets": 2, "n_targets_w": 1}}


def test_an_unweighted_file_records_only_the_unweighted_size() -> None:
    artifact = GridArtifact(
        rows=[_row("molecular_function", 2.0), _row("molecular_function", 1.0)],
        variants=("unweighted",),
        n_tau=3,
        dropped=[],
    )
    assert cell_populations(artifact) == {"molecular_function": {"n_targets": 2}}


def test_the_sizes_land_beside_the_metrics_of_the_cells_the_result_reports() -> None:
    artifact = GridArtifact(
        rows=[
            _row("biological_process", 2.0, 0.0),
            _row("biological_process", 1.0, 3.5),
            _row("cellular_component", 4.0, 1.0),
        ],
        variants=("weighted", "unweighted"),
        n_tau=3,
        dropped=[],
    )
    result = {"BPO": {"f_micro_w": 0.2}, "MFO": {"f_micro_w": 0.4}}
    _record_cell_populations(result, artifact)
    assert result == {
        "BPO": {"f_micro_w": 0.2, "n_targets": 2, "n_targets_w": 1},
        "MFO": {"f_micro_w": 0.4},
    }
    assert not np.isnan(result["BPO"]["n_targets"])
