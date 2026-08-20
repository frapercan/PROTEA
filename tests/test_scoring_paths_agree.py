"""The baseline and the arm it is the baseline for must agree.

`embedding_only` weights embedding_similarity at 1.0 and everything else at
0, which computes exactly what the None fallback computes. They are the
same arm measured twice, and any disagreement between them is a defect in
one of the two paths rather than a result.

Measured on rung 1 across 396 cells before this was fixed: 387 identical,
9 differing, every difference exactly 0.0001, which is one unit in the
last written digit. The cause was the ScoringConfig path rounding to six
decimals before formatting to four while the fallback went straight to
four.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from protea.core.operations._run_cafa_artifacts import _write_scored_base


class _Config:
    """Minimal stand-in: only the fields the writer reads."""

    formula = "linear"
    weights = {"embedding_similarity": 1.0}
    evidence_weights = None
    params = None


def _frame() -> pd.DataFrame:
    # Distances chosen to land the score near a fourth-decimal boundary,
    # which is where double rounding and single rounding disagree.
    return pd.DataFrame(
        {
            "protein_accession": [f"P{i}" for i in range(6)],
            "go_id": ["GO:0000001"] * 6,
            "distance": [0.000_05, 0.000_15, 0.000_25, 0.1, 0.333_333, 1.999_95],
            "evidence_code": ["EXP"] * 6,
            "taxonomic_distance": [0.0] * 6,
            "neighbor_vote_fraction": [1.0] * 6,
            "identity_nw": [1.0] * 6,
            "identity_sw": [1.0] * 6,
            "alignment_length_nw": [100] * 6,
            "gaps_pct_nw": [0.0] * 6,
            "alignment_length_sw": [100] * 6,
            "gaps_pct_sw": [0.0] * 6,
            "length_query": [100] * 6,
            "ref_annotation_density": [1.0] * 6,
            "anc2vec_neighbor_cos": [0.0] * 6,
            "anc2vec_neighbor_maxcos": [0.0] * 6,
            "go_term_frequency": [0.1] * 6,
        }
    )


def _written(tmp_path: Path, config) -> list[str]:
    out = tmp_path / f"{'cfg' if config else 'fallback'}.tsv"
    _write_scored_base(_frame(), config, str(out))
    return out.read_text().splitlines()


def test_the_config_path_writes_what_the_fallback_writes(tmp_path):
    # The whole point: embedding_only IS the fallback, arithmetically.
    assert _written(tmp_path, _Config()) == _written(tmp_path, None)


def test_they_agree_on_values_near_a_rounding_boundary(tmp_path):
    # Where they used to differ. A midpoint rounded twice can move by one
    # unit in the last digit; rounded once it cannot.
    cfg = [line.split("\t")[2] for line in _written(tmp_path, _Config())]
    fb = [line.split("\t")[2] for line in _written(tmp_path, None)]
    assert cfg == fb


def test_every_score_is_written_to_four_decimals(tmp_path):
    for line in _written(tmp_path, None):
        assert len(line.split("\t")[2].split(".")[1]) == 4


def test_an_empty_frame_writes_an_empty_file(tmp_path):
    out = tmp_path / "empty.tsv"
    _write_scored_base(pd.DataFrame(), None, str(out))
    assert out.read_text() == ""
