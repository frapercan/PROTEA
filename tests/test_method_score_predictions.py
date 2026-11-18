"""The method-score path of the evaluator writes the base frame path's file, byte for byte.

A method's prediction set (here the count map's, built as ``tests/test_run_count_map.py``
builds it) is written by both paths and compared; the denial rule, vectorised in the
new path, removes exactly what ``drop_contradictions`` removes.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pytest
from sqlalchemy.orm import Session

from protea.core.operations import _method_score_predictions as fast
from protea.core.operations import _run_cafa_artifacts as art
from protea.core.operations._base_frame_recount import build_base_frame
from protea.core.operations._denials import apply_denials, drop_contradictions
from protea.infrastructure.orm.models.embedding.scoring_config import (
    FORMULA_LINEAR,
    FORMULA_METHOD_SCORE,
    ScoringConfig,
)
from tests.test_run_count_map import _run, _scenario, pg  # noqa: F401

DELTA = {"D1", "T1", "T2", "T3"}


@pytest.fixture()
def db(pg: Session) -> Session:  # noqa: F811
    """The count map test's fresh schema."""
    return pg


def _ctx(pred: str, path: Path, **depth: float) -> art.WritePredictionsContext:
    return art.WritePredictionsContext(
        pred_set_id=uuid.UUID(pred),
        delta_proteins=DELTA,
        max_distance=depth.get("d"),
        path=str(path),
    )


@pytest.mark.integration
def test_both_paths_write_the_same_file(
    db: Session, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pred = _run(db, _scenario(db, tmp_path, monkeypatch))["prediction_set_id"]
    config = ScoringConfig(name="m", formula=FORMULA_METHOD_SCORE, weights={})
    new = fast.write_method_predictions(db, _ctx(pred, tmp_path / "new.tsv"))
    ctx = _ctx(pred, tmp_path / "old.tsv")
    base, _ = build_base_frame(db, ctx, art._base_select, art._BASE_SCORE_COLS)
    art._write_scored_base(apply_denials(db, ctx, base), config, ctx.path)
    old_bytes = (tmp_path / "old.tsv").read_bytes()
    assert new > 0 and (tmp_path / "new.tsv").read_bytes() == old_bytes
    # and the writer routes a method set to the new path by itself
    art.write_predictions(db, _ctx(pred, tmp_path / "routed.tsv"), scoring_config=config)
    assert (tmp_path / "routed.tsv").read_bytes() == old_bytes


def test_the_path_applies_only_to_a_method_score_without_a_depth_cut(tmp_path: Path) -> None:
    pred = str(uuid.uuid4())
    method = ScoringConfig(name="m", formula=FORMULA_METHOD_SCORE, weights={})
    linear = ScoringConfig(name="l", formula=FORMULA_LINEAR, weights={})
    assert fast.applies(_ctx(pred, tmp_path / "a"), method)
    assert not fast.applies(_ctx(pred, tmp_path / "a"), linear)
    assert not fast.applies(_ctx(pred, tmp_path / "a"), None)
    assert not fast.applies(_ctx(pred, tmp_path / "a", d=0.5), method)


def test_the_vectorised_denials_remove_what_the_frame_rule_removes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = pa.table(
        {
            "protein_accession": ["P1", "P1", "P2", "P3", "P3"],
            "go_id": ["GO:1", "GO:2", "GO:1", "GO:3", "GO:4"],
            "method_score": [0.9, 0.8, 0.7, 0.6, 0.5],
        }
    )
    denied = {("P1", "GO:2"), ("P3", "GO:3"), ("P9", "GO:1")}
    monkeypatch.setattr(fast, "denied_pairs", lambda session, ctx: denied)
    kept = fast.without_denials(None, None, rows)  # type: ignore[arg-type]
    frame, removed = drop_contradictions(pd.DataFrame(rows.to_pydict()), denied)
    assert removed == 2
    assert kept.to_pydict() == {k: list(v) for k, v in frame.to_dict(orient="list").items()}
