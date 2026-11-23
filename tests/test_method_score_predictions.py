"""The evaluator writes a method's stored scores, as stored, and refuses a set without them.

The evaluator scores what a method stored and nothing else (the closure's
question 6, 2026-11-20). A method's prediction set (here the count map's, built
as ``tests/test_run_count_map.py`` builds it) carries a ``method_score`` on
every row; the prediction file cafaeval reads is those scores, ordered by
protein and term, at four decimals, with nothing removed or rescored on the
way. A set that stores candidates without scores is refused before anything is
staged, with the step that scores them.
"""

from __future__ import annotations

import uuid
from pathlib import Path
from unittest.mock import MagicMock

import pyarrow as pa
import pytest
from sqlalchemy import text
from sqlalchemy.orm import Session

from protea.core.operations import _method_score_predictions as fast
from protea.core.operations import _run_cafa_artifacts as art
from protea.core.scoring import MethodScoreMissing
from tests.test_run_count_map import _run, _scenario, pg  # noqa: F401

DELTA = {"D1", "T1", "T2", "T3"}


@pytest.fixture()
def db(pg: Session) -> Session:  # noqa: F811
    """The count map test's fresh schema."""
    return pg


def _ctx(pred: str, path: Path) -> art.WritePredictionsContext:
    return art.WritePredictionsContext(
        pred_set_id=uuid.UUID(pred), delta_proteins=DELTA, path=str(path)
    )


@pytest.mark.integration
def test_the_file_is_the_stored_scores_as_stored(
    db: Session, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pred = _run(db, _scenario(db, tmp_path, monkeypatch))["prediction_set_id"]
    written = art.write_predictions(db, _ctx(pred, tmp_path / "scores.tsv"))
    stored = db.execute(
        text(
            "SELECT p.protein_accession, t.go_id, p.method_score FROM go_prediction p "
            "JOIN go_term t ON t.id = p.go_term_id "
            "WHERE p.prediction_set_id = :s AND p.protein_accession = ANY(:d)"
        ),
        {"s": pred, "d": sorted(DELTA)},
    ).all()
    expected = "".join(
        f"{protein}\t{go_id}\t{score:.4f}\n" for protein, go_id, score in sorted(stored)
    )
    assert written == len(stored) > 0
    assert (tmp_path / "scores.tsv").read_text() == expected


@pytest.mark.integration
def test_a_scored_set_passes_the_check(
    db: Session, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pred = _run(db, _scenario(db, tmp_path, monkeypatch))["prediction_set_id"]
    fast.refuse_unless_scored(db, uuid.UUID(pred))


@pytest.mark.integration
def test_a_set_of_candidates_is_refused_with_the_step_that_scores_it(
    db: Session, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pred = _run(db, _scenario(db, tmp_path, monkeypatch))["prediction_set_id"]
    db.execute(
        text(
            "UPDATE go_prediction SET method_score = NULL WHERE id = "
            "(SELECT min(id) FROM go_prediction WHERE prediction_set_id = :s)"
        ),
        {"s": pred},
    )
    with pytest.raises(MethodScoreMissing, match="score_prediction_set"):
        fast.refuse_unless_scored(db, uuid.UUID(pred))


@pytest.mark.integration
def test_an_empty_set_is_refused(db: Session) -> None:
    with pytest.raises(MethodScoreMissing, match="no predictions"):
        fast.refuse_unless_scored(db, uuid.uuid4())


def test_nothing_is_removed_or_rescored_on_the_way(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Scored as delivered: no query beyond the rows, no row dropped, no score moved."""
    rows = pa.table(
        {
            "protein_accession": ["P2", "P1", "P1"],
            "go_id": ["GO:1", "GO:2", "GO:1"],
            "method_score": [0.7, 0.81234, 0.9],
        }
    )
    monkeypatch.setattr(fast, "method_rows", lambda _session, _ctx: rows)
    session = MagicMock()
    path = tmp_path / "scores.tsv"
    fast.write_method_predictions(session, _ctx(str(uuid.uuid4()), path))
    assert path.read_text() == "P1\tGO:1\t0.9000\nP1\tGO:2\t0.8123\nP2\tGO:1\t0.7000\n"
    session.execute.assert_not_called()
