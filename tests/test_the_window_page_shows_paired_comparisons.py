"""The window page shows the paired comparisons of its results.

``compare_paired_panels`` writes no row of its own; its job is its record. The
window's comparisons are read from those jobs: the latest whole-panel comparison
of each pair of the window's results, with per panel the difference, its
interval, the interval's method, the verdict and the paired population.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.orm import Session

from protea.infrastructure.orm.models.annotation.evaluation_result import EvaluationResult
from protea.infrastructure.orm.models.job import Job, JobEvent, JobStatus
from protea.services._annotations_paired_helpers import (
    comparison_to_dict,
    list_paired_comparisons_data,
)
from tests.helpers.windows import make_window
from tests.test_run_count_map import pg  # noqa: F401

PANEL = {
    "delta": 0.012,
    "ci_low": 0.004,
    "ci_high": 0.02,
    "interval_method": "bca",
    "verdict": "a_greater",
    "status": "ok",
    "n_paired": 22681,
    "effect_of_interest": 0.01,
    "minimum_detectable_effect": 0.0034,
    "diagnostics": {"kept": "in the job only"},
}


def _result(delta: float) -> dict[str, Any]:
    return {
        "method_axis": ["embedding_config_id"],
        "operating_point": "reselected_per_resample",
        "weighting": "ia_weighted",
        "n_resamples": 2000,
        "seed": 0,
        "confidence": 0.95,
        "panels": {"PK:BPO": {**PANEL, "delta": delta}},
    }


def test_a_comparison_keeps_what_the_page_shows() -> None:
    finished = datetime(2026, 11, 21, 12, tzinfo=UTC)
    row = {
        "job_id": "j1",
        "evaluation_result_id": "a",
        "baseline_evaluation_result_id": "b",
        "finished_at": finished,
        "result": _result(0.012),
    }
    shown = comparison_to_dict(row)
    assert shown["panels"]["PK:BPO"] == {k: v for k, v in PANEL.items() if k != "diagnostics"}
    assert shown["method_axis"] == ["embedding_config_id"]
    assert shown["finished_at"] == finished.isoformat()


def _job(pg: Session, payload: dict[str, Any], result: dict[str, Any], minute: int) -> uuid.UUID:  # noqa: F811
    job = Job(
        operation="compare_paired_panels",
        queue_name="protea.evaluations",
        status=JobStatus.SUCCEEDED,
        payload=payload,
        finished_at=datetime(2026, 11, 21, 12, tzinfo=UTC) + timedelta(minutes=minute),
    )
    pg.add(job)
    pg.flush()
    pg.add(JobEvent(job_id=job.id, event="job.succeeded", fields={"result": result}))
    pg.flush()
    return job.id


@pytest.mark.integration
def test_the_window_shows_the_latest_whole_panel_comparison_of_each_pair(pg: Session) -> None:  # noqa: F811
    window = make_window(pg, snapshot_version="paired")
    other = make_window(pg, snapshot_version="paired-elsewhere")
    # The results' prediction sets are beside the point here.
    pg.execute(text("SET session_replication_role = 'replica'"))
    ids = []
    for eset in (window, window, other):
        r = EvaluationResult(evaluation_set_id=eset, prediction_set_id=uuid.uuid4(), results={})
        pg.add(r)
        pg.flush()
        ids.append(str(r.id))
    a, b, foreign = ids
    pair = {"evaluation_result_id": a, "baseline_evaluation_result_id": b}
    _job(pg, pair, _result(0.010), minute=0)
    latest = _job(pg, pair, _result(0.012), minute=5)
    _job(pg, {**pair, "restrict_to_stratum": {"homology": ">90"}}, _result(0.5), minute=9)
    _job(pg, {"evaluation_result_id": foreign, "baseline_evaluation_result_id": b}, _result(0.9), 1)
    pg.commit()

    shown = list_paired_comparisons_data(pg, window)

    assert [(c["job_id"], c["panels"]["PK:BPO"]["delta"]) for c in shown] == [(str(latest), 0.012)]
    assert list_paired_comparisons_data(pg, other) == []
