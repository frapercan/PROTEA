"""Each result names how its scores were read before cafaeval thresholded them.

Via 1 evaluates every scored set twice, read raw (LAFA's headline) and by
percentile rank (ADR-D55's comparison reading). Both results sit on one window
over one prediction set, and the record said nothing of the reading, so the
window page could not tell them apart. The evaluator now records it with the
threshold step (``results.reading``), and the results list names it, falling
back to the evaluating job's payload for a result written before.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

from protea.services._annotations_evaluation_helpers import evaluation_result_to_dict


def _result(results: dict[str, Any]) -> Any:
    return SimpleNamespace(
        id=uuid.uuid4(),
        evaluation_set_id=uuid.uuid4(),
        prediction_set_id=uuid.uuid4(),
        scoring_config_id=None,
        reranker_model_id=None,
        reranker_config=None,
        job_id=uuid.uuid4(),
        created_at=datetime(2026, 11, 21, tzinfo=UTC),
        frame=None,
        frame_digest=None,
        temporal_window=None,
        arms_enabled=None,
        leakage_role=None,
        results=results,
    )


def test_the_record_names_the_reading() -> None:
    record = {"reading": {"score_reading": "percentile_rank", "th_step": 0.01}}
    shown = evaluation_result_to_dict(_result(record), score_reading="raw")  # type: ignore[arg-type]
    assert shown["score_reading"] == "percentile_rank"


def test_an_older_result_takes_the_reading_its_job_was_asked_for() -> None:
    shown = evaluation_result_to_dict(_result({}), score_reading="percentile_rank")  # type: ignore[arg-type]
    assert shown["score_reading"] == "percentile_rank"
    assert evaluation_result_to_dict(_result({}))["score_reading"] is None  # type: ignore[arg-type]
