"""Write worker: bulk-insert GOPrediction rows + close parent job.

Extracted from the monolithic ``predict_go_terms.py`` as part of T2B.6.
"""

from __future__ import annotations

import uuid
from typing import Any
from uuid import UUID

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, OperationResult
from protea.core.contracts.parent_progress import update_parent_progress
from protea.core.operations.predict_go_terms._common import (
    StorePredictionsPayload,
    _row_from_prediction,
)
from protea.infrastructure.orm.models.embedding.go_prediction import GOPrediction
from protea.infrastructure.orm.models.job import Job, JobStatus


class StorePredictionsOperation:
    """Write worker: bulk-inserts GOPrediction rows and updates parent job progress.

    Receives serialized prediction dicts from PredictGOTermsBatchOperation,
    inserts them into the DB, and atomically increments the parent Job's
    progress counter.  When the last batch is stored the parent Job is closed
    as SUCCEEDED.
    """

    name = "store_predictions"
    description = (
        "CPU child job: bulk-insert GOPrediction rows from a batch and "
        "atomically advance the parent predict_go_terms progress counter."
    )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        p = payload or {}
        n = len(p.get("predictions") or [])
        return f"n={n}" if n else ""

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = StorePredictionsPayload.model_validate(payload)
        parent_job_id = UUID(p.parent_job_id)
        prediction_set_id = uuid.UUID(p.prediction_set_id)

        parent = session.get(Job, parent_job_id)
        if parent is not None and parent.status in (JobStatus.CANCELLED, JobStatus.FAILED):
            emit(
                "store_predictions.skipped", None, {"parent_job_id": str(parent_job_id)}, "warning"
            )
            return OperationResult(result={"skipped": True})

        if p.predictions:
            session.execute(
                pg_insert(GOPrediction).on_conflict_do_nothing(),
                [_row_from_prediction(pred, prediction_set_id) for pred in p.predictions],
            )

        emit(
            "store_predictions.done",
            None,
            {
                "predictions_inserted": len(p.predictions),
                "parent_job_id": str(parent_job_id),
            },
            "info",
        )

        if p.is_final_chunk:
            self._update_parent_progress(session, parent_job_id, emit)

        return OperationResult(result={"predictions_inserted": len(p.predictions)})

    def _update_parent_progress(self, session: Session, parent_job_id: UUID, emit: EmitFn) -> None:
        update_parent_progress(
            session,
            parent_job_id,
            emit,
            event_name="store_predictions.parent_succeeded",
        )
