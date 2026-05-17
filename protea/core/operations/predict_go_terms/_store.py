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
    _WRITE_QUEUE,
    StorePredictionsPayload,
    _row_from_prediction,
)
from protea.infrastructure.orm.models.embedding.go_prediction import GOPrediction
from protea.infrastructure.orm.models.job import Job, JobStatus


def chunked_publish(
    *,
    parent_job_id: UUID,
    prediction_set_id: uuid.UUID,
    prediction_dicts: list[dict[str, Any]],
) -> list[tuple[str, dict[str, Any]]]:
    """Split predictions into RabbitMQ-sized chunks for the write queue.

    RabbitMQ caps message size at 128 MB; ancestor-expanded batches
    serialise to ~250-300 MB and silently land in the dead-letter
    queue. Splitting into ~10k-row chunks (~20-25 MB each) keeps the
    broker happy and lets the parent job's batch counter advance only
    on the final chunk via ``is_final_chunk``.
    """
    from protea.config.tuning import get_tuning

    store_chunk_size = get_tuning().operation.store_chunk_size
    chunks: list[list[dict[str, Any]]] = [
        prediction_dicts[s : s + store_chunk_size]
        for s in range(0, len(prediction_dicts), store_chunk_size)
    ] or [[]]
    return [
        (
            _WRITE_QUEUE,
            {
                "operation": "store_predictions",
                "job_id": str(parent_job_id),
                "payload": {
                    "parent_job_id": str(parent_job_id),
                    "prediction_set_id": str(prediction_set_id),
                    "predictions": chunk,
                    "is_final_chunk": i == len(chunks) - 1,
                },
            },
        )
        for i, chunk in enumerate(chunks)
    ]


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
