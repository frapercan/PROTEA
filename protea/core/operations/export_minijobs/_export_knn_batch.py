"""Stub: export_knn_batch OperationConsumer.

Full implementation in F-EXPORT-MINIJOB.3.  This stub:
- Accepts the per-pair KNN payload.
- Emits a ``pair_knn_done`` event against the parent job.
- Returns immediately (no actual KNN work yet).
"""

from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload


class ExportKnnBatchPayload(ProteaPayload, frozen=True):
    """Payload for a single per-pair KNN minijob dispatched by the coordinator."""

    coordinator_job_id: str
    pair_id: str
    train_snapshot_id: int
    test_snapshot_id: int
    embedding_config_id: str
    annotation_set_id: str
    ontology_snapshot_id: str
    k: int = 5
    search_backend: str = "faiss"


class ExportKnnBatchOperation:
    """Stub: run KNN for one snapshot pair and emit pair_knn_done.

    F-EXPORT-MINIJOB.3 replaces this stub with the real GPU KNN path.
    """

    name = "export_knn_batch"
    description = (
        "OperationConsumer: run KNN for one snapshot pair "
        "(stub; full impl in F-EXPORT-MINIJOB.3)."
    )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        p = payload or {}
        parts: list[str] = []
        if p.get("pair_id"):
            parts.append(f"pair={p['pair_id']}")
        if p.get("k"):
            parts.append(f"k={p['k']}")
        return " · ".join(parts)

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = ExportKnnBatchPayload.model_validate(payload)
        emit(
            "export_knn_batch.noop",
            None,
            {
                "coordinator_job_id": p.coordinator_job_id,
                "pair_id": p.pair_id,
                "note": "stub; F-EXPORT-MINIJOB.3 implements real KNN",
            },
            "info",
        )
        emit(
            "pair_knn_done",
            None,
            {
                "coordinator_job_id": p.coordinator_job_id,
                "pair_id": p.pair_id,
                "temp_uri": None,
            },
            "info",
        )
        return OperationResult(result={"pair_id": p.pair_id, "stub": True})
