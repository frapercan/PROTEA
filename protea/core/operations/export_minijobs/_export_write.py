"""Stub: export_write OperationConsumer.

Full implementation in F-EXPORT-MINIJOB.4.  This stub:
- Accepts the write payload (coordinator_job_id + all temp shard URIs).
- Emits an ``export_write_done`` event against the parent job.
- Returns immediately (no actual parquet assembly or upload yet).
"""

from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload


class ExportWritePayload(ProteaPayload, frozen=True):
    """Payload for the final write / assembly minijob."""

    coordinator_job_id: str
    output_name: str
    embedding_config_id: str
    ontology_snapshot_id: str
    annotation_set_id: str
    k: int = 5
    annotation_source: str = "goa"
    train_pair_uris: list[str] = []
    eval_pair_uri: str | None = None


class ExportWriteOperation:
    """Stub: assemble shards, upload, register Dataset.

    F-EXPORT-MINIJOB.4 replaces this stub with the real assembly path.
    """

    name = "export_write"
    description = (
        "OperationConsumer: assemble shards and upload dataset "
        "(stub; full impl in F-EXPORT-MINIJOB.4)."
    )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        p = payload or {}
        parts: list[str] = []
        if p.get("output_name"):
            parts.append(str(p["output_name"]))
        return " · ".join(parts)

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = ExportWritePayload.model_validate(payload)
        emit(
            "export_write.noop",
            None,
            {
                "coordinator_job_id": p.coordinator_job_id,
                "output_name": p.output_name,
                "note": "stub; F-EXPORT-MINIJOB.4 implements real assembly",
            },
            "info",
        )
        emit(
            "export_write_done",
            None,
            {
                "coordinator_job_id": p.coordinator_job_id,
                "output_name": p.output_name,
                "dataset_id": None,
            },
            "info",
        )
        return OperationResult(result={"output_name": p.output_name, "stub": True})
