"""Stub: export_features_batch OperationConsumer.

Full implementation in F-EXPORT-MINIJOB.3.  This stub:
- Accepts the per-pair features payload (including the temp KNN URI).
- Emits a ``pair_features_done`` event against the parent job.
- Returns immediately (no actual feature computation yet).
"""

from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload


class ExportFeaturesBatchPayload(ProteaPayload, frozen=True):
    """Payload for a single per-pair feature-computation minijob."""

    coordinator_job_id: str
    pair_id: str
    temp_knn_uri: str | None
    embedding_config_id: str
    annotation_set_id: str
    ontology_snapshot_id: str
    compute_alignments: bool = False
    compute_taxonomy: bool = False
    expand_votes_to_ancestors: bool = False
    use_embedding_pca: bool = False


class ExportFeaturesBatchOperation:
    """Stub: compute features for one snapshot pair and emit pair_features_done.

    F-EXPORT-MINIJOB.3 replaces this stub with the real CPU feature path.
    """

    name = "export_features_batch"
    description = (
        "OperationConsumer: compute CPU features for one snapshot pair "
        "(stub; full impl in F-EXPORT-MINIJOB.3)."
    )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        p = payload or {}
        parts: list[str] = []
        if p.get("pair_id"):
            parts.append(f"pair={p['pair_id']}")
        return " · ".join(parts)

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = ExportFeaturesBatchPayload.model_validate(payload)
        emit(
            "export_features_batch.noop",
            None,
            {
                "coordinator_job_id": p.coordinator_job_id,
                "pair_id": p.pair_id,
                "note": "stub; F-EXPORT-MINIJOB.3 implements real features",
            },
            "info",
        )
        emit(
            "pair_features_done",
            None,
            {
                "coordinator_job_id": p.coordinator_job_id,
                "pair_id": p.pair_id,
                "temp_uri": None,
            },
            "info",
        )
        return OperationResult(result={"pair_id": p.pair_id, "stub": True})
