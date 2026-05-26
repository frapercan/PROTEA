"""Scaffolding for the export minijob pipeline (F-EXPORT-MINIJOB.1).

This module registers four stub operations that form the building blocks
of the decomposed export pipeline gated behind ``PROTEA_EXPORT_MINIJOBS``.
All handlers are no-ops in this slice; logic is added in subsequent slices
(F-EXPORT-MINIJOB.2 through .4).

Queue routing:
  - ``protea.training``          -> ExportCoordinatorOperation
  - ``protea.training.knn-batch``-> ExportKnnBatchOperation  (OperationConsumer)
  - ``protea.training.features`` -> ExportFeaturesBatchOperation (OperationConsumer)
  - ``protea.training.write``    -> ExportWriteOperation (OperationConsumer)
"""

from __future__ import annotations

import os
from typing import Any

from protea_contracts import ProteaPayload
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, OperationResult

# ---------------------------------------------------------------------------
# Feature gate
# ---------------------------------------------------------------------------

EXPORT_MINIJOBS_ENV = "PROTEA_EXPORT_MINIJOBS"


def _minijobs_enabled() -> bool:
    """Return True when the ``PROTEA_EXPORT_MINIJOBS`` env var is truthy."""
    return os.environ.get(EXPORT_MINIJOBS_ENV, "0").strip() not in ("", "0", "false", "False")


# ---------------------------------------------------------------------------
# Payloads
# ---------------------------------------------------------------------------


class ExportCoordinatorPayload(ProteaPayload, frozen=True):
    """Coordinator payload: cell spec that will be partitioned into minijobs.

    In this stub slice no fields are required; the full set of cell-spec
    fields (output_name, k, train_versions, …) are added in
    F-EXPORT-MINIJOB.2 when the actual partitioning logic lands.
    """

    cell_spec: dict[str, Any] = {}


class ExportKnnBatchPayload(ProteaPayload, frozen=True):
    """Payload for a single KNN minijob (one snapshot-pair per message).

    Populated by the coordinator in F-EXPORT-MINIJOB.2.
    """

    coordinator_job_id: str = ""
    pair_id: str = ""


class ExportFeaturesBatchPayload(ProteaPayload, frozen=True):
    """Payload for a single feature-generation minijob.

    Populated by ``export_knn_batch`` after KNN results are written to
    the temp artifact store prefix.
    """

    coordinator_job_id: str = ""
    pair_id: str = ""
    knn_artifact_uri: str = ""


class ExportWritePayload(ProteaPayload, frozen=True):
    """Payload for the final assembly + upload step.

    Populated by the coordinator once all per-pair feature shards are done.
    """

    coordinator_job_id: str = ""
    output_name: str = ""


# ---------------------------------------------------------------------------
# Operations
# ---------------------------------------------------------------------------


class ExportCoordinatorOperation:
    """Coordinator stub: receives a cell spec and returns SUCCEEDED (no-op).

    Consumed from ``protea.training``.  In F-EXPORT-MINIJOB.2 this will
    partition the cell into per-pair KNN minijobs and publish them to
    ``protea.training.knn-batch``.

    When ``PROTEA_EXPORT_MINIJOBS=0`` (default) the existing
    ``export_research_dataset`` operation handles everything; this
    coordinator is only dispatched when the flag is set.
    """

    name = "export_coordinator"
    description = (
        "Minijob coordinator stub: receives an export cell spec and delegates "
        "to per-pair KNN, feature, and write workers (env-gated; no-op in F-EXPORT-MINIJOB.1)."
    )

    def summarize_payload(self, payload: dict[str, Any], *, session: Session | None = None) -> str:
        cell = (payload or {}).get("cell_spec") or {}
        output_name = cell.get("output_name", "")
        return f"cell={output_name!r}" if output_name else "stub"

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        ExportCoordinatorPayload.model_validate(payload)
        emit(
            "export_coordinator.noop",
            None,
            {"minijobs_enabled": _minijobs_enabled()},
            "info",
        )
        return OperationResult(result={"status": "stub_noop"})


class ExportKnnBatchOperation:
    """KNN minijob stub: accepts a per-pair payload and emits a noop event.

    Consumed from ``protea.training.knn-batch`` as an OperationConsumer
    (no DB Job row per message, mirrors ``compute_embeddings_batch``).
    Real KNN logic lands in F-EXPORT-MINIJOB.3.
    """

    name = "export_knn_batch"
    description = (
        "KNN minijob stub: runs KNN for one snapshot-pair and forwards "
        "results to the features worker (env-gated; no-op in F-EXPORT-MINIJOB.1)."
    )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        p = payload or {}
        pair_id = p.get("pair_id", "")
        return f"pair={pair_id}" if pair_id else "stub"

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        ExportKnnBatchPayload.model_validate(payload)
        emit("export_knn_batch.noop", None, {}, "info")
        return OperationResult(result={"status": "stub_noop"})


class ExportFeaturesBatchOperation:
    """Feature minijob stub: accepts a per-pair payload and emits a noop event.

    Consumed from ``protea.training.features`` as an OperationConsumer.
    Real feature-generation logic lands in F-EXPORT-MINIJOB.3.
    """

    name = "export_features_batch"
    description = (
        "Feature-generation minijob stub: runs CPU feature computation for "
        "one snapshot-pair (env-gated; no-op in F-EXPORT-MINIJOB.1)."
    )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        p = payload or {}
        pair_id = p.get("pair_id", "")
        return f"pair={pair_id}" if pair_id else "stub"

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        ExportFeaturesBatchPayload.model_validate(payload)
        emit("export_features_batch.noop", None, {}, "info")
        return OperationResult(result={"status": "stub_noop"})


class ExportWriteOperation:
    """Write minijob stub: assembles shards and uploads parquet (future).

    Consumed from ``protea.training.write`` as an OperationConsumer.
    Real assembly + upload logic lands in F-EXPORT-MINIJOB.4.
    """

    name = "export_write"
    description = (
        "Export write stub: assembles per-pair feature shards and uploads "
        "the final dataset to the artifact store (env-gated; no-op in F-EXPORT-MINIJOB.1)."
    )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        p = payload or {}
        output_name = p.get("output_name", "")
        return f"output={output_name!r}" if output_name else "stub"

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        ExportWritePayload.model_validate(payload)
        emit("export_write.noop", None, {}, "info")
        return OperationResult(result={"status": "stub_noop"})
