"""Export coordinator: partition cell spec into per-snapshot KNN minijobs.

Implements F-EXPORT-MINIJOB.2.

When ``PROTEA_EXPORT_MINIJOBS=0`` (the default) the coordinator
delegates directly to the legacy ``ExportResearchDatasetOperation`` so
today's monolithic path is completely unaffected.

When ``PROTEA_EXPORT_MINIJOBS=1``:
1. Validate the cell payload.
2. Partition snapshot versions into per-snapshot KNN minijob payloads:
   - One ``export_knn_batch`` per training version (len(train_versions)).
   - One ``export_knn_batch`` for the eval (test) version.
   Total dispatched = len(train_versions) + len(test_versions).
3. Publish each to ``protea.training.knn-batch``.
4. Return a deferred ``OperationResult``: the coordinator stays RUNNING
   until all minijobs complete via the ``update_parent_progress`` helper
   (one increment per ``pair_knn_done`` event that lands from a batch
   worker that calls this module's ``report_pair_done`` helper).

Failure semantics (implemented in F-EXPORT-MINIJOB.3 when real batch
workers run): any batch worker that raises publishes a ``kill`` message
to the remaining ``protea.training.knn-batch`` queue entries. The
coordinator's parent_progress update check will not reach total, so the
reaper eventually times it out; the coordinator itself has no
synchronous failure path here (it dispatched and deferred).
"""

from __future__ import annotations

import os
import uuid
from typing import Any

from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload

_KNN_BATCH_QUEUE = "protea.training.knn-batch"
_FEATURES_QUEUE = "protea.training.features"
_WRITE_QUEUE = "protea.training.write"


class ExportCoordinatorPayload(ProteaPayload, frozen=True):
    """Cell specification for one export run."""

    output_name: str
    embedding_config_id: str
    annotation_set_id: str
    ontology_snapshot_id: str
    train_versions: list[int]
    test_versions: list[int]
    annotation_source: str = "goa"
    k: int = 5
    search_backend: str = "faiss"
    compute_alignments: bool = False
    compute_taxonomy: bool = False
    expand_votes_to_ancestors: bool = False
    use_embedding_pca: bool = False


class ExportCoordinatorOperation:
    """Coordinator: dispatch per-snapshot KNN minijobs for one export cell.

    Env gate: ``PROTEA_EXPORT_MINIJOBS`` (default ``"0"``).
    When off, delegates to ``ExportResearchDatasetOperation`` in-process.
    """

    name = "export_coordinator"
    description = (
        "Coordinator: partition an export cell into per-snapshot KNN minijobs "
        "(env-gated; delegates to monolithic path when PROTEA_EXPORT_MINIJOBS=0)."
    )

    def summarize_payload(self, payload: dict[str, Any], *, session: Session | None = None) -> str:
        p = payload or {}
        parts: list[str] = []
        if p.get("output_name"):
            parts.append(str(p["output_name"]))
        train = p.get("train_versions") or []
        test = p.get("test_versions") or []
        if train:
            parts.append(f"train_v={train[0]}..{train[-1]}(n={len(train)})")
        if test:
            parts.append(f"test_v={','.join(str(v) for v in test)}")
        if p.get("k"):
            parts.append(f"k={p['k']}")
        return " · ".join(parts)

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        if not _minijobs_enabled():
            return self._delegate_legacy(session, payload, emit=emit)
        return self._dispatch_minijobs(session, payload, emit=emit)

    def _delegate_legacy(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        """Fall through to monolithic ExportResearchDatasetOperation."""
        from protea.core.operations.export_research_dataset import (
            ExportResearchDatasetOperation,
        )

        emit(
            "export_coordinator.legacy_delegate",
            None,
            {"reason": "PROTEA_EXPORT_MINIJOBS=0"},
            "info",
        )
        return ExportResearchDatasetOperation().execute(session, payload, emit=emit)

    def _dispatch_minijobs(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = ExportCoordinatorPayload.model_validate(payload)
        coordinator_job_id = payload.get("_job_id") or str(uuid.uuid4())
        n_total = len(p.train_versions) + len(p.test_versions)

        emit(
            "export_coordinator.dispatching",
            None,
            {
                "output_name": p.output_name,
                "coordinator_job_id": coordinator_job_id,
                "train_versions": p.train_versions,
                "test_versions": p.test_versions,
                "n_minijobs": n_total,
            },
            "info",
        )

        operations = _build_all_knn_messages(p, coordinator_job_id)
        return OperationResult(
            result={
                "output_name": p.output_name,
                "coordinator_job_id": coordinator_job_id,
                "n_minijobs": n_total,
                "train_versions": p.train_versions,
                "test_versions": p.test_versions,
            },
            progress_current=0,
            progress_total=n_total,
            deferred=True,
            publish_operations=operations,
        )


def _minijobs_enabled() -> bool:
    """Return True when ``PROTEA_EXPORT_MINIJOBS=1``."""
    return os.environ.get("PROTEA_EXPORT_MINIJOBS", "0").strip() == "1"


def _build_all_knn_messages(
    p: ExportCoordinatorPayload,
    coordinator_job_id: str,
) -> list[tuple[str, dict[str, Any]]]:
    """Build all per-snapshot KNN minijob dispatch tuples for one cell."""
    ops: list[tuple[str, dict[str, Any]]] = []
    for version in p.train_versions:
        ops.append(
            (
                _KNN_BATCH_QUEUE,
                _build_knn_batch_msg(
                    coordinator_job_id=coordinator_job_id,
                    pair_id=f"train-{version}",
                    snapshot_version=version,
                    p=p,
                    is_eval=False,
                ),
            )
        )
    for version in p.test_versions:
        ops.append(
            (
                _KNN_BATCH_QUEUE,
                _build_knn_batch_msg(
                    coordinator_job_id=coordinator_job_id,
                    pair_id=f"eval-{version}",
                    snapshot_version=version,
                    p=p,
                    is_eval=True,
                ),
            )
        )
    return ops


def _build_knn_batch_msg(
    *,
    coordinator_job_id: str,
    pair_id: str,
    snapshot_version: int,
    p: ExportCoordinatorPayload,
    is_eval: bool,
) -> dict[str, Any]:
    """Serialise one KNN minijob dispatch payload."""
    return {
        "operation": "export_knn_batch",
        "job_id": coordinator_job_id,
        "payload": {
            "coordinator_job_id": coordinator_job_id,
            "pair_id": pair_id,
            "train_snapshot_id": snapshot_version,
            "test_snapshot_id": snapshot_version,
            "embedding_config_id": p.embedding_config_id,
            "annotation_set_id": p.annotation_set_id,
            "ontology_snapshot_id": p.ontology_snapshot_id,
            "k": p.k,
            "search_backend": p.search_backend,
            "is_eval": is_eval,
            "output_name": p.output_name,
            "annotation_source": p.annotation_source,
            "compute_alignments": p.compute_alignments,
            "compute_taxonomy": p.compute_taxonomy,
            "expand_votes_to_ancestors": p.expand_votes_to_ancestors,
            "use_embedding_pca": p.use_embedding_pca,
        },
    }
