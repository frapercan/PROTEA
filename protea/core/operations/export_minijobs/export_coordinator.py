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

Failure semantics (F-OPS-COORD-FAIL-PROPAGATE, 2026-05-26):

1. **Fast-fail pre-flight.** Invalid payload values that we know will
   cause every child to raise a non-retryable error (today: any
   ``search_backend`` outside ``{"numpy", "faiss"}``) are rejected
   inside ``_dispatch_minijobs`` before a single child is published.
   The coordinator raises ``ValueError`` and ``BaseWorker`` transitions
   the parent job to ``FAILED`` immediately. No zombie ``RUNNING``
   rows, no doomed dispatch wave.
2. **Aggregate failure.** When a child operation does end up raising
   despite the pre-flight (race, partial outage, transient-turned-
   permanent), ``protea.core.contracts.parent_failure.report_child_failure``
   is invoked by the consumer to accumulate the failure on
   ``parent.meta`` and transition the parent to ``FAILED`` once every
   dispatched child has failed (configurable via ``failure_ratio``).
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

#: Backends the export pipeline actually implements end-to-end.
#: ``protea_method.knn_search`` ships numpy, faiss and torch backends;
#: the torch path was re-enabled in protea-method 5dd737d (PR #564)
#: alongside the cu128 wheel default in scripts/install_gpu_torch.sh.
_VALID_SEARCH_BACKENDS = frozenset({"numpy", "faiss", "torch"})


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

        _preflight_search_backend(p, coordinator_job_id, n_total, emit)

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
                # ``dispatched_total`` is convenience metadata for the
                # aggregate-failure path; the source of truth is the
                # parent's ``progress_total`` column.
                "dispatched_total": n_total,
            },
            progress_current=0,
            progress_total=n_total,
            deferred=True,
            publish_operations=operations,
        )


def _preflight_search_backend(
    p: ExportCoordinatorPayload,
    coordinator_job_id: str,
    n_total: int,
    emit: EmitFn,
) -> None:
    """Refuse the dispatch wave when ``search_backend`` is unsupported.

    F-OPS-COORD-FAIL-PROPAGATE: letting an unsupported backend reach
    the children produced the 2026-05-26 incident — 5 coordinators
    stayed ``RUNNING`` for >1h while every child raised
    ``ValueError("Unknown search backend: 'torch'…")``. Failing here
    surfaces the misconfig as a clean ``job.failed`` instead.
    """
    if p.search_backend in _VALID_SEARCH_BACKENDS:
        return
    valid = ", ".join(sorted(_VALID_SEARCH_BACKENDS))
    reason = (
        f"first child failed with non-retryable error: "
        f"unsupported search_backend={p.search_backend!r}. "
        f"Choose one of: {valid}."
    )
    emit(
        "export_coordinator.failed",
        reason,
        {
            "output_name": p.output_name,
            "coordinator_job_id": coordinator_job_id,
            "search_backend": p.search_backend,
            "valid_search_backends": sorted(_VALID_SEARCH_BACKENDS),
            "n_minijobs_planned": n_total,
        },
        "error",
    )
    raise ValueError(reason)


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
