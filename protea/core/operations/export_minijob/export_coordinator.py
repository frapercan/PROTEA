"""export_coordinator: partition a dataset cell into per-pair KNN minijobs.

When ``PROTEA_EXPORT_MINIJOBS=1`` this operation replaces the monolithic
``export_research_dataset`` path for the fan-out phase. When the env gate
is off (default) it delegates to ``ExportResearchDatasetOperation`` so the
existing CLI / API surface is unchanged.

Pipeline with the gate on:

1. Validate the cell payload.
2. Build one ``export_knn_batch`` message per snapshot pair
   (train pairs: ``len(train_versions) - 1`` consecutive version pairs,
   plus one eval pair from the last train version to the first test version).
3. Publish all messages to ``protea.training.knn-batch``.
4. Mark the coordinator job as *deferred* so BaseWorker does not
   immediately transition it to SUCCEEDED. The last ``export_features_batch``
   worker to finish is responsible for that transition via
   ``update_parent_progress``.
"""

from __future__ import annotations

import os
import uuid
from typing import Any

from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, OperationResult
from protea.core.operations.export_minijob._payloads import (
    ExportCoordinatorPayload,
    PairSpec,
    build_knn_batch_payload,
)

_KNN_BATCH_QUEUE = "protea.training.knn-batch"


class ExportCoordinatorOperation:
    """Coordinator: partition a cell spec into per-pair KNN minijobs.

    Registered on ``protea.training`` (alongside the legacy
    ``export_research_dataset`` operation). When ``PROTEA_EXPORT_MINIJOBS``
    is falsy the operation delegates to the monolithic path so no existing
    workflow is disrupted.
    """

    name = "export_coordinator"
    description = (
        "Minijob coordinator: parse the cell spec and publish one "
        "export_knn_batch message per snapshot pair to "
        "protea.training.knn-batch. Env-gated by PROTEA_EXPORT_MINIJOBS."
    )

    def summarize_payload(self, payload: dict[str, Any], *, session: Session | None = None) -> str:
        p = payload or {}
        bits: list[str] = []
        if p.get("output_name"):
            bits.append(str(p["output_name"]))
        train = p.get("train_versions") or []
        test = p.get("test_versions") or []
        if train:
            bits.append(f"train={train[0]}→{train[-1]}")
        if test:
            bits.append(f"test={','.join(str(v) for v in test)}")
        if p.get("k"):
            bits.append(f"k={p['k']}")
        return " · ".join(bits)

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        if not _minijob_enabled():
            return self._fallback_monolithic(session, payload, emit)

        p = ExportCoordinatorPayload.model_validate(payload)
        raw_job_id = payload.get("_job_id") if isinstance(payload, dict) else None
        coordinator_job_id = str(uuid.UUID(raw_job_id)) if raw_job_id else str(uuid.uuid4())

        pairs = _build_snapshot_pairs(p)
        total = len(pairs)

        emit(
            "export_coordinator.dispatching",
            None,
            {
                "output_name": p.output_name,
                "total_pairs": total,
                "train_versions": p.train_versions,
                "test_versions": p.test_versions,
            },
            "info",
        )

        publish_operations: list[tuple[str, dict[str, Any]]] = []
        for pair_id, pair_type, q_ver, r_ver in pairs:
            spec = PairSpec(
                coordinator_job_id=coordinator_job_id,
                pair_id=pair_id,
                pair_type=pair_type,
                query_version=q_ver,
                ref_version=r_ver,
                total=total,
            )
            publish_operations.append((_KNN_BATCH_QUEUE, build_knn_batch_payload(spec, p)))

        return OperationResult(
            result={
                "coordinator_job_id": coordinator_job_id,
                "total_pairs": total,
                "output_name": p.output_name,
            },
            progress_current=0,
            progress_total=total,
            deferred=True,
            publish_operations=publish_operations,
        )

    def _fallback_monolithic(
        self,
        session: Session,
        payload: dict[str, Any],
        emit: EmitFn,
    ) -> OperationResult:
        """Delegate to the legacy monolithic path (PROTEA_EXPORT_MINIJOBS=0)."""
        from protea.core.operations.export_research_dataset import (
            ExportResearchDatasetOperation,
        )

        emit(
            "export_coordinator.minijob_gate_off",
            None,
            {"msg": "PROTEA_EXPORT_MINIJOBS not set; using monolithic path"},
            "info",
        )
        return ExportResearchDatasetOperation().execute(session, payload, emit=emit)


def _minijob_enabled() -> bool:
    """Return True when ``PROTEA_EXPORT_MINIJOBS`` is set to a truthy value."""
    return os.environ.get("PROTEA_EXPORT_MINIJOBS", "0").strip() not in ("0", "", "false", "False")


def _build_snapshot_pairs(
    p: ExportCoordinatorPayload,
) -> list[tuple[str, str, int, int]]:
    """Derive the list of (pair_id, pair_type, query_version, ref_version) tuples.

    Train pairs: consecutive version windows in ``train_versions``.
    Eval pair: the latest train version as ref, first test version as query.

    Returns a list of 4-tuples ``(pair_id, type, query_ver, ref_ver)``
    where ``pair_id`` is a stable slug usable as an artifact-store key
    component.
    """
    pairs: list[tuple[str, str, int, int]] = []

    # Train pairs: (v[i], v[i+1]) windows
    for i in range(len(p.train_versions) - 1):
        q_ver = p.train_versions[i + 1]
        r_ver = p.train_versions[i]
        pair_id = f"train_{r_ver}_{q_ver}"
        pairs.append((pair_id, "train", q_ver, r_ver))

    # Eval pair: last train as ref, first test as query
    q_ver = p.test_versions[0]
    r_ver = p.train_versions[-1]
    pair_id = f"eval_{r_ver}_{q_ver}"
    pairs.append((pair_id, "eval", q_ver, r_ver))

    return pairs
