"""The write stage of the embedding pipeline, which runs where the state is.

Split out of ``compute_embeddings`` on 2026-10-28 because the smell budget
refused that file at 843 lines against 800, and this was the part of it that
never belonged: ``store_embeddings`` is a DIFFERENT operation on a DIFFERENT
queue (``protea.embeddings.write``, consumed on the host that owns the
database) with no forward pass in it at all. The coordinator and the batch
worker share a model and a device; this shares a session. They sat in one file
by history.

``compute_embeddings`` re-imports the three names, so every caller that reads
them from there keeps working and nothing in the registry moves.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

from pydantic import model_validator
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.core.contracts.parent_progress import update_parent_progress
from protea.core.operations._compute_embeddings_helpers import (
    fold_legacy_store_groups,
    store_group_rows,
)
from protea.core.utils import contract_payload
from protea.infrastructure.orm.models.job import Job, JobStatus


class StoredConfigEmbeddings(ProteaPayload, frozen=True):
    """One config's share of a batch's vectors inside a store message."""

    embedding_config_id: str
    sequences: list[dict[str, Any]]  # [{"sequence_id": int, "chunks": [...]}]


class StoreEmbeddingsPayload(ProteaPayload, frozen=True):
    """Payload published by ComputeEmbeddingsBatchOperation after inference.

    One message per batch carrying every config the pass served, so the write is
    all or nothing; see :func:`~protea.core.operations
    ._compute_embeddings_helpers.build_store_message`. A message in the
    one-config shape is folded into a single group and behaves as it did.
    """

    parent_job_id: str
    skip_existing: bool = True
    groups: list[StoredConfigEmbeddings]

    _accept_legacy = model_validator(mode="before")(fold_legacy_store_groups)


class StoreEmbeddingsOperation:
    """Writes pre-computed embeddings to the DB and updates parent job progress.

    Runs on a CPU-only worker (protea.embeddings.write queue) so the GPU
    worker is free to start the next inference batch immediately.
    """

    name = "store_embeddings"
    description = (
        "CPU child job: bulk-insert pre-computed pgvector embeddings and "
        "atomically increment the parent compute_embeddings job's progress."
    )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        p = payload or {}
        groups = p.get("groups") or [p]
        n = sum(len(g.get("sequences") or []) for g in groups if isinstance(g, dict))
        bits = [f"n={n}"] if n else []
        if len(groups) > 1:
            bits.append(f"{len(groups)} configs")
        return " · ".join(bits)

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = StoreEmbeddingsPayload.model_validate(contract_payload(payload))
        parent_job_id = UUID(p.parent_job_id)

        parent = session.get(Job, parent_job_id)
        if parent is not None and parent.status in (JobStatus.CANCELLED, JobStatus.FAILED):
            emit(
                "store_embeddings.skipped",
                None,
                {"reason": "parent_not_running", "parent_status": parent.status.value},
                "warning",
            )
            return OperationResult(result={"skipped": True})

        # Every config in one call, so every config lands in one transaction.
        counts = store_group_rows(session, p)
        if counts["components_clipped"]:
            emit(
                "store_embeddings.halfvec_clipped",
                None,
                {
                    "components_clipped": counts["components_clipped"],
                    "reason": "embedding_scale too small; values exceeded fp16 range "
                    "and were clipped to [-65504, 65504]",
                },
                "warning",
            )

        emit("store_embeddings.done", None, counts, "info")

        self._update_parent_progress(session, parent_job_id, emit)

        return OperationResult(result=counts)

    def _update_parent_progress(self, session: Session, parent_job_id: UUID, emit: EmitFn) -> None:
        update_parent_progress(
            session,
            parent_job_id,
            emit,
            event_name="store_embeddings.parent_succeeded",
        )


# ---------------------------------------------------------------------------
# Shared model loader (with process-level cache)
# ---------------------------------------------------------------------------

# Keyed by (model_name, model_backend, device) — one entry per worker process.
# Workers are long-lived processes, so the model is loaded once and reused for
# all subsequent batch messages with the same config.  Max 1 entry to avoid
# accumulating multi-GB models in GPU memory when configs change.
