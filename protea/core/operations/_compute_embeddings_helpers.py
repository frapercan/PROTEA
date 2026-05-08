"""Pure helpers extracted from ``compute_embeddings`` operation classes.

Keeps the operation file (``compute_embeddings.py``) under the master
plan v3.2 §3 method-LOC ceiling while leaving the operation classes
focused on payload validation, model loading, and the publish path.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from protea.core.operations.compute_embeddings import ComputeEmbeddingsPayload

_BATCH_QUEUE = "protea.embeddings.batch"


def build_batch_dispatch_messages(
    p: ComputeEmbeddingsPayload,
    parent_job_id: uuid.UUID,
    sequence_ids: list[int],
) -> list[tuple[str, dict]]:
    """Partition sequences into per-job batches and build their queue messages.

    Coordinator-side helper for ``ComputeEmbeddingsOperation.execute``:
    splits the full ``sequence_ids`` list into chunks of
    ``p.sequences_per_job`` and emits one ``compute_embeddings_batch``
    message per chunk addressed to the GPU batch queue. The messages
    carry every payload field that the child workers need (no DB
    lookups happen between coordinator and worker).
    """
    batches = [
        sequence_ids[i : i + p.sequences_per_job]
        for i in range(0, len(sequence_ids), p.sequences_per_job)
    ]
    parent_job_str = str(parent_job_id)
    return [
        (
            _BATCH_QUEUE,
            {
                "operation": "compute_embeddings_batch",
                "job_id": parent_job_str,
                "payload": {
                    "embedding_config_id": p.embedding_config_id,
                    "sequence_ids": batch_seq_ids,
                    "parent_job_id": parent_job_str,
                    "device": p.device,
                    "skip_existing": p.skip_existing,
                    "batch_size": p.batch_size,
                },
            },
        )
        for batch_seq_ids in batches
    ]
