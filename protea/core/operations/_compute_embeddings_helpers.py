"""Pure helpers extracted from ``compute_embeddings`` operation classes.

Keeps the operation file (``compute_embeddings.py``) under the master
plan v3.2 §3 method-LOC ceiling while leaving the operation classes
focused on payload validation, model loading, and the publish path.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

from sqlalchemy.orm import Session

from protea.infrastructure.orm.models.embedding.sequence_embedding import SequenceEmbedding

if TYPE_CHECKING:
    from protea.core.operations.compute_embeddings import (
        ChunkEmbedding,
        ComputeEmbeddingsBatchPayload,
        ComputeEmbeddingsPayload,
        StoreEmbeddingsPayload,
    )
    from protea.infrastructure.orm.models.sequence.sequence import Sequence

_BATCH_QUEUE = "protea.embeddings.batch"
_WRITE_QUEUE = "protea.embeddings.write"


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


def serialize_inferred_chunks(
    sequences: list[Sequence],
    batch_chunks: list[list[ChunkEmbedding]],
) -> list[dict]:
    """Build per-sequence dicts the store_embeddings worker consumes.

    Pairs each input ``Sequence`` row with its inferred chunk list and
    flattens each ``ChunkEmbedding`` into JSON-friendly fields. The
    write worker uses these dicts directly without re-fetching from
    the DB.
    """
    return [
        {
            "sequence_id": seq.id,
            "chunks": [
                {
                    "chunk_index_s": c.chunk_index_s,
                    "chunk_index_e": c.chunk_index_e,
                    "vector": c.vector.tolist(),
                    "embedding_dim": int(c.vector.shape[0]),
                }
                for c in chunks
            ],
        }
        for seq, chunks in zip(sequences, batch_chunks, strict=False)
    ]


def build_store_message(
    parent_job_id: uuid.UUID,
    p: ComputeEmbeddingsBatchPayload,
    write_sequences: list[dict],
) -> tuple[str, dict]:
    """Build the queue tuple that hands off inferred batches to the write worker."""
    return (
        _WRITE_QUEUE,
        {
            "operation": "store_embeddings",
            "job_id": str(parent_job_id),
            "payload": {
                "parent_job_id": str(parent_job_id),
                "embedding_config_id": p.embedding_config_id,
                "skip_existing": p.skip_existing,
                "sequences": write_sequences,
            },
        },
    )


def build_embedding_rows(
    session: Session,
    p: StoreEmbeddingsPayload,
    config_id: uuid.UUID,
) -> tuple[list[dict], int, int]:
    """Materialise SequenceEmbedding insert rows for a store-embeddings batch.

    Iterates ``p.sequences`` and skips entries whose ``(sequence_id,
    config_id)`` pair already exists when ``p.skip_existing`` is true;
    otherwise deletes the existing rows so the bulk insert can replace
    them. Returns ``(rows, embeddings_stored, sequences_skipped)``;
    callers run the bulk insert + per-job-progress update.
    """
    rows: list[dict] = []
    embeddings_stored = 0
    sequences_skipped = 0
    for seq_data in p.sequences:
        sequence_id = seq_data["sequence_id"]
        chunks = seq_data["chunks"]
        if p.skip_existing:
            existing = (
                session.query(SequenceEmbedding)
                .filter_by(sequence_id=sequence_id, embedding_config_id=config_id)
                .first()
            )
            if existing is not None:
                sequences_skipped += 1
                continue
        else:
            session.query(SequenceEmbedding).filter_by(
                sequence_id=sequence_id, embedding_config_id=config_id
            ).delete()
        for chunk in chunks:
            rows.append(
                {
                    "sequence_id": sequence_id,
                    "embedding_config_id": config_id,
                    "chunk_index_s": chunk["chunk_index_s"],
                    "chunk_index_e": chunk.get("chunk_index_e"),
                    "embedding": chunk["vector"],
                    "embedding_dim": chunk["embedding_dim"],
                }
            )
            embeddings_stored += 1
    return rows, embeddings_stored, sequences_skipped
