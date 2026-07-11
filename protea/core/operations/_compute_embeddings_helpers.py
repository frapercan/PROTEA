"""Pure helpers extracted from ``compute_embeddings`` operation classes.

Keeps the operation file (``compute_embeddings.py``) under the master
plan v3.2 §3 method-LOC ceiling while leaving the operation classes
focused on payload validation, model loading, and the publish path.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any

from sqlalchemy.orm import Session

from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
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

# pgvector HALFVEC is fp16; the largest finite fp16 magnitude is 65504. Anything
# beyond this becomes +/-inf, which Postgres rejects
# (``psycopg.errors.DataException: infinite value not allowed in halfvec``).
HALFVEC_FP16_MAX = 65504.0


def fetch_embedding_scale(session: Session, config_id: uuid.UUID) -> float:
    """Return the owning config's uniform ``embedding_scale`` (constant per config).

    Defaults to ``1.0`` (no-op) for a missing config or a missing/zero value,
    so every existing config keeps its byte-for-byte store behaviour.
    """
    config = session.get(EmbeddingConfig, config_id)
    if config is None:
        return 1.0
    scale = float(getattr(config, "embedding_scale", 1.0) or 1.0)
    return scale if scale > 0.0 else 1.0


def scale_and_clip_embedding(
    vector: list[float], scale: float
) -> tuple[list[float], int]:
    """Divide a per-sequence embedding by ``scale`` and clip it to the fp16 range.

    Returns ``(scaled_vector, n_clipped)``. ``scale == 1.0`` (the default for
    every existing config, including the champion) is a pure passthrough: the
    exact input list object is returned unchanged, so no numpy round-trip and no
    clip can perturb those values. For any non-default scale the vector is
    divided uniformly, then clipped to ``[-65504, 65504]`` as a safety net so an
    over-small configured scale can never let an inf reach the halfvec column;
    ``n_clipped`` counts how many components were clipped (a WARN signal that the
    scale was too small).

    A uniform per-config divisor is safe for every downstream consumer:
    cosine-KNN is scale-invariant and the per-dim z-score standardisation
    absorbs a uniform scale, so ``embedding / scale`` is equivalent to the raw
    embedding.
    """
    if scale == 1.0:
        return vector, 0
    import numpy as np

    arr = np.asarray(vector, dtype=np.float64) / scale
    n_clipped = int(np.count_nonzero(np.abs(arr) > HALFVEC_FP16_MAX))
    if n_clipped:
        np.clip(arr, -HALFVEC_FP16_MAX, HALFVEC_FP16_MAX, out=arr)
    return arr.tolist(), n_clipped


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
) -> tuple[list[dict], int, int, int]:
    """Materialise SequenceEmbedding insert rows for a store-embeddings batch.

    Iterates ``p.sequences`` and skips entries whose ``(sequence_id,
    config_id)`` pair already exists when ``p.skip_existing`` is true;
    otherwise deletes the existing rows so the bulk insert can replace
    them. Each stored vector is divided by the owning config's uniform
    ``embedding_scale`` and clipped to the fp16 halfvec range (see
    :func:`scale_and_clip_embedding`); ``scale == 1.0`` (the default) is a
    no-op. Returns ``(rows, embeddings_stored, sequences_skipped,
    components_clipped)``; callers run the bulk insert, emit a WARN when
    ``components_clipped`` is non-zero, and update per-job progress.
    """
    scale = fetch_embedding_scale(session, config_id)
    rows: list[dict] = []
    embeddings_stored = 0
    sequences_skipped = 0
    components_clipped = 0
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
            scaled_vector, n_clipped = scale_and_clip_embedding(chunk["vector"], scale)
            components_clipped += n_clipped
            rows.append(
                {
                    "sequence_id": sequence_id,
                    "embedding_config_id": config_id,
                    "chunk_index_s": chunk["chunk_index_s"],
                    "chunk_index_e": chunk.get("chunk_index_e"),
                    "embedding": scaled_vector,
                    "embedding_dim": chunk["embedding_dim"],
                }
            )
            embeddings_stored += 1
    return rows, embeddings_stored, sequences_skipped, components_clipped


def t5_forward_pass(model: Any, input_ids: Any, attention_mask: Any) -> Any:
    """Run a T5 encoder forward pass under ``no_grad``; return ``hidden_states``.

    Lazy-imports ``torch`` so this module stays importable on machines
    without it (e.g. the API process where embedding code is never
    exercised). Frees the ``outputs`` reference and triggers a CUDA
    cache flush before returning so the caller can reuse the residual
    GPU memory for the per-sequence pool step.

    Pulled out of ``_embed_t5`` to keep that backend entry point under
    the §3 60-LOC ceiling.
    """
    import torch

    with torch.no_grad():
        outputs = model(
            input_ids=input_ids,
            attention_mask=attention_mask,
            output_hidden_states=True,
        )
    hidden_states = outputs.hidden_states  # tuple of (B, L, D)
    del outputs
    torch.cuda.empty_cache()
    return hidden_states
