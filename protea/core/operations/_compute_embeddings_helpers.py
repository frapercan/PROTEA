"""Pure helpers extracted from ``compute_embeddings`` operation classes.

Keeps the operation file (``compute_embeddings.py``) under the master
plan v3.2 §3 method-LOC ceiling while leaving the operation classes
focused on payload validation, model loading, and the publish path.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any

from sqlalchemy.dialects.postgresql import insert as pg_insert
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

_LEGACY_CONFIG_KEY = "embedding_config_id"
_GROUP_CONFIG_KEY = "embedding_config_ids"

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


def _residue_budget(sequences: list[Sequence], config: EmbeddingConfig) -> dict[str, int]:
    """What the forward pass actually consumed, so cost has a comparable unit.

    The event carried a clock and no residues, and the backends carried residues
    and no clock, so residues per second could not be formed from either. It is
    the unit that matters here because the corpus is heavy-tailed -- median 318
    residues against a maximum of 35,991 -- and the layer grid compares four
    lineages with different tokenizers. A batch of 256 short sequences and a
    batch of 256 long ones are two different jobs under one name, so sequences
    per second is not comparable across them.

    Two numbers rather than one, because they differ and the difference is
    itself unmeasured. ``residues_available`` is what the corpus holds;
    ``residues_processed`` is what the model saw. Without chunking a sequence
    longer than ``max_length`` is truncated, so the second is the cost driver
    and the gap between them says how much of the corpus never reaches the
    model at all.
    """
    lengths = [len(s.sequence) for s in sequences]
    available = sum(lengths)
    if config.use_chunking:
        processed = available
    else:
        processed = sum(min(n, config.max_length) for n in lengths)
    return {
        "residues_available": available,
        "residues_processed": processed,
        "residues_truncated": available - processed,
    }


def normalise_config_ids(data: Any) -> Any:
    """Fold the legacy singular ``embedding_config_id`` into the plural field.

    Twelve historical jobs are on record carrying ``embedding_config_id`` as a
    bare string, and they have to keep validating exactly as they did, so the
    field cannot simply be renamed. The fold happens here, before pydantic looks
    at the keys, because ``ProteaPayload`` forbids extras: a legacy key left in
    place would be refused rather than ignored.

    Carrying both keys is a refusal and not a merge. Reconciling them would take
    a rule nobody has written down, and the group a worker ended up computing
    would then depend on which version of that rule its code happens to hold.
    """
    if not isinstance(data, dict) or _LEGACY_CONFIG_KEY not in data:
        return data
    if _GROUP_CONFIG_KEY in data:
        raise ValueError(
            f"payload carries both {_LEGACY_CONFIG_KEY!r} and {_GROUP_CONFIG_KEY!r}; "
            f"name the configs once"
        )
    folded = {k: v for k, v in data.items() if k != _LEGACY_CONFIG_KEY}
    folded[_GROUP_CONFIG_KEY] = [data[_LEGACY_CONFIG_KEY]]
    return folded


def clean_config_ids(value: Any) -> Any:
    """Validate a config group: a non-empty list of non-empty, stripped strings."""
    if not isinstance(value, list) or not value:
        raise ValueError("embedding_config_ids must name at least one embedding config")
    cleaned: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item.strip():
            raise ValueError("every embedding config id must be a non-empty string")
        cleaned.append(item.strip())
    return cleaned


def missing_for_any_config(config_ids: list[uuid.UUID]) -> Any:
    """A ``skip_existing`` predicate: keep a sequence ANY config in the group lacks.

    One pass serves the whole group, so the sequence is still needed as long as a
    single config has no row for it. Intersecting instead (keep it only when
    EVERY config lacks it) would leave the newest layer of a grid unembedded for
    every sequence an older layer already held, and the job would report success
    having written nothing for it. With one config this is the ``NOT EXISTS`` the
    coordinator has always filtered on.
    """
    from sqlalchemy import exists, or_

    from protea.infrastructure.orm.models.sequence.sequence import Sequence as SequenceRow

    return or_(
        *[
            ~exists().where(
                SequenceEmbedding.sequence_id == SequenceRow.id,
                SequenceEmbedding.embedding_config_id == config_id,
            )
            for config_id in config_ids
        ]
    )


def config_group_keys(config_ids: list[str]) -> dict[str, Any]:
    """Name a config group in the payload shape its consumer can actually read.

    One config is written in the legacy singular shape so a worker running code
    older than this change still does exactly the right work. More than one is
    written in the plural shape precisely so that worker REFUSES: a stale
    consumer silently dropping the key it does not declare would embed one layer
    of a group of three, store it, and report the job succeeded. That is the
    version-skew failure ``ProteaPayload``'s ``extra="forbid"`` exists to turn
    into a loud one, and it only stays loud if the new shape is on the wire.
    """
    if len(config_ids) == 1:
        return {_LEGACY_CONFIG_KEY: config_ids[0]}
    return {_GROUP_CONFIG_KEY: list(config_ids)}


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
    lookups happen between coordinator and worker), including the whole
    config group: one message runs one forward pass and serves all of them.
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
                    **config_group_keys(p.embedding_config_ids),
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

    The pairing is strict. It is positional, and the ``sequence_id`` comes from
    one side while the vectors come from the other, so a short inference result
    would not drop the tail: it would store one sequence's embedding under a
    different sequence's id. The write worker takes these dicts on trust, so
    nothing downstream would notice.
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
        for seq, chunks in zip(sequences, batch_chunks, strict=True)
    ]


def fold_legacy_store_groups(data: Any) -> Any:
    """Fold a one-config store payload into the ``groups`` list.

    The write worker used to be told one config and one list of sequences. It is
    now told a whole group, so a message published by a worker running older code
    (or sitting in the queue across a deploy) still has to validate: its
    ``embedding_config_id`` and ``sequences`` become a single group.
    """
    if not isinstance(data, dict) or _LEGACY_CONFIG_KEY not in data:
        return data
    folded = {k: v for k, v in data.items() if k not in (_LEGACY_CONFIG_KEY, "sequences")}
    folded.setdefault(
        "groups",
        [
            {
                _LEGACY_CONFIG_KEY: data[_LEGACY_CONFIG_KEY],
                "sequences": data.get("sequences"),
            }
        ],
    )
    return folded


def build_store_message(
    parent_job_id: uuid.UUID,
    p: ComputeEmbeddingsBatchPayload,
    per_config: list[tuple[str, list[dict]]],
) -> tuple[str, dict]:
    """Build ONE write-queue message carrying every config the pass served.

    One message rather than one per config, because the write has to be all or
    nothing. A batch that stored config A and then failed on B would leave the
    group at unequal coverage, which is the bank imbalance this fan-out exists to
    remove, arriving by the back door: spread irregularly across eleven configs
    with similar-looking counts instead of showing up as one visible gap. One
    message is one ``store_embeddings`` execution and so one transaction.

    It also keeps the parent's ``progress_total`` counting batches, as it always
    has: one batch, one write, one increment.
    """
    return (
        _WRITE_QUEUE,
        {
            "operation": "store_embeddings",
            "job_id": str(parent_job_id),
            "payload": {
                "parent_job_id": str(parent_job_id),
                "skip_existing": p.skip_existing,
                "groups": [
                    {_LEGACY_CONFIG_KEY: config_id, "sequences": write_sequences}
                    for config_id, write_sequences in per_config
                ],
            },
        },
    )


def store_group_rows(session: Session, p: StoreEmbeddingsPayload) -> dict[str, int]:
    """Insert every config's rows for one batch, in the caller's single transaction.

    The scale and the fp16 clip are resolved per config INSIDE the loop, not once
    for the message. ``embedding_scale`` is a property of the config that owns
    the rows, so hoisting it would store every config's vectors divided by the
    first config's scale: no error, no warning, just wrong magnitudes under
    right-looking ids.
    """
    counts = {
        "embeddings_stored": 0,
        "sequences_skipped": 0,
        "components_clipped": 0,
        "configs_written": len(p.groups),
    }
    for group in p.groups:
        config_id = uuid.UUID(group.embedding_config_id)
        rows, stored, skipped, clipped = build_embedding_rows(
            session, config_id, group.sequences, p.skip_existing
        )
        counts["embeddings_stored"] += stored
        counts["sequences_skipped"] += skipped
        counts["components_clipped"] += clipped
        if rows:
            session.execute(pg_insert(SequenceEmbedding).on_conflict_do_nothing(), rows)
    return counts


def build_embedding_rows(
    session: Session,
    config_id: uuid.UUID,
    sequences: list[dict[str, Any]],
    skip_existing: bool,
) -> tuple[list[dict], int, int, int]:
    """Materialise SequenceEmbedding insert rows for ONE config of a store batch.

    Iterates ``sequences`` and skips entries whose ``(sequence_id,
    config_id)`` pair already exists when ``skip_existing`` is true;
    otherwise deletes the existing rows so the bulk insert can replace
    them. Each stored vector is divided by THIS config's uniform
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
    for seq_data in sequences:
        sequence_id = seq_data["sequence_id"]
        chunks = seq_data["chunks"]
        if skip_existing:
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
