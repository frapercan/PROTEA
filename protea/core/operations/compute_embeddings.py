from __future__ import annotations

import re
import time
import uuid
from dataclasses import dataclass
from typing import Annotated, Any
from uuid import UUID

import numpy as np
from pydantic import Field, field_validator
from sqlalchemy import exists, select
from sqlalchemy import update as sa_update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload, RetryLaterError
from protea.core.contracts.parent_progress import update_parent_progress
from protea.core.utils import utcnow
from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
from protea.infrastructure.orm.models.embedding.sequence_embedding import SequenceEmbedding
from protea.infrastructure.orm.models.job import Job, JobEvent, JobStatus
from protea.infrastructure.orm.models.protein.protein import Protein
from protea.infrastructure.orm.models.query.query_set import QuerySetEntry
from protea.infrastructure.orm.models.sequence.sequence import Sequence

PositiveInt = Annotated[int, Field(gt=0)]

_BATCH_QUEUE = "protea.embeddings.batch"
_WRITE_QUEUE = "protea.embeddings.write"


# ---------------------------------------------------------------------------
# Data container
# ---------------------------------------------------------------------------


@dataclass
class ChunkEmbedding:
    """One pooled embedding for a contiguous residue span of a sequence.

    ``chunk_index_s`` and ``chunk_index_e`` use the same convention as the
    DB columns: start is 0-based inclusive, end is exclusive.  When chunking
    is disabled, ``chunk_index_s=0`` and ``chunk_index_e=None`` (full sequence).
    """

    chunk_index_s: int
    chunk_index_e: int | None
    vector: np.ndarray  # 1-D float32


# ---------------------------------------------------------------------------
# Payloads
# ---------------------------------------------------------------------------


class ComputeEmbeddingsPayload(ProteaPayload, frozen=True):
    """Coordinator payload: decides *which* sequences to embed and how to batch.

    The coordinator publishes N ephemeral operation messages to
    ``protea.embeddings.batch``.  Any worker consuming that queue picks up a
    message and runs ``ComputeEmbeddingsBatchOperation`` — no child Job rows
    are created in the DB.

    Fields
    ------
    embedding_config_id : str
        UUID of the EmbeddingConfig row that defines the model and strategy.
    accessions : list[str] | None
        Restrict to proteins with these UniProt accessions.  None = all.
    sequences_per_job : int
        How many sequences each batch message processes.  Tune to GPU memory.
    device : str
        Device passed down to each batch worker (``"cuda"`` or ``"cpu"``).
    skip_existing : bool
        Skip sequences that already have an embedding for this config.
    batch_size : int
        Model forward-pass batch size inside each batch worker.  Defaults to
        ``1`` because the largest supported backend (``prot_t5_xl_uniref50``
        at ``max_length=2048``) OOMs on a 12 GB GPU with anything higher.
        Callers running smaller models on roomier GPUs can raise it explicitly.
    """

    embedding_config_id: str
    accessions: list[str] | None = None
    query_set_id: str | None = None
    sequences_per_job: PositiveInt = 64
    device: str = "cuda"
    skip_existing: bool = True
    batch_size: PositiveInt = 1

    @field_validator("embedding_config_id", mode="before")
    @classmethod
    def must_be_non_empty(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("embedding_config_id must be a non-empty string")
        return v.strip()


class ComputeEmbeddingsBatchPayload(ProteaPayload, frozen=True):
    """Payload for a single batch operation message published by the coordinator."""

    embedding_config_id: str
    sequence_ids: list[int]
    parent_job_id: str
    device: str = "cuda"
    skip_existing: bool = True
    batch_size: PositiveInt = 1


# ---------------------------------------------------------------------------
# Operation
# ---------------------------------------------------------------------------


class ComputeEmbeddingsOperation:
    """Computes protein language model embeddings using a stored EmbeddingConfig.

    Backends
    --------
    - **esm / auto** : HuggingFace ``EsmModel`` (ESM-2 family).
      Sequences are processed one at a time.  CLS and EOS special tokens
      are stripped before residue-level pooling.

    - **esm3c** : ESM SDK ``ESMC`` (ESM3c family).
      No external tokenizer; uses ``ESMProtein`` + ``LogitsConfig``.
      Runs FP16 on GPU; BOS and EOS stripped before pooling.

    - **t5** : HuggingFace ``T5EncoderModel`` (ProstT5, prot_t5_xl…).
      Sequences are batched.  ProSTT5 mode (``<AA2fold>`` prefix) is
      auto-detected from ``model_name``.  EOS token is included in the
      residue tensor (consistent with PIS behaviour).

    - **ankh** : HuggingFace ``T5EncoderModel`` loaded via ``AutoTokenizer``
      (``ElnaggarLab/ankh-base``, ``ElnaggarLab/ankh-large``).  Shares the
      batched T5 pipeline with ``t5`` but never injects the ``<AA2fold>``
      prefix.  Ambiguous residues (``U``, ``Z``, ``O``, ``B``) are replaced
      with ``X`` before tokenisation.

    Layer indexing (reverse convention, matches PIS)
    ------------------------------------------------
    ``layer_indices = [0]`` → last (most semantic) layer.
    ``layer_indices = [1]`` → penultimate layer.  And so on.

    Pipeline per sequence
    ---------------------
    1. Forward pass → raw hidden states per layer.
    2. Extract layers using reverse indexing; validate against model depth.
    3. Aggregate layers (``mean`` / ``last`` / ``concat``).
    4. Optional per-residue L2 normalisation (``normalize_residues``).
    5. Apply chunking if ``use_chunking=True``.
    6. Pool each chunk (``mean`` / ``max`` / ``mean_max`` / ``cls``).
    7. Optional final L2 normalisation (``normalize``).
    """

    name = "compute_embeddings"
    description = (
        "Coordinator: partition sequences for an EmbeddingConfig into GPU batches "
        "and dispatch them to compute_embeddings_batch workers."
    )

    def summarize_payload(self, payload: dict[str, Any], *, session: Session | None = None) -> str:
        p = payload or {}
        bits: list[str] = []

        cfg_id_raw = p.get("embedding_config_id")
        if cfg_id_raw and session is not None:
            try:
                cfg = session.get(EmbeddingConfig, uuid.UUID(str(cfg_id_raw)))
            except Exception:
                cfg = None
            if cfg is not None:
                model_label = cfg.display_name or cfg.model_name or str(cfg.id)[:8]
                head = f"{model_label} ({cfg.model_backend})"
                bits.append(head)
                bits.append(f"max_len={cfg.max_length}")
                bits.append(f"pool={cfg.pooling}")
                if cfg.normalize:
                    bits.append("L2")
                if cfg.use_chunking:
                    bits.append(f"chunk={cfg.chunk_size}/{cfg.chunk_overlap}")
        elif cfg_id_raw:
            bits.append(f"cfg={str(cfg_id_raw)[:8]}")

        if p.get("query_set_id"):
            bits.append(f"qs={str(p['query_set_id'])[:8]}")
        if p.get("accessions"):
            bits.append(f"n_acc={len(p['accessions'])}")
        if p.get("sequences_per_job"):
            bits.append(f"per_job={p['sequences_per_job']}")
        if p.get("batch_size") is not None:
            bits.append(f"bs={p['batch_size']}")
        bits.append(f"dev={p.get('device', 'cuda')}")
        if p.get("skip_existing") is False:
            bits.append("overwrite")
        return " · ".join(bits)

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        """Coordinator: partition sequences into child jobs and dispatch them."""
        p = ComputeEmbeddingsPayload.model_validate(payload)
        parent_job_id = UUID(payload["_job_id"])
        config_id = uuid.UUID(p.embedding_config_id)

        config = session.get(EmbeddingConfig, config_id)
        if config is None:
            raise ValueError(f"EmbeddingConfig {p.embedding_config_id} not found")

        # Only one compute_embeddings job at a time — the GPU is a shared resource.
        conflict = (
            session.query(Job)
            .filter(
                Job.operation == "compute_embeddings",
                Job.status == JobStatus.RUNNING,
                Job.id != parent_job_id,
            )
            .first()
        )
        if conflict is not None:
            raise RetryLaterError(
                f"GPU busy: compute_embeddings job {conflict.id} is already running. "
                f"Will retry automatically.",
                delay_seconds=60,
            )

        sequence_ids = self._load_sequence_ids(session, p, config_id, emit)
        if not sequence_ids:
            emit("compute_embeddings.no_sequences", None, {}, "warning")
            return OperationResult(result={"batches": 0, "sequences": 0})

        # Partition into batches and create one child Job per batch.
        batches = [
            sequence_ids[i : i + p.sequences_per_job]
            for i in range(0, len(sequence_ids), p.sequences_per_job)
        ]
        n_batches = len(batches)

        emit(
            "compute_embeddings.dispatching",
            None,
            {
                "total_sequences": len(sequence_ids),
                "sequences_per_job": p.sequences_per_job,
                "batches": n_batches,
            },
            "info",
        )

        operations: list[tuple[str, dict]] = []
        for batch_seq_ids in batches:
            operations.append(
                (
                    _BATCH_QUEUE,
                    {
                        "operation": "compute_embeddings_batch",
                        "job_id": str(parent_job_id),
                        "payload": {
                            "embedding_config_id": p.embedding_config_id,
                            "sequence_ids": batch_seq_ids,
                            "parent_job_id": str(parent_job_id),
                            "device": p.device,
                            "skip_existing": p.skip_existing,
                            "batch_size": p.batch_size,
                        },
                    },
                )
            )

        return OperationResult(
            result={"batches": n_batches, "sequences": len(sequence_ids)},
            progress_current=0,
            progress_total=n_batches,
            deferred=True,
            publish_operations=operations,
        )

    def _load_sequence_ids(
        self,
        session: Session,
        p: ComputeEmbeddingsPayload,
        config_id: uuid.UUID,
        emit: EmitFn,
    ) -> list[int]:
        emit("compute_embeddings.load_sequences_start", None, {}, "info")

        if p.query_set_id:
            query_set_uuid = uuid.UUID(p.query_set_id)
            seq_ids_q = (
                session.query(QuerySetEntry.sequence_id)
                .filter(QuerySetEntry.query_set_id == query_set_uuid)
                .distinct()
                .subquery()
            )
            q = session.query(Sequence.id).filter(Sequence.id.in_(select(seq_ids_q)))
        elif p.accessions:
            seq_ids_q = (
                session.query(Protein.sequence_id)
                .filter(Protein.accession.in_(p.accessions))
                .filter(Protein.sequence_id.isnot(None))
                .distinct()
                .subquery()
            )
            q = session.query(Sequence.id).filter(Sequence.id.in_(select(seq_ids_q)))
        else:
            q = session.query(Sequence.id)

        if p.skip_existing:
            already_embedded = exists().where(
                SequenceEmbedding.sequence_id == Sequence.id,
                SequenceEmbedding.embedding_config_id == config_id,
            )
            q = q.filter(~already_embedded)

        ids = [row[0] for row in q.all()]
        emit(
            "compute_embeddings.load_sequences_done", None, {"sequences_to_embed": len(ids)}, "info"
        )
        return ids

    def _embed_batch(
        self,
        model: Any,
        tokenizer: Any,
        sequences: list[str],
        config: EmbeddingConfig,
        device: str,
    ) -> list[list[ChunkEmbedding]]:
        """Embed a list of sequences, returning per-chunk results for each."""
        if config.model_backend == "esm3c":
            return _embed_esm3c(model, sequences, config, device)
        elif config.model_backend == "t5":
            return _embed_t5(model, tokenizer, sequences, config, device)
        elif config.model_backend == "ankh":
            return _embed_ankh(model, tokenizer, sequences, config, device)
        else:  # esm / auto
            return _embed_esm(model, tokenizer, sequences, config, device)


# ---------------------------------------------------------------------------
# Batch operation (child job)
# ---------------------------------------------------------------------------


class ComputeEmbeddingsBatchOperation:
    """Processes one batch of sequences for a parent compute_embeddings job.

    Reads ``sequence_ids`` from the payload, loads the model, runs inference,
    stores embeddings, and atomically increments the parent job's
    ``progress_current``.  The last batch to finish marks the parent SUCCEEDED.
    """

    name = "compute_embeddings_batch"
    description = (
        "GPU child job: run a forward pass on a small batch of sequences "
        "and forward the resulting vectors to the store_embeddings worker."
    )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        p = payload or {}
        n = len(p.get("sequence_ids") or [])
        bits = []
        if n:
            bits.append(f"n={n}")
        if p.get("device"):
            bits.append(p["device"])
        return " · ".join(bits)

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = ComputeEmbeddingsBatchPayload.model_validate(payload)
        config_id = uuid.UUID(p.embedding_config_id)
        parent_job_id = UUID(p.parent_job_id)

        # Skip processing if the parent job was cancelled or failed while this
        # batch message was waiting in the queue.
        parent = session.get(Job, parent_job_id)
        if parent is not None and parent.status in (JobStatus.CANCELLED, JobStatus.FAILED):
            emit(
                "compute_embeddings_batch.skipped",
                None,
                {"reason": "parent_not_running", "parent_status": parent.status.value},
                "warning",
            )
            return OperationResult(result={"skipped": True})

        config = session.get(EmbeddingConfig, config_id)
        if config is None:
            raise ValueError(f"EmbeddingConfig {p.embedding_config_id} not found")

        sequences = session.query(Sequence).filter(Sequence.id.in_(p.sequence_ids)).all()

        t0 = time.perf_counter()
        emit(
            "compute_embeddings_batch.start",
            None,
            {
                "sequences": len(sequences),
                "parent_job_id": str(parent_job_id),
            },
            "info",
        )

        model, tokenizer = self._load_model(config, p.device, emit)

        # Run inference only — no DB writes here.
        write_sequences = []
        for i in range(0, len(sequences), p.batch_size):
            batch = sequences[i : i + p.batch_size]
            seq_strs = [s.sequence for s in batch]
            batch_chunks = self._embed_batch(model, tokenizer, seq_strs, config, p.device)
            for seq, chunks in zip(batch, batch_chunks, strict=False):
                write_sequences.append(
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
                )

        elapsed = time.perf_counter() - t0
        emit(
            "compute_embeddings_batch.done",
            None,
            {
                "sequences_inferred": len(write_sequences),
                "elapsed_seconds": elapsed,
            },
            "info",
        )

        # Hand off to the write worker — GPU is free to take the next batch.
        return OperationResult(
            result={"sequences_inferred": len(write_sequences)},
            publish_operations=[
                (
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
            ],
        )

    def _load_model(self, config: EmbeddingConfig, device: str, emit: EmitFn) -> tuple[Any, Any]:
        return _get_or_load_model(config, device, emit)

    def _embed_batch(
        self,
        model: Any,
        tokenizer: Any,
        sequences: list[str],
        config: EmbeddingConfig,
        device: str,
    ) -> list[list[ChunkEmbedding]]:
        if config.model_backend == "esm3c":
            return _embed_esm3c(model, sequences, config, device)
        elif config.model_backend == "t5":
            return _embed_t5(model, tokenizer, sequences, config, device)
        elif config.model_backend == "ankh":
            return _embed_ankh(model, tokenizer, sequences, config, device)
        else:
            return _embed_esm(model, tokenizer, sequences, config, device)


# ---------------------------------------------------------------------------
# Write operation (CPU worker — no GPU required)
# ---------------------------------------------------------------------------


class StoreEmbeddingsPayload(ProteaPayload, frozen=True):
    """Payload published by ComputeEmbeddingsBatchOperation after inference."""

    parent_job_id: str
    embedding_config_id: str
    skip_existing: bool = True
    sequences: list[dict[str, Any]]  # [{"sequence_id": int, "chunks": [...]}]


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
        n = len(p.get("sequences") or [])
        return f"n={n}" if n else ""

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = StoreEmbeddingsPayload.model_validate(payload)
        config_id = uuid.UUID(p.embedding_config_id)
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

        embeddings_stored = 0
        sequences_skipped = 0

        rows_to_insert: list[dict] = []

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
                rows_to_insert.append(
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

        if rows_to_insert:
            session.execute(
                pg_insert(SequenceEmbedding).on_conflict_do_nothing(),
                rows_to_insert,
            )

        emit(
            "store_embeddings.done",
            None,
            {
                "embeddings_stored": embeddings_stored,
                "sequences_skipped": sequences_skipped,
            },
            "info",
        )

        self._update_parent_progress(session, parent_job_id, emit)

        return OperationResult(
            result={
                "embeddings_stored": embeddings_stored,
                "sequences_skipped": sequences_skipped,
            }
        )

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
_MODEL_CACHE: dict[tuple[str, str, str], tuple[Any, Any]] = {}


def _get_or_load_model(config: EmbeddingConfig, device: str, emit: EmitFn) -> tuple[Any, Any]:
    from protea.config.tuning import get_tuning

    cache_max = get_tuning().worker.model_cache_max
    key = (config.model_name, config.model_backend, device)
    if key not in _MODEL_CACHE:
        if len(_MODEL_CACHE) >= cache_max:
            evict_key = next(iter(_MODEL_CACHE))
            old_model, old_tokenizer = _MODEL_CACHE.pop(evict_key)
            del old_model, old_tokenizer
            import gc

            import torch

            gc.collect()
            if torch.cuda.is_available():
                torch.cuda.empty_cache()
                torch.cuda.synchronize()
        _MODEL_CACHE[key] = _load_model(config, device, emit)
    return _MODEL_CACHE[key]


def _load_model(config: EmbeddingConfig, device: str, emit: EmitFn) -> tuple[Any, Any]:
    import torch

    emit(
        "compute_embeddings.model_load_start",
        None,
        {"model_name": config.model_name, "backend": config.model_backend},
        "info",
    )

    if config.model_backend == "esm3c":
        from esm.models.esmc import ESMC

        device_obj = torch.device(device)
        dtype = torch.float16 if device_obj.type == "cuda" else torch.float32
        model = ESMC.from_pretrained(config.model_name)
        model = model.to(device)
        model = model.to(dtype)
        model.eval()
        tokenizer = None

    elif config.model_backend in ("esm", "auto"):
        from transformers import AutoTokenizer, EsmModel

        device_obj = torch.device(device)
        dtype = torch.float16 if device_obj.type == "cuda" else torch.float32
        tokenizer = AutoTokenizer.from_pretrained(config.model_name)
        model = EsmModel.from_pretrained(config.model_name, output_hidden_states=True)
        model.eval()
        model.to(device)
        model.to(dtype)

    elif config.model_backend == "t5":
        from transformers import T5EncoderModel, T5Tokenizer

        device_obj = torch.device(device)
        dtype = torch.float16 if device_obj.type == "cuda" else torch.float32
        tokenizer = T5Tokenizer.from_pretrained(config.model_name, do_lower_case=False)
        model = T5EncoderModel.from_pretrained(
            config.model_name,
            output_hidden_states=True,
            torch_dtype=dtype,
        )
        model.eval()
        model.to(device)

    elif config.model_backend == "ankh":
        # Ankh ships a SentencePiece tokenizer; AutoTokenizer resolves to the
        # correct class (T5TokenizerFast) without hard-coding T5Tokenizer,
        # which has caused loading issues with some Ankh revisions.
        #
        # Precision: Ankh was pre-trained on TPU in bfloat16 and its
        # T5-encoder LayerNorm overflows in FP16 — every forward collapses
        # to NaN.  Verified on ElnaggarLab/ankh-base 2026-04-10.  Use
        # bfloat16 on CUDA (same VRAM footprint as FP16 but with FP32
        # dynamic range) and FP32 on CPU.
        from transformers import AutoTokenizer, T5EncoderModel

        device_obj = torch.device(device)
        dtype = torch.bfloat16 if device_obj.type == "cuda" else torch.float32
        tokenizer = AutoTokenizer.from_pretrained(config.model_name)
        model = T5EncoderModel.from_pretrained(
            config.model_name,
            output_hidden_states=True,
            torch_dtype=dtype,
        )
        model.eval()
        model.to(device)

    else:
        raise ValueError(f"Unknown model_backend: {config.model_backend!r}")

    emit("compute_embeddings.model_load_done", None, {}, "info")
    return model, tokenizer


# ---------------------------------------------------------------------------
# Backend: ESM (HuggingFace EsmModel)
# ---------------------------------------------------------------------------


def _embed_esm(
    model: Any,
    tokenizer: Any,
    sequences: list[str],
    config: EmbeddingConfig,
    device: str,
) -> list[list[ChunkEmbedding]]:
    """Embed sequences with ESM-2 / EsmModel.

    Processes one sequence at a time to handle variable lengths without
    OOM issues.  CLS (position 0) and EOS (last valid position) tokens are
    excluded from all residue-level operations.
    """
    import torch
    import torch.nn.functional as F

    results: list[list[ChunkEmbedding]] = []

    with torch.no_grad():
        for seq_str in sequences:
            tokens = tokenizer(
                seq_str,
                return_tensors="pt",
                truncation=True,
                max_length=config.max_length,
                add_special_tokens=True,
            )
            tokens = {k: v.to(device) for k, v in tokens.items()}
            outputs = model(**tokens, output_hidden_states=True)
            hidden_states = outputs.hidden_states  # tuple of (1, L, D)

            valid_layers = _validate_layers(
                config.layer_indices, hidden_states, "ESM", seq_str[:20]
            )

            if config.pooling == "cls":
                # CLS token at position 0 of the raw hidden states
                layer_tensors_1d = [
                    hidden_states[-(li + 1)][0, 0, :].float() for li in valid_layers
                ]
                pooled = _aggregate_1d(layer_tensors_1d, config.layer_agg)
                if config.normalize:
                    pooled = F.normalize(pooled.unsqueeze(0), p=2, dim=1).squeeze(0)
                results.append([ChunkEmbedding(0, None, pooled.cpu().numpy())])
            else:
                # Strip CLS (pos 0) and EOS (last valid pos)
                # attention_mask.sum() = CLS + content + EOS
                actual_len = int(tokens["attention_mask"].sum().item())
                layer_tensors_2d = [
                    hidden_states[-(li + 1)][0, 1 : actual_len - 1, :].float()
                    for li in valid_layers
                ]
                residues = _aggregate_residue_layers(layer_tensors_2d, config.layer_agg)
                if config.normalize_residues:
                    residues = F.normalize(residues, p=2, dim=1)
                results.append(_chunk_and_pool(residues, config))

            del outputs, hidden_states
            torch.cuda.empty_cache()

    return results


# ---------------------------------------------------------------------------
# Backend: T5 (HuggingFace T5EncoderModel)
# ---------------------------------------------------------------------------


def _embed_t5(
    model: Any,
    tokenizer: Any,
    sequences: list[str],
    config: EmbeddingConfig,
    device: str,
    *,
    use_aa2fold: bool | None = None,
    split_into_words: bool = False,
) -> list[list[ChunkEmbedding]]:
    """Embed sequences with T5EncoderModel (ProstT5, prot_t5_xl, Ankh, …).

    Sequences are processed as a padded batch.  ProSTT5 mode is auto-detected
    from ``config.model_name`` (looks for ``prostt5`` substring, case-insensitive)
    when ``use_aa2fold`` is ``None``; callers (e.g. the Ankh backend) can pass
    ``use_aa2fold=False`` to disable the prefix unconditionally.

    Tokeniser input format
    ----------------------
    Two tokenisation strategies are supported:

    * ``split_into_words=False`` (default) — each sequence is joined with
      single spaces (``"A C D E"``) and passed as a string.  This matches the
      ProstT5 / ``prot_t5_xl_uniref50`` SentencePiece models, which recognise
      space-separated amino acids as individual tokens.
    * ``split_into_words=True`` — each sequence is passed as a list of
      characters with ``is_split_into_words=True``.  Required by Ankh
      (``ElnaggarLab/ankh-base`` / ``-large``): Ankh's SentencePiece
      tokeniser maps a literal space to ``<unk>``, so the space-joined path
      produces ~50% ``<unk>`` tokens and NaN outputs under FP16.

    Residue slicing
    ---------------
    T5 has no CLS token at position 0, but ProstT5 injects a ``<AA2fold>``
    prefix token there, and every T5 tokenizer appends an EOS at the last
    valid position.  The residue-level slice strips both so that
    ``residues[0]`` is always the first amino acid and ``residues.shape[0]``
    equals the amino-acid count, consistent with ``_embed_esm`` /
    ``_embed_esm3c`` which strip CLS/BOS+EOS:

        start = 1 if use_aa2fold else 0   # skip <AA2fold> on ProstT5 only
        end   = actual_len - 1            # drop trailing EOS

    This makes ``chunk_index_s`` / ``chunk_index_e`` mean the same thing on
    every backend: indices into the amino-acid sequence, not into the
    backend-specific residue tensor.  The CLS pooling path is unchanged
    (position 0 = ``<AA2fold>`` for ProstT5, arbitrary first AA otherwise).
    """
    import torch
    import torch.nn.functional as F

    if use_aa2fold is None:
        use_aa2fold = "prostt5" in config.model_name.lower()

    # Replace ambiguous amino acids (U/Z/O/B → X) regardless of tokenisation mode.
    cleaned = [re.sub(r"[UZOB]", "X", seq_str) for seq_str in sequences]

    if split_into_words:
        # Ankh path: pass list-of-chars with is_split_into_words=True so the
        # tokeniser treats each residue as one word and never falls back to <unk>.
        inputs = tokenizer.batch_encode_plus(
            [list(c) for c in cleaned],
            padding="longest",
            truncation=True,
            max_length=config.max_length,
            add_special_tokens=True,
            is_split_into_words=True,
            return_tensors="pt",
        )
    else:
        processed = [
            ("<AA2fold> " if use_aa2fold else "") + " ".join(c) for c in cleaned
        ]
        inputs = tokenizer.batch_encode_plus(
            processed,
            padding="longest",
            truncation=True,
            max_length=config.max_length,
            add_special_tokens=True,
            return_tensors="pt",
        )
    inputs = {k: v.to(device) for k, v in inputs.items()}

    with torch.no_grad():
        outputs = model(
            input_ids=inputs["input_ids"],
            attention_mask=inputs["attention_mask"],
            output_hidden_states=True,
        )

    hidden_states = outputs.hidden_states  # tuple of (B, L, D)
    del outputs
    torch.cuda.empty_cache()

    valid_layers = _validate_layers(config.layer_indices, hidden_states, "T5", "batch")

    # On ProstT5 the <AA2fold> prefix lives at token position 0; skip it.
    start_idx = 1 if use_aa2fold else 0

    results: list[list[ChunkEmbedding]] = []
    for i in range(len(sequences)):
        # actual_len = (optional <AA2fold>) + N residues + EOS
        actual_len = int(inputs["attention_mask"][i].sum().item())

        if config.pooling == "cls":
            # CLS pooling on T5 uses position 0 — the <AA2fold> hidden state
            # on ProstT5, otherwise the first amino-acid hidden state.
            layer_tensors_1d = [hidden_states[-(li + 1)][i, 0, :].float() for li in valid_layers]
            pooled = _aggregate_1d(layer_tensors_1d, config.layer_agg)
            if config.normalize:
                pooled = F.normalize(pooled.unsqueeze(0), p=2, dim=1).squeeze(0)
            results.append([ChunkEmbedding(0, None, pooled.cpu().numpy())])
        else:
            # Residue slice: strip <AA2fold> (if present) and trailing EOS so
            # residues[0] is AA 0 and residues.shape[0] == amino-acid count.
            layer_tensors_2d = [
                hidden_states[-(li + 1)][i, start_idx : actual_len - 1, :].float()
                for li in valid_layers
            ]
            residues = _aggregate_residue_layers(layer_tensors_2d, config.layer_agg)
            if config.normalize_residues:
                residues = F.normalize(residues, p=2, dim=1)
            results.append(_chunk_and_pool(residues, config))

    del hidden_states
    torch.cuda.empty_cache()

    return results


# ---------------------------------------------------------------------------
# Backend: Ankh (HuggingFace T5EncoderModel, loaded via AutoTokenizer)
# ---------------------------------------------------------------------------


def _embed_ankh(
    model: Any,
    tokenizer: Any,
    sequences: list[str],
    config: EmbeddingConfig,
    device: str,
) -> list[list[ChunkEmbedding]]:
    """Embed sequences with Ankh (base / large).

    Ankh is a T5 encoder-decoder; we reuse the shared T5 batched pipeline but
    with two deviations:

    * never inject the ProstT5 ``<AA2fold>`` prefix (Ankh was pre-trained on
      plain amino-acid sequences);
    * tokenise via ``is_split_into_words=True`` with a list of per-residue
      characters.  Ankh's SentencePiece tokeniser maps a literal space to
      ``<unk>``, so the space-joined path used for ProstT5 produces ~50%
      ``<unk>`` tokens and collapses to NaN under FP16.  Verified on
      ``ElnaggarLab/ankh-base``.
    """
    return _embed_t5(
        model,
        tokenizer,
        sequences,
        config,
        device,
        use_aa2fold=False,
        split_into_words=True,
    )


# ---------------------------------------------------------------------------
# Backend: ESM3c (ESM SDK ESMC)
# ---------------------------------------------------------------------------


def _embed_esm3c(
    model: Any,
    sequences: list[str],
    config: EmbeddingConfig,
    device: str,
) -> list[list[ChunkEmbedding]]:
    """Embed sequences with ESMC (ESM3c family).

    Uses the ESM SDK directly — no external tokenizer.  The model must have
    been loaded with ``ESMC.from_pretrained`` and cast to FP16.  Hidden states
    are returned via ``LogitsConfig(return_hidden_states=True)``.

    BOS (position 0) and EOS (position -1) tokens are stripped before all
    residue-level operations, matching PIS / FANTASIA behaviour.
    """
    import torch
    import torch.nn.functional as F
    from esm.sdk.api import ESMProtein, LogitsConfig

    device_obj = torch.device(device) if isinstance(device, str) else device
    results: list[list[ChunkEmbedding]] = []

    with torch.no_grad():
        for seq_str in sequences:
            protein = ESMProtein(sequence=seq_str[: config.max_length])

            with torch.autocast(
                device_type=device_obj.type,
                dtype=torch.float16,
                enabled=(device_obj.type == "cuda"),
            ):
                protein_tensor = model.encode(protein)
                logits_output = model.logits(
                    protein_tensor,
                    LogitsConfig(sequence=True, return_hidden_states=True),
                )

            hs = logits_output.hidden_states
            if hs is None:
                raise RuntimeError(f"ESM3c returned no hidden_states for sequence {seq_str[:20]!r}")

            # Normalise to a list of per-layer tensors [1, L, D]
            if isinstance(hs, torch.Tensor):
                hs = [hs[i] for i in range(hs.shape[0])]

            valid_layers = _validate_layers(config.layer_indices, hs, "ESM3c", seq_str[:20])

            if config.pooling == "cls":
                # BOS token at position 0 (before stripping)
                layer_tensors_1d = [hs[-(li + 1)][0, 0, :].float() for li in valid_layers]
                pooled = _aggregate_1d(layer_tensors_1d, config.layer_agg)
                if config.normalize:
                    pooled = F.normalize(pooled.unsqueeze(0), p=2, dim=1).squeeze(0)
                results.append([ChunkEmbedding(0, None, pooled.cpu().numpy())])
            else:
                # Strip BOS (0) and EOS (-1): positions [1:-1]
                layer_tensors_2d = [hs[-(li + 1)][0, 1:-1, :].float() for li in valid_layers]
                residues = _aggregate_residue_layers(layer_tensors_2d, config.layer_agg)
                if config.normalize_residues:
                    residues = F.normalize(residues, p=2, dim=1)
                results.append(_chunk_and_pool(residues, config))

            del logits_output, hs
            torch.cuda.empty_cache()

    return results


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _validate_layers(
    layer_indices: list[int],
    hidden_states: Any,
    model_tag: str,
    seq_id: str,
) -> list[int]:
    """Validate reverse-indexed layer indices against the model's hidden states.

    ``layer_indices = [0]`` → last layer; ``[1]`` → penultimate; etc.
    Raises ``ValueError`` if any index is out of range.
    Returns a sorted, deduplicated list of valid indices.
    """
    import torch

    if isinstance(hidden_states, torch.Tensor):
        total = int(hidden_states.shape[0])
    else:
        total = len(hidden_states)

    req = sorted(set(int(li) for li in layer_indices))
    invalid = [li for li in req if not (0 <= li < total)]
    if invalid:
        raise ValueError(
            f"[{model_tag}] seq={seq_id!r}: invalid layer_indices {invalid}. "
            f"Valid range: 0..{total - 1}  (0 = last layer)."
        )
    return req


def _aggregate_residue_layers(layer_tensors: list[Any], layer_agg: str) -> Any:
    """Combine [L, D] tensors from multiple layers into one [L, D] tensor."""
    import torch

    if layer_agg == "last":
        return layer_tensors[-1]
    elif layer_agg == "mean":
        return torch.stack(layer_tensors, dim=0).mean(dim=0)
    elif layer_agg == "concat":
        return torch.cat(layer_tensors, dim=-1)
    else:
        raise ValueError(f"Unknown layer_agg: {layer_agg!r}. Choose: last, mean, concat")


def _aggregate_1d(layer_tensors: list[Any], layer_agg: str) -> Any:
    """Combine [D] tensors from multiple layers into one [D] tensor (CLS path)."""
    import torch

    if layer_agg == "last":
        return layer_tensors[-1]
    elif layer_agg == "mean":
        return torch.stack(layer_tensors, dim=0).mean(dim=0)
    elif layer_agg == "concat":
        return torch.cat(layer_tensors, dim=-1)
    else:
        raise ValueError(f"Unknown layer_agg: {layer_agg!r}. Choose: last, mean, concat")


def _chunk_and_pool(residues: Any, config: EmbeddingConfig) -> list[ChunkEmbedding]:
    """Apply chunking (optional) and pooling to a residue tensor [L, D].

    Returns one ``ChunkEmbedding`` per chunk.  Without chunking, returns a
    single element covering the full sequence.
    """
    import torch
    import torch.nn.functional as F

    if config.use_chunking:
        spans = _compute_chunk_spans(residues.shape[0], config.chunk_size, config.chunk_overlap)
    else:
        spans = [(0, residues.shape[0])]

    results: list[ChunkEmbedding] = []
    for start, end in spans:
        chunk = residues[start:end]  # [chunk_L, D]

        if config.pooling == "mean":
            pooled = chunk.mean(dim=0)
        elif config.pooling == "max":
            pooled = chunk.max(dim=0).values
        elif config.pooling == "mean_max":
            pooled = torch.cat([chunk.mean(dim=0), chunk.max(dim=0).values])
        else:
            raise ValueError(
                f"Pooling {config.pooling!r} is not supported in residue-level mode. "
                f"Use 'cls' for CLS token pooling."
            )

        if config.normalize:
            pooled = F.normalize(pooled.unsqueeze(0), p=2, dim=1).squeeze(0)

        chunk_index_e = end if config.use_chunking else None
        results.append(
            ChunkEmbedding(
                chunk_index_s=start,
                chunk_index_e=chunk_index_e,
                vector=pooled.float().cpu().numpy(),
            )
        )

    return results


def _compute_chunk_spans(length: int, chunk_size: int, overlap: int) -> list[tuple[int, int]]:
    """Compute (start, end) spans for overlapping chunks over a sequence of ``length`` residues.

    Raises ``ValueError`` if ``overlap >= chunk_size`` — such a configuration
    would produce O(L) single-residue chunks or an infinite loop.
    """
    if overlap >= chunk_size:
        raise ValueError(
            f"chunk_overlap ({overlap}) must be strictly less than chunk_size ({chunk_size})"
        )
    step = chunk_size - overlap
    spans: list[tuple[int, int]] = []
    start = 0
    while start < length:
        end = min(start + chunk_size, length)
        spans.append((start, end))
        start += step
    return spans
