from __future__ import annotations

import re
import time
import uuid
from dataclasses import dataclass
from typing import Annotated, Any, NamedTuple
from uuid import UUID

import numpy as np
from pydantic import Field, field_validator
from sqlalchemy import exists, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload, RetryLaterError
from protea.core.contracts.parent_progress import update_parent_progress
from protea.core.operations._compute_embeddings_helpers import (
    build_batch_dispatch_messages,
    build_embedding_rows,
    build_store_message,
    serialize_inferred_chunks,
    t5_forward_pass,
)
from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
from protea.infrastructure.orm.models.embedding.sequence_embedding import SequenceEmbedding
from protea.infrastructure.orm.models.job import Job, JobStatus
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
    message and runs ``ComputeEmbeddingsBatchOperation``: no child Job rows
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

        operations = build_batch_dispatch_messages(p, parent_job_id, sequence_ids)
        n_batches = len(operations)

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
        """Embed a list of sequences, returning per-chunk results for each.

        Dispatches to the per-backend embed function via ``_EMBED_BACKENDS``
        (T2A.5). Falls back to ``_embed_esm`` for unknown backends.
        """
        return _dispatch_embed(model, tokenizer, sequences, config, device)


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
            {"sequences": len(sequences), "parent_job_id": str(parent_job_id)},
            "info",
        )

        write_sequences = self._infer_all(config, sequences, p, emit)

        emit(
            "compute_embeddings_batch.done",
            None,
            {
                "sequences_inferred": len(write_sequences),
                "elapsed_seconds": time.perf_counter() - t0,
            },
            "info",
        )
        return OperationResult(
            result={"sequences_inferred": len(write_sequences)},
            publish_operations=[build_store_message(parent_job_id, p, write_sequences)],
        )

    def _infer_all(
        self,
        config: EmbeddingConfig,
        sequences: list[Sequence],
        p: ComputeEmbeddingsBatchPayload,
        emit: EmitFn,
    ) -> list[dict]:
        """Run model inference over ``sequences`` in batches of ``p.batch_size``."""
        model, tokenizer = self._load_model(config, p.device, emit)
        write_sequences: list[dict] = []
        for i in range(0, len(sequences), p.batch_size):
            batch = sequences[i : i + p.batch_size]
            seq_strs = [s.sequence for s in batch]
            batch_chunks = self._embed_batch(model, tokenizer, seq_strs, config, p.device)
            write_sequences.extend(serialize_inferred_chunks(batch, batch_chunks))
        return write_sequences

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
        """Per-batch dispatch shim; delegates to ``_dispatch_embed`` (T2A.5)."""
        return _dispatch_embed(model, tokenizer, sequences, config, device)


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

        rows_to_insert, embeddings_stored, sequences_skipped = build_embedding_rows(
            session, p, config_id
        )
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


# Cached map of backend plugins resolved from the ``protea.backends``
# entry_points group.  Lazy: populated on first call to ``_load_model``.
# ``None`` means "not yet discovered"; an empty dict means "no backends
# installed" (which is a hard error at load time, not a registry warning).
_BACKEND_PLUGINS: dict[str, Any] | None = None


def _get_backend_plugins() -> dict[str, Any]:
    """Discover and cache backend plugins via ``entry_points``.

    Returns a dict keyed by ``plugin.name``. Each plugin must implement
    :class:`protea_contracts.EmbeddingBackend`. Discovery is performed
    once per process; subsequent calls return the cached map.

    A plugin whose ``name`` attribute disagrees with its entry_point
    name is a hard error: the entry_points file and the class
    declaration must agree, and silently letting them drift would make
    "Unknown model_backend" errors confusing.
    """
    global _BACKEND_PLUGINS
    if _BACKEND_PLUGINS is None:
        from importlib.metadata import entry_points

        cache: dict[str, Any] = {}
        for ep in entry_points(group="protea.backends"):
            plugin = ep.load()
            if getattr(plugin, "name", None) != ep.name:
                raise RuntimeError(
                    f"Backend plugin name mismatch: entry_point {ep.name!r} "
                    f"resolves to plugin with name "
                    f"{getattr(plugin, 'name', None)!r}"
                )
            cache[ep.name] = plugin
        _BACKEND_PLUGINS = cache
    return _BACKEND_PLUGINS


def _resolve_backend(backend_name: str) -> Any:
    """Resolve a ``model_backend`` identifier to a plugin instance.

    The ``"auto"`` legacy alias maps to ``"esm"``. Unknown identifiers
    raise ``ValueError`` listing the discovered backends so the failure
    message is actionable.
    """
    plugins = _get_backend_plugins()
    key = "esm" if backend_name == "auto" else backend_name
    if key not in plugins:
        raise ValueError(
            f"Unknown model_backend: {backend_name!r}. "
            f"Discovered: {sorted(plugins)}"
        )
    return plugins[key]


def _load_model(config: EmbeddingConfig, device: str, emit: EmitFn) -> tuple[Any, Any]:
    """Load ``(model, tokenizer)`` via the ``protea.backends`` plugin
    matching ``config.model_backend``.

    Each plugin owns its own torch / transformers / esm imports (lazy
    inside ``plugin.load_model``) and the device + dtype dance.  The
    return shape ``(model, tokenizer)`` matches the legacy hardcoded
    dispatch exactly; for ESM-C the tokenizer slot is ``None`` because
    the standalone ``esm`` SDK takes raw sequence strings.
    """
    emit(
        "compute_embeddings.model_load_start",
        None,
        {"model_name": config.model_name, "backend": config.model_backend},
        "info",
    )
    plugin = _resolve_backend(config.model_backend)
    model, tokenizer = plugin.load_model(config.model_name, device, emit)
    emit("compute_embeddings.model_load_done", None, {}, "info")
    return model, tokenizer


# ---------------------------------------------------------------------------
# Backend: ESM (HuggingFace EsmModel)
# ---------------------------------------------------------------------------


def _embed_esm_one(
    model: Any,
    tokenizer: Any,
    seq_str: str,
    config: EmbeddingConfig,
    device: str,
) -> list[ChunkEmbedding]:
    """ESM-2 forward pass + pooling for one sequence.

    Used by :func:`_embed_esm` to keep the per-batch loop body
    readable. Excludes CLS (position 0) and EOS (last valid position)
    from residue-level operations. ``attention_mask.sum()`` covers
    CLS + content + EOS, so the residue slice is ``[1:actual_len-1]``.
    """
    import torch
    import torch.nn.functional as F

    tokens = tokenizer(
        seq_str,
        return_tensors="pt",
        truncation=True,
        max_length=config.max_length,
        add_special_tokens=True,
    )
    tokens = {k: v.to(device) for k, v in tokens.items()}
    outputs = model(**tokens, output_hidden_states=True)
    hidden_states = outputs.hidden_states
    valid_layers = _validate_layers(config.layer_indices, hidden_states, "ESM", seq_str[:20])
    if config.pooling == "cls":
        layer_tensors_1d = [
            hidden_states[-(li + 1)][0, 0, :].float() for li in valid_layers
        ]
        pooled = _aggregate_1d(layer_tensors_1d, config.layer_agg)
        if config.normalize:
            pooled = F.normalize(pooled.unsqueeze(0), p=2, dim=1).squeeze(0)
        chunks = [ChunkEmbedding(0, None, pooled.cpu().numpy())]
    else:
        actual_len = int(tokens["attention_mask"].sum().item())
        layer_tensors_2d = [
            hidden_states[-(li + 1)][0, 1 : actual_len - 1, :].float() for li in valid_layers
        ]
        residues = _aggregate_residue_layers(layer_tensors_2d, config.layer_agg)
        if config.normalize_residues:
            residues = F.normalize(residues, p=2, dim=1)
        chunks = _chunk_and_pool(residues, config)
    del outputs, hidden_states
    torch.cuda.empty_cache()
    return chunks


def _embed_esm(
    model: Any,
    tokenizer: Any,
    sequences: list[str],
    config: EmbeddingConfig,
    device: str,
) -> list[list[ChunkEmbedding]]:
    """Embed sequences with ESM-2 / EsmModel.

    Processes one sequence at a time to handle variable lengths
    without OOM issues.
    """
    import torch

    results: list[list[ChunkEmbedding]] = []
    with torch.no_grad():
        for seq_str in sequences:
            results.append(_embed_esm_one(model, tokenizer, seq_str, config, device))
    return results


# ---------------------------------------------------------------------------
# Backend: T5 (HuggingFace T5EncoderModel)
# ---------------------------------------------------------------------------


class _T5Mode(NamedTuple):
    """Tokenisation/prefix mode for ``_embed_t5``.

    ``use_aa2fold=None`` triggers auto-detect from ``config.model_name``
    (ProstT5 substring); callers like Ankh override with explicit values.
    """

    use_aa2fold: bool | None = None
    split_into_words: bool = False


def _t5_tokenise(
    tokenizer: Any,
    cleaned: list[str],
    config: EmbeddingConfig,
    mode: _T5Mode,
    use_aa2fold: bool,
) -> Any:
    """Apply the tokeniser branch picked by ``mode.split_into_words``.

    ``use_aa2fold`` here is the resolved boolean (auto-detected or
    explicitly set), used only on the space-joined path.
    """
    if mode.split_into_words:
        # Ankh path: list-of-chars with is_split_into_words=True so the
        # tokeniser treats each residue as one word and never falls back to <unk>.
        return tokenizer.batch_encode_plus(
            [list(c) for c in cleaned],
            padding="longest",
            truncation=True,
            max_length=config.max_length,
            add_special_tokens=True,
            is_split_into_words=True,
            return_tensors="pt",
        )
    processed = [("<AA2fold> " if use_aa2fold else "") + " ".join(c) for c in cleaned]
    return tokenizer.batch_encode_plus(
        processed,
        padding="longest",
        truncation=True,
        max_length=config.max_length,
        add_special_tokens=True,
        return_tensors="pt",
    )


def _t5_pool_one(
    seq_idx: int,
    hidden_states: Any,
    attention_mask: Any,
    valid_layers: list[int],
    start_idx: int,
    config: EmbeddingConfig,
) -> list[ChunkEmbedding]:
    """Pool one batched sequence's hidden states into ChunkEmbedding rows.

    Two pooling paths:
    * ``cls``: position 0 (``<AA2fold>`` on ProstT5, otherwise first AA).
    * residue: strip prefix (``start_idx``) and trailing EOS so residues
      start at AA 0 and ``residues.shape[0]`` equals the amino-acid count.
    """
    import torch.nn.functional as F

    actual_len = int(attention_mask[seq_idx].sum().item())
    if config.pooling == "cls":
        layer_tensors_1d = [
            hidden_states[-(li + 1)][seq_idx, 0, :].float() for li in valid_layers
        ]
        pooled = _aggregate_1d(layer_tensors_1d, config.layer_agg)
        if config.normalize:
            pooled = F.normalize(pooled.unsqueeze(0), p=2, dim=1).squeeze(0)
        return [ChunkEmbedding(0, None, pooled.cpu().numpy())]
    layer_tensors_2d = [
        hidden_states[-(li + 1)][seq_idx, start_idx : actual_len - 1, :].float()
        for li in valid_layers
    ]
    residues = _aggregate_residue_layers(layer_tensors_2d, config.layer_agg)
    if config.normalize_residues:
        residues = F.normalize(residues, p=2, dim=1)
    return _chunk_and_pool(residues, config)


def _embed_t5(
    model: Any,
    tokenizer: Any,
    sequences: list[str],
    config: EmbeddingConfig,
    device: str,
    mode: _T5Mode = _T5Mode(),
) -> list[list[ChunkEmbedding]]:
    """Embed sequences with T5EncoderModel (ProstT5, prot_t5_xl, Ankh, …).

    Sequences are processed as a padded batch.  ProSTT5 mode is auto-detected
    from ``config.model_name`` (looks for ``prostt5`` substring, case-insensitive)
    when ``mode.use_aa2fold`` is ``None``; callers (e.g. the Ankh backend) can
    pass ``use_aa2fold=False`` via ``mode`` to disable the prefix unconditionally.

    Tokenisation strategies live on ``mode.split_into_words``:
    ``False`` → space-joined string (ProstT5 / prot_t5_xl SentencePiece);
    ``True`` → per-residue list with ``is_split_into_words=True`` (Ankh,
    whose tokeniser maps a literal space to ``<unk>``).

    Residue slicing strips the optional ``<AA2fold>`` prefix and trailing
    EOS so ``residues[0]`` is always AA 0 and ``residues.shape[0]`` equals
    the amino-acid count, matching the convention used by ``_embed_esm`` /
    ``_embed_esm3c``.
    """
    import torch

    use_aa2fold = (
        mode.use_aa2fold
        if mode.use_aa2fold is not None
        else "prostt5" in config.model_name.lower()
    )
    cleaned = [re.sub(r"[UZOB]", "X", seq_str) for seq_str in sequences]
    inputs = _t5_tokenise(tokenizer, cleaned, config, mode, use_aa2fold)
    inputs = {k: v.to(device) for k, v in inputs.items()}

    hidden_states = t5_forward_pass(
        model, inputs["input_ids"], inputs["attention_mask"]
    )

    valid_layers = _validate_layers(config.layer_indices, hidden_states, "T5", "batch")
    start_idx = 1 if use_aa2fold else 0  # skip <AA2fold> on ProstT5
    results: list[list[ChunkEmbedding]] = [
        _t5_pool_one(
            i, hidden_states, inputs["attention_mask"], valid_layers, start_idx, config
        )
        for i in range(len(sequences))
    ]

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
        mode=_T5Mode(use_aa2fold=False, split_into_words=True),
    )


# ---------------------------------------------------------------------------
# Backend: ESM3c (ESM SDK ESMC)
# ---------------------------------------------------------------------------


def _embed_esm3c_one(
    model: Any,
    seq_str: str,
    config: EmbeddingConfig,
    device_obj: Any,
) -> list[ChunkEmbedding]:
    """ESM3c forward pass + pooling for one sequence.

    Used by :func:`_embed_esm3c` to keep the per-batch loop body readable.
    Strips BOS (position 0) and EOS (position -1) tokens before residue-level
    pooling, matching PIS / FANTASIA behaviour.
    """
    import torch
    import torch.nn.functional as F
    from esm.sdk.api import ESMProtein, LogitsConfig

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
    if isinstance(hs, torch.Tensor):
        hs = [hs[i] for i in range(hs.shape[0])]

    valid_layers = _validate_layers(config.layer_indices, hs, "ESM3c", seq_str[:20])

    if config.pooling == "cls":
        # BOS token at position 0 (before stripping)
        layer_tensors_1d = [hs[-(li + 1)][0, 0, :].float() for li in valid_layers]
        pooled = _aggregate_1d(layer_tensors_1d, config.layer_agg)
        if config.normalize:
            pooled = F.normalize(pooled.unsqueeze(0), p=2, dim=1).squeeze(0)
        chunks = [ChunkEmbedding(0, None, pooled.cpu().numpy())]
    else:
        # Strip BOS (0) and EOS (-1): positions [1:-1]
        layer_tensors_2d = [hs[-(li + 1)][0, 1:-1, :].float() for li in valid_layers]
        residues = _aggregate_residue_layers(layer_tensors_2d, config.layer_agg)
        if config.normalize_residues:
            residues = F.normalize(residues, p=2, dim=1)
        chunks = _chunk_and_pool(residues, config)

    del logits_output, hs
    torch.cuda.empty_cache()
    return chunks


def _embed_esm3c(
    model: Any,
    sequences: list[str],
    config: EmbeddingConfig,
    device: str,
) -> list[list[ChunkEmbedding]]:
    """Embed sequences with ESMC (ESM3c family).

    Uses the ESM SDK directly: no external tokenizer.  The model must have
    been loaded with ``ESMC.from_pretrained`` and cast to FP16.  Hidden states
    are returned via ``LogitsConfig(return_hidden_states=True)``.
    """
    import torch

    device_obj = torch.device(device) if isinstance(device, str) else device
    results: list[list[ChunkEmbedding]] = []
    with torch.no_grad():
        for seq_str in sequences:
            results.append(_embed_esm3c_one(model, seq_str, config, device_obj))
    return results


# ---------------------------------------------------------------------------
# Backend dispatch registry (T2A.5 of master plan v3.2)
# ---------------------------------------------------------------------------

_BACKEND_FN_NAMES: dict[str, str] = {
    "esm3c": "_embed_esm3c",
    "t5": "_embed_t5",
    "ankh": "_embed_ankh",
    "esm": "_embed_esm",
}
"""Per-``model_backend`` lookup replacing the duplicated
``if model_backend ==`` chains in ``ComputeEmbeddings(Batch)Operation._embed_batch``.

When ``model_backend`` is unset or unknown the dispatch falls back to
``_embed_esm`` (HuggingFace ``EsmModel``), matching the legacy
``# esm / auto`` branch.

Stored as function names rather than direct references so
``unittest.mock.patch("protea.core.operations.compute_embeddings._embed_ankh", ...)``
behaves correctly: the dispatcher resolves the name via ``getattr`` on
this module each call, so monkey-patching the symbol routes through.

Once ``protea-backends`` exposes its plugin entry_points (T2A.1-T2A.4),
this dict can be replaced with an ``importlib.metadata.entry_points``
lookup without touching any caller.
"""


def _dispatch_embed(
    model: Any,
    tokenizer: Any,
    sequences: list[str],
    config: EmbeddingConfig,
    device: str,
) -> list[list[ChunkEmbedding]]:
    """Look up the per-backend embed function in ``_BACKEND_FN_NAMES`` and call it.

    Defaults to ``_embed_esm`` (HuggingFace ``EsmModel``) when
    ``config.model_backend`` is missing from the registry. ESM3c is
    special-cased because the ESMC SDK exposes a tokenizer-free interface
    via ``model.encode``; every other backend takes a HuggingFace tokenizer.
    """
    import sys

    backend = config.model_backend
    fn_name = _BACKEND_FN_NAMES.get(backend, "_embed_esm")
    fn = getattr(sys.modules[__name__], fn_name)
    if backend == "esm3c":
        return fn(model, sequences, config, device)
    return fn(model, tokenizer, sequences, config, device)


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

    Raises ``ValueError`` if ``overlap >= chunk_size``; such a configuration
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
