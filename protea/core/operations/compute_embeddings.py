from __future__ import annotations

import time
import uuid
from typing import Annotated, Any
from uuid import UUID

from pydantic import BeforeValidator, Field, model_validator
from sqlalchemy import select
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload, RetryLaterError
from protea.core.contracts.parent_progress import update_parent_progress
from protea.core.operations._compute_embeddings_backends import (
    ChunkEmbedding,
    _aggregate_1d,
    _aggregate_residue_layers,
    _chunk_and_pool,
    _compute_chunk_spans,
    _embed_ankh,
    _embed_esm,
    _embed_esm3c,
    _embed_t5,
    _validate_layers,
)
from protea.core.operations._compute_embeddings_helpers import (
    _residue_budget,
    build_batch_dispatch_messages,
    build_store_message,
    clean_config_ids,
    fold_legacy_store_groups,
    missing_for_any_config,
    normalise_config_ids,
    serialize_inferred_chunks,
    store_group_rows,
)
from protea.core.operations._embedding_pass_group import (
    assert_layers_available,
    load_pass_group,
    run_group_inference,
    shared_pass_emitter,
    warn_if_pass_not_shared,
)
from protea.core.utils import contract_payload
from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
from protea.infrastructure.orm.models.job import Job, JobStatus
from protea.infrastructure.orm.models.protein.protein import Protein
from protea.infrastructure.orm.models.query.query_set import QuerySetEntry
from protea.infrastructure.orm.models.sequence.sequence import Sequence

# Re-export the backend embed functions so:
#   * ``getattr(sys.modules[__name__], fn_name)`` in ``_dispatch_embed``
#     resolves the entry from this module's namespace, and
#   * the documented ``unittest.mock.patch(
#       "protea.core.operations.compute_embeddings._embed_*")``
#     pattern keeps working without any test changes.
__all__ = [
    "ChunkEmbedding",
    "_aggregate_1d",
    "_aggregate_residue_layers",
    "_chunk_and_pool",
    "_compute_chunk_spans",
    "_embed_ankh",
    "_embed_esm",
    "_embed_esm3c",
    "_embed_t5",
    "_validate_layers",
]

PositiveInt = Annotated[int, Field(gt=0)]
#: A config group, accepting the one-config payload shape (see ``clean_config_ids``).
ConfigIdGroup = Annotated[list[str], BeforeValidator(clean_config_ids)]


# ---------------------------------------------------------------------------
# Payloads
# ---------------------------------------------------------------------------


class _ConfigGroupPayload(ProteaPayload, frozen=True):
    """Base for the payloads that carry a group of configs sharing one forward pass."""

    embedding_config_ids: ConfigIdGroup

    _accept_legacy = model_validator(mode="before")(normalise_config_ids)

    @property
    def embedding_config_id(self) -> str:
        """The group's first config, for callers that only ever meant one.

        A read-only view, not a second stored field: two fields holding one fact
        drift, and the one that drifts is whichever a reader happened to pick.
        """
        return self.embedding_config_ids[0]


class ComputeEmbeddingsPayload(_ConfigGroupPayload, frozen=True):
    """Coordinator payload: decides *which* sequences to embed and how to batch.

    The coordinator publishes N ephemeral operation messages to
    ``protea.embeddings.batch``.  Any worker consuming that queue picks up a
    message and runs ``ComputeEmbeddingsBatchOperation``: no child Job rows
    are created in the DB.

    Fields
    ------
    embedding_config_ids : list[str]
        UUIDs of the EmbeddingConfig rows to compute from ONE forward pass, in
        practice a layer grid over one model.  The group is checked by
        ``_embedding_pass_group``.  A payload written in the one-config shape
        (``embedding_config_id`` as a string, as the twelve historical jobs are)
        is folded into this field and behaves exactly as it did.
    accessions : list[str] | None
        Restrict to proteins with these UniProt accessions.  None = all.
    sequences_per_job : int
        How many sequences each batch message processes.  Tune to GPU memory.
    device : str
        Device passed down to each batch worker (``"cuda"`` or ``"cpu"``).
    skip_existing : bool
        Skip sequences that already have an embedding for EVERY config in the
        group.  A sequence one config is missing is still inferred, and the
        configs that already hold it skip it again at write time.
    batch_size : int
        Model forward-pass batch size inside each batch worker.  Defaults to
        ``1`` because the largest supported backend (``prot_t5_xl_uniref50``
        at ``max_length=2048``) OOMs on a 12 GB GPU with anything higher.
        Callers running smaller models on roomier GPUs can raise it explicitly.
    """

    accessions: list[str] | None = None
    query_set_id: str | None = None
    sequences_per_job: PositiveInt = 64
    device: str = "cuda"
    skip_existing: bool = True
    batch_size: PositiveInt = 1


class ComputeEmbeddingsBatchPayload(_ConfigGroupPayload, frozen=True):
    """Payload for a batch message: one forward pass serving the whole config group."""

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

    - **protst** : text-aligned ProtST-ESM1b (``mila-intel/ProtST-esm1b``).
      Returns the whole-protein ``protein_feature`` projection (512-d,
      orthogonal text-aligned signal); one full-sequence chunk per
      sequence, honouring only ``normalize`` (residue-level knobs do not
      apply).  Served entirely by the ``protst`` protea-backends plugin.

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

        # Both shapes read straight from the raw payload: summaries render for
        # jobs already on record, so nothing here can assume the model saw it.
        cfg_ids_raw = p.get("embedding_config_ids") or (
            [p["embedding_config_id"]] if p.get("embedding_config_id") else []
        )
        cfg_id_raw = cfg_ids_raw[0] if cfg_ids_raw else None
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
        if len(cfg_ids_raw) > 1:
            bits.append(f"{len(cfg_ids_raw)} configs/pass")

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
        p = ComputeEmbeddingsPayload.model_validate(contract_payload(payload))
        parent_job_id = UUID(payload["_job_id"])

        # Refused here as well as in the batch worker so a group that does not
        # share a pass costs no GPU time at all, rather than one batch's worth.
        configs = load_pass_group(session, p.embedding_config_ids)

        self._refuse_if_gpu_busy(session, parent_job_id)

        config_ids = [cfg.id for cfg in configs]
        sequence_ids = self._load_sequence_ids(session, p, config_ids, emit)
        n_configs = len(configs)
        if not sequence_ids:
            emit("compute_embeddings.no_sequences", None, {}, "warning")
            return OperationResult(result={"batches": 0, "sequences": 0, "configs": n_configs})

        operations = build_batch_dispatch_messages(p, parent_job_id, sequence_ids)
        n_batches = len(operations)
        result = {
            "batches": n_batches,
            "sequences": len(sequence_ids),
            "configs": n_configs,
        }

        emit(
            "compute_embeddings.dispatching",
            None,
            {
                "total_sequences": len(sequence_ids),
                "sequences_per_job": p.sequences_per_job,
                "batches": n_batches,
                "configs": n_configs,
            },
            "info",
        )

        return OperationResult(
            result=result,
            progress_current=0,
            progress_total=n_batches,
            deferred=True,
            publish_operations=operations,
        )

    def _refuse_if_gpu_busy(self, session: Session, parent_job_id: UUID) -> None:
        """Only one compute_embeddings job at a time: the GPU is a shared resource."""
        conflict = (
            session.query(Job)
            .filter(
                Job.operation == "compute_embeddings",
                Job.status == JobStatus.RUNNING,
                Job.id != parent_job_id,
            )
            .first()
        )
        if conflict is None:
            return
        from protea.config.tuning import get_tuning

        raise RetryLaterError(
            f"GPU busy: compute_embeddings job {conflict.id} is already running. "
            f"Will retry automatically.",
            delay_seconds=get_tuning().operation.gpu_busy_retry_seconds,
        )

    def _load_sequence_ids(
        self,
        session: Session,
        p: ComputeEmbeddingsPayload,
        config_ids: list[uuid.UUID],
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
            q = q.filter(missing_for_any_config(config_ids))

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

        Delegates to ``_dispatch_embed``, which routes through the
        backend plugin's ``embed_chunks`` (T2A.5b plugin-only path).
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
        p = ComputeEmbeddingsBatchPayload.model_validate(contract_payload(payload))
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

        configs = load_pass_group(session, p.embedding_config_ids)

        sequences = session.query(Sequence).filter(Sequence.id.in_(p.sequence_ids)).all()
        t0 = time.perf_counter()
        emit(
            "compute_embeddings_batch.start",
            None,
            {
                "sequences": len(sequences),
                "configs": len(configs),
                "parent_job_id": str(parent_job_id),
            },
            "info",
        )

        per_config = self._infer_group(session, configs, sequences, p, emit)

        total_s = time.perf_counter() - t0
        phases = getattr(self, "_last_phase_timings", None) or {}
        inferred = len(per_config[0][1]) if per_config else 0
        fields: dict[str, Any] = {
            "sequences_inferred": inferred,
            "configs_written": len(per_config),
            "elapsed_seconds": total_s,
            # First config, and exact: the guard pinned ``max_length`` group-wide.
            **_residue_budget(sequences, configs[0]),
            **phases,
        }
        # The fraction the substrate plan turns on. Reported per batch so it can
        # be aggregated per model and per device without re-deriving it from
        # timestamps, which do not survive concurrent workers.
        if phases and total_s > 0:
            accounted = sum(phases.values())
            fields["inference_fraction"] = round(phases["inference_s"] / total_s, 4)
            fields["unaccounted_s"] = round(total_s - accounted, 3)
        emit("compute_embeddings_batch.done", None, fields, "info")
        return OperationResult(
            result={"sequences_inferred": inferred, "configs_written": len(per_config)},
            publish_operations=[build_store_message(parent_job_id, p, per_config)],
        )

    def _infer_group(
        self,
        session: Session,
        configs: list[EmbeddingConfig],
        sequences: list[Sequence],
        p: ComputeEmbeddingsBatchPayload,
        emit: EmitFn,
    ) -> list[tuple[str, list[dict]]]:
        """Infer the whole group as ``(config_id, write_sequences)`` pairs.

        The model is loaded from the group's first config because the guard has
        pinned ``model_name`` and ``model_backend``, and with them the
        ``(name, backend, device)`` cache key: every config in the group resolves
        the same weights. The learned-code path is reached only for a group of
        one, because ``assert_shared_forward_pass`` refuses to group a learned
        config: its pass is a base config's, which the group does not describe.
        """
        from protea.core.operations._learned_code_embed import is_learned_code_config

        if len(configs) == 1 and is_learned_code_config(configs[0]):
            rows = self._infer_all_learned_code(session, configs[0], sequences, p, emit)
            return [(str(configs[0].id), rows)]

        t_load0 = time.perf_counter()
        model, tokenizer = self._load_model(configs[0], p.device, emit)
        load_s = time.perf_counter() - t_load0
        assert_layers_available(configs, model)
        warn_if_pass_not_shared(configs, emit)

        def embed_group(seq_strs: list[str]) -> list[list[list[ChunkEmbedding]]]:
            return self._embed_batch_group(model, tokenizer, seq_strs, configs, p.device)

        per_config, self._last_phase_timings = run_group_inference(
            embed_group, configs, sequences, p.batch_size, load_s
        )
        return per_config

    def _infer_all_learned_code(
        self,
        session: Session,
        config: EmbeddingConfig,
        sequences: list[Sequence],
        p: ComputeEmbeddingsBatchPayload,
        emit: EmitFn,
    ) -> list[dict]:
        """Learned-code path: base-embed each query on the fly, then apply the head.

        A learned-code config (e.g. the pinned ``d8979601`` k-WTA retrieval
        encoder) has no HuggingFace model to load; its codes are produced from a
        BASE PLM config's embeddings. ``embed_learned_code`` resolves the base
        config + head artifact and returns the 2048-d codes as one ChunkEmbedding
        per sequence, which serialise + persist under the learned config exactly
        like any other embedding (KNN then reuses them; computed once per query).
        """
        from protea.core.operations._learned_code_embed import embed_learned_code

        def embed_base(
            base_config: EmbeddingConfig, seq_batch: list[str]
        ) -> list[list[ChunkEmbedding]]:
            model, tokenizer = _get_or_load_model(base_config, p.device, emit)
            return _dispatch_embed(model, tokenizer, seq_batch, base_config, p.device)

        batch_chunks = embed_learned_code(
            session,
            config,
            [s.sequence for s in sequences],
            emit,
            embed_base=embed_base,
            batch_size=p.batch_size,
        )
        return serialize_inferred_chunks(sequences, batch_chunks)

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
        """Per-batch dispatch shim; delegates to ``_dispatch_embed`` (T2A.5b)."""
        return _dispatch_embed(model, tokenizer, sequences, config, device)

    def _embed_batch_group(
        self,
        model: Any,
        tokenizer: Any,
        sequences: list[str],
        configs: list[EmbeddingConfig],
        device: str,
    ) -> list[list[list[ChunkEmbedding]]]:
        """Per-batch group dispatch: one chunk-list-per-sequence per config, in group order.

        ``embed_chunks_multi`` when the backend offers it (the route the saved
        hours live on); one ``self._embed_batch`` per config otherwise, correct
        and N passes. See ``shared_pass_emitter`` and ``warn_if_pass_not_shared``.
        Routing through ``self._embed_batch`` keeps the documented
        ``patch.object(op, "_embed_batch")`` seam covering the group path too.
        """
        shared = shared_pass_emitter(configs[0])
        if shared is not None:
            return shared(model, tokenizer, sequences, configs, _effective_device(device))
        return [self._embed_batch(model, tokenizer, sequences, cfg, device) for cfg in configs]


# ---------------------------------------------------------------------------
# Write operation (CPU worker — no GPU required)
# ---------------------------------------------------------------------------


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


def _get_backend_plugins() -> dict[str, Any]:
    """Thin wrapper around ``protea.core.plugins.discover_plugins``.

    The shared discovery helper handles the cache + name-mismatch hard
    error; this shim exists so the resolver below has a stable internal
    name for monkey-patching in tests.
    """
    from protea.core.plugins import discover_plugins

    return discover_plugins("protea.backends")


def _resolve_backend(backend_name: str) -> Any:
    """Resolve a ``model_backend`` identifier to a plugin instance.

    The ``"auto"`` legacy alias maps to ``"esm"``. Unknown identifiers
    raise ``ValueError`` listing the discovered backends so the failure
    message is actionable.
    """
    plugins = _get_backend_plugins()
    key = "esm" if backend_name == "auto" else backend_name
    if key not in plugins:
        raise ValueError(f"Unknown model_backend: {backend_name!r}. Discovered: {sorted(plugins)}")
    return plugins[key]


def _effective_device(device: str, emit: EmitFn | None = None) -> str:
    """Return the device to actually use, degrading ``cuda`` to ``cpu`` when
    no CUDA runtime is available.

    A config (or default) asking for ``cuda`` on a host whose torch build is
    CPU-only previously surfaced as a hard, non-retryable
    ``AssertionError: Torch not compiled with CUDA enabled`` that failed the
    whole embedding job (and any annotation of a novel sequence). Falling back
    to CPU keeps the instance operational; it is slower, but it works.
    """
    if not device.lower().startswith("cuda"):
        return device
    import torch

    if torch.cuda.is_available():
        return device
    if emit is not None:
        emit(
            "compute_embeddings.cuda_unavailable_cpu_fallback",
            None,
            {"requested": device},
            "warning",
        )
    return "cpu"


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
    model, tokenizer = plugin.load_model(config.model_name, _effective_device(device, emit), emit)
    emit("compute_embeddings.model_load_done", None, {}, "info")
    return model, tokenizer


# ---------------------------------------------------------------------------
# Backend dispatch (T2A.5b: pure plugin path)
# ---------------------------------------------------------------------------


def _dispatch_embed(
    model: Any,
    tokenizer: Any,
    sequences: list[str],
    config: EmbeddingConfig,
    device: str,
) -> list[list[ChunkEmbedding]]:
    """Route the batch to the resolved backend plugin's ``embed_chunks``.

    T2A.5b collapsed the legacy ``_BACKEND_FN_NAMES`` fall-back into a
    single line: every ``model_backend`` PROTEA supports out of the box
    (``esm``, ``auto``, ``t5``, ``ankh``, ``esm3c``) is now served by the
    matching ``protea-backends`` plugin via ``plugin.embed_chunks``.
    ``_resolve_backend`` raises ``ValueError`` for unknown identifiers,
    so the dispatch never silently falls back on a wrong backend. The
    legacy ``_embed_*`` shims in ``_compute_embeddings_backends`` are
    retained as the bit-exact regression reference for the parity tests.
    """
    plugin = _resolve_backend(config.model_backend)
    # Match the device the model was actually loaded on (see _effective_device):
    # if cuda was requested but is unavailable, the model lives on CPU and the
    # inputs must follow, or embed_chunks would try to move them to a missing
    # GPU and re-trigger the "not compiled with CUDA" failure.
    return plugin.embed_chunks(model, tokenizer, sequences, config, _effective_device(device))  # type: ignore[no-any-return]
