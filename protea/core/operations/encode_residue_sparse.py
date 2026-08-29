"""Encode a corpus with a residue-level sparse encoder, which pooled embeddings cannot express.

WHY THIS IS NOT ``apply_learned_encoder``

That operation projects a protein's ALREADY POOLED embedding through a learned map.
It is the right shape for the encoder currently deployed, and it is structurally
unable to run the recipe measured here, because the recipe selects atoms per residue
BEFORE the residues are averaged. Top-k and averaging do not commute: the window
where they disagree is real and it is where the gain lives. By the time a pooled
vector exists, which residue activated which atom is gone and no map applied
afterwards can recover it.

So this operation runs the language model. That is the cost, and it is the whole
cost: everything downstream is unchanged.

WHAT IT WRITES, AND WHY THAT CHOICE

``SequenceEmbedding`` rows under a fresh ``EmbeddingConfig``, exactly as
``apply_learned_encoder`` does. That is deliberate rather than convenient. The
unchanged ``predict_go_terms`` / KNN / re-ranker / ``run_cafa_evaluation`` path
consumes them by pointing at the new ``embedding_config_id``, so a better encoder
reaches the evaluation without a schema change, without a migration, and without a
second retrieval path to keep in step with the first.

THE RECIPE IS FROZEN IN THE ARTIFACT, NOT IN THIS FILE

``k_residue``, ``k_sequence``, the layer and the dictionary width live in the
artifact's ``meta`` and are read from it. An artifact that does not declare them is
REFUSED rather than defaulted, because a pooled-encoder artifact would otherwise run
through this path and produce a complete, plausible, differently-computed set of
codes. The measured recipe is layer -1, ``k_residue=4``, mean over residues,
``k_sequence=128``, over a 2048-atom dictionary.

WHAT IT DOES NOT DO

It does not train. Fitting the map belongs in ``protea-reranker-lab``, like every
other training in this system, and the map arrives here frozen through the artifact
store.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Annotated, Any

import numpy as np
from pydantic import Field, field_validator, model_validator
from sqlalchemy import select
from sqlalchemy.orm import Session

from protea.core.contracts.operation import (
    EmitFn,
    Operation,
    OperationResult,
    ProteaPayload,
)
from protea.core.operations._compute_embeddings_helpers import (
    fetch_embedding_scale,
    scale_and_clip_embedding,
)
from protea.core.operations._encode_residue_sparse_batch import encode_until_done
from protea.core.operations._encoder_artifact import (
    resolve_encoder_artifact,
    resolve_training_cut,
)
from protea.core.utils import contract_payload
from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
from protea.infrastructure.orm.models.embedding.sequence_embedding import SequenceEmbedding
from protea.infrastructure.orm.models.sequence.sequence import Sequence

#: Fields the artifact must declare. Absent any of them the artifact is refused:
#: defaulting them would let a pooled-encoder artifact run through the residue path
#: and produce codes that look right and were computed from something else.
#:
#: ``aggregate`` joined this list once the aggregate stopped being a constant. A code
#: built from two moments and one built from the mean have the same weights and
#: different widths, so a default here would silently halve or double a corpus.
REQUIRED_META = (
    "k_residue",
    "k_sequence",
    "dict_dim",
    "in_dim",
    "layer_indices",
    "aggregate",
    "order",
    "training_release",
)

#: Which of two pipelines the map was fitted for.
#:
#: ``select-then-pool`` applies the map per residue, keeps ``k_residue``
#: atoms of each, aggregates, then keeps ``k_sequence``. It is what this
#: operation implements and what every shipped recipe uses.
#:
#: ``pool-then-select`` aggregates the residues first and applies the map
#: to the result. It is the natural control arm, and it is NOT implemented
#: here: declaring it is refused with a message saying so, rather than
#: executed as the other order.
#:
#: The distinction cannot be recovered from the weights. The map is affine,
#: so mean(X @ W + b) equals mean(X) @ W + b to within 7e-07, and an
#: artifact fitted either way has identical shapes and passes every check
#: on them. Served under the wrong order it produced a code sharing 130 of
#: 2048 atoms with the intended one, at cosine 0.10. Not a degradation, a
#: different encoder.
ORDERS = ("select-then-pool", "pool-then-select")

#: How the per-residue codes become one code. ``mean`` is the first moment alone;
#: ``moments`` concatenates the mean with the per-atom dispersion and therefore emits
#: twice the dictionary width. Both are a fixed-width reduction over residues, which is
#: what keeps a corpus pass costing one protein of memory rather than the corpus.
AGGREGATES = ("mean", "moments")

#: Backend tag on the produced config, so a code produced here is never confused
#: with one produced from pooled vectors when both are in the same table.
TARGET_BACKEND = "residue-sparse"

PositiveInt = Annotated[int, Field(gt=0)]


#: Queue the per-batch messages are addressed to. It is the same queue
#: ``compute_embeddings`` fans out to, because the requirement is identical: a
#: worker with a card. Reusing it means this operation reaches the card through
#: machinery that already exists rather than through a second path to keep in step.
_BATCH_QUEUE = "protea.embeddings.batch"


class EncodeResidueSparsePayload(ProteaPayload, frozen=True):
    """Coordinator: which corpus, which frozen encoder, under what name, in what batches.

    The coordinator does no inference. It resolves the artifact, creates the target
    config, decides which sequences still need a code, and publishes one message per
    batch to the queue a worker with a card consumes. That split is not tidiness: the
    process that consumes the operations queue runs on the host that owns the state,
    and that host has no card, so an operation that does its work inline cannot run at
    all in the deployed topology.
    """

    source_embedding_config_id: str
    encoder_artifact_path: str | None = None
    encoder_artifact_uri: str | None = None
    target_model_name: str = "residue-sparse"
    device: str = "cuda"
    batch_size: PositiveInt = 32
    residue_budget: PositiveInt = 4096
    sequences_per_job: PositiveInt = 512
    sequence_id_limit: int | None = None
    skip_existing: bool = True

    @field_validator("source_embedding_config_id", mode="before")
    @classmethod
    def _non_empty(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("must be a non-empty string")
        return v.strip()

    @model_validator(mode="after")
    def _exactly_one_address(self) -> EncodeResidueSparsePayload:
        """Refuse both addresses and refuse neither.

        A local path resolves against whichever host runs the work, and with the
        dispatcher and the card on different machines there is no path that means the
        same thing on both. The URI is the address that does, so both are accepted and
        the ambiguity of carrying both is not.
        """
        has_path = bool((self.encoder_artifact_path or "").strip())
        has_uri = bool((self.encoder_artifact_uri or "").strip())
        if has_path == has_uri:
            raise ValueError(
                "give exactly one of encoder_artifact_path and encoder_artifact_uri. "
                "The path resolves on whichever host runs the work, so it is usable "
                "only when the dispatcher and the card are the same machine; the URI "
                "resolves through the artifact store and is usable when they are not"
            )
        return self


class EncodeResidueSparseBatchPayload(ProteaPayload, frozen=True):
    """One batch of sequences, addressed to a worker that has a card.

    Carries every field the worker needs, so no lookup happens between coordinator and
    worker beyond reading the sequences themselves. The target config already exists by
    the time this runs: the coordinator created it, so a batch never races another batch
    to create it.
    """

    source_embedding_config_id: str
    target_embedding_config_id: str
    sequence_ids: list[int]
    parent_job_id: str
    encoder_artifact_path: str | None = None
    encoder_artifact_uri: str | None = None
    device: str = "cuda"
    batch_size: PositiveInt = 32
    residue_budget: PositiveInt = 4096


def refuse_backend_without_residues(backend: Any, model_backend: str) -> None:
    """Refuse a backend that cannot emit per-residue output, before any work is done.

    Called at dispatch time rather than inside a batch. Without it the failure is an
    attribute error on a worker, after a model has been loaded, once per batch, and it
    names a missing method rather than the reason the request could never have worked.
    """
    if not hasattr(backend, "embed_batch_per_residue"):
        raise ValueError(
            f"backend {model_backend!r} has no embed_batch_per_residue, so it cannot "
            "produce the per-residue output this operation selects atoms from. Use a "
            "backend that emits residues, or apply_learned_encoder if a pooled code is "
            "what is wanted"
        )


def load_frozen_encoder(path: str) -> tuple[np.ndarray, np.ndarray, dict]:
    """Read the map and its recipe, refusing anything that does not declare one.

    The refusal is the point. An artifact fitted for pooled use has the same tensor
    shapes as one fitted for residue use, so nothing about the weights themselves
    would reveal the mistake, and the codes it produced would be complete and wrong.
    """
    data = np.load(path, allow_pickle=True)
    missing = [k for k in REQUIRED_META if k not in data]
    if missing:
        raise ValueError(
            f"{path} declares no {missing}; a residue-level encoder must state its "
            "recipe, and defaulting it would let a pooled-encoder artifact produce "
            "codes through this path that look right and were computed differently"
        )
    meta = {k: data[k].tolist() for k in REQUIRED_META}
    weight = np.asarray(data["W"], dtype=np.float32)
    bias = np.asarray(data["b"], dtype=np.float32)
    if weight.shape != (meta["in_dim"], meta["dict_dim"]):
        raise ValueError(
            f"the map is {weight.shape} but the recipe declares "
            f"({meta['in_dim']}, {meta['dict_dim']})"
        )
    if str(meta["aggregate"]) not in AGGREGATES:
        raise ValueError(
            f"{path} declares aggregate {meta['aggregate']!r}, which is not one of "
            f"{list(AGGREGATES)}. The aggregate decides the code's width, so an "
            "unknown one cannot be guessed"
        )
    order = str(meta["order"])
    if order not in ORDERS:
        raise ValueError(
            f"{path} declares order {order!r}, which is not one of {list(ORDERS)}. "
            "The order decides what the code IS, and it cannot be recovered from the "
            "weights: an artifact fitted either way has identical shapes"
        )
    if order == "pool-then-select":
        raise ValueError(
            f"{path} declares order 'pool-then-select', which this operation does not "
            "implement. Refused rather than run as 'select-then-pool', because the two "
            "produce different codes from the same weights and the difference is "
            "invisible in the output"
        )
    if int(meta["k_residue"]) >= int(meta["dict_dim"]):
        raise ValueError(
            f"k_residue {meta['k_residue']} selects the whole {meta['dict_dim']}-atom "
            "dictionary, which is not a sparse code. Note that under "
            "'pool-then-select' this is exactly the right value, since the map is "
            "affine and k_residue = dict_dim expresses pooling exactly; that is the "
            "hole this check used to have, and the order field is what closes it"
        )
    return weight, bias, meta


def topk_real(matrix: np.ndarray, keep: int) -> np.ndarray:
    """Keep the ``keep`` largest-magnitude entries per row, values preserved."""
    if keep >= matrix.shape[1]:
        return matrix
    out = np.zeros_like(matrix)
    idx = np.argpartition(-np.abs(matrix), keep - 1, axis=1)[:, :keep]
    np.put_along_axis(out, idx, np.take_along_axis(matrix, idx, axis=1), axis=1)
    return out


def reduce_residues(selected: np.ndarray, aggregate: str) -> np.ndarray:
    """Fold per-residue codes into one vector, by the aggregate the recipe declares.

    ``moments`` carries the per-atom dispersion beside the mean and so returns twice
    the dictionary width. It is worth the width for a reason that shows only when the
    code is read as a FEATURE rather than used as a metric: a cosine is one scalar over
    the whole code and cannot separate a dispersion that the mean already encodes
    another way, while a model reading every column can. Measured, the two are
    indistinguishable for retrieval and the second moment is clearly the better input
    to a downstream learner.

    Both reductions are fixed width in the number of residues, so neither changes what
    a corpus pass costs.
    """
    mean = selected.mean(axis=0)
    if aggregate == "mean":
        return mean
    var = np.maximum((selected * selected).mean(axis=0) - mean * mean, 0.0)
    return np.concatenate([mean, np.sqrt(var + 1e-12)])


def encode_one(
    residues: np.ndarray,
    weight: np.ndarray,
    bias: np.ndarray,
    k_residue: int,
    k_sequence: int,
    aggregate: str = "mean",
) -> np.ndarray:
    """One protein's code: project, select per residue, reduce, select again.

    The order is the finding. Selecting per residue first makes the reduction a
    magnitude-weighted usage histogram over the dictionary, where a feature intense
    in one region survives; averaging first blends it away before anything is
    selected, and no later selection brings it back.
    """
    projected = residues.astype(np.float32) @ weight + bias
    pooled = reduce_residues(topk_real(projected, k_residue), aggregate)
    return topk_real(pooled[None, :], k_sequence)[0]


def code_density(code: np.ndarray) -> float:
    """Share of atoms a code actually uses, which a caller can gate a run on."""
    return float(np.count_nonzero(code) / code.size) if code.size else 0.0


def _emit_scope(emit: EmitFn, pending: list, source: EmbeddingConfig, meta: dict) -> None:
    """Say what is about to be dispatched, including the order the artifact declares.

    The order is on the scope event rather than only in the result, because it is the
    field that decides what the codes ARE and a reader watching a run should not have
    to wait for the end to see which one they are getting.
    """
    emit(
        "encode.scope",
        f"{len(pending)} sequences to encode through {source.model_name}",
        {
            "pending": len(pending),
            "source": source.model_name,
            "order": meta["order"],
            "k_residue": meta["k_residue"],
            "k_sequence": meta["k_sequence"],
            "dictionary": meta["dict_dim"],
        },
        "info",
    )


def _dispatch_result(
    operations: list[tuple[str, dict]],
    sequence_ids: list[int],
    target_id: uuid.UUID,
    meta: dict,
) -> OperationResult:
    """What the coordinator returns: the scope of the fan-out and where to read the codes."""
    return OperationResult(
        result={
            "batches": len(operations),
            "sequences": len(sequence_ids),
            "embedding_config_id": str(target_id),
            "recipe": {k: meta[k] for k in REQUIRED_META},
            "caveat": (
                "point predict_go_terms at this embedding_config_id to retrieve on "
                "these codes. Retrieval, the re-ranker and the evaluation are "
                "unchanged; only what they retrieve on differs"
            ),
        },
        progress_total=len(operations),
        publish_operations=operations,
    )


def build_encode_batch_messages(
    p: EncodeResidueSparsePayload,
    parent_job_id: uuid.UUID,
    source_id: uuid.UUID,
    target_id: uuid.UUID,
    sequence_ids: list[int],
) -> list[tuple[str, dict]]:
    """Partition the pending sequences and address one message per batch to the card queue.

    Every field a worker needs travels in the message, so nothing is looked up between
    coordinator and worker beyond reading the sequences themselves. The target config
    id is passed rather than derived, so two batches never race each other to create it.
    """
    batches = [
        sequence_ids[i : i + p.sequences_per_job]
        for i in range(0, len(sequence_ids), p.sequences_per_job)
    ]
    parent = str(parent_job_id)
    return [
        (
            _BATCH_QUEUE,
            {
                "operation": "encode_residue_sparse_batch",
                "job_id": parent,
                "payload": {
                    "source_embedding_config_id": str(source_id),
                    "target_embedding_config_id": str(target_id),
                    "sequence_ids": chunk,
                    "parent_job_id": parent,
                    "encoder_artifact_path": p.encoder_artifact_path,
                    "encoder_artifact_uri": p.encoder_artifact_uri,
                    "device": p.device,
                    "batch_size": p.batch_size,
                },
            },
        )
        for chunk in batches
    ]


def _ensure_target_config(
    session: Session, p: EncodeResidueSparsePayload, meta: dict, source: EmbeddingConfig
) -> uuid.UUID:
    """Create, idempotently on name, the config the codes are stored under.

    The name carries the recipe rather than a version number, so two runs that
    differ in k produce two configs instead of silently sharing one.
    """
    name = (
        f"{p.target_model_name}:k{meta['k_residue']}:d{meta['dict_dim']}:"
        f"s{meta['k_sequence']}:{str(p.source_embedding_config_id)[:8]}"
    )
    existing = session.execute(
        select(EmbeddingConfig).where(EmbeddingConfig.model_name == name)
    ).scalar_one_or_none()
    if existing is not None:
        return existing.id
    config = EmbeddingConfig(
        model_name=name,
        model_backend=TARGET_BACKEND,
        layer_indices=list(meta["layer_indices"]),
        layer_agg="mean",
        pooling="residue-sparse-mean",
        normalize_residues=False,
        normalize=False,
        use_chunking=bool(source.use_chunking),
        chunk_size=source.chunk_size,
        chunk_overlap=source.chunk_overlap,
        description=(
            f"residue-level sparse code: top-{meta['k_residue']} atoms per residue of "
            f"a {meta['dict_dim']}-atom dictionary, averaged over residues, then the "
            f"top {meta['k_sequence']}. Produced from a forward pass of "
            f"{source.model_name}, not from its pooled embeddings, because selection "
            "per residue does not commute with pooling"
        ),
        display_name=name,
        family=TARGET_BACKEND,
        trained_on_annotation_set_id=resolve_training_cut(session, meta),
    )
    session.add(config)
    session.flush()
    return config.id


def _pending_sequences(
    session: Session, source_id: uuid.UUID, target_id: uuid.UUID, p: EncodeResidueSparsePayload
) -> list[tuple[int, str]]:
    """Sequences in the source corpus that have no code yet, with their residues.

    Scoped to the SOURCE config rather than to every sequence in the database, so
    the population encoded is the one the caller named and not whatever happens to
    have a sequence row.

    Both memberships are EXISTS rather than a join and a NOT IN, which is not a matter
    of taste at this size. A join to the embedding table returns one row per chunk, so
    the DISTINCT that removed the duplicates had to sort the sequence TEXT of the whole
    corpus, and 210 million residues do not sort for free. NOT IN over a set this large
    cannot become an anti-join, because a single NULL in the subquery would change the
    answer and the planner has to assume one. Measured on a resumed corpus run: 22
    minutes and still going, on a query whose job is to decide what to do next.
    """
    in_source = (
        select(1)
        .where(
            SequenceEmbedding.sequence_id == Sequence.id,
            SequenceEmbedding.embedding_config_id == source_id,
        )
        .exists()
    )
    query = select(Sequence.id, Sequence.sequence).where(in_source).order_by(Sequence.id)
    if p.skip_existing:
        already_coded = (
            select(1)
            .where(
                SequenceEmbedding.sequence_id == Sequence.id,
                SequenceEmbedding.embedding_config_id == target_id,
            )
            .exists()
        )
        query = query.where(~already_coded)
    if p.sequence_id_limit is not None:
        query = query.limit(p.sequence_id_limit)
    return [(int(i), s) for i, s in session.execute(query).all()]


@dataclass(frozen=True)
class _Run:
    """Everything a batch needs that does not change between batches.

    A dataclass rather than ten parameters, because a helper that takes ten is one
    a caller can silently pass in the wrong order.
    """

    backend: Any
    model: Any
    tokenizer: Any
    weight: np.ndarray
    bias: np.ndarray
    meta: dict
    target_id: uuid.UUID
    scale: float
    batch_size: int
    residue_budget: int


def _encode_batch(run: _Run, batch: list[tuple[int, str]], emit: EmitFn) -> tuple[list[dict], dict]:
    """One forward pass, one code per sequence, plus what the caller reports on."""
    result = run.backend.embed_batch_per_residue(
        run.model,
        run.tokenizer,
        [s for _i, s in batch],
        emit=emit,
        layers=list(run.meta["layer_indices"]),
    )
    rows: list[dict] = []
    densities: list[float] = []
    residues_seen = clipped = 0
    for (sequence_id, _seq), residues in zip(batch, result.residues, strict=True):
        residues_seen += int(residues.shape[0])
        code = encode_one(
            residues,
            run.weight,
            run.bias,
            int(run.meta["k_residue"]),
            int(run.meta["k_sequence"]),
            str(run.meta["aggregate"]),
        )
        densities.append(code_density(code))
        vector, n_clipped = scale_and_clip_embedding(code.tolist(), run.scale)
        clipped += n_clipped
        rows.append(
            {
                "sequence_id": sequence_id,
                "embedding_config_id": run.target_id,
                "chunk_index_s": 0,
                "chunk_index_e": None,
                "embedding": vector,
                "embedding_dim": int(code.size),
            }
        )
    return rows, {"clipped": clipped, "residues": residues_seen, "densities": densities}


class EncodeResidueSparseOperation(Operation):
    name = "encode_residue_sparse"
    description = (
        "Encode a corpus with a residue-level sparse encoder by running the language "
        "model and selecting atoms per residue BEFORE pooling, which apply_learned_encoder "
        "cannot do because it starts from vectors that are already pooled and top-k does "
        "not commute with averaging. Codes are stored as SequenceEmbedding rows under a "
        "new EmbeddingConfig, so KNN, the re-ranker and the evaluation consume them "
        "unchanged. The recipe is read from the artifact and an artifact that does not "
        "declare one is refused. Trains nothing."
    )

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        """Resolve, scope, and hand the work to whoever has a card.

        Nothing is inferred here. The coordinator runs on the host that owns the
        state, which has no card, so every decision that needs the database is made
        here and every decision that needs the model is made in a batch.
        """
        p = EncodeResidueSparsePayload.model_validate(contract_payload(payload))
        parent_job_id = uuid.UUID(payload["_job_id"])
        source = session.get(EmbeddingConfig, uuid.UUID(p.source_embedding_config_id))
        if source is None:
            raise ValueError(f"EmbeddingConfig {p.source_embedding_config_id} not found")

        from protea.core.operations.compute_embeddings import _resolve_backend

        refuse_backend_without_residues(
            _resolve_backend(source.model_backend), source.model_backend
        )

        artifact = resolve_encoder_artifact(p.encoder_artifact_path, p.encoder_artifact_uri)
        _weight, _bias, meta = load_frozen_encoder(artifact)
        target_id = _ensure_target_config(session, p, meta, source)
        pending = _pending_sequences(session, source.id, target_id, p)
        _emit_scope(emit, pending, source, meta)
        if not pending:
            return OperationResult(
                result={
                    "encoded": 0,
                    "embedding_config_id": str(target_id),
                    "note": "every sequence already had a code",
                }
            )

        ids = [i for i, _s in pending]
        operations = build_encode_batch_messages(p, parent_job_id, source.id, target_id, ids)
        emit(
            "encode.dispatching",
            f"{len(operations)} batches to {_BATCH_QUEUE}",
            {"batches": len(operations), "sequences": len(ids), "device": p.device},
            "info",
        )
        return _dispatch_result(operations, ids, target_id, meta)

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        return (
            f"encode the corpus of embedding config "
            f"{payload.get('source_embedding_config_id')} with a residue-level sparse encoder"
        )


def _drop_already_encoded(
    session: Session,
    target_id: uuid.UUID,
    pending: list[tuple[int, str]],
    emit: EmitFn,
) -> list[tuple[int, str]]:
    """Remove the sequences this batch has already written, so a retry can finish it.

    The batch commits every ``batch_size`` sequences, which keeps peak memory at one
    forward pass rather than the batch. That makes a failure halfway through leave a
    committed prefix, and a retry that started from the beginning collided with its own
    prefix on the very first insert. So the backoff was written for a transient fault
    and the state it left made the fault permanent: five retries, five identical unique
    violations, and a log naming the collision rather than the cause.

    Skipping the prefix is not only about the constraint, which the idempotent insert
    now absorbs anyway. It is about the forward pass: recomputing sixteen proteins on
    the card to discard the result is the expensive half of the mistake.
    """
    if not pending:
        return pending
    done = {
        int(i)
        for (i,) in session.execute(
            select(SequenceEmbedding.sequence_id).where(
                SequenceEmbedding.embedding_config_id == target_id,
                SequenceEmbedding.sequence_id.in_([i for i, _s in pending]),
            )
        )
    }
    if not done:
        return pending
    emit(
        "encode.resuming",
        f"{len(done)} of {len(pending)} sequences already have a code, resuming after them",
        {"already_encoded": len(done), "batch": len(pending)},
        "info",
    )
    return [(i, s) for i, s in pending if i not in done]


class EncodeResidueSparseBatchOperation(Operation):
    """One batch of sequences, encoded where the card is.

    Loads the model on the device the payload names rather than on a hardcoded one,
    so a host without a card refuses at dispatch instead of raising inside a forward
    pass, and a host with two cards can be told which.
    """

    name = "encode_residue_sparse_batch"
    description = (
        "GPU child job: run the language model over one batch of sequences, select "
        "atoms per residue, reduce, and store one sparse code per sequence under the "
        "target embedding config the coordinator created."
    )

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = EncodeResidueSparseBatchPayload.model_validate(contract_payload(payload))
        source = session.get(EmbeddingConfig, uuid.UUID(p.source_embedding_config_id))
        if source is None:
            raise ValueError(f"EmbeddingConfig {p.source_embedding_config_id} not found")
        target_id = uuid.UUID(p.target_embedding_config_id)

        artifact = resolve_encoder_artifact(p.encoder_artifact_path, p.encoder_artifact_uri)
        weight, bias, meta = load_frozen_encoder(artifact)

        rows = session.execute(
            select(Sequence.id, Sequence.sequence).where(Sequence.id.in_(p.sequence_ids))
        ).all()
        pending = [(int(i), s) for i, s in rows]
        if len(pending) != len(p.sequence_ids):
            missing = set(p.sequence_ids) - {i for i, _ in pending}
            raise ValueError(
                f"{len(missing)} of {len(p.sequence_ids)} sequences in this batch no longer "
                f"exist, for example {sorted(missing)[:3]}. The batch is not encoded rather "
                "than silently short, because a short batch and a finished one look alike"
            )

        from protea.core.operations.compute_embeddings import (
            _get_or_load_model,
            _resolve_backend,
        )

        backend = _resolve_backend(source.model_backend)
        refuse_backend_without_residues(backend, source.model_backend)
        pending = _drop_already_encoded(session, target_id, pending, emit)
        if not pending:
            return OperationResult(
                result={"encoded": 0, "embedding_config_id": str(target_id),
                        "note": "every sequence in this batch already had a code"}
            )
        model, tokenizer = _get_or_load_model(source, p.device, emit)
        run = _Run(
            backend=backend,
            model=model,
            tokenizer=tokenizer,
            weight=weight,
            bias=bias,
            meta=meta,
            target_id=target_id,
            scale=fetch_embedding_scale(session, target_id),
            batch_size=p.batch_size,
            residue_budget=p.residue_budget,
        )
        return self._encode(session, run, pending, emit)

    def _encode(
        self,
        session: Session,
        run: _Run,
        pending: list[tuple[int, str]],
        emit: EmitFn,
    ) -> OperationResult:
        """Run the forward in batches and store one code per sequence.

        Residues are consumed as they come off the card and never accumulated: the
        code is a fixed-width vector per protein, so peak memory is one batch of
        residues rather than the corpus, and a long protein costs time and not
        headroom.
        """
        encoded, clipped, residues_seen, densities, oversized = encode_until_done(
            session, run, pending, emit
        )
        mean_density = float(np.mean(densities)) if densities else 0.0
        emit(
            "encode.done",
            f"{encoded} codes, {mean_density:.4f} mean density over {residues_seen} residues",
            {
                "encoded": encoded,
                "mean_density": mean_density,
                "residues": residues_seen,
                "clipped": clipped,
            },
            "warning" if clipped else "info",
        )
        return OperationResult(
            result={
                "encoded": encoded,
                "embedding_config_id": str(run.target_id),
                "residues": residues_seen,
                "mean_density": mean_density,
                "clipped_components": clipped,
                "too_large_to_encode": oversized,
            }
        )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        pl = payload or {}
        n = len(pl.get("sequence_ids") or [])
        bits = [f"n={n}"] if n else []
        if pl.get("device"):
            bits.append(str(pl["device"]))
        return " · ".join(bits)
