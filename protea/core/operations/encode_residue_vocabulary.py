"""Encode a corpus with a frozen residue vocabulary and keep the code of every residue.

ADR-D51. ``encode_residue_sparse`` selects atoms per residue and then pools, so nothing
per residue survives it. This operation keeps the residues: a GPU batch runs Ankh with
exact attention, one hidden state per selected layer (``embed_batch_per_layer``), passes
every residue through the vocabulary (:mod:`protea.core.residue_vocabulary`) and puts ONE
compressed shard with the codes of its sequences in the artifact store. The database is
written on the server: the batch publishes one message to ``protea.embeddings.write``,
where ``store_residue_codes`` checks the shard and inserts one ``residue_code`` row per
sequence, as ``store_embeddings`` does for ``compute_embeddings``.

WHERE THE SHARD GOES FIRST

A node that sets ``operation.residue_shard_spool_dir`` does not put the shard in the store
from the card's loop. The batch writes it to that folder on its own disk and publishes the
message to ``upload_residue_shard`` on ``protea.embeddings.upload``, a second worker of the
same node, which puts it in the store and only then publishes the message to
``store_residue_codes``. On 2026-11-03 the inline put took 35 % of every batch over the
node's link, with the card idle throughout; with the spool the card encodes the next batch
while the shard goes out. A node that sets no spool keeps the inline put.

WHY THE STATES ARE NOT POOLED OR AVERAGED ANYWHERE

The vocabulary standardises each layer separately and rounds afterwards, and its code is
only the lab's code when the states are the ones it was trained on: the layers apart, in
single precision, from exact attention. The backend's ``embed_batch_per_residue`` averages
the selected layers and casts to half precision, which is a different input.

THE POPULATION IS THE SOURCE CONFIG'S

As in ``encode_residue_sparse``, the sequences encoded are those that have an embedding
under ``source_embedding_config_id``, and the model that runs is that config's. A sequence
already holding a row under the target config is skipped, so a corpus run resumes.

It trains nothing. The vocabulary arrives frozen through the artifact store.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any

import numpy as np
from pydantic import Field, field_validator, model_validator
from sqlalchemy import select
from sqlalchemy.orm import Session

from protea.config.tuning import get_tuning
from protea.core.contracts.operation import (
    EmitFn,
    Operation,
    OperationResult,
    ProteaPayload,
)
from protea.core.contracts.parent_progress import update_parent_progress
from protea.core.operations._card_guard import refuse_if_card_busy
from protea.core.operations._encoder_artifact import (
    resolve_encoder_artifact,
    resolve_training_cut,
)
from protea.core.operations._parent_guard import parent_cancelled
from protea.core.operations._residue_shard_spool import (
    UPLOAD_QUEUE,
    refuse_without_room,
    spool_backlog,
    spool_root,
    spooled_path,
    write_spooled,
)
from protea.core.operations.store_residue_codes import WRITE_QUEUE
from protea.core.residue_code_shards import pack_shard
from protea.core.residue_vocabulary import (
    KIND,
    ResidueCode,
    ResidueVocabulary,
    load_residue_vocabulary,
)
from protea.core.utils import contract_payload
from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
from protea.infrastructure.orm.models.embedding.residue_code import ResidueCode as ResidueCodeRow
from protea.infrastructure.orm.models.embedding.sequence_embedding import SequenceEmbedding
from protea.infrastructure.orm.models.sequence.sequence import Sequence

#: Backend tag on the produced config: the producer, not an architecture.
TARGET_BACKEND = "residue-vocabulary"

#: The queue a worker with a card consumes, as for every other per-residue pass.
_BATCH_QUEUE = "protea.embeddings.batch"

PositiveInt = Annotated[int, Field(gt=0)]

#: One vocabulary per (artifact, device) per process: 260 MB of weights is not reloaded
#: for every message.
_VOCABULARIES: dict[tuple[str, str], ResidueVocabulary] = {}
_DIGESTS: dict[str, str] = {}


def _one_address(path: str | None, uri: str | None) -> None:
    given = [a for a in (path, uri) if (a or "").strip()]
    if len(given) != 1:
        raise ValueError("give exactly one of vocabulary_artifact_path and vocabulary_artifact_uri")


class EncodeResidueVocabularyPayload(ProteaPayload, frozen=True):
    """Coordinator: which corpus, which frozen vocabulary, in what batches."""

    source_embedding_config_id: str
    vocabulary_artifact_uri: str | None = None
    vocabulary_artifact_path: str | None = None
    device: str = "cuda"
    sequences_per_job: PositiveInt = 256
    residues_per_call: PositiveInt = 32768
    sequence_id_limit: int | None = None

    @field_validator("source_embedding_config_id", mode="before")
    @classmethod
    def _non_empty(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("must be a non-empty string")
        return v.strip()

    @model_validator(mode="after")
    def _exactly_one_address(self) -> EncodeResidueVocabularyPayload:
        _one_address(self.vocabulary_artifact_path, self.vocabulary_artifact_uri)
        return self


class EncodeResidueVocabularyBatchPayload(ProteaPayload, frozen=True):
    """One batch of sequences, addressed to a worker that has a card."""

    source_embedding_config_id: str
    target_embedding_config_id: str
    vocabulary_artifact_uri: str | None = None
    vocabulary_artifact_path: str | None = None
    sequence_ids: list[int] = Field(default_factory=list)
    parent_job_id: str = ""
    device: str = "cuda"
    residues_per_call: PositiveInt = 32768

    @model_validator(mode="after")
    def _exactly_one_address(self) -> EncodeResidueVocabularyBatchPayload:
        _one_address(self.vocabulary_artifact_path, self.vocabulary_artifact_uri)
        return self


def read_vocabulary_meta(path: str) -> dict[str, Any]:
    """The artifact's meta without loading its arrays, refusing any other kind."""
    with np.load(path, allow_pickle=False) as data:
        if "meta_json" not in data:
            raise ValueError(f"{path} carries no meta_json, so it does not declare a recipe")
        meta = json.loads(str(data["meta_json"]))
    if meta.get("kind") != KIND:
        raise ValueError(f"artifact kind {meta.get('kind')!r} is not {KIND!r}")
    return dict(meta)


def file_sha256(path: str) -> str:
    """sha256 of a local file, cached per process."""
    if path not in _DIGESTS:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for block in iter(lambda: f.read(1 << 22), b""):
                h.update(block)
        _DIGESTS[path] = h.hexdigest()
    return _DIGESTS[path]


def refuse_backend_without_layers(backend: Any, model_backend: str) -> None:
    """Refuse, before any work, a backend that cannot return the layers apart."""
    if not hasattr(backend, "embed_batch_per_layer"):
        raise ValueError(
            f"backend {model_backend!r} has no embed_batch_per_layer, so it cannot give the "
            "per-layer states a residue vocabulary reads. Update protea-backends or use a "
            "backend that implements it"
        )


def calls_by_residues(
    pending: list[tuple[int, str]], residues_per_call: int
) -> list[list[tuple[int, str]]]:
    """Split a batch into backend calls of at most ``residues_per_call`` residues.

    A sequence longer than the limit goes alone, since the backend's attention is linear
    in memory and the limit only bounds the host copy of the states.
    """
    calls: list[list[tuple[int, str]]] = []
    cur: list[tuple[int, str]] = []
    residues = 0
    for item in pending:
        n = len(item[1])
        if cur and residues + n > residues_per_call:
            calls.append(cur)
            cur, residues = [], 0
        cur.append(item)
        residues += n
    if cur:
        calls.append(cur)
    return calls


@dataclass(frozen=True)
class ModelPass:
    """What runs the language model: the backend plugin, the loaded model and its tokenizer."""

    backend: Any
    model: Any
    tokenizer: Any


def encode_sequences(
    model_pass: ModelPass,
    vocabulary: ResidueVocabulary,
    pending: list[tuple[int, str]],
    *,
    emit: EmitFn,
    residues_per_call: int,
) -> list[tuple[int, ResidueCode]]:
    """The code of every pending sequence, in the order given."""
    layers = [int(li) for li in vocabulary.meta["hidden_state_indices"]]
    codes: list[tuple[int, ResidueCode]] = []
    for call in calls_by_residues(pending, residues_per_call):
        states = model_pass.backend.embed_batch_per_layer(
            model_pass.model,
            model_pass.tokenizer,
            [s for _i, s in call],
            emit=emit,
            hidden_state_indices=layers,
        )
        for (sequence_id, seq), st in zip(call, states, strict=True):
            if st.shape[0] != len(seq):
                raise ValueError(
                    f"sequence {sequence_id} has {len(seq)} residues and the backend "
                    f"returned {st.shape[0]} rows"
                )
            code = vocabulary.encode_states([st[:, i] for i in range(st.shape[1])])
            codes.append((sequence_id, code))
    return codes


def shard_key(target_id: uuid.UUID | str, codes: list[tuple[int, ResidueCode]], sha: str) -> str:
    """Store key of a shard: the config, the id range it covers, and its own digest."""
    ids = [i for i, _c in codes]
    return f"residue-codes/{target_id}/{min(ids)}-{max(ids)}-{sha[:16]}.bin"


def build_messages(
    p: EncodeResidueVocabularyPayload,
    parent_job_id: uuid.UUID,
    source_id: uuid.UUID,
    target_id: uuid.UUID,
    sequence_ids: list[int],
) -> list[tuple[str, dict[str, Any]]]:
    """One message per batch, addressed to the card queue, carrying everything it needs."""
    parent = str(parent_job_id)
    return [
        (
            _BATCH_QUEUE,
            {
                "operation": "encode_residue_vocabulary_batch",
                "job_id": parent,
                "payload": {
                    "source_embedding_config_id": str(source_id),
                    "target_embedding_config_id": str(target_id),
                    "vocabulary_artifact_uri": p.vocabulary_artifact_uri,
                    "vocabulary_artifact_path": p.vocabulary_artifact_path,
                    "sequence_ids": sequence_ids[i : i + p.sequences_per_job],
                    "parent_job_id": parent,
                    "device": p.device,
                    "residues_per_call": p.residues_per_call,
                },
            },
        )
        for i in range(0, len(sequence_ids), p.sequences_per_job)
    ]


def dispatched_result(
    messages: list[tuple[str, dict[str, Any]]], sequences: int, target_id: uuid.UUID, digest: str
) -> OperationResult:
    """The coordinator's result: deferred, so the job stays RUNNING until the last write.

    ``store_residue_codes`` advances the parent once per batch and closes it on the last
    one, as ``store_embeddings`` does for ``compute_embeddings``. Without ``deferred`` the
    worker marked the job SUCCEEDED the moment the batches were published, so the job's
    state said nothing about whether any code had been written.
    """
    return OperationResult(
        result={
            "batches": len(messages),
            "sequences": sequences,
            "embedding_config_id": str(target_id),
            "vocabulary_sha256": digest,
        },
        progress_total=len(messages),
        publish_operations=messages,
        deferred=True,
    )


def _ensure_target_config(
    session: Session, meta: dict[str, Any], source: EmbeddingConfig, digest: str
) -> uuid.UUID:
    """Create, idempotently on name, the config the codes are indexed under.

    The name carries the artifact's digest and the source, so another vocabulary or
    another backbone pass is another config rather than a silent overwrite.
    """
    name = f"{TARGET_BACKEND}:{digest[:16]}:{str(source.id)[:8]}"
    existing = session.execute(
        select(EmbeddingConfig).where(EmbeddingConfig.model_name == name)
    ).scalar_one_or_none()
    if existing is not None:
        return existing.id
    config = EmbeddingConfig(
        model_name=name,
        model_backend=TARGET_BACKEND,
        layer_indices=list(meta["layer_indices"]),
        layer_agg="concat",
        pooling="per-residue",
        normalize_residues=False,
        normalize=False,
        use_chunking=False,
        description=(
            f"per-residue code of the residue vocabulary {meta.get('name', digest[:16])}: "
            f"{meta['dict_dim']} features over hidden states {meta['hidden_state_indices']}, "
            f"{meta['actives']} active per residue, {meta['levels']} levels. Stored as shards "
            f"indexed by residue_code (ADR-D51), from an exact-attention pass of "
            f"{source.model_name}"
        ),
        display_name=name,
        family=TARGET_BACKEND,
        kind="learned",
        derived_from_embedding_config_id=source.id,
        trained_on_annotation_set_id=resolve_training_cut(session, meta),
    )
    session.add(config)
    session.flush()
    return config.id


def _pending_sequence_ids(
    session: Session, source_id: uuid.UUID, target_id: uuid.UUID, limit: int | None
) -> list[int]:
    """Sequences of the source corpus without a code under the target config."""
    in_source = (
        select(1)
        .where(
            SequenceEmbedding.sequence_id == Sequence.id,
            SequenceEmbedding.embedding_config_id == source_id,
        )
        .exists()
    )
    coded = (
        select(1)
        .where(
            ResidueCodeRow.sequence_id == Sequence.id,
            ResidueCodeRow.embedding_config_id == target_id,
        )
        .exists()
    )
    query = select(Sequence.id).where(in_source).where(~coded).order_by(Sequence.id)
    if limit is not None:
        query = query.limit(limit)
    return [int(i) for (i,) in session.execute(query).all()]


def _vocabulary(path: str, device: str) -> ResidueVocabulary:
    key = (path, device)
    if key not in _VOCABULARIES:
        _VOCABULARIES[key] = load_residue_vocabulary(path, device=device)
    return _VOCABULARIES[key]


def _artifact_store() -> Any:
    from protea.infrastructure.settings import load_settings
    from protea.infrastructure.storage import get_artifact_store

    return get_artifact_store(load_settings(Path(__file__).resolve().parents[3]))


class EncodeResidueVocabularyOperation(Operation):
    name = "encode_residue_vocabulary"
    description = (
        "Encode a corpus with a frozen residue vocabulary and keep the code of every residue "
        "(ADR-D51): exact-attention Ankh per layer on a card, the vocabulary's code per "
        "residue, one compressed shard per batch in the artifact store and one residue_code "
        "row per sequence. Trains nothing."
    )

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        """Resolve, scope and hand the work to whoever has a card."""
        p = EncodeResidueVocabularyPayload.model_validate(contract_payload(payload))
        parent_job_id = uuid.UUID(payload["_job_id"])
        # A corpus pass holds the card for hours; a second one waits for it rather than
        # interleave with the first (the guard every card operation shares).
        refuse_if_card_busy(session, parent_job_id)
        source = session.get(EmbeddingConfig, uuid.UUID(p.source_embedding_config_id))
        if source is None:
            raise ValueError(f"EmbeddingConfig {p.source_embedding_config_id} not found")

        from protea.core.operations.compute_embeddings import _resolve_backend

        refuse_backend_without_layers(_resolve_backend(source.model_backend), source.model_backend)
        local = resolve_encoder_artifact(p.vocabulary_artifact_path, p.vocabulary_artifact_uri)
        meta = read_vocabulary_meta(local)
        digest = file_sha256(local)
        target_id = _ensure_target_config(session, meta, source, digest)
        pending = _pending_sequence_ids(session, source.id, target_id, p.sequence_id_limit)
        emit(
            "encode.scope",
            f"{len(pending)} sequences to encode through {source.model_name}",
            {
                "pending": len(pending),
                "vocabulary": meta.get("name"),
                "vocabulary_sha256": digest,
                "hidden_state_indices": meta["hidden_state_indices"],
                "target_embedding_config_id": str(target_id),
            },
            "info",
        )
        if not pending:
            return OperationResult(
                result={
                    "encoded": 0,
                    "embedding_config_id": str(target_id),
                    "note": "every sequence already had a code",
                }
            )
        messages = build_messages(p, parent_job_id, source.id, target_id, pending)
        return dispatched_result(messages, len(pending), target_id, digest)

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        return (
            f"keep the per-residue code of the corpus of embedding config "
            f"{payload.get('source_embedding_config_id')}"
        )


def _pending_in_batch(
    session: Session, p: EncodeResidueVocabularyBatchPayload, target_id: uuid.UUID
) -> list[tuple[int, str]]:
    """The batch's sequences that still have no code, refusing a batch that lost any."""
    rows = session.execute(
        select(Sequence.id, Sequence.sequence).where(Sequence.id.in_(p.sequence_ids))
    ).all()
    found = {int(i): s for i, s in rows}
    missing = [i for i in p.sequence_ids if i not in found]
    if missing:
        raise ValueError(
            f"{len(missing)} of {len(p.sequence_ids)} sequences in this batch no longer "
            f"exist, for example {missing[:3]}"
        )
    done = {
        int(i)
        for (i,) in session.execute(
            select(ResidueCodeRow.sequence_id).where(
                ResidueCodeRow.embedding_config_id == target_id,
                ResidueCodeRow.sequence_id.in_(p.sequence_ids),
            )
        )
    }
    return [(i, found[i]) for i in p.sequence_ids if i not in done]


#: Waits between attempts to put a shard, in seconds. The card and the store are different
#: machines here, joined by a link that drops now and then; a put of a content-addressed
#: key is idempotent, so trying again is safe.
PUT_RETRY_WAITS: tuple[float, ...] = (5.0, 30.0, 120.0)


def put_with_retries(
    store: Any, key: str, blob: bytes, waits: tuple[float, ...] = PUT_RETRY_WAITS
) -> None:
    """Put a shard, retrying after each wait; the last failure is raised."""
    import time

    for wait in (*waits, None):
        try:
            store.put(key, blob)
            return
        except Exception:  # noqa: BLE001 - any transport fault; the last one is re-raised
            if wait is None:
                raise
            time.sleep(wait)


def upload_shard(
    store: Any, target_id: uuid.UUID, codes: list[tuple[int, ResidueCode]], header: dict[str, Any]
) -> tuple[str, str, int]:
    """Put one shard in the store, retrying; the card writes nothing to the database."""
    blob, sha = pack_shard(codes, header)
    key = shard_key(target_id, codes, sha)
    put_with_retries(store, key, blob)
    return key, sha, len(blob)


def shard_header(
    vocabulary: ResidueVocabulary,
    local: str,
    source_id: str,
    target_id: uuid.UUID,
    parent_job_id: str,
) -> dict[str, Any]:
    """What a shard declares about its writer: the vocabulary, its hash, the configs and the job."""
    return {
        "vocabulary": vocabulary.meta.get("name"),
        "vocabulary_sha256": file_sha256(local),
        "hidden_state_indices": vocabulary.meta["hidden_state_indices"],
        "precision_policy": vocabulary.meta.get("precision_policy"),
        "source_embedding_config_id": source_id,
        "target_embedding_config_id": str(target_id),
        "parent_job_id": parent_job_id,
    }


def spool_shard(
    root: Path, target_id: uuid.UUID, codes: list[tuple[int, ResidueCode]], header: dict[str, Any]
) -> tuple[str, str, int]:
    """Write one shard to the node's spool, on the disk, for the upload worker to put in the store."""
    blob, sha = pack_shard(codes, header)
    key = shard_key(target_id, codes, sha)
    write_spooled(spooled_path(root, key), blob)
    return key, sha, len(blob)


def build_upload_message(
    parent_job_id: str, key: str, sha: str, store_message: tuple[str, dict[str, Any]]
) -> tuple[str, dict[str, Any]]:
    """The upload-queue message of a spooled shard: which shard, and what to publish after the put."""
    _queue, store = store_message
    return (
        UPLOAD_QUEUE,
        {
            "operation": "upload_residue_shard",
            "job_id": parent_job_id,
            "payload": {"shard_key": key, "shard_sha256": sha, "store": store["payload"]},
        },
    )


def hand_off(
    root: Path | None,
    target_id: uuid.UUID,
    codes: list[tuple[int, ResidueCode]],
    header: dict[str, Any],
    parent_job_id: str,
) -> tuple[str, str, int, tuple[str, dict[str, Any]]]:
    """Put the shard where the next stage reads it, and return that stage's message.

    With no spool the card puts the shard in the store and the message goes to
    ``store_residue_codes``. With a spool the shard goes to the node's disk and the message
    to ``upload_residue_shard``, which carries the same message for ``store_residue_codes``
    and publishes it once the shard is in the store.
    """
    if root is None:
        key, sha, nbytes = upload_shard(_artifact_store(), target_id, codes, header)
        return key, sha, nbytes, build_store_message(parent_job_id, target_id, key, sha, codes)
    key, sha, nbytes = spool_shard(root, target_id, codes, header)
    store_message = build_store_message(parent_job_id, target_id, key, sha, codes)
    return key, sha, nbytes, build_upload_message(parent_job_id, key, sha, store_message)


def build_store_message(
    parent_job_id: str,
    target_id: uuid.UUID,
    key: str,
    sha: str,
    codes: list[tuple[int, ResidueCode]],
) -> tuple[str, dict[str, Any]]:
    """The write-queue message of a shard: what the server checks and indexes."""
    return (
        WRITE_QUEUE,
        {
            "operation": "store_residue_codes",
            "job_id": parent_job_id,
            "payload": {
                "parent_job_id": parent_job_id,
                "target_embedding_config_id": str(target_id),
                "shard_key": key,
                "shard_sha256": sha,
                "sequences": [
                    {
                        "sequence_id": int(sequence_id),
                        "position": position,
                        "residue_count": code.residues,
                        "entry_count": code.entries,
                    }
                    for position, (sequence_id, code) in enumerate(codes)
                ],
            },
        },
    )


class EncodeResidueVocabularyBatchOperation(Operation):
    """One batch of sequences, encoded where the card is."""

    name = "encode_residue_vocabulary_batch"
    description = (
        "GPU child job: exact-attention Ankh per layer over one batch of sequences, the residue "
        "vocabulary's code per residue, one shard in the artifact store, and one message to "
        "store_residue_codes, which indexes it on the server."
    )

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = EncodeResidueVocabularyBatchPayload.model_validate(contract_payload(payload))
        source = session.get(EmbeddingConfig, uuid.UUID(p.source_embedding_config_id))
        if source is None:
            raise ValueError(f"EmbeddingConfig {p.source_embedding_config_id} not found")
        target_id = uuid.UUID(p.target_embedding_config_id)
        if parent_cancelled(session, p.parent_job_id, emit, "encode"):
            return OperationResult(result={"skipped": True})
        pending = _pending_in_batch(session, p, target_id)
        if not pending:
            # no write message follows, so the batch advances the parent itself, as a
            # compute_embeddings batch that was already stored does
            if p.parent_job_id:
                update_parent_progress(
                    session, uuid.UUID(p.parent_job_id), emit, event_name="encode.parent_succeeded"
                )
            return OperationResult(
                result={"encoded": 0, "note": "every sequence already had a code"}
            )

        root = spool_root()
        if root is not None:
            refuse_without_room(root, get_tuning().operation.residue_shard_spool_min_free_gib)

        from protea.core.operations.compute_embeddings import _get_or_load_model, _resolve_backend

        backend = _resolve_backend(source.model_backend)
        refuse_backend_without_layers(backend, source.model_backend)
        local = resolve_encoder_artifact(p.vocabulary_artifact_path, p.vocabulary_artifact_uri)
        vocabulary = _vocabulary(local, p.device)
        model, tokenizer = _get_or_load_model(source, p.device, emit)
        codes = encode_sequences(
            ModelPass(backend, model, tokenizer),
            vocabulary,
            pending,
            emit=emit,
            residues_per_call=p.residues_per_call,
        )
        header = shard_header(vocabulary, local, str(source.id), target_id, p.parent_job_id)
        key, sha, nbytes, message = hand_off(root, target_id, codes, header, p.parent_job_id)
        residues = sum(c.residues for _i, c in codes)
        entries = sum(c.entries for _i, c in codes)
        stats = {"encoded": len(codes), "residues": residues, "entries": entries, "shard_key": key}
        fields: dict[str, Any] = {**stats, "shard_to": "store" if root is None else "spool"}
        if root is not None:
            fields["spool_waiting"], fields["spool_waiting_bytes"] = spool_backlog(root)
        emit("encode.done", f"{len(codes)} sequences, shard of {nbytes} bytes", fields, "info")
        return OperationResult(
            result={**stats, "shard_sha256": sha, "embedding_config_id": str(target_id)},
            publish_operations=[message],
        )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        pl = payload or {}
        n = len(pl.get("sequence_ids") or [])
        return f"n={n}" if n else ""
