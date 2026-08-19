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
from pydantic import Field, field_validator
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
)

#: How the per-residue codes become one code. ``mean`` is the first moment alone;
#: ``moments`` concatenates the mean with the per-atom dispersion and therefore emits
#: twice the dictionary width. Both are a fixed-width reduction over residues, which is
#: what keeps a corpus pass costing one protein of memory rather than the corpus.
AGGREGATES = ("mean", "moments")

#: Backend tag on the produced config, so a code produced here is never confused
#: with one produced from pooled vectors when both are in the same table.
TARGET_BACKEND = "residue-sparse"

PositiveInt = Annotated[int, Field(gt=0)]


class EncodeResidueSparsePayload(ProteaPayload, frozen=True):
    """Which corpus, which frozen encoder, and under what name."""

    source_embedding_config_id: str
    encoder_artifact_path: str
    target_model_name: str = "residue-sparse"
    batch_size: PositiveInt = 32
    sequence_id_limit: int | None = None
    skip_existing: bool = True

    @field_validator("source_embedding_config_id", "encoder_artifact_path", mode="before")
    @classmethod
    def _non_empty(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("must be a non-empty string")
        return v.strip()


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
    if int(meta["k_residue"]) >= int(meta["dict_dim"]):
        raise ValueError(
            f"k_residue {meta['k_residue']} selects the whole {meta['dict_dim']}-atom "
            "dictionary, which is not a sparse code"
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
    """
    query = (
        select(Sequence.id, Sequence.sequence)
        .join(SequenceEmbedding, SequenceEmbedding.sequence_id == Sequence.id)
        .where(SequenceEmbedding.embedding_config_id == source_id)
        .distinct()
        .order_by(Sequence.id)
    )
    if p.skip_existing:
        done = select(SequenceEmbedding.sequence_id).where(
            SequenceEmbedding.embedding_config_id == target_id
        )
        query = query.where(Sequence.id.not_in(done))
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
        p = EncodeResidueSparsePayload.model_validate(payload)
        source = session.get(EmbeddingConfig, uuid.UUID(p.source_embedding_config_id))
        if source is None:
            raise ValueError(f"EmbeddingConfig {p.source_embedding_config_id} not found")

        weight, bias, meta = load_frozen_encoder(p.encoder_artifact_path)
        target_id = _ensure_target_config(session, p, meta, source)
        pending = _pending_sequences(session, source.id, target_id, p)
        emit(
            "encode.scope",
            f"{len(pending)} sequences to encode through {source.model_name}",
            {
                "pending": len(pending),
                "source": source.model_name,
                "k_residue": meta["k_residue"],
                "k_sequence": meta["k_sequence"],
                "dictionary": meta["dict_dim"],
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
        from protea.core.operations.compute_embeddings import (
            _get_or_load_model,
            _resolve_backend,
        )

        model, tokenizer = _get_or_load_model(source, "cuda", emit)
        run = _Run(
            backend=_resolve_backend(source.model_backend),
            model=model,
            tokenizer=tokenizer,
            weight=weight,
            bias=bias,
            meta=meta,
            target_id=target_id,
            scale=fetch_embedding_scale(session, target_id),
            batch_size=p.batch_size,
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
        encoded = clipped = residues_seen = 0
        densities: list[float] = []

        for start in range(0, len(pending), run.batch_size):
            batch = pending[start : start + run.batch_size]
            rows, stats = _encode_batch(run, batch, emit)
            session.bulk_insert_mappings(SequenceEmbedding, rows)
            session.commit()
            encoded += len(rows)
            clipped += stats["clipped"]
            residues_seen += stats["residues"]
            densities.extend(stats["densities"])
            emit(
                "encode.progress",
                f"{encoded}/{len(pending)} sequences",
                {"encoded": encoded, "total": len(pending), "residues": residues_seen},
                "info",
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
                "recipe": {k: run.meta[k] for k in REQUIRED_META},
                "caveat": (
                    "point predict_go_terms at this embedding_config_id to retrieve on "
                    "these codes. Retrieval, the re-ranker and the evaluation are "
                    "unchanged; only what they retrieve on differs"
                ),
            }
        )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        return (
            f"encode the corpus of embedding config "
            f"{payload.get('source_embedding_config_id')} with a residue-level sparse encoder"
        )
