"""Which residue-level heads can share one forward pass, and which cannot.

THE COST IS IN THE PASS, NOT IN THE HEAD. A residue-level encoder is a matmul
over the residue tensor; producing that tensor is a full-length forward pass of
the PLM, which on this corpus is 299.373.587 residues with no truncation by
design. ``encode_residue_sparse`` accepted exactly one artifact, so five heads
over the same states were five passes of the model and one arm's worth of
arithmetic each.

``_embedding_pass_group`` already drew this line one level up, for pooled
embeddings, and this is the same split applied where it costs more: there the
pass is expensive and the reduction trivial, here the pass is far more
expensive because the whole protein enters the model.

TWO THINGS THIS REFUSES THAT NOTHING REFUSED BEFORE, and both were found by
reading rather than by being hit:

* THE LAYERS COME FROM THE ARTIFACT, not from the source config.
  ``_encode_batch`` passes ``layers=list(run.meta["layer_indices"])``, so two
  heads asking for different layers are two different selections of
  ``hidden_states`` and cannot share a pass. Nothing said so, and a group that
  shared one would have encoded every head against the first head's layers.
* ``in_dim`` IS CHECKED TOO LATE. ``encode_one`` does
  ``residues.astype(np.float32) @ weight`` with ``weight`` of shape
  ``(in_dim, dict_dim)``. A head whose ``in_dim`` does not match the residue
  width raises an unguarded numpy error on the card worker, AFTER the model has
  loaded, once per batch. Alone that wastes a load; in a group it takes the
  pass of every other head with it.

WHAT MAY DIFFER is everything read after the residues exist: the dictionary
size, how many atoms survive per residue and per sequence, how they are
aggregated, and the name the codes are stored under. Those are the axis the
group exists to fan out over.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import numpy as np
from sqlalchemy import select
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn
from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
from protea.infrastructure.orm.models.embedding.sequence_embedding import SequenceEmbedding

if TYPE_CHECKING:  # pragma: no cover - annotations only
    from protea.core.operations.encode_residue_sparse import (
        EncodeResidueSparseBatchPayload,
        EncodeResidueSparsePayload,
    )

#: Meta fields that decide what the model is fed and which states are selected.
#: Two heads differing in any of them are two passes, and no grouping makes
#: them one.
PASS_FIELDS: tuple[str, ...] = ("in_dim", "layer_indices")

#: Meta fields read strictly after the residue tensor exists, which may
#: therefore differ inside one group.
REDUCTION_FIELDS: tuple[str, ...] = (
    "activation",
    "aggregate",
    "dict_dim",
    "k_residue",
    "k_sequence",
    "order",
)


class SharedResiduePassError(ValueError):
    """A head group would not, in fact, share one forward pass.

    A ``ValueError`` so a worker treats it as a hard, non-retryable bad-payload
    refusal like every other on this path: the same group re-dispatched would
    disagree in the same way.
    """


def _as_key(value: Any) -> Any:
    """A comparable form of a meta value, with lists flattened to tuples."""
    return tuple(value) if isinstance(value, list) else value


def assert_shared_residue_pass(metas: list[dict[str, Any]], *, names: list[str]) -> None:
    """Refuse a head group whose members do not share one forward pass.

    Compares only :data:`PASS_FIELDS`, and names both values of the first field
    that disagrees along with the two heads that disagree in it, because a
    refusal that says "the group is inconsistent" sends a reader to read six
    artifacts.

    One head is trivially a group and passes.
    """
    if len(metas) < 2:
        return
    first, first_name = metas[0], names[0]
    for meta, name in zip(metas[1:], names[1:], strict=True):
        for field in PASS_FIELDS:
            a, b = _as_key(first.get(field)), _as_key(meta.get(field))
            if a == b:
                continue
            raise SharedResiduePassError(
                f"heads {first_name!r} and {name!r} differ in {field!r} "
                f"({a!r} against {b!r}), so they do not share a forward pass. "
                f"{_why(field)} Dispatch them as separate jobs; grouping them would "
                "encode one head against the other's states."
            )


def _why(field: str) -> str:
    """The one sentence a reader needs for the field that disagreed."""
    if field == "layer_indices":
        return (
            "The layers are read from the ARTIFACT and handed to the backend, so a "
            "different selection is a different set of hidden states."
        )
    return (
        "The residue width is the model's output, so two heads expecting different "
        "widths cannot both be right about the same pass."
    )


def assert_in_dim_matches_source(
    metas: list[dict[str, Any]], *, names: list[str], source_dim: int | None
) -> None:
    """Refuse at dispatch a head whose ``in_dim`` is not the residue width.

    The alternative is where it was found: an unguarded matmul inside
    ``encode_one``, raised on the card worker after the model has loaded, naming
    shapes rather than the head that is wrong.

    Silent when the source config records no dimension, which is a property of
    the config row and not of the head; refusing there would block a group the
    guard has nothing to say about.
    """
    if source_dim is None:
        return
    for meta, name in zip(metas, names, strict=True):
        got = meta.get("in_dim")
        if got is None or int(got) == int(source_dim):
            continue
        raise SharedResiduePassError(
            f"head {name!r} expects residues of width {int(got)} and "
            f"{source_dim} is what this source produces. The matmul inside encode_one "
            "would raise on the card, after the model had loaded, once per batch, and "
            "in a group it would take every other head's pass with it."
        )


# ---------------------------------------------------------------------------
# The heads themselves: resolving a group, and deciding what the pass owes it.
#
# They live beside the guard rather than in the operation because they are one
# subject -- what a group IS -- and because the operation is at its file size
# ceiling: inlining them took it to 971 lines against a budget of 800, which
# the smell gate refused. The gate was right; a group of heads is its own
# object.
#
# The imports from the operation are LOCAL, inside each function, because the
# operation imports this module. A module-level import either way would be a
# cycle, and the pattern is the one this codebase already uses for tuning and
# for the backend resolver.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ResidueHead:
    """One frozen encoder and the config its codes are written under.

    Its own object because the group's whole point is that these differ while
    the pass does not: the weights, the dictionary size, how many atoms survive
    per residue and per sequence, the aggregation, and the scale applied at
    write time all belong to one head. What they share -- the layers and the
    residue width -- is checked once in ``_residue_pass_group`` and then read
    through ``_Run.meta``.
    """

    weight: np.ndarray
    bias: np.ndarray
    meta: dict
    target_id: uuid.UUID
    scale: float


def load_batch_heads(
    session: Session, p: EncodeResidueSparseBatchPayload
) -> list[ResidueHead]:
    """Resolve and load every head this batch writes.

    The singular shape is folded in so the historical payload still runs: a
    message carrying ``target_embedding_config_id`` and one artifact address is
    a group of one.
    """
    from protea.core.operations._encoder_artifact import resolve_encoder_artifact
    from protea.core.operations.encode_residue_sparse import (
        fetch_embedding_scale,
        load_frozen_encoder,
    )

    entries = list(p.heads)
    if not entries:
        address = p.encoder_artifact_uri or p.encoder_artifact_path or ""
        entries = [
            {"address": address, "target_embedding_config_id": str(p.target_embedding_config_id)}
        ]
    heads: list[ResidueHead] = []
    for entry in entries:
        address = entry["address"]
        local = resolve_encoder_artifact(None, address) if "/" in address else address
        weight, bias, meta = load_frozen_encoder(local)
        target_id = uuid.UUID(entry["target_embedding_config_id"])
        heads.append(
            ResidueHead(
                weight=weight,
                bias=bias,
                meta=meta,
                target_id=target_id,
                scale=fetch_embedding_scale(session, target_id),
            )
        )
    return heads


def drop_encoded_by_every_head(
    session: Session,
    heads: list[ResidueHead],
    pending: list[tuple[int, str]],
    emit: EmitFn,
) -> list[tuple[int, str]]:
    """Keep a sequence while ANY head still lacks its code.

    The intersection and not the union, and the difference is the pass: a
    sequence dropped because one head has it would never be encoded for the
    others, and a sequence kept because one head lacks it costs one matmul for
    the heads that do not need it, which is what the write-time skip is for.
    """
    from protea.core.operations.encode_residue_sparse import _drop_already_encoded

    still_needed: set[int] = set()
    for head in heads:
        still_needed |= {i for i, _s in _drop_already_encoded(session, head.target_id, pending, emit)}
    return [(i, s) for i, s in pending if i in still_needed]


def source_width(session: Session, source: EmbeddingConfig) -> int | None:
    """The residue width this source produces, read off a stored row.

    From a row rather than from the config, because the config records the
    POOLED dimension and a ``mean_max`` source pools to twice the width its
    residues have. A corpus with no rows yet reads as unknown, and the guard
    stays silent rather than guessing.
    """
    row = session.execute(
        select(SequenceEmbedding.embedding_dim)
        .where(SequenceEmbedding.embedding_config_id == source.id)
        .limit(1)
    ).scalar_one_or_none()
    return None if row is None else int(row)


def pending_sequences_for_group(
    session: Session,
    source_id: uuid.UUID,
    target_ids: list[uuid.UUID],
    p: EncodeResidueSparsePayload,
) -> list[tuple[int, str]]:
    """Sequences missing a code for AT LEAST ONE head of the group.

    Not "missing for every head": a sequence one head still needs is a sequence
    the pass has to run for, and the heads that already hold it skip it at write
    time. That is the rule ``compute_embeddings`` states for its own group, and
    the reason is the same -- the pass is the cost, so it is run once for
    whoever needs it.
    """
    from protea.core.operations.encode_residue_sparse import _pending_sequences

    by_id: dict[int, str] = {}
    for target_id in target_ids:
        by_id.update(dict(_pending_sequences(session, source_id, target_id, p)))
    return sorted(by_id.items())


def resolve_head_group(
    session: Session, p: EncodeResidueSparsePayload, source: EmbeddingConfig
) -> list[tuple[str, dict, uuid.UUID]]:
    """Resolve every head, refuse a group that cannot share a pass, create its configs.

    Returns ``(address, meta, target_config_id)`` per head, in the order the
    payload named them, so the batch messages and the reported result agree with
    what the caller wrote.

    EVERY REFUSAL HAPPENS HERE, before a single message is published, because
    the alternative is where the failures used to land: on the card worker,
    after a model has loaded, once per batch. See ``_residue_pass_group``.
    """
    from protea.core.operations._encoder_artifact import resolve_encoder_artifact
    from protea.core.operations.encode_residue_sparse import (
        _ensure_target_config as ensure_target,
    )
    from protea.core.operations.encode_residue_sparse import (
        load_frozen_encoder,
    )

    addresses = list(p.encoder_artifact_addresses)
    resolved = [resolve_encoder_artifact(None, a) if "/" in a else a for a in addresses]
    metas = [load_frozen_encoder(local)[2] for local in resolved]
    assert_shared_residue_pass(metas, names=addresses)
    assert_in_dim_matches_source(metas, names=addresses, source_dim=source_width(session, source))
    return [
        (addr, meta, ensure_target(session, p, meta, source))
        for addr, meta in zip(addresses, metas, strict=True)
    ]
