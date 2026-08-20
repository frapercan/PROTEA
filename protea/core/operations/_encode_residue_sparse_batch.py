"""The batch loop of ``encode_residue_sparse``, and what it does when the card runs out.

Kept beside the operation rather than inside it because the loop carries two concerns the
operation does not: how much work the card can hold at once, which is a property of the
machine and not of the recipe, and how to continue after a failure that left rows behind.

THE CARD IS NOT A BOOLEAN

Both machines in this topology have a card and only one of them can hold a batch of eight
sequences at five thousand residues. So "has a GPU" is not a useful predicate, a queue
named for the capability would route the work to a machine that then fails, and a unit
that checks for a card before starting cannot know whether its memory is enough for a
batch it has not seen.

Capacity is not a boolean and it changes whenever anybody upgrades, so nothing here
encodes a table of it. The batch simply halves on a memory fault and carries on. A
smaller card makes this operation slower rather than broken, and no coordination between
machines is needed to keep that true.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn
from protea.infrastructure.orm.models.embedding.sequence_embedding import SequenceEmbedding


def _is_out_of_memory(exc: BaseException) -> bool:
    """Whether a failure is the card running out of room, rather than anything else.

    Matched on the message rather than the type, because the same condition surfaces as
    ``torch.cuda.OutOfMemoryError``, as a plain ``RuntimeError`` from older allocators,
    and as an allocator message from the backend, and importing torch here to name the
    type would load it on a host that has no reason to.
    """
    text = f"{type(exc).__name__}: {exc}".lower()
    return "out of memory" in text or "outofmemory" in text or "cuda oom" in text


def _release_card() -> None:
    """Give the freed blocks back before retrying, or the smaller batch inherits the fault."""
    try:
        import torch

        if torch.cuda.is_available():
            torch.cuda.empty_cache()
    except Exception:  # noqa: BLE001 - a host without torch or without a card needs nothing
        pass


#: The largest batch this PROCESS has completed, or None before the first one.
#:
#: A worker consumes many messages and the card does not change between them, so
#: rediscovering the size costs an out-of-memory fault per step of the descent, per
#: message, forever. Measured on a card that settles at one sequence: three faults per
#: message and 1,224 of 1,304 batches ultimately run at one.
#:
#: This still encodes nobody's memory. The number is learned from what this machine
#: actually did, it lives only in the process, and a machine that never faults never
#: descends and so never sets it.
_LAST_GOOD_SIZE: int | None = None


def _starting_size(requested: int) -> int:
    """Begin where this process left off, never above what the payload asked for."""
    if _LAST_GOOD_SIZE is None:
        return requested
    return max(1, min(requested, _LAST_GOOD_SIZE))


def encode_until_done(
    session: Session,
    run: Any,
    pending: list[tuple[int, str]],
    emit: EmitFn,
) -> tuple[int, int, int, list[float]]:
    """Encode every sequence, shrinking the batch whenever the card refuses one.

    Residues are consumed as they come off the card and never accumulated: the code is a
    fixed-width vector per protein, so peak memory is one batch of residues rather than
    the corpus, and a long protein costs time and not headroom.

    Committing every batch is what makes the shrink safe. Work already written stays
    written, the retry resumes at the sequence that failed rather than at the first, and
    the size that failed is not tried again for the rest of the batch.
    """
    from protea.core.operations.encode_residue_sparse import _encode_batch

    global _LAST_GOOD_SIZE
    encoded = clipped = residues_seen = 0
    densities: list[float] = []
    size = _starting_size(run.batch_size)
    start = 0
    while start < len(pending):
        batch = pending[start : start + size]
        try:
            rows, stats = _encode_batch(run, batch, emit)
        except Exception as exc:  # noqa: BLE001 - re-raised unless it is a memory fault
            if not _is_out_of_memory(exc) or len(batch) == 1:
                raise
            size = max(1, len(batch) // 2)
            _release_card()
            emit(
                "encode.shrinking",
                f"out of memory on {len(batch)} sequences, retrying {size} at a time",
                {"was": len(batch), "now": size,
                 "residues": sum(len(s) for _i, s in batch)},
                "warning",
            )
            continue
        session.execute(pg_insert(SequenceEmbedding).on_conflict_do_nothing(), rows)
        session.commit()
        _LAST_GOOD_SIZE = len(batch)
        start += len(batch)
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
    return encoded, clipped, residues_seen, densities
