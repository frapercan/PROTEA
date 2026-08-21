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


#: How many residues a batch may carry. The unit is residues rather than sequences
#: because nothing here truncates: a 35,991-residue protein enters the model whole, and
#: eight of them are four thousand times the work of eight short ones. A count of
#: sequences is therefore the wrong unit, and the card refuses batches for a reason that
#: has nothing to do with how many proteins are in them.
#:
#: Measured on this corpus: at one sequence per batch the remaining 477,407 proteins need
#: 477,407 round trips to the database; at a 4,000-residue budget they need 48,705.
_DEFAULT_RESIDUE_BUDGET = 4096


#: The largest residue budget this PROCESS has completed, or None before the first one.
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


def take_batch(
    pending: list[tuple[int, str]], start: int, budget: int, cap: int
) -> list[tuple[int, str]]:
    """Sequences from ``start`` that together fit the residue budget, at most ``cap``.

    A sequence longer than the whole budget goes alone rather than being skipped or split,
    because splitting it would change what is encoded and skipping it would silently drop
    a protein. The budget is a target for the common case, not a hard limit on one.
    """
    batch: list[tuple[int, str]] = []
    residues = 0
    for i in range(start, len(pending)):
        seq = pending[i][1]
        if batch and (residues + len(seq) > budget or len(batch) >= cap):
            break
        batch.append(pending[i])
        residues += len(seq)
    return batch


def _progress(emit: EmitFn, encoded: int, total: int, residues: int) -> None:
    """Report after every commit, so a stalled run is distinguishable from a slow one."""
    emit(
        "encode.progress",
        f"{encoded}/{total} sequences",
        {"encoded": encoded, "total": total, "residues": residues},
        "info",
    )


def _commit(session: Session, rows: list[dict]) -> None:
    """Write this batch and make it durable before the next one is attempted.

    Idempotent, so a redelivered message cannot duplicate what it already wrote, and
    committing per batch is what lets a failure resume rather than restart.
    """
    session.execute(pg_insert(SequenceEmbedding).on_conflict_do_nothing(), rows)
    session.commit()


def _skip_oversized(item: tuple[int, str], emit: EmitFn) -> dict:
    """Record and announce a sequence the card cannot hold at any budget.

    Skipping it costs one protein. Raising costs every protein behind it in the message,
    because the consumer retries the whole batch and eventually dead-letters it. That is how
    a 35,991-residue protein stopped a corpus run at 91 per cent with 44,904 sequences
    unencoded, almost all of them ordinary.

    It is skipped LOUDLY. The identifier and its length go on an event and into the operation
    result, so what did not fit is a recorded fact rather than a silent gap, which is the only
    thing that makes skipping defensible at all.
    """
    sid, seq = item
    emit(
        "encode.too_large",
        f"sequence {sid} of {len(seq)} residues does not fit this card, skipped",
        {"sequence_id": sid, "residues": len(seq)},
        "warning",
    )
    return {"sequence_id": sid, "residues": len(seq)}


def encode_until_done(
    session: Session,
    run: Any,
    pending: list[tuple[int, str]],
    emit: EmitFn,
) -> tuple[int, int, int, list[float], list[dict]]:
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
    oversized: list[dict] = []
    budget = _starting_size(getattr(run, "residue_budget", _DEFAULT_RESIDUE_BUDGET))
    cap = run.batch_size
    start = 0
    while start < len(pending):
        batch = take_batch(pending, start, budget, cap)
        try:
            rows, stats = _encode_batch(run, batch, emit)
        except Exception as exc:  # noqa: BLE001 - re-raised unless it is a memory fault
            carried = sum(len(s) for _i, s in batch)
            if not _is_out_of_memory(exc):
                raise
            _release_card()
            if len(batch) == 1:
                oversized.append(_skip_oversized(batch[0], emit))
                start += 1
                continue
            budget = max(1, carried // 2)
            emit(
                "encode.shrinking",
                f"out of memory on {len(batch)} sequences carrying {carried} residues, "
                f"retrying with a budget of {budget}",
                {"sequences": len(batch), "residues": carried, "budget": budget},
                "warning",
            )
            continue
        _commit(session, rows)
        _LAST_GOOD_SIZE = max(_LAST_GOOD_SIZE or 0, sum(len(s) for _i, s in batch))
        start += len(batch)
        encoded += len(rows)
        clipped += stats["clipped"]
        residues_seen += stats["residues"]
        densities.extend(stats["densities"])
        _progress(emit, encoded, len(pending), residues_seen)
    return encoded, clipped, residues_seen, densities, oversized
