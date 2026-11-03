"""Honour ``skip_existing`` BEFORE the forward pass, not only at write time.

``ComputeEmbeddingsBatchPayload`` declares ``skip_existing`` and the dispatch
message carries it, but the batch worker never read it: it loaded its 64
sequences and went straight into inference. The flag was honoured one stage too
late, inside ``store_group_rows``, which skips the INSERT for a
``(sequence, config)`` pair that already exists. So a batch whose work was
already done paid the whole pass and then threw the result away.

WHY THAT IS NOT A HYPOTHETICAL. The coordinator filters at DISPATCH time, and
between dispatch and consumption the queue can hold a message for hours. On
2026-10-28 the compute node went off the network at 18:40:51 UTC with 8.524 of
10.689 batches still queued. The reaper's rule for a lease-less RUNNING job
past its six-hour timeout is to RE-ENQUEUE it (three attempts before it fails),
so the coordinator re-runs and dispatches a second message for every sequence
still missing -- 8.524 of them, on top of the 8.524 already waiting. Every
duplicate would have inferred 64 sequences again and discarded the rows: at the
4,38 s per batch this run measured over its 2.165 completed batches, **10,5
hours of card time** to recompute what was already stored.

THE OUTPUT IS UNCHANGED, WHICH IS WHAT MAKES THIS SAFE. The write stage already
skips exactly these pairs, so filtering before the pass removes work and not
rows. The predicate is the coordinator's own ``missing_for_any_config``: one
pass serves the whole config group, so a sequence stays as long as a single
config lacks it. Using anything narrower here would drop a sequence the group
still needs.

A batch left with nothing to do skips the pass and says so, because a silent
zero-sequence batch is indistinguishable from a batch that inferred nothing by
mistake -- and that is the shape of the bug this module exists to close.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, OperationResult
from protea.core.contracts.parent_progress import update_parent_progress
from protea.core.operations._compute_embeddings_helpers import missing_for_any_config
from protea.infrastructure.orm.models.sequence.sequence import Sequence


def sequences_still_missing(
    session: Session, p: Any, configs: list[Any], emit: EmitFn
) -> list[Sequence]:
    """The batch's sequences, minus the ones every config already holds.

    With ``skip_existing`` false this is the whole batch, unchanged: the flag
    means what it says and turning it off still re-infers everything.

    The saving is reported rather than silent, because the only visible
    difference between this and the old behaviour is a smaller
    ``sequences_inferred``, which on its own reads like a batch that lost rows.
    """
    sequence_ids = list(p.sequence_ids)
    q = session.query(Sequence).filter(Sequence.id.in_(sequence_ids))
    if not p.skip_existing:
        return q.all()
    kept = q.filter(missing_for_any_config([c.id for c in configs])).all()
    saved = len(sequence_ids) - len(kept)
    if saved:
        emit(
            "compute_embeddings_batch.already_stored",
            None,
            {
                "requested": len(sequence_ids),
                "still_missing": len(kept),
                "already_stored": saved,
                "whole_batch": not kept,
                "reason": "every config in the group already holds these, so the "
                          "forward pass would be recomputed and the rows discarded "
                          "at write time; skip_existing is honoured here instead",
            },
            "info",
        )
    return kept


def close_fully_stored_batch(
    session: Session, parent_job_id: UUID, requested: int, emit: EmitFn
) -> OperationResult:
    """Finish a batch whose every sequence is already stored, counter included.

    THE COUNTER IS THE POINT, and it is why this is a function and not two
    lines inline. The WRITE stage is what normally advances the parent, and a
    batch that skips the pass publishes no write message. Left out, the
    counter would stop one short for every skipped batch and the job would sit
    at ``progress_current < progress_total`` for ever: a change that saved ten
    hours of card time and hung the run would be worse than the waste it
    removed.

    Emitted at info and not warning: nothing is wrong. A duplicate message is
    how a re-enqueued coordinator works, and the correct response to one is to
    cost nothing.
    """
    emit(
        "compute_embeddings_batch.skipped",
        None,
        {
            "reason": "already_stored",
            "requested": requested,
            "parent_job_id": str(parent_job_id),
        },
        "info",
    )
    update_parent_progress(
        session, parent_job_id, emit,
        event_name="compute_embeddings_batch.parent_succeeded",
    )
    return OperationResult(result={"skipped": True, "already_stored": True})


__all__ = ["close_fully_stored_batch", "sequences_still_missing"]
