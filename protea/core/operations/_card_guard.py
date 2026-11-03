"""One guard for the GPU node's card, shared by the coordinators that send it batches.

``compute_embeddings``, ``encode_residue_vocabulary`` and ``encode_residue_sparse`` all
dispatch batches to ``protea.embeddings.batch``, which one card consumes. Before this
module only ``compute_embeddings`` checked whether the card was busy, and only against
other ``compute_embeddings`` jobs in RUNNING: a residue pass was invisible to it, and a
second residue pass could be queued behind a first with nothing in the way,
interleaving two passes on one card for hours.

The card is busy for a coordinator when another coordinator of the three still has
batches in flight, which the job table says on its own:

* **RUNNING.** All three coordinators wait for their batches (``deferred``: #1091 for
  the residue vocabulary, #1098 for the sparse encoder), so a pass is RUNNING until its
  last batch is stored.
* **FAILED by the reaper while its batches still arrive.** A waiting coordinator that the
  reaper gave up on (``error_code`` ``lease_expired``) keeps receiving its batches, which
  advance it and close it (#1097). It holds the card while its count is short of its
  total AND it received an event within the reaper's liveness window; one with no event
  for that long is dead, as is one whose batches were discarded before #1095.

The guard reads only the database. It does not ask the broker how many batches are
queued: that made every test of these operations talk to whatever broker listens on
localhost, which on the host that runs PROTEA is the live one. A coordinator from
before #1091, closed SUCCEEDED as it dispatched, is the one case the job table cannot
see; the corpus pass of 2026-11-03 is one, and it ends before this is deployed.

A busy card is not a failure: the coordinator raises :class:`RetryLaterError`, the worker
waits and publishes it again, and the pass starts when the card is free.

``apply_learned_encoder`` also uses a card, but inline, wherever its queue is consumed,
and it sends nothing to ``protea.embeddings.batch``; it is not one of these coordinators.
"""

from __future__ import annotations

from uuid import UUID

from sqlalchemy.orm import Session

from protea.config.tuning import get_tuning
from protea.core.contracts.operation import RetryLaterError
from protea.core.contracts.parent_progress import in_flight
from protea.core.utils import utcnow
from protea.infrastructure.orm.models.job import Job

#: The coordinators whose batches go to ``protea.embeddings.batch``, the card's queue.
BATCH_QUEUE_COORDINATORS: tuple[str, ...] = (
    "compute_embeddings",
    "encode_residue_vocabulary",
    "encode_residue_sparse",
)


def card_busy(session: Session, own_job_id: UUID) -> str | None:
    """Why the card is busy for the coordinator ``own_job_id``, or None when it is free."""
    other = (
        session.query(Job)
        .filter(
            Job.operation.in_(BATCH_QUEUE_COORDINATORS),
            Job.id != own_job_id,
            in_flight(utcnow()),
        )
        .first()
    )
    if other is None:
        return None
    return f"{other.operation} job {other.id} still has batches on the card"


def refuse_if_card_busy(session: Session, own_job_id: UUID) -> None:
    """Wait for the card: raise :class:`RetryLaterError` while another pass holds it."""
    reason = card_busy(session, own_job_id)
    if reason is None:
        return
    raise RetryLaterError(
        f"The card is busy: {reason}. This job waits and starts when it is free.",
        delay_seconds=get_tuning().operation.gpu_busy_retry_seconds,
    )
