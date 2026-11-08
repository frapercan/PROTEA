"""One guard for the GPU node's card, shared by the operations that hold it for long.

``compute_embeddings``, ``encode_residue_vocabulary`` and ``encode_residue_sparse`` dispatch
batches to ``protea.embeddings.batch``, which one card consumes, and ``run_lab_method`` runs a
method's container on the same card for hours (ADR-D53). Before this module only
``compute_embeddings`` checked whether the card was busy, and only against other
``compute_embeddings`` jobs in RUNNING: a residue pass was invisible to it, and a second
residue pass could be queued behind a first with nothing in the way, interleaving two passes
on one card for hours. ``run_lab_method`` was invisible to all of them, and it read only the
card's free memory, a measure of one instant that says nothing about the batches a pass has
yet to send.

The card is busy for one of these operations when another of the four still holds it, which
the job table says on its own:

* **RUNNING.** The three coordinators wait for their batches (``deferred``: #1091 for
  the residue vocabulary, #1098 for the sparse encoder), so a pass is RUNNING until its
  last batch is stored, and a method run is RUNNING while its container runs.
* **FAILED by the reaper while its batches still arrive.** A waiting coordinator that the
  reaper gave up on (``error_code`` ``lease_expired``) keeps receiving its batches, which
  advance it and close it (#1097). It holds the card while its count is short of its
  total AND it received an event within the reaper's liveness window; one with no event
  for that long is dead, as is one whose batches were discarded before #1095. A method run
  has no total, so a reaped one holds nothing here; a container its dead worker left
  running shows only in the card's free memory, which ``run_lab_method`` still reads, and
  the job's retry removes it by name before it starts again.

The guard reads only the database. It does not ask the broker how many batches are
queued: that made every test of these operations talk to whatever broker listens on
localhost, which on the host that runs PROTEA is the live one. A coordinator from
before #1091, closed SUCCEEDED as it dispatched, is the one case the job table cannot
see; the corpus pass of 2026-11-03 is one, and it ends before this is deployed.

A busy card is not a failure: the operation raises :class:`RetryLaterError`, the worker
waits and publishes it again, and the work starts when the card is free.

``apply_learned_encoder`` also uses a card, but inline, wherever its queue is consumed,
and it sends nothing to ``protea.embeddings.batch``; it is not one of these operations.
"""

from __future__ import annotations

from uuid import UUID

from sqlalchemy.orm import Session

from protea.config.tuning import get_tuning
from protea.core.contracts.operation import RetryLaterError
from protea.core.contracts.parent_progress import in_flight
from protea.core.utils import utcnow
from protea.infrastructure.orm.models.job import Job

#: The operations that hold the node's card while they are in flight: the three coordinators
#: whose batches go to ``protea.embeddings.batch``, and the method runs on ``protea.methods``.
CARD_HOLDING_OPERATIONS: tuple[str, ...] = (
    "compute_embeddings",
    "encode_residue_vocabulary",
    "encode_residue_sparse",
    "run_lab_method",
)


def card_busy(session: Session, own_job_id: UUID | None) -> str | None:
    """Why the card is busy for the job ``own_job_id``, or None when it is free.

    ``own_job_id`` is None only when an operation runs outside a job, and then every
    holder counts.
    """
    holds = [Job.operation.in_(CARD_HOLDING_OPERATIONS), in_flight(utcnow())]
    if own_job_id is not None:
        holds.append(Job.id != own_job_id)
    other = session.query(Job).filter(*holds).first()
    if other is None:
        return None
    return f"{other.operation} job {other.id} still holds the card"


def refuse_if_card_busy(session: Session, own_job_id: UUID) -> None:
    """Wait for the card: raise :class:`RetryLaterError` while another operation holds it."""
    reason = card_busy(session, own_job_id)
    if reason is None:
        return
    raise RetryLaterError(
        f"The card is busy: {reason}. This job waits and starts when it is free.",
        delay_seconds=get_tuning().operation.gpu_busy_retry_seconds,
    )
