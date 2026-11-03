"""One guard for the GPU node's card, shared by every operation that sends work to it.

``compute_embeddings``, ``encode_residue_vocabulary`` and ``encode_residue_sparse`` all
dispatch batches to the same queue, ``protea.embeddings.batch``, which the same card
consumes. Before this module only ``compute_embeddings`` checked whether the card was
busy, and only against other ``compute_embeddings`` jobs in RUNNING: a residue pass was
invisible to it, and a second residue pass could be queued behind a first with nothing
in the way, interleaving two passes on one card for hours.

Busy means either of two things, because each one misses a case the other sees:

* **A coordinator of a card operation is RUNNING.** A coordinator that waits for its
  batches (``deferred``) stays RUNNING until the last one is stored.
* **The batch queue holds messages.** A coordinator that closed the moment it dispatched
  (the code before #1091 did, and the corpus pass of 2026-11-03 was such a one) is
  SUCCEEDED while its batches still wait, and only the queue knows.

A busy card is not a failure: the coordinator raises :class:`RetryLaterError`, the worker
waits and publishes it again, and the pass starts when the card is free. When the broker
cannot be asked, the queue is not counted as busy: not knowing is not evidence of work,
and the coordinator's own dispatch would fail on the same broker anyway.
"""

from __future__ import annotations

from pathlib import Path
from uuid import UUID

from sqlalchemy.orm import Session

from protea.config.tuning import get_tuning
from protea.core.contracts.operation import RetryLaterError
from protea.infrastructure.orm.models.job import Job, JobStatus

#: The coordinators whose batches run on the card.
CARD_OPERATIONS: tuple[str, ...] = (
    "compute_embeddings",
    "encode_residue_vocabulary",
    "encode_residue_sparse",
)

#: The queue the card consumes.
CARD_BATCH_QUEUE = "protea.embeddings.batch"


def _amqp_url() -> str | None:
    try:
        from protea.infrastructure.settings import load_settings

        return load_settings(Path(__file__).resolve().parents[3]).amqp_url
    except Exception:
        return None


def _pending_batches(amqp_url: str | None) -> int | None:
    if not amqp_url:
        return None
    from protea.infrastructure.queue.publisher import pending_message_count

    return pending_message_count(amqp_url, CARD_BATCH_QUEUE)


def card_busy(session: Session, own_job_id: UUID, amqp_url: str | None = None) -> str | None:
    """Why the card is busy for the coordinator ``own_job_id``, or None when it is free."""
    other = (
        session.query(Job)
        .filter(
            Job.operation.in_(CARD_OPERATIONS),
            Job.status == JobStatus.RUNNING,
            Job.id != own_job_id,
        )
        .first()
    )
    if other is not None:
        return f"{other.operation} job {other.id} is running on the card"
    pending = _pending_batches(amqp_url if amqp_url is not None else _amqp_url())
    if pending:
        return f"{pending} batches of another pass are waiting in {CARD_BATCH_QUEUE}"
    return None


def refuse_if_card_busy(session: Session, own_job_id: UUID, amqp_url: str | None = None) -> None:
    """Wait for the card: raise :class:`RetryLaterError` while another pass holds it."""
    reason = card_busy(session, own_job_id, amqp_url)
    if reason is None:
        return
    raise RetryLaterError(
        f"The card is busy: {reason}. This job waits and starts when it is free.",
        delay_seconds=get_tuning().operation.gpu_busy_retry_seconds,
    )
