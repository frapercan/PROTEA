"""One guard for the card, shared by the coordinators that send it batches.

WHAT WENT WRONG WITHOUT IT. Only ``compute_embeddings`` checked whether the card was busy,
and only against other ``compute_embeddings`` jobs in RUNNING. A residue pass was
invisible to it, and a second ``encode_residue_vocabulary`` pass, one click away on the
model weights page, would have been queued behind a running one with nothing in the way.

So the card is busy when another coordinator of the three still has batches in flight:
RUNNING, or FAILED by the reaper with its count still short of its total. A busy card makes
the coordinator wait (``RetryLaterError``), never fail.

WHAT AN EARLIER VERSION GOT WRONG. It also asked the broker how many batches were queued,
discovering the broker from the project's settings. In a fresh checkout those fall back to
localhost, and on the host that runs PROTEA localhost is the live broker: a unit test read
the queue of the pass that was running. The guard now reads only the database, and
``test_the_guard_never_reaches_a_broker`` keeps it that way.
"""

from __future__ import annotations

import uuid
from datetime import timedelta
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from protea.core.contracts.operation import RetryLaterError
from protea.core.contracts.parent_progress import REAPED
from protea.core.operations import _card_guard as guard
from protea.core.utils import utcnow
from protea.infrastructure.orm.models.job import Job, JobEvent, JobStatus


def _session(holder: Any = None) -> MagicMock:
    session = MagicMock()
    session.query.return_value.filter.return_value.first.return_value = holder
    return session


def test_another_coordinator_in_flight_makes_this_one_wait() -> None:
    other = MagicMock(operation="encode_residue_vocabulary", id=uuid.uuid4())
    with pytest.raises(RetryLaterError, match="encode_residue_vocabulary job"):
        guard.refuse_if_card_busy(_session(other), uuid.uuid4())


def test_a_free_card_lets_it_start() -> None:
    guard.refuse_if_card_busy(_session(None), uuid.uuid4())


def test_the_wait_uses_the_configured_delay() -> None:
    other = MagicMock(operation="compute_embeddings", id=uuid.uuid4())
    with pytest.raises(RetryLaterError) as err:
        guard.refuse_if_card_busy(_session(other), uuid.uuid4())
    from protea.config.tuning import get_tuning

    assert err.value.delay_seconds == get_tuning().operation.gpu_busy_retry_seconds


def test_the_guard_never_reaches_a_broker() -> None:
    """A test of these operations must not be able to talk to whatever broker is on localhost."""
    with patch("pika.BlockingConnection", side_effect=AssertionError("the guard opened a broker")):
        assert guard.card_busy(_session(None), uuid.uuid4()) is None


def test_every_coordinator_that_feeds_the_card_asks_the_guard() -> None:
    """compute_embeddings and both residue coordinators wait on a busy card, before any work."""
    from protea.core.operations.compute_embeddings import ComputeEmbeddingsOperation
    from protea.core.operations.encode_residue_sparse import EncodeResidueSparseOperation
    from protea.core.operations.encode_residue_vocabulary import EncodeResidueVocabularyOperation

    job_id = str(uuid.uuid4())
    with patch.object(guard, "card_busy", return_value="a pass is running"):
        with pytest.raises(RetryLaterError):
            EncodeResidueVocabularyOperation().execute(
                MagicMock(),
                {
                    "source_embedding_config_id": str(uuid.uuid4()),
                    "vocabulary_artifact_uri": "residue-vocabulary/v-0123456789ab.npz",
                    "_job_id": job_id,
                },
                emit=lambda *a, **k: None,
            )
        with pytest.raises(RetryLaterError):
            EncodeResidueSparseOperation().execute(
                MagicMock(),
                {
                    "source_embedding_config_id": str(uuid.uuid4()),
                    "encoder_artifact_uri": "encoders/e-0123456789ab.npz",
                    "_job_id": job_id,
                },
                emit=lambda *a, **k: None,
            )
        with pytest.raises(RetryLaterError):
            ComputeEmbeddingsOperation()._refuse_if_gpu_busy(MagicMock(), uuid.UUID(job_id))


def _fresh(postgres_url: str) -> Any:
    from protea.infrastructure.orm.base import Base

    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    return engine


def _job(
    session: Session,
    operation: str,
    status: JobStatus,
    *,
    event_age: timedelta | None = None,
    **fields: Any,
) -> uuid.UUID:
    job = Job(
        id=uuid.uuid4(),
        operation=operation,
        queue_name="protea.embeddings",
        status=status,
        payload={},
        meta={},
        **fields,
    )
    session.add(job)
    session.flush()
    if event_age is not None:
        session.add(
            JobEvent(
                job_id=job.id,
                event="child.store_embeddings.done",
                fields={},
                ts=utcnow() - event_age,
            )
        )
        session.flush()
    return job.id


def test_which_rows_hold_the_card(postgres_url: str) -> None:
    engine = _fresh(postgres_url)
    with Session(engine) as session:
        me = _job(session, "encode_residue_vocabulary", JobStatus.RUNNING)
        _job(session, "encode_residue_vocabulary", JobStatus.SUCCEEDED)
        _job(session, "predict_go_terms", JobStatus.RUNNING)
        _job(session, "compute_embeddings", JobStatus.FAILED, error_code="HoldoutTouchedError")
        _job(
            session,
            "compute_embeddings",
            JobStatus.FAILED,
            error_code=REAPED,
            progress_current=7,
            progress_total=7,
        )
        assert guard.card_busy(session, me) is None, (
            "itself, a finished pass, an operation off the card, a real failure and a reaped "
            "pass whose batches all arrived do not hold it"
        )
        # b185a6d0, measured on the live base: reaped on 2026-10-30 at 0 of 7, its batches
        # skipped the next day, nothing since. Incomplete forever; it must not hold the card.
        _job(
            session,
            "compute_embeddings",
            JobStatus.FAILED,
            error_code=REAPED,
            progress_current=0,
            progress_total=7,
            event_age=timedelta(days=3),
        )
        assert guard.card_busy(session, me) is None, "a reaped pass silent for days is dead"
        reaped = _job(
            session,
            "encode_residue_sparse",
            JobStatus.FAILED,
            error_code=REAPED,
            progress_current=2,
            progress_total=7,
            event_age=timedelta(minutes=5),
        )
        reason = guard.card_busy(session, me)
        assert reason is not None and str(reaped) in reason, (
            "a reaped pass still receiving batches does"
        )


def test_a_running_pass_of_another_operation_holds_it(postgres_url: str) -> None:
    engine = _fresh(postgres_url)
    with Session(engine) as session:
        me = _job(session, "encode_residue_vocabulary", JobStatus.RUNNING)
        other = _job(session, "compute_embeddings", JobStatus.RUNNING)
        reason = guard.card_busy(session, me)
        assert reason is not None and str(other) in reason
