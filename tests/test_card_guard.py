"""One guard for the card, shared by every operation that sends it work.

WHAT WENT WRONG WITHOUT IT. Only ``compute_embeddings`` checked whether the card was busy,
and only against other ``compute_embeddings`` jobs in RUNNING. On 2026-11-03 a corpus pass of
``encode_residue_vocabulary`` held the card with about 3,900 batches queued while its
coordinator, which ran the code from before #1091, sat SUCCEEDED. A second pass, one click
away on the model weights page, would have been queued behind it with nothing in the way.

So busy means a coordinator of ANY card operation in RUNNING, or batches waiting in the
card's queue; a busy card makes the coordinator wait (``RetryLaterError``), never fail.
"""

from __future__ import annotations

import uuid
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from protea.core.contracts.operation import RetryLaterError
from protea.core.operations import _card_guard as guard
from protea.infrastructure.orm.models.job import Job, JobStatus


def _session(running: Any = None) -> MagicMock:
    session = MagicMock()
    session.query.return_value.filter.return_value.first.return_value = running
    return session


def test_another_card_operation_running_makes_the_coordinator_wait() -> None:
    other = MagicMock(operation="encode_residue_vocabulary", id=uuid.uuid4())
    with pytest.raises(RetryLaterError, match="encode_residue_vocabulary job"):
        guard.refuse_if_card_busy(_session(other), uuid.uuid4(), amqp_url="amqp://x")


def test_batches_waiting_on_the_card_make_it_wait_even_with_no_coordinator_running() -> None:
    """The case of 2026-11-03: the coordinator closed when it dispatched; only the queue knew."""
    with (
        patch.object(guard, "_pending_batches", return_value=3900),
        pytest.raises(RetryLaterError, match="3900 batches"),
    ):
        guard.refuse_if_card_busy(_session(None), uuid.uuid4(), amqp_url="amqp://x")


@pytest.mark.parametrize("pending", [0, None])
def test_a_free_card_or_an_unreachable_broker_lets_it_start(pending: int | None) -> None:
    """Not knowing is not evidence of work: an unreachable broker does not hold a pass back."""
    with patch.object(guard, "_pending_batches", return_value=pending):
        guard.refuse_if_card_busy(_session(None), uuid.uuid4(), amqp_url="amqp://x")


def test_the_wait_uses_the_configured_delay() -> None:
    other = MagicMock(operation="compute_embeddings", id=uuid.uuid4())
    with pytest.raises(RetryLaterError) as err:
        guard.refuse_if_card_busy(_session(other), uuid.uuid4(), amqp_url="amqp://x")
    from protea.config.tuning import get_tuning

    assert err.value.delay_seconds == get_tuning().operation.gpu_busy_retry_seconds


def test_every_coordinator_that_feeds_the_card_asks_the_guard() -> None:
    """compute_embeddings and both residue coordinators wait on a busy card, before any work."""
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
        from protea.core.operations.compute_embeddings import ComputeEmbeddingsOperation

        with pytest.raises(RetryLaterError):
            ComputeEmbeddingsOperation()._refuse_if_gpu_busy(MagicMock(), uuid.UUID(job_id))


def _fresh(postgres_url: str) -> Any:
    from protea.infrastructure.orm.base import Base

    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    return engine


def _job(session: Session, operation: str, status: JobStatus) -> uuid.UUID:
    job = Job(
        id=uuid.uuid4(),
        operation=operation,
        queue_name="protea.embeddings",
        status=status,
        payload={},
        meta={},
    )
    session.add(job)
    session.flush()
    return job.id


def test_which_rows_hold_the_card(postgres_url: str) -> None:
    engine = _fresh(postgres_url)
    with Session(engine) as session, patch.object(guard, "_pending_batches", return_value=0):
        me = _job(session, "encode_residue_vocabulary", JobStatus.RUNNING)
        _job(session, "encode_residue_vocabulary", JobStatus.SUCCEEDED)
        _job(session, "predict_go_terms", JobStatus.RUNNING)
        assert guard.card_busy(session, me, amqp_url="amqp://x") is None, (
            "itself, a finished pass and an operation off the card do not hold it"
        )
        other = _job(session, "compute_embeddings", JobStatus.RUNNING)
        reason = guard.card_busy(session, me, amqp_url="amqp://x")
        assert reason is not None and str(other) in reason
