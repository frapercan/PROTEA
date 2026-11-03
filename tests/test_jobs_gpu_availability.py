"""Tests for ``GET /jobs/gpu-availability`` (FIX-ANNOTATE-BANNER-ACCURACY).

The annotation form's "GPU busy" banner must reflect GENUINELY active GPU
work only. A stale/zombie RUNNING job (dead worker, expired lease) must NOT
trip the busy signal. These tests pin that contract against a real Postgres
(the Job model uses PG-specific column types).
"""

from __future__ import annotations

from datetime import timedelta

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import protea.infrastructure.orm.models  # noqa: F401
from protea.api.routers.jobs_availability import router
from protea.core.utils import utcnow
from protea.infrastructure.orm.models.job import Job, JobEvent, JobStatus


def _client(factory: sessionmaker) -> TestClient:
    app = FastAPI()
    app.state.session_factory = factory
    app.include_router(router)
    return TestClient(app)


def _fresh_engine(postgres_url: str):
    """Create an engine with only the ``job`` table materialised.

    The endpoint only reads ``Job``; creating the full metadata would drag
    in the ``halfvec``-typed ``sequence_embedding`` table and couple this
    suite to the pgvector extension for no reason.
    """
    engine = create_engine(postgres_url, future=True)
    JobEvent.__table__.drop(engine, checkfirst=True)
    Job.__table__.drop(engine, checkfirst=True)
    Job.__table__.create(engine, checkfirst=True)
    JobEvent.__table__.create(engine, checkfirst=True)
    return engine


@pytest.mark.integration
def test_no_gpu_work_is_free(postgres_url: str):
    engine = _fresh_engine(postgres_url)
    factory = sessionmaker(engine, future=True)
    client = _client(factory)

    body = client.get("/jobs/gpu-availability").json()
    assert body["busy"] is False
    assert body["running_fresh"] == 0
    assert body["queued"] == 0
    assert body["running_stale"] == 0
    assert body["active_operation"] is None


@pytest.mark.integration
def test_stale_running_job_does_not_trip_busy(postgres_url: str):
    """A RUNNING job whose lease has expired is a zombie; never busy."""
    engine = _fresh_engine(postgres_url)
    factory = sessionmaker(engine, future=True)

    now = utcnow()
    with factory() as session:
        session.add(
            Job(
                operation="compute_embeddings",
                queue_name="protea.embeddings",
                status=JobStatus.RUNNING,
                started_at=now - timedelta(hours=2),
                leased_until=now - timedelta(hours=1),  # expired
            )
        )
        # A RUNNING job with no lease at all is also stale.
        session.add(
            Job(
                operation="predict_go_terms",
                queue_name="protea.predictions",
                status=JobStatus.RUNNING,
                started_at=now - timedelta(hours=3),
                leased_until=None,
            )
        )
        session.commit()

    body = _client(factory).get("/jobs/gpu-availability").json()
    assert body["busy"] is False
    assert body["running_fresh"] == 0
    assert body["running_stale"] == 2
    assert body["active_operation"] is None


@pytest.mark.integration
def test_fresh_running_job_is_busy(postgres_url: str):
    engine = _fresh_engine(postgres_url)
    factory = sessionmaker(engine, future=True)

    now = utcnow()
    with factory() as session:
        session.add(
            Job(
                operation="compute_embeddings",
                queue_name="protea.embeddings",
                status=JobStatus.RUNNING,
                started_at=now,
                leased_until=now + timedelta(minutes=2),  # live lease
                progress_current=3,
                progress_total=10,
            )
        )
        session.commit()

    body = _client(factory).get("/jobs/gpu-availability").json()
    assert body["busy"] is True
    assert body["running_fresh"] == 1
    assert body["active_operation"] == "compute_embeddings"
    assert body["progress_current"] == 3
    assert body["progress_total"] == 10


@pytest.mark.integration
def test_queued_gpu_job_is_busy(postgres_url: str):
    engine = _fresh_engine(postgres_url)
    factory = sessionmaker(engine, future=True)

    with factory() as session:
        session.add(
            Job(
                operation="predict_go_terms",
                queue_name="protea.predictions",
                status=JobStatus.QUEUED,
            )
        )
        session.commit()

    body = _client(factory).get("/jobs/gpu-availability").json()
    assert body["busy"] is True
    assert body["queued"] == 1
    assert body["active_operation"] == "predict_go_terms"


@pytest.mark.integration
def test_non_gpu_jobs_are_ignored(postgres_url: str):
    """A running non-GPU op (e.g. insert_proteins) must not trip the banner."""
    engine = _fresh_engine(postgres_url)
    factory = sessionmaker(engine, future=True)

    now = utcnow()
    with factory() as session:
        session.add(
            Job(
                operation="insert_proteins",
                queue_name="protea.jobs",
                status=JobStatus.RUNNING,
                started_at=now,
                leased_until=now + timedelta(minutes=2),
            )
        )
        session.commit()

    body = _client(factory).get("/jobs/gpu-availability").json()
    assert body["busy"] is False
    assert body["running_fresh"] == 0


@pytest.mark.integration
def test_fresh_running_preferred_over_queued_representative(postgres_url: str):
    engine = _fresh_engine(postgres_url)
    factory = sessionmaker(engine, future=True)

    now = utcnow()
    with factory() as session:
        session.add(
            Job(
                operation="predict_go_terms",
                queue_name="protea.predictions",
                status=JobStatus.QUEUED,
            )
        )
        session.add(
            Job(
                operation="compute_embeddings",
                queue_name="protea.embeddings",
                status=JobStatus.RUNNING,
                started_at=now,
                leased_until=now + timedelta(minutes=2),
            )
        )
        session.commit()

    body = _client(factory).get("/jobs/gpu-availability").json()
    assert body["busy"] is True
    assert body["running_fresh"] == 1
    assert body["queued"] == 1
    # The live running job is the more informative representative.
    assert body["active_operation"] == "compute_embeddings"


def _dispatched(session, job: Job) -> None:
    session.add(job)
    session.flush()
    session.add(
        JobEvent(job_id=job.id, event="job.dispatched", fields={"result": {}}, level="info")
    )


@pytest.mark.integration
def test_a_coordinator_waiting_for_its_batches_is_busy(postgres_url: str):
    """The corpus pass of 2026-11-03 held the card for a day and the banner said free.

    A coordinator that waits for its batches clears its lease on purpose and emits
    ``job.dispatched``; it holds the card until the last batch is stored. The residue
    passes use the same card and were not in the list at all.
    """
    engine = _fresh_engine(postgres_url)
    factory = sessionmaker(engine, future=True)
    now = utcnow()
    with factory() as session:
        _dispatched(
            session,
            Job(
                operation="encode_residue_vocabulary",
                queue_name="protea.embeddings",
                status=JobStatus.RUNNING,
                started_at=now - timedelta(hours=8),
                leased_until=None,
                progress_current=1500,
                progress_total=5329,
            ),
        )
        session.commit()

    body = _client(factory).get("/jobs/gpu-availability").json()
    assert body["busy"] is True
    assert body["running_dispatched"] == 1
    assert body["running_stale"] == 0
    assert body["active_operation"] == "encode_residue_vocabulary"
    assert (body["progress_current"], body["progress_total"]) == (1500, 5329)


@pytest.mark.integration
def test_a_reaped_coordinator_still_receiving_batches_is_busy(postgres_url: str):
    """Failed by the reaper, its batches still land and advance it (#1097): the card is held."""
    engine = _fresh_engine(postgres_url)
    factory = sessionmaker(engine, future=True)
    now = utcnow()
    with factory() as session:
        for current, last_event in (
            (2, timedelta(minutes=5)),
            (7, timedelta(minutes=5)),
            (0, timedelta(days=3)),
        ):
            job = Job(
                operation="compute_embeddings",
                queue_name="protea.embeddings",
                status=JobStatus.FAILED,
                error_code="lease_expired",
                started_at=now - timedelta(hours=9),
                progress_current=current,
                progress_total=7,
            )
            session.add(job)
            session.flush()
            session.add(
                JobEvent(
                    job_id=job.id,
                    event="child.store_embeddings.done",
                    fields={},
                    ts=now - last_event,
                )
            )
        session.commit()

    body = _client(factory).get("/jobs/gpu-availability").json()
    assert body["busy"] is True
    assert body["running_dispatched"] == 1, (
        "only the reaped pass whose batches still arrive: not the one whose batches all "
        "arrived, nor the one silent for three days (b185a6d0's shape on the live base)"
    )


@pytest.mark.integration
def test_a_leaseless_running_row_that_never_dispatched_is_still_a_zombie(postgres_url: str):
    """Without job.dispatched a RUNNING row with no lease is what it always was: stale."""
    engine = _fresh_engine(postgres_url)
    factory = sessionmaker(engine, future=True)
    now = utcnow()
    with factory() as session:
        session.add(
            Job(
                operation="encode_residue_sparse",
                queue_name="protea.embeddings",
                status=JobStatus.RUNNING,
                started_at=now - timedelta(hours=3),
                leased_until=None,
            )
        )
        session.commit()

    body = _client(factory).get("/jobs/gpu-availability").json()
    assert body["busy"] is False
    assert body["running_stale"] == 1
    assert body["running_dispatched"] == 0
