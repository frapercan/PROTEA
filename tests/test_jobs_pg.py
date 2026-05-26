import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

import protea.infrastructure.orm.models  # noqa: F401
from protea.infrastructure.orm.base import Base
from protea.infrastructure.orm.models.job import Job, JobEvent, JobStatus
from protea.services.jobs_service import compute_dedup_key


@pytest.mark.integration
def test_jobs_postgres_roundtrip(postgres_url: str):
    engine = create_engine(postgres_url, future=True)

    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    with Session(engine, future=True) as session:
        job = Job(operation="insert_proteins", queue_name="protea.insert_proteins")
        session.add(job)
        session.flush()

        job_id = job.id  # <- IMPORTANT: capture while still in-session

        ev = JobEvent(job_id=job_id, event="job.created", fields={"x": 1})
        session.add(ev)
        session.commit()

    with Session(engine, future=True) as session:
        j = session.get(Job, job_id)  # <- use the plain id
        assert j is not None
        assert len(j.events) == 1
        assert j.events[0].fields["x"] == 1


@pytest.mark.integration
def test_dedup_key_partial_unique_index(postgres_url: str):
    """F-OPS-JOBS.1a: two QUEUED jobs with the same dedup_key violate the partial index."""
    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    key = compute_dedup_key("export_research_dataset", {"k": 5})

    with Session(engine, future=True) as session:
        job1 = Job(
            operation="export_research_dataset",
            queue_name="protea.training",
            payload={"k": 5},
            dedup_key=key,
            status=JobStatus.QUEUED,
        )
        session.add(job1)
        session.commit()

    # Second job with same dedup_key + active status should violate the unique index.
    with Session(engine, future=True) as session:
        job2 = Job(
            operation="export_research_dataset",
            queue_name="protea.training",
            payload={"k": 5},
            dedup_key=key,
            status=JobStatus.QUEUED,
        )
        session.add(job2)
        with pytest.raises(IntegrityError):
            session.commit()


@pytest.mark.integration
def test_dedup_key_allows_same_key_after_terminal(postgres_url: str):
    """F-OPS-JOBS.1a: same dedup_key is allowed once the first job reaches a terminal state."""
    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    key = compute_dedup_key("export_research_dataset", {"k": 5})

    with Session(engine, future=True) as session:
        job1 = Job(
            operation="export_research_dataset",
            queue_name="protea.training",
            payload={"k": 5},
            dedup_key=key,
            status=JobStatus.SUCCEEDED,
        )
        session.add(job1)
        session.commit()

    # SUCCEEDED is not an active status, so the same dedup_key is allowed.
    with Session(engine, future=True) as session:
        job2 = Job(
            operation="export_research_dataset",
            queue_name="protea.training",
            payload={"k": 5},
            dedup_key=key,
            status=JobStatus.QUEUED,
        )
        session.add(job2)
        session.commit()  # must not raise

    with Session(engine, future=True) as session:
        j2 = session.get(Job, job2.id)
        assert j2 is not None
        assert j2.dedup_key == key


@pytest.mark.integration
def test_dedup_key_null_does_not_violate_index(postgres_url: str):
    """F-OPS-JOBS.1a: legacy rows with dedup_key=NULL never block each other."""
    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    with Session(engine, future=True) as session:
        for _ in range(3):
            job = Job(
                operation="insert_proteins",
                queue_name="protea.jobs",
                dedup_key=None,
                status=JobStatus.QUEUED,
            )
            session.add(job)
        session.commit()  # three NULLs must coexist without unique violation


@pytest.mark.integration
def test_leased_until_stored_and_retrieved(postgres_url: str):
    """F-OPS-JOBS.1a: leased_until column round-trips correctly through Postgres."""
    from datetime import UTC, datetime, timedelta

    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    now = datetime.now(UTC)
    lease = now + timedelta(seconds=120)

    with Session(engine, future=True) as session:
        job = Job(
            operation="ping",
            queue_name="protea.ping",
            status=JobStatus.RUNNING,
            leased_until=lease,
        )
        session.add(job)
        session.commit()
        job_id = job.id

    with Session(engine, future=True) as session:
        j = session.get(Job, job_id)
        assert j is not None
        assert j.leased_until is not None
        # Allow 1 second of rounding from DB storage
        diff = abs((j.leased_until - lease).total_seconds())
        assert diff < 1.0
