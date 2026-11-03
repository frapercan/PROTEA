"""``update_parent_progress`` against a real parent row, including a reaped one.

The companion file pins the predicate; this one pins what the two statements do
to a row, which needs Postgres: ``job.id`` is a ``postgresql.UUID`` and both
statements use ``RETURNING``.

WHAT WENT WRONG WITHOUT THIS. The increment ran ``WHERE status = RUNNING``, so a
coordinator the reaper had failed stopped counting. Its children kept arriving
and their rows kept landing -- the write is idempotent, so nothing was lost
there -- while the parent sat FAILED with its progress frozen at whatever it was
when the reaper reached it. Nothing could then say the work had finished.

Five cases, because the interesting ones are the refusals: a parent that really
failed and a parent somebody cancelled must NOT be advanced, or this change
would turn a guard into a hole.
"""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from protea.core.contracts.parent_progress import REAPED, update_parent_progress
from protea.infrastructure.orm.models.job import Job, JobEvent, JobStatus


def _emit_recorder() -> tuple[Any, list[tuple[str, dict[str, Any]]]]:
    seen: list[tuple[str, dict[str, Any]]] = []

    def emit(event: str, _message: Any, fields: Any, _level: str) -> None:
        seen.append((event, dict(fields or {})))

    return emit, seen


def _parent(
    session: Session,
    *,
    status: JobStatus,
    current: int,
    total: int,
    error_code: str | None = None,
) -> uuid.UUID:
    job = Job(
        id=uuid.uuid4(),
        operation="encode_residue_vocabulary",
        queue_name="protea.embeddings",
        status=status,
        payload={},
        meta={},
        progress_current=current,
        progress_total=total,
        error_code=error_code,
    )
    session.add(job)
    session.flush()
    return job.id


def _fresh(postgres_url: str) -> Any:
    from protea.infrastructure.orm.base import Base

    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    return engine


def _reload(session: Session, job_id: uuid.UUID) -> Job:
    session.expire_all()
    job = session.get(Job, job_id)
    assert job is not None
    return job


def _succeeded_event(session: Session, job_id: uuid.UUID) -> JobEvent | None:
    return session.scalars(
        select(JobEvent).where(JobEvent.job_id == job_id, JobEvent.event == "job.succeeded")
    ).first()


def test_a_running_parent_closes_on_its_last_batch(postgres_url: str) -> None:
    """The path that always worked: no recovery marker anywhere on it."""
    engine = _fresh(postgres_url)
    with Session(engine) as session:
        parent = _parent(session, status=JobStatus.RUNNING, current=6, total=7)
        emit, seen = _emit_recorder()
        update_parent_progress(session, parent, emit, event_name="x.parent_succeeded")
        session.flush()

        job = _reload(session, parent)
        assert job.status == JobStatus.SUCCEEDED
        assert job.progress_current == 7
        event = _succeeded_event(session, parent)
        assert event is not None
        assert event.fields == {"via": "last_batch_stored"}
        assert seen and "recovered_from" not in seen[0][1]


def test_a_reaped_parent_closes_and_says_it_was_recovered(postgres_url: str) -> None:
    """The case this change exists for.

    The code is cleared on the row, because a SUCCEEDED job carrying the reason
    it failed contradicts itself, and it is kept in the event so the recovery is
    still on the record.
    """
    engine = _fresh(postgres_url)
    with Session(engine) as session:
        parent = _parent(
            session, status=JobStatus.FAILED, current=6, total=7, error_code=REAPED
        )
        emit, seen = _emit_recorder()
        update_parent_progress(session, parent, emit, event_name="x.parent_succeeded")
        session.flush()

        job = _reload(session, parent)
        assert job.status == JobStatus.SUCCEEDED
        assert job.progress_current == 7
        assert job.error_code is None, "a SUCCEEDED job must not carry why it failed"
        event = _succeeded_event(session, parent)
        assert event is not None
        assert event.fields["recovered_from"] == REAPED
        assert event.fields["was"] == JobStatus.FAILED.value
        assert seen[0][1]["recovered_from"] == REAPED


def test_a_reaped_parent_advances_without_closing_mid_flight(postgres_url: str) -> None:
    """Counting resumes before the last batch, and the status waits for it.

    A parent that is advanced but not yet complete stays FAILED: the recovery is
    the close, not the increment, so a run that never finishes arriving does not
    get quietly marked successful.
    """
    engine = _fresh(postgres_url)
    with Session(engine) as session:
        parent = _parent(
            session, status=JobStatus.FAILED, current=2, total=7, error_code=REAPED
        )
        emit, seen = _emit_recorder()
        update_parent_progress(session, parent, emit, event_name="x.parent_succeeded")
        session.flush()

        job = _reload(session, parent)
        assert job.progress_current == 3
        assert job.status == JobStatus.FAILED
        assert job.error_code == REAPED
        assert _succeeded_event(session, parent) is None
        assert seen == []


def test_a_parent_that_really_failed_is_left_alone(postgres_url: str) -> None:
    """The refusal that keeps this from being a hole.

    ``HoldoutTouchedError`` is one of the codes this deployment actually holds.
    A batch still in flight must not advance a run that raised, and must not
    close it.
    """
    engine = _fresh(postgres_url)
    with Session(engine) as session:
        parent = _parent(
            session,
            status=JobStatus.FAILED,
            current=6,
            total=7,
            error_code="HoldoutTouchedError",
        )
        emit, seen = _emit_recorder()
        update_parent_progress(session, parent, emit, event_name="x.parent_succeeded")
        session.flush()

        job = _reload(session, parent)
        assert job.progress_current == 6, "a real failure must not be advanced"
        assert job.status == JobStatus.FAILED
        assert job.error_code == "HoldoutTouchedError"
        assert _succeeded_event(session, parent) is None
        assert seen == []


def test_a_cancelled_parent_is_left_alone(postgres_url: str) -> None:
    """A person saying stop is not the reaper being wrong."""
    engine = _fresh(postgres_url)
    with Session(engine) as session:
        parent = _parent(session, status=JobStatus.CANCELLED, current=6, total=7)
        emit, seen = _emit_recorder()
        update_parent_progress(session, parent, emit, event_name="x.parent_succeeded")
        session.flush()

        job = _reload(session, parent)
        assert job.progress_current == 6
        assert job.status == JobStatus.CANCELLED
        assert _succeeded_event(session, parent) is None
        assert seen == []
