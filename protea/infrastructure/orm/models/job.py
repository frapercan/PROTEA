from __future__ import annotations

import enum
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import BigInteger, DateTime, Enum, ForeignKey, Index, Text, func
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from protea.infrastructure.orm.base import Base


class JobStatus(enum.StrEnum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


#: Statuses that are considered "active" for deduplication purposes.
#: A new job with a matching dedup_key is rejected (409) only when an
#: existing job is in one of these states.
ACTIVE_STATUSES: tuple[str, ...] = (
    JobStatus.QUEUED,
    JobStatus.RUNNING,
)


class Job(Base):
    __tablename__ = "job"

    id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)

    operation: Mapped[str] = mapped_column(Text, nullable=False)
    queue_name: Mapped[str] = mapped_column(Text, nullable=False)

    status: Mapped[JobStatus] = mapped_column(
        Enum(JobStatus, name="job_status"),
        nullable=False,
        default=JobStatus.QUEUED,
    )

    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    meta: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)

    #: Postgres stamps it. See the note on JobEvent.ts for why the Python
    #: default is removed rather than accompanied.
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    progress_current: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    progress_total: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    parent_job_id: Mapped[UUID | None] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("job.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    error_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Narrative fields (D11). ``description`` is set at creation time by the
    # orchestrator; ``findings`` is set post-completion by a worker or curator;
    # ``tags`` are lightweight grouping tokens that default to an empty list.
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    findings: Mapped[str | None] = mapped_column(Text, nullable=True)
    tags: Mapped[list[str]] = mapped_column(
        ARRAY(Text), nullable=False, default=list
    )

    # F-OPS-JOBS.1: deduplication key.
    # SHA-256[:16] of canonical(operation, payload). NULL on legacy rows.
    # Partial unique index (below) enforces uniqueness only among active jobs
    # (queued or running), so the same op+payload can be re-submitted once the
    # previous instance reaches a terminal state.
    dedup_key: Mapped[str | None] = mapped_column(Text, nullable=True)

    # F-OPS-JOBS.1: lease expiry timestamp.
    # Set by the worker on claim and renewed every
    # PROTEA_JOB_HEARTBEAT_INTERVAL_SECONDS. The stale-job reaper uses this
    # instead of started_at so a live heartbeating job is never killed.
    leased_until: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    events: Mapped[list[JobEvent]] = relationship(
        "JobEvent",
        back_populates="job",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    __table_args__ = (
        Index("ix_job_operation_created_at", "operation", "created_at"),
        Index("ix_job_status_created_at", "status", "created_at"),
        Index("ix_job_created_at", "created_at"),
        # Partial unique index: dedup_key must be unique across active jobs.
        # NULLS are excluded by the WHERE clause so legacy rows (dedup_key IS
        # NULL) never block new submissions.
        Index(
            "uq_job_dedup_key_active",
            "dedup_key",
            unique=True,
            postgresql_where=(
                "status IN ('QUEUED', 'RUNNING') AND dedup_key IS NOT NULL"
            ),
        ),
    )


class JobEvent(Base):
    __tablename__ = "job_event"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    job_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("job.id", ondelete="CASCADE"),
        nullable=False,
    )

    #: Stamped by POSTGRES, not by the process that emits. The Python default
    #: was removed rather than accompanied: SQLAlchemy resolves `default=` at
    #: flush and puts the column in the INSERT, so a server_default sitting
    #: beside one never fires. The pair would pass review, apply cleanly and
    #: change nothing.
    #:
    #: It matters because this stream is shared. A compute node whose clock is
    #: wrong writes fiction into the common history: on 2026-08-29 a node
    #: booted two hours ahead for twenty seconds and its three events made two
    #: readers reconstruct a 110-minute stall that never happened. Sweeping the
    #: table before this change found 207,802 events across 53 jobs stamped
    #: after their own job finished, up to 20 hours out, and 8,637 stamped
    #: before their job began. A quarter of the stream.
    ts: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    level: Mapped[str] = mapped_column(Text, nullable=False, default="info")
    event: Mapped[str] = mapped_column(Text, nullable=False)
    message: Mapped[str | None] = mapped_column(Text, nullable=True)

    fields: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)

    job: Mapped[Job] = relationship("Job", back_populates="events")

    __table_args__ = (
        Index("ix_job_event_job_id_ts_desc", "job_id", "ts"),
        Index("ix_job_event_event_ts_desc", "event", "ts"),
        # T3.5: ``base_worker._on_retry_later`` counts events filtered
        # by ``(job_id, event)``; composite index keeps the count
        # index-only without scanning every event for that job.
        Index("ix_job_event_job_id_event", "job_id", "event"),
    )


class JobComment(Base):
    """Free-form note attached to a Job (decision D11 thread).

    Unlike :class:`JobEvent` (machine-emitted, structured fields), a
    ``JobComment`` carries an opinionated curator/operator note plus its
    author tag. Cascades on parent delete; sorted chronologically by
    ``created_at`` via the supporting index.
    """

    __tablename__ = "job_comment"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    job_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("job.id", ondelete="CASCADE"),
        nullable=False,
    )
    author: Mapped[str | None] = mapped_column(Text, nullable=True)
    body: Mapped[str] = mapped_column(Text, nullable=False)
    #: Postgres stamps it, like every other time on these tables.
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        Index("ix_job_comment_job_id_created_at", "job_id", "created_at"),
    )
