from __future__ import annotations

import enum
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import BigInteger, DateTime, Enum, ForeignKey, Index, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from protea.infrastructure.orm.base import Base
from protea.core.utils import utcnow


class JobStatus(enum.StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


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

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow
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
    )


class JobEvent(Base):
    __tablename__ = "job_event"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    job_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("job.id", ondelete="CASCADE"),
        nullable=False,
    )

    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utcnow)
    level: Mapped[str] = mapped_column(Text, nullable=False, default="info")
    event: Mapped[str] = mapped_column(Text, nullable=False)
    message: Mapped[str | None] = mapped_column(Text, nullable=True)

    fields: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)

    job: Mapped[Job] = relationship("Job", back_populates="events")

    __table_args__ = (
        Index("ix_job_event_job_id_ts_desc", "job_id", "ts"),
        Index("ix_job_event_event_ts_desc", "event", "ts"),
    )
