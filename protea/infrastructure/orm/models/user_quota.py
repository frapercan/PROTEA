"""``UserQuota`` ORM model -- per-user daily operation quota (FARM-AUTH.7).

Tracks how many times a given authenticated user has invoked an expensive
operation (``predict``, ``export_research_dataset``, ``run_cafa_evaluation``)
on a given UTC calendar day.  One row exists per (user_id, day, operation)
triple; the ``count`` column is incremented on each call.

Schema notes
------------

* ``user_id`` is a FK to ``user(id)`` with CASCADE DELETE so quota rows are
  cleaned up automatically when a user account is removed.
* ``day`` is a plain SQL DATE (no timezone component) whose value is always
  interpreted as UTC midnight.  The application writes ``date.today(datetime.UTC)``
  equivalents; downstream readers should treat the column the same way.
* ``operation`` is a free-text label matching the operation names registered
  in ``Settings.user_quota_per_day``.  No DB enum is used so new operation
  names can be added without a migration.
* The unique constraint on ``(user_id, day, operation)`` lets the increment
  path use an INSERT ... ON CONFLICT UPDATE pattern in future if needed;
  currently the dependency reads then writes inside the same session.
* ``updated_at`` is refreshed on every increment so operators can spot stale
  rows and troubleshoot quota resets.
"""

from __future__ import annotations

import uuid
from datetime import date, datetime

from sqlalchemy import Date, DateTime, ForeignKey, Index, Integer, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from protea.core.utils import utcnow
from protea.infrastructure.orm.base import Base


class UserQuota(Base):
    """Daily per-user operation quota counter."""

    __tablename__ = "user_quota"

    __table_args__ = (
        UniqueConstraint("user_id", "day", "operation", name="uq_user_quota_user_day_op"),
        Index("ix_user_quota_user_day", "user_id", "day"),
        Index("ix_user_quota_operation", "operation"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )

    user_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("user.id", ondelete="CASCADE"),
        nullable=False,
    )

    day: Mapped[date] = mapped_column(Date, nullable=False)

    operation: Mapped[str] = mapped_column(Text, nullable=False)

    count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow, onupdate=utcnow
    )

    def __repr__(self) -> str:
        return (
            f"<UserQuota(user_id={self.user_id}, day={self.day}, "
            f"operation={self.operation!r}, count={self.count})>"
        )


__all__ = ["UserQuota"]
