"""Anonymous-user daily quota row.

One row per (ip_hash, day) pair. The ``ip_hash`` column stores the
sha256 hex digest of the client IP so no plaintext IP is ever persisted.
The quota dependency increments ``count`` on every anonymous hit and
rejects further requests once ``count`` reaches the configured limit.

The row naturally expires after the UTC day rolls over: the next day a
new row is inserted for the same hash.
"""

from __future__ import annotations

import uuid
from datetime import date, datetime

from sqlalchemy import Date, DateTime, Index, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from protea.core.utils import utcnow
from protea.infrastructure.orm.base import Base


class AnonQuota(Base):
    """Daily quota bucket for unauthenticated callers.

    Columns:

    - ``id``: UUID primary key.
    - ``ip_hash``: sha256 hex digest of the client IP (64 chars).
    - ``day``: UTC calendar date this row covers.
    - ``count``: number of anonymous requests consumed today.
    - ``updated_at``: wall-clock timestamp of the last increment.

    Unique constraint on ``(ip_hash, day)`` ensures exactly one row per
    IP hash per calendar day; the quota dependency uses an upsert path
    to avoid race conditions.
    """

    __tablename__ = "anon_quota"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    ip_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    day: Mapped[date] = mapped_column(Date, nullable=False)
    count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow
    )

    __table_args__ = (
        UniqueConstraint("ip_hash", "day", name="uq_anon_quota_ip_day"),
        Index("ix_anon_quota_ip_hash_day", "ip_hash", "day"),
    )
