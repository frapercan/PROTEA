from __future__ import annotations

from datetime import date, datetime

from sqlalchemy import BigInteger, Date, DateTime, Index, Integer, String

from sqlalchemy.orm import Mapped, mapped_column

from protea.core.utils import utcnow
from protea.infrastructure.orm.base import Base


class VisitorEvent(Base):
    """Anonymous visit record used for aggregate traffic analytics.

    Privacy model: the ``visitor_hash`` column never stores an IP address.
    It is the first 16 hex chars of ``sha256(daily_salt || client_ip)`` where
    ``daily_salt`` is a 32-byte random value kept only in process memory and
    regenerated every calendar day. Once the day rolls over, correlating a
    visitor across days becomes cryptographically infeasible. This is the same
    approach Plausible and Fathom use to avoid storing PII while still being
    able to report "unique visitors per day".
    """

    __tablename__ = "visitor_event"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    day: Mapped[date] = mapped_column(Date, nullable=False)
    visitor_hash: Mapped[str] = mapped_column(String(16), nullable=False)

    path: Mapped[str] = mapped_column(String(255), nullable=False)
    method: Mapped[str] = mapped_column(String(8), nullable=False)
    status: Mapped[int] = mapped_column(Integer, nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow
    )

    __table_args__ = (
        Index("ix_visitor_event_day_hash", "day", "visitor_hash"),
        Index("ix_visitor_event_created_at", "created_at"),
        Index("ix_visitor_event_path", "path"),
    )
