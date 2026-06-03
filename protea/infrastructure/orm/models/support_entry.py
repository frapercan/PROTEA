from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, Index, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from protea.infrastructure.orm.base import Base


class SupportEntry(Base):
    """A thumbs-up + optional comment submitted by a visitor."""

    __tablename__ = "support_entry"
    __table_args__ = (
        # T3.5: support listing orders by ``created_at DESC``.
        Index("ix_support_entry_created_at", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    comment: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
