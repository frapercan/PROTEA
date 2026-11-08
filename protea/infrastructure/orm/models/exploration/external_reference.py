from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import JSON, CheckConstraint, DateTime, String, Text, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from protea.core.explorations import REFERENCE_KINDS, check_in
from protea.infrastructure.orm.base import Base

_JSONB = JSONB().with_variant(JSON(), "sqlite")


class ExternalReference(Base):
    """A figure published by others, kept as context and never on the same axis (ADR-D55).

    Reusable across explorations, which link to it. The copy in the store is
    kept with its sha256, so the page a figure was read from can be shown as it
    was when it was captured. ``profile`` says how the figure was measured:
    window, targets, evidence codes, protein-binding rule, ontology closure,
    metric and aggregation. A reference measured another way is context for a
    result, and the profile is what lets a reader see how far.
    """

    __tablename__ = "external_reference"
    __table_args__ = (
        CheckConstraint(check_in("kind", REFERENCE_KINDS), name="ck_external_reference_kind"),
        CheckConstraint(
            "(store_key IS NULL) = (store_sha256 IS NULL)",
            name="ck_external_reference_store_key_with_its_sha",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    title: Mapped[str] = mapped_column(String(300), nullable=False)
    kind: Mapped[str] = mapped_column(String(16), nullable=False)
    url: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    citation: Mapped[str | None] = mapped_column(Text, nullable=True)
    captured_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    captured_by: Mapped[str] = mapped_column(String(200), nullable=False)
    store_key: Mapped[str | None] = mapped_column(String(512), nullable=True)
    store_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    profile: Mapped[dict[str, Any]] = mapped_column(_JSONB, nullable=False, default=dict)
