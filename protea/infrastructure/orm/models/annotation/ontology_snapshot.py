from __future__ import annotations

import uuid
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, String, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from protea.infrastructure.orm.base import Base

if TYPE_CHECKING:
    from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
    from protea.infrastructure.orm.models.annotation.go_term import GOTerm


class OntologySnapshot(Base):
    """One row per loaded go.obo file.

    ``obo_version`` is extracted from the ``data-version:`` header of the OBO
    file (e.g. ``releases/2024-01-17``). ``obo_url`` is the URL from which the
    file was downloaded, providing full provenance.  Multiple ``AnnotationSet``
    rows can reference the same snapshot when they were built against the same
    ontology release.
    """

    __tablename__ = "ontology_snapshot"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    obo_url: Mapped[str] = mapped_column(String, nullable=False)
    obo_version: Mapped[str] = mapped_column(String, nullable=False, unique=True, index=True)
    loaded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    go_terms: Mapped[list[GOTerm]] = relationship(
        "GOTerm", back_populates="ontology_snapshot", cascade="all, delete-orphan"
    )
    annotation_sets: Mapped[list[AnnotationSet]] = relationship(
        "AnnotationSet", back_populates="ontology_snapshot"
    )
