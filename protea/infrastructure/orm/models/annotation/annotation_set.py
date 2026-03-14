from __future__ import annotations

import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import DateTime, ForeignKey, String, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from protea.infrastructure.orm.base import Base

if TYPE_CHECKING:
    from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
    from protea.infrastructure.orm.models.annotation.protein_go_annotation import (
        ProteinGOAnnotation,
    )
    from protea.infrastructure.orm.models.job import Job


class AnnotationSet(Base):
    """A versioned batch of GO annotations from a single source.

    Each load operation (QuickGO download, GOA GAF ingest, CAFA dataset) creates
    one ``AnnotationSet`` row. This allows multiple temporal snapshots of the
    same source to coexist and be queried independently.

    ``ontology_snapshot_id`` pins the exact GO ontology release used to
    interpret the annotations in this set. ``job_id`` links back to the PROTEA
    job that created it, providing full audit trail.
    """

    __tablename__ = "annotation_set"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    source: Mapped[str] = mapped_column(String, nullable=False, index=True)
    source_version: Mapped[str | None] = mapped_column(String, nullable=True)
    ontology_snapshot_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("ontology_snapshot.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    job_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("job.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    meta: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)

    ontology_snapshot: Mapped[OntologySnapshot] = relationship(
        "OntologySnapshot", back_populates="annotation_sets"
    )
    job: Mapped[Job | None] = relationship("Job")
    annotations: Mapped[list[ProteinGOAnnotation]] = relationship(
        "ProteinGOAnnotation", back_populates="annotation_set", cascade="all, delete-orphan"
    )
