from __future__ import annotations

import uuid
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from protea.infrastructure.orm.base import Base

if TYPE_CHECKING:
    from protea.infrastructure.orm.models.protein.protein import Protein


class InterProAnnotation(Base):
    """One InterProScan domain hit persisted for a protein.

    Mirrors the IP.1a parser record (``protea_sources.interpro.parser.
    InterProAnnotation``) one-to-one, plus a surrogate UUID primary
    key and the ``ipr_release`` tag so multiple InterProScan runs at
    different release versions can coexist for the same protein
    without conflict.

    The ``protein_accession`` FK follows the same pattern as
    :class:`ProteinGOAnnotation`: PROTEA's :class:`Protein` PK is the
    UniProt accession string, not a surrogate integer, so domain
    tables key by ``accession`` directly. The IP.2 spec language
    ``protein_id`` is preserved at the relationship level
    (``self.protein``) but the column is ``protein_accession`` to
    stay consistent with the rest of the annotation schema.

    Coordinate invariants (``start >= 1``, ``end >= start``) are
    enforced as table-level ``CHECK`` constraints so a buggy
    upstream caller cannot smuggle nonsense rows past the database.
    """

    __tablename__ = "interpro_annotation"
    __table_args__ = (
        CheckConstraint('"start" >= 1', name="ck_interpro_annotation_start_positive"),
        CheckConstraint('"end" >= "start"', name="ck_interpro_annotation_end_ge_start"),
        Index("idx_interpro_annotation_protein_id", "protein_accession"),
        Index("idx_interpro_annotation_accession", "accession"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    protein_accession: Mapped[str] = mapped_column(
        String,
        ForeignKey("protein.accession", ondelete="CASCADE"),
        nullable=False,
    )
    source_db: Mapped[str] = mapped_column(Text, nullable=False)
    accession: Mapped[str] = mapped_column(Text, nullable=False)
    start: Mapped[int] = mapped_column(Integer, nullable=False)
    end: Mapped[int] = mapped_column(Integer, nullable=False)
    evidence: Mapped[str | None] = mapped_column(Text, nullable=True)
    ipr_release: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    protein: Mapped[Protein] = relationship("Protein")

    def __repr__(self) -> str:
        return (
            "<InterProAnnotation("
            f"protein_accession={self.protein_accession}, "
            f"source_db={self.source_db}, accession={self.accession}, "
            f"start={self.start}, end={self.end}, "
            f"ipr_release={self.ipr_release})>"
        )
