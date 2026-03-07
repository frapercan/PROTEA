from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

from sqlalchemy import BigInteger, ForeignKey, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from protea.infrastructure.orm.base import Base

if TYPE_CHECKING:
    from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
    from protea.infrastructure.orm.models.annotation.go_term import GOTerm
    from protea.infrastructure.orm.models.protein.protein import Protein


class ProteinGOAnnotation(Base):
    """Association between a protein and a GO term within an annotation set.

    Fields map directly from GAF/QuickGO columns:

    - ``qualifier``:      e.g. ``enables``, ``involved_in``, ``located_in``
    - ``evidence_code``:  GO evidence code resolved from ECO (IDA, IEA, ISS…)
    - ``assigned_by``:    database that made the annotation (UniProt, RHEA…)
    - ``db_reference``:   supporting reference (PMID:..., GO_REF:...)
    - ``with_from``:      with/from field from GAF column 8
    - ``annotation_date``: YYYYMMDD string from the source file
    """

    __tablename__ = "protein_go_annotation"
    __table_args__ = (
        UniqueConstraint(
            "annotation_set_id",
            "protein_accession",
            "go_term_id",
            "evidence_code",
            name="uq_pga_set_protein_term_evidence",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    annotation_set_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("annotation_set.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    protein_accession: Mapped[str] = mapped_column(
        String,
        ForeignKey("protein.accession", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    go_term_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("go_term.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    qualifier: Mapped[str | None] = mapped_column(String, nullable=True)
    evidence_code: Mapped[str | None] = mapped_column(String, nullable=True, index=True)
    assigned_by: Mapped[str | None] = mapped_column(String, nullable=True)
    db_reference: Mapped[str | None] = mapped_column(String, nullable=True)
    with_from: Mapped[str | None] = mapped_column(String, nullable=True)
    annotation_date: Mapped[str | None] = mapped_column(String(8), nullable=True)

    annotation_set: Mapped[AnnotationSet] = relationship(
        "AnnotationSet", back_populates="annotations"
    )
    go_term: Mapped[GOTerm] = relationship("GOTerm", back_populates="annotations")
    protein: Mapped[Protein] = relationship("Protein")
