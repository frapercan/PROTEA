from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

from sqlalchemy import BigInteger, ForeignKey, Index, String, func, text
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

    #: Identidad de una anotacion, y por que es un INDICE sobre ``coalesce`` y no
    #: una ``UniqueConstraint``.
    #:
    #: QUE SE PERDIA. La restriccion anterior era
    #: ``(set, protein, go_term, evidence_code)``, asi que dos filas del GAF que
    #: difirieran solo en ``qualifier`` o en ``db_reference`` colapsaban en una, y
    #: el cargador usa ``on_conflict_do_nothing``: ganaba la PRIMERA en orden de
    #: fichero, que no es una decision curatorial. Medido sobre GOA 156, 875.990
    #: filas fiables caian a 620.565 tripletas, el 29,2%. De eso, 74.346
    #: tripletas tenian mas de una referencia bibliografica --se perdia el numero
    #: de apoyos independientes-- y 358 pares (proteina, termino) perdian el NOT
    #: del todo, que es el dato mas escaso que existe y el que
    #: ``_reconcile_not_side`` necesita para restar.
    #:
    #: POR QUE NO SE PUEDE AMPLIAR LA ``UniqueConstraint`` SIN MAS. En Postgres
    #: ``NULL`` nunca entra en conflicto con ``NULL``, y en GOA 156 el qualifier
    #: esta vacio en 871.484 de las 875.990 filas fiables -- el plugin lo guarda
    #: como ``None``. Una restriccion que incluyera ``qualifier`` dejaria de
    #: deduplicar justo en el 99,5% de las filas, y cada reintento de una carga
    #: interrumpida duplicaria el corpus en silencio. Comprobado contra Postgres:
    #: dos filas con la misma clave y ``NULL`` conviven bajo ``UNIQUE``.
    #:
    #: Un indice unico sobre ``coalesce(col, '')`` si deduplica, y
    #: ``on_conflict`` puede apuntarlo por sus ``index_elements``. Eso conserva la
    #: resumibilidad de la que depende el cargador y la polaridad a la vez.
    __table_args__ = (
        Index(
            "uq_pga_annotation_identity",
            "annotation_set_id",
            "protein_accession",
            "go_term_id",
            func.coalesce(text("evidence_code"), text("''")),
            func.coalesce(text("qualifier"), text("''")),
            func.coalesce(text("db_reference"), text("''")),
            unique=True,
        ),
        Index("ix_pga_set_accession", "annotation_set_id", "protein_accession"),
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
