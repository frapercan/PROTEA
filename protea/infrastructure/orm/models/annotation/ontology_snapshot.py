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

    ``ia_url`` optionally points to the Information Accretion (IA) TSV file
    associated with this ontology release.  IA files are published alongside
    each CAFA benchmark (e.g. ``IA_cafa6.tsv``) and contain per-term
    information-content weights that make cafaeval penalise predictions of
    common, easy-to-predict terms less than rare, specific ones.  When present,
    ``run_cafa_evaluation`` downloads and passes this file to cafaeval
    automatically — no manual path is required in the job payload.  Set a new
    ``ia_url`` on each future snapshot (CAFA7, etc.) to keep evaluations
    comparable across benchmark generations.
    """

    __tablename__ = "ontology_snapshot"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    obo_url: Mapped[str] = mapped_column(String, nullable=False)
    obo_version: Mapped[str] = mapped_column(String, nullable=False, unique=True, index=True)
    ia_url: Mapped[str | None] = mapped_column(
        String,
        nullable=True,
        comment=(
            "URL of the Information Accretion TSV for this ontology release "
            "(two columns: go_id, ia_value). Used by run_cafa_evaluation to "
            "weight GO terms by information content. NULL means uniform IC=1."
        ),
    )
    loaded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    go_terms: Mapped[list[GOTerm]] = relationship(
        "GOTerm", back_populates="ontology_snapshot", cascade="all, delete-orphan"
    )
    annotation_sets: Mapped[list[AnnotationSet]] = relationship(
        "AnnotationSet", back_populates="ontology_snapshot"
    )
