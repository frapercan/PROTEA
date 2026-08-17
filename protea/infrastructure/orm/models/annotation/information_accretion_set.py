from __future__ import annotations

import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import (
    BigInteger,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from protea.infrastructure.orm.base import Base

if TYPE_CHECKING:
    from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
    from protea.infrastructure.orm.models.annotation.ontology_snapshot import (
        OntologySnapshot,
    )
    from protea.infrastructure.orm.models.job import Job


class InformationAccretionSet(Base):
    """One computed Information Accretion table, with its corpus pinned.

    IA(v) = -log2( P(v | parents(v)) ) is estimated from term frequencies over
    an annotation corpus, so it is NOT a function of the ontology alone. A table
    is identified by three axes, all three of them foreign keys or a closed
    vocabulary here:

    * ``ontology_snapshot_id`` -- the DAG the terms and True Path Rule edges
      come from, and the term universe of the output.
    * ``annotation_set_id`` -- the corpus the frequencies are counted over.
    * ``evidence_regime`` -- which evidence codes of that corpus were counted.

    The predecessor of this table was a single nullable ``ia_url`` string column
    on ``ontology_snapshot``, which pinned none of the three: two IA tables from
    different corpora were distinguishable only by their file basename. See
    ``docs/IA_PROVENANCE_v227.md`` for the incident that motivated this.

    ``evidence_codes`` stores the RESOLVED code list rather than only the regime
    name, so a table keeps its meaning if a regime definition is later revised.

    The summary statistics are not decoration. ``ia_max`` and ``ia_mean`` differ
    sharply between regimes (measured: 18.956 / 3.261 over all evidence versus
    15.943 / 2.681 over the LAFA regime on the same snapshot and corpus), so a
    table loaded under the wrong provenance is visible from its own row without
    downloading the artifact.
    """

    __tablename__ = "information_accretion_set"
    __table_args__ = (
        # The three axes together identify one IA table. A second row for the
        # same triple is a duplicate by construction.
        UniqueConstraint(
            "ontology_snapshot_id",
            "annotation_set_id",
            "evidence_regime",
            name="uq_ia_set_snapshot_corpus_regime",
        ),
        Index("ix_information_accretion_set_created_at", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    ontology_snapshot_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("ontology_snapshot.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    annotation_set_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("annotation_set.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    #: Regime name from ``protea.core.ia_regimes.EVIDENCE_REGIMES``.
    evidence_regime: Mapped[str] = mapped_column(String(32), nullable=False)
    #: The resolved evidence code list, or ``null`` for the unrestricted regime.
    evidence_codes: Mapped[list[str] | None] = mapped_column(JSONB, nullable=True)

    job_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("job.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    #: Backend-specific URI of the TSV (``s3://bucket/key`` or ``file:///...``).
    artifact_uri: Mapped[str] = mapped_column(String(512), nullable=False)
    #: sha256 of the TSV bytes, so two tables can be compared without fetching.
    content_sha256: Mapped[str] = mapped_column(String(64), nullable=False)

    term_count: Mapped[int] = mapped_column(Integer, nullable=False)
    nonzero_count: Mapped[int] = mapped_column(Integer, nullable=False)
    annotation_count: Mapped[int] = mapped_column(BigInteger, nullable=False)
    protein_count: Mapped[int] = mapped_column(Integer, nullable=False)
    propagated_pairs: Mapped[int] = mapped_column(BigInteger, nullable=False)
    ia_max: Mapped[float] = mapped_column(Float, nullable=False)
    ia_mean: Mapped[float] = mapped_column(Float, nullable=False)

    #: Gate results and shape counters recorded at computation time.
    stats: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    ontology_snapshot: Mapped[OntologySnapshot] = relationship("OntologySnapshot")
    annotation_set: Mapped[AnnotationSet] = relationship("AnnotationSet")
    job: Mapped[Job | None] = relationship("Job")
