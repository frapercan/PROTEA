from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from protea.infrastructure.orm.base import Base

if TYPE_CHECKING:
    from protea.infrastructure.orm.models.protein.protein import Protein


class ProteinUniProtMetadata(Base):
    """
    Raw UniProt metadata stored ONCE per canonical accession.

    - Primary key: canonical_accession (e.g., X6R8D5)
    - Isoforms reuse the same metadata via Protein.canonical_accession.
    - Relationship to Protein is view-only (no FK required).
    """

    __tablename__ = "protein_uniprot_metadata"

    canonical_accession: Mapped[str] = mapped_column(String, primary_key=True, nullable=False)

    absorption: Mapped[str | None] = mapped_column(Text, nullable=True)
    active_site: Mapped[str | None] = mapped_column(Text, nullable=True)
    binding_site: Mapped[str | None] = mapped_column(Text, nullable=True)
    catalytic_activity: Mapped[str | None] = mapped_column(Text, nullable=True)
    cofactor: Mapped[str | None] = mapped_column(Text, nullable=True)
    dna_binding: Mapped[str | None] = mapped_column(Text, nullable=True)
    ec_number: Mapped[str | None] = mapped_column(Text, nullable=True)
    activity_regulation: Mapped[str | None] = mapped_column(Text, nullable=True)
    function_cc: Mapped[str | None] = mapped_column(Text, nullable=True)
    pathway: Mapped[str | None] = mapped_column(Text, nullable=True)
    kinetics: Mapped[str | None] = mapped_column(Text, nullable=True)
    ph_dependence: Mapped[str | None] = mapped_column(Text, nullable=True)
    redox_potential: Mapped[str | None] = mapped_column(Text, nullable=True)
    rhea_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    site: Mapped[str | None] = mapped_column(Text, nullable=True)
    temperature_dependence: Mapped[str | None] = mapped_column(Text, nullable=True)
    keywords: Mapped[str | None] = mapped_column(Text, nullable=True)
    features: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    proteins: Mapped[list[Protein]] = relationship(
        "Protein",
        primaryjoin="Protein.canonical_accession == foreign(ProteinUniProtMetadata.canonical_accession)",
        viewonly=True,
    )
