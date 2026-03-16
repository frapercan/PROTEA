from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, String, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from protea.infrastructure.orm.base import Base

if TYPE_CHECKING:
    from protea.infrastructure.orm.models.protein.protein_metadata import ProteinUniProtMetadata
    from protea.infrastructure.orm.models.sequence.sequence import Sequence


class Protein(Base):
    """One row per UniProt accession, including isoforms (``<canonical>-<n>``).

    Isoforms are grouped by ``canonical_accession``. Many proteins can share
    the same ``Sequence`` row — ``sequence_id`` is deliberately non-unique.
    The ``uniprot_metadata`` relationship is view-only, joined by
    ``canonical_accession``.
    """

    __tablename__ = "protein"

    # UniProt Entry (accession). Isoforms: "<canonical>-<n>".
    accession: Mapped[str] = mapped_column(String, primary_key=True, nullable=False)

    # UniProt Entry Name (NOT unique across isoforms)
    entry_name: Mapped[str | None] = mapped_column(String, nullable=True, index=True)

    # Swiss-Prot reviewed vs TrEMBL
    reviewed: Mapped[bool | None] = mapped_column(Boolean, nullable=True, index=True)

    # Isoform grouping (metadata is keyed by canonical_accession)
    canonical_accession: Mapped[str] = mapped_column(String, nullable=False, index=True)
    isoform_index: Mapped[int | None] = mapped_column(Integer, nullable=True)
    is_canonical: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    # Core FASTA-header derived fields
    taxonomy_id: Mapped[str | None] = mapped_column(String, nullable=True, index=True)  # OX=
    organism: Mapped[str | None] = mapped_column(String, nullable=True)  # OS=
    gene_name: Mapped[str | None] = mapped_column(String, nullable=True)  # GN=
    length: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )  # len(sequence) or UniProt length

    # MANY proteins can share one Sequence
    sequence_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("sequence.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, server_default=func.now(), onupdate=func.now()
    )

    # Relationships
    sequence: Mapped[Sequence | None] = relationship(
        "Sequence", back_populates="proteins", uselist=False
    )

    # Optional UniProt raw metadata (defined in another module). View-only join by canonical_accession.
    uniprot_metadata: Mapped[ProteinUniProtMetadata | None] = relationship(
        "ProteinUniProtMetadata",
        primaryjoin="Protein.canonical_accession == foreign(ProteinUniProtMetadata.canonical_accession)",
        uselist=False,
        viewonly=True,
    )

    @staticmethod
    def parse_isoform(accession: str) -> tuple[str, bool, int | None]:
        """
        Parse isoform accession pattern "<canonical>-<n>".
        Returns: (canonical_accession, is_canonical, isoform_index)
        """
        if "-" in accession:
            left, right = accession.rsplit("-", 1)
            if right.isdigit():
                return left, False, int(right)
        return accession, True, None

    def __repr__(self) -> str:
        return (
            f"<Protein(accession={self.accession}, entry_name={self.entry_name}, "
            f"canonical_accession={self.canonical_accession}, isoform_index={self.isoform_index})>"
        )
