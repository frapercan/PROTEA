# protein_information_system/sql/model/entities/protein/protein.py
from __future__ import annotations

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    func,
)
from sqlalchemy.orm import relationship

from protea.infrastructure.orm.base import Base


class Protein(Base):
    """
    Production-grade Protein schema for the ingestion phase.

    Key points:
    - One row per UniProt accession (including isoforms like X6R8D5-2).
    - Isoforms grouped by canonical_accession.
    - MANY proteins can point to the SAME Sequence (sequence dedup).
      => protein.sequence_id MUST NOT be UNIQUE.
    - Optional metadata relationship is view-only and joins by canonical_accession.
    """

    __tablename__ = "protein"

    # UniProt Entry (accession). Isoforms: "<canonical>-<n>".
    accession = Column(String, primary_key=True, nullable=False)

    # UniProt Entry Name (NOT unique across isoforms)
    entry_name = Column(String, nullable=True, index=True)

    # Swiss-Prot reviewed vs TrEMBL
    reviewed = Column(Boolean, nullable=True, index=True)

    # Isoform grouping (metadata is keyed by canonical_accession)
    canonical_accession = Column(String, nullable=False, index=True)
    isoform_index = Column(Integer, nullable=True)
    is_canonical = Column(Boolean, nullable=False, default=True)

    # Core FASTA-header derived fields
    taxonomy_id = Column(String, nullable=True, index=True)  # OX=
    organism = Column(String, nullable=True)                 # OS=
    gene_name = Column(String, nullable=True)                # GN=
    length = Column(Integer, nullable=True)                  # len(sequence) or UniProt length

    # MANY proteins can share one Sequence
    sequence_id = Column(
        Integer,
        ForeignKey("sequence.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
        unique=False,  # IMPORTANT: do NOT make this unique
    )

    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    # Relationships
    sequence = relationship("Sequence", back_populates="proteins", uselist=False)

    # Optional UniProt raw metadata (defined in another module). View-only join by canonical_accession.
    uniprot_metadata = relationship(
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
