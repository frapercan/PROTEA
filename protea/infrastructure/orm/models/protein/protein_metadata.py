from sqlalchemy import Column, String, Text, DateTime, func
from sqlalchemy.orm import relationship

from protea.infrastructure.orm.base import Base


class ProteinUniProtMetadata(Base):
    """
    Raw UniProt metadata stored ONCE per canonical accession.

    - Primary key: canonical_accession (e.g., X6R8D5)
    - Isoforms reuse the same metadata via Protein.canonical_accession.
    - Relationship to Protein is view-only (no FK required).
    """

    __tablename__ = "protein_uniprot_metadata"

    canonical_accession = Column(String, primary_key=True, nullable=False)

    absorption = Column(Text, nullable=True)
    active_site = Column(Text, nullable=True)
    binding_site = Column(Text, nullable=True)
    catalytic_activity = Column(Text, nullable=True)
    cofactor = Column(Text, nullable=True)
    dna_binding = Column(Text, nullable=True)
    ec_number = Column(Text, nullable=True)
    activity_regulation = Column(Text, nullable=True)
    function_cc = Column(Text, nullable=True)
    pathway = Column(Text, nullable=True)
    kinetics = Column(Text, nullable=True)
    ph_dependence = Column(Text, nullable=True)
    redox_potential = Column(Text, nullable=True)
    rhea_id = Column(Text, nullable=True)
    site = Column(Text, nullable=True)
    temperature_dependence = Column(Text, nullable=True)
    keywords = Column(Text, nullable=True)
    features = Column(Text, nullable=True)

    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    proteins = relationship(
        "Protein",
        primaryjoin="Protein.canonical_accession == foreign(ProteinUniProtMetadata.canonical_accession)",
        viewonly=True,
    )
