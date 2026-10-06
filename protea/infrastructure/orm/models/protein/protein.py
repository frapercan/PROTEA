from __future__ import annotations

from datetime import date, datetime
from typing import TYPE_CHECKING

from sqlalchemy import Boolean, Date, DateTime, ForeignKey, Integer, String, func
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
    #: UniProt audit dates, from the same batch request that fetches the sequence.
    #:
    #: ``date_created`` separates KNOWLEDGE GAIN from ENTRY CREATION. Without it, a
    #: protein first published in 2022 sits in the universe of a 2016 release with
    #: no annotations, and a release-to-release delta reads that as ``NK ->
    #: gained``. It is not: the entry did not exist. "Did this protein exist at
    #: release N" is a comparison against this column.
    #:
    #: ``date_sequence_modified`` turns the sequence leak into a NAMED SUBSET. The
    #: stored sequence comes from today's UniProt; where the last sequence update
    #: is at or before a window's ``t0``, today's sequence IS the sequence of then
    #: and nothing leaks for that protein. Measured against Swiss-Prot release
    #: 2024_02: 350 of 73,863 differ, 0.47%.
    #:
    #: ``sequence_version`` of 1 means the sequence never changed over the entry's
    #: lifetime. Measured over 109,320 canonical accessions: 71.5% are at
    #: version 1.
    #:
    #: Still deliberately absent: a ``first_release`` column meaning "the earliest
    #: release that existed once this entry existed". It is derivable from
    #: ``date_created`` against ``annotation_set.source_published_at``, which is
    #: already in this database, so a stored copy would be a second source of truth.
    #: :attr:`first_admitted_release` is a DIFFERENT quantity and is stored; see
    #: migration ``f2a8c41d9e37``.
    date_created: Mapped[date | None] = mapped_column(Date, nullable=True, index=True)
    date_sequence_modified: Mapped[date | None] = mapped_column(
        Date, nullable=True, index=True
    )
    sequence_version: Mapped[int | None] = mapped_column(Integer, nullable=True)

    #: The lowest GOA release number whose GAF admitted this accession into the
    #: universe, written by ``extract_goa_universe``. KNOWLEDGE GAIN, not entry
    #: creation: a protein can have existed since 1998 and have acquired its first
    #: experimental annotation in 2019, and ``date_created`` can only say 1998.
    #:
    #: Written as a MINIMUM (``IS NULL OR > N``), so the value is the true earliest
    #: release even if the series is ever walked out of order, and a repeated pass
    #: cannot raise it. NULL means no pass has admitted this row yet -- which is the
    #: state of every protein ``insert_proteins`` loaded.
    #:
    #: Not recoverable afterwards for a protein admitted ONLY as a reviewed entry of
    #: its release, because per-release Swiss-Prot membership is read from the entry
    #: name the GAF carries and no table keeps it.
    first_admitted_release: Mapped[int | None] = mapped_column(
        Integer, nullable=True, index=True
    )

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
        """Parse isoform accession pattern ``"<canonical>-<n>"``.

        Forwards to :func:`protea_contracts.parse_isoform`. The
        canonical implementation lives in ``protea-contracts.bio_utils``
        so the FASTA parser in ``protea-sources`` can reuse it
        without inverting the C-stack dependency direction (D-MIGR-04
        of master plan v3).
        """
        from protea_contracts import parse_isoform as _parse_isoform

        return _parse_isoform(accession)

    def __repr__(self) -> str:
        return (
            f"<Protein(accession={self.accession}, entry_name={self.entry_name}, "
            f"canonical_accession={self.canonical_accession}, isoform_index={self.isoform_index})>"
        )
