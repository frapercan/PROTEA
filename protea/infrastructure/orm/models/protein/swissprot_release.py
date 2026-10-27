from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import (
    Date,
    DateTime,
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
    from protea.infrastructure.orm.models.job import Job


class SwissProtRelease(Base):
    """Swiss-Prot membership as of one archived UniProt release.

    ``protein.reviewed`` is a single boolean describing *today*, so it cannot
    answer "was this entry reviewed then" without repeating the scope defect it
    was created by: describing the present over data from the past. The
    injection direction is the large one -- 4.3% of today's Swiss-Prot was not
    in Swiss-Prot in 2016, against 0.1% in the loss direction -- so a temporal
    window filtered by ``reviewed`` is wrong by a wide margin.

    Why this is not read off the GAF
    --------------------------------

    It can be, but only below release 179.
    :func:`protea.core.operations._universe_sources.is_swissprot_entry` reads
    contemporaneous membership out of the GAF's DB Object Synonym column, which
    carries the entry name of its own release, and that is free and exact where
    the column survives. Measured 2026-10-06 over the cached series: releases
    164..178 carry an entry name on 100% of rows, **179 and 180 on 0%, and 231
    on 5%** -- GOA replaced it with the gene symbol, and the name is in no other
    column.

    The campaign's evaluation window is GOA 227 to 230, both inside that dead
    stretch. So for the window that actually gets scored the GAF route does not
    exist, and membership has to come from UniProt's own archived release.

    What a row pins
    ---------------

    ``content_sha256`` is the digest of the canonical table this release
    resolves to -- ``"<accession>\\t<md5>\\n"`` per entry, sorted by accession --
    not of whatever file happened to be read. That makes a download from
    UniProt and a pre-extracted table interchangeable and mutually checkable:
    both reduce to the same bytes or the load is refused.

    ``accession_count`` is stored rather than counted on demand because it is
    the number the provenance claims, and a disagreement between it and the
    rows actually written is the failure this table exists to make visible.
    """

    __tablename__ = "swissprot_release"
    __table_args__ = (
        # One row per archived release. A second is a duplicate by construction.
        UniqueConstraint("release", name="uq_swissprot_release_release"),
        Index("ix_swissprot_release_published_at", "published_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    #: UniProt release name, e.g. ``2025_03``.
    release: Mapped[str] = mapped_column(String(16), nullable=False)
    #: UniProt's own publication date for the release. Windows pair GOA to
    #: UniProt by publication date, never by version string.
    published_at: Mapped[date] = mapped_column(Date, nullable=False)
    source_url: Mapped[str] = mapped_column(String(512), nullable=False)
    accession_count: Mapped[int] = mapped_column(Integer, nullable=False)
    #: sha256 of the canonical ``accession\\tmd5`` table, sorted by accession.
    content_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    job_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("job.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    meta: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)

    job: Mapped[Job | None] = relationship("Job")
    members: Mapped[list[SwissProtMembership]] = relationship(
        "SwissProtMembership",
        back_populates="release_row",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"<SwissProtRelease {self.release} n={self.accession_count}>"


class SwissProtMembership(Base):
    """One accession in Swiss-Prot at one release, with its sequence md5.

    The md5 is not decoration. LAFA's target population is the accessions in
    Swiss-Prot at both ends of the window **and carrying the same sequence at
    both**, and the two conditions are not the same set: of the 573,640
    accessions present in both 2025_03 and 2026_01, 180 changed sequence, and
    14 of those are targets. Membership alone admits 7,415 targets where the
    sequence rule admits LAFA's 7,401.

    The primary key is ``(release_id, accession)`` rather than a surrogate: it
    is the natural key, it gives the uniqueness for free, and it is the index
    the cross-release join scans. The md5 is fetched from the heap on that join,
    which is cheap at this cardinality (about 574k rows per release).
    """

    __tablename__ = "swissprot_membership"

    release_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("swissprot_release.id", ondelete="CASCADE"),
        primary_key=True,
    )
    accession: Mapped[str] = mapped_column(String(32), primary_key=True)
    #: md5 of the sequence as published in THIS release, hex, lowercase.
    sequence_md5: Mapped[str] = mapped_column(String(32), nullable=False)

    release_row: Mapped[SwissProtRelease] = relationship(
        "SwissProtRelease", back_populates="members"
    )

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"<SwissProtMembership {self.accession}>"
