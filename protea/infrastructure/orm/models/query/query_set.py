from __future__ import annotations

import uuid
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import (
    BigInteger,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from protea.infrastructure.orm.base import Base

if TYPE_CHECKING:
    from protea.infrastructure.orm.models.sequence.sequence import Sequence


class QuerySet(Base):
    """A named collection of sequences uploaded by the user for GO term prediction.

    Each uploaded FASTA file creates one ``QuerySet`` row. Entries preserve the
    original accession strings from the FASTA headers and link to the deduplicated
    ``Sequence`` rows. This allows the same physical sequence to appear in multiple
    query sets without duplication.
    """

    __tablename__ = "query_set"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    name: Mapped[str] = mapped_column(String, nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    entries: Mapped[list[QuerySetEntry]] = relationship(
        "QuerySetEntry", back_populates="query_set", cascade="all, delete-orphan"
    )


class QuerySetEntry(Base):
    """One sequence within a QuerySet, preserving the original FASTA accession.

    ``accession`` is the raw identifier from the FASTA header (may not exist in
    the ``protein`` table). ``sequence_id`` links to the deduplicated ``Sequence``
    row used for embedding computation and similarity search.
    """

    __tablename__ = "query_set_entry"

    __table_args__ = (
        UniqueConstraint("query_set_id", "accession", name="uq_query_set_entry_set_accession"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    query_set_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("query_set.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    sequence_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("sequence.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    accession: Mapped[str] = mapped_column(String, nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    query_set: Mapped[QuerySet] = relationship("QuerySet", back_populates="entries")
    sequence: Mapped[Sequence] = relationship("Sequence")
