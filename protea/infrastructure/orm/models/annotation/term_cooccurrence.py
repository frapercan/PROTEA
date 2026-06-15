from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

from sqlalchemy import BigInteger, ForeignKey, Index, Integer
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from protea.infrastructure.orm.base import Base

if TYPE_CHECKING:
    pass


class TermCooccurrence(Base):
    """Per-annotation-set GO term co-occurrence counts (lafa-integrate INT-3).

    One row per ``(annotation_set_id, known_term_id, candidate_term_id)``
    triple. ``cooccurrence_count`` is the number of DISTINCT proteins in the
    annotation set that carry BOTH ``known_term_id`` and ``candidate_term_id``
    among their propagated EXPERIMENTAL annotations.

    The cross-aspect association feature reads ``P(t | k) = cooccurrence /
    freq(k)`` from this table together with :class:`TermFrequency`. The
    ``known_term_id`` (the ``k`` side) is scoped at build time to "specific"
    terms (training frequency below a cap) so uninformative high-frequency
    terms are never stored.

    This table is populated offline by the ``build_go_cooccurrence`` operation
    and read at predict time; it is never written on the prediction path.
    """

    __tablename__ = "term_cooccurrence"
    __table_args__ = (
        Index(
            "ix_term_cooccurrence_set_known",
            "annotation_set_id",
            "known_term_id",
        ),
    )

    annotation_set_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("annotation_set.id", ondelete="CASCADE"),
        primary_key=True,
    )
    known_term_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("go_term.id", ondelete="CASCADE"),
        primary_key=True,
    )
    candidate_term_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("go_term.id", ondelete="CASCADE"),
        primary_key=True,
    )
    cooccurrence_count: Mapped[int] = mapped_column(Integer, nullable=False)


class TermFrequency(Base):
    """Per-annotation-set per-term protein frequency (lafa-integrate INT-3).

    ``freq`` is the number of DISTINCT proteins in the annotation set that
    carry ``term_id`` among their propagated EXPERIMENTAL annotations. It is
    the denominator of ``P(t | k) = cooccurrence(k, t) / freq(k)`` used by the
    cross-aspect association feature. Populated alongside
    :class:`TermCooccurrence` by ``build_go_cooccurrence``.
    """

    __tablename__ = "term_frequency"

    annotation_set_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("annotation_set.id", ondelete="CASCADE"),
        primary_key=True,
    )
    term_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("go_term.id", ondelete="CASCADE"),
        primary_key=True,
    )
    freq: Mapped[int] = mapped_column(Integer, nullable=False)
