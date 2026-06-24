from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

from sqlalchemy import BigInteger, ForeignKey, Index, Integer, String
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

    Snapshot invariance: GO term integer ids are per ``ontology_snapshot_id``,
    so a build keyed only on int ids only matches candidates that share the t0
    set's snapshot. ``known_go_id`` / ``candidate_go_id`` carry the
    snapshot-INVARIANT ``go_id`` STRING alongside the int FK; the loader and
    scorer match on those strings so association is correct whether or not the
    t0 set's snapshot equals the candidate snapshot. The int FK columns stay
    (nullable) for ``go_term`` integrity and backward readability.

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
        Index(
            "ix_term_cooccurrence_set_known_go",
            "annotation_set_id",
            "known_go_id",
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
    # Snapshot-invariant string keys (the matching keys at score time).
    known_go_id: Mapped[str | None] = mapped_column(String(15), nullable=True)
    candidate_go_id: Mapped[str | None] = mapped_column(String(15), nullable=True)
    cooccurrence_count: Mapped[int] = mapped_column(Integer, nullable=False)


class TermFrequency(Base):
    """Per-annotation-set per-term protein frequency (lafa-integrate INT-3).

    ``freq`` is the number of DISTINCT proteins in the annotation set that
    carry ``term_id`` among their propagated EXPERIMENTAL annotations. It is
    the denominator of ``P(t | k) = cooccurrence(k, t) / freq(k)`` used by the
    cross-aspect association feature. Populated alongside
    :class:`TermCooccurrence` by ``build_go_cooccurrence``.

    ``go_id`` carries the snapshot-invariant string for ``term_id`` so the
    scorer can look up ``freq(k)`` by go_id string (see
    :class:`TermCooccurrence`).
    """

    __tablename__ = "term_frequency"
    __table_args__ = (
        Index(
            "ix_term_frequency_set_go",
            "annotation_set_id",
            "go_id",
        ),
    )

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
    # Snapshot-invariant string key for the term (the freq lookup key).
    go_id: Mapped[str | None] = mapped_column(String(15), nullable=True)
    freq: Mapped[int] = mapped_column(Integer, nullable=False)
