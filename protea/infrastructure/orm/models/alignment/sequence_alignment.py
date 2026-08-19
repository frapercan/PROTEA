from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Float, String, func
from sqlalchemy.orm import Mapped, mapped_column

from protea.infrastructure.orm.base import Base


class SequenceAlignment(Base):
    """A parasail NW+SW alignment, keyed by the two sequences it aligned.

    Keyed by ``sequence_hash`` rather than by accession on purpose. An
    alignment is a function of the two sequences and nothing else: not the
    embedding model, not K, not the temporal window, not the donor policy. An
    accession, by contrast, can point at a different sequence after a UniProt
    release, and a cache keyed on it would then serve an alignment of a
    sequence that no longer exists under that name.

    Measured on the rung-1 grid, this is worth caching: within one model the
    K=3 pairs are a strict subset of the K=30 pairs (1,239 of 1,239), and
    ACROSS models 1,063 of 1,216 pairs recurred (87%). Alignments are 63% of
    a batch's time, so most of that work is currently repeated.
    """

    __tablename__ = "sequence_alignment"

    # Both are Sequence.sequence_hash (MD5, 32 chars). Not foreign keys: the
    # cache must survive a sequence row being pruned, and an orphan entry is
    # harmless because it is only ever looked up by a hash someone still holds.
    query_hash: Mapped[str] = mapped_column(String(32), primary_key=True)
    ref_hash: Mapped[str] = mapped_column(String(32), primary_key=True)

    identity_nw: Mapped[float] = mapped_column(Float, nullable=False)
    similarity_nw: Mapped[float] = mapped_column(Float, nullable=False)
    alignment_score_nw: Mapped[float] = mapped_column(Float, nullable=False)
    gaps_pct_nw: Mapped[float] = mapped_column(Float, nullable=False)
    alignment_length_nw: Mapped[float] = mapped_column(Float, nullable=False)
    identity_sw: Mapped[float] = mapped_column(Float, nullable=False)
    similarity_sw: Mapped[float] = mapped_column(Float, nullable=False)
    alignment_score_sw: Mapped[float] = mapped_column(Float, nullable=False)
    gaps_pct_sw: Mapped[float] = mapped_column(Float, nullable=False)
    alignment_length_sw: Mapped[float] = mapped_column(Float, nullable=False)
    length_query: Mapped[float] = mapped_column(Float, nullable=False)
    length_ref: Mapped[float] = mapped_column(Float, nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, server_default=func.now()
    )
