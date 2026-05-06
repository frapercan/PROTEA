from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from protea.infrastructure.orm.base import Base

if TYPE_CHECKING:
    from protea.infrastructure.orm.models.protein.protein import Protein


class Sequence(Base):
    """
    Production-grade Sequence schema.

    Key points:
    - Stores raw amino-acid sequence.
    - Deduplicated by sequence_hash (MD5).
    - A Sequence can be referenced by MANY proteins (Sequence.proteins).
    """

    __tablename__ = "sequence"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    sequence: Mapped[str] = mapped_column(Text, nullable=False)
    sequence_hash: Mapped[str] = mapped_column(String(32), unique=True, index=True, nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, server_default=func.now(), onupdate=func.now()
    )

    # Relationships (1-N)
    proteins: Mapped[list[Protein]] = relationship("Protein", back_populates="sequence")

    @staticmethod
    def compute_hash(seq: str) -> str:
        """Forward to :func:`protea_contracts.compute_sequence_hash`.

        The canonical implementation lives in ``protea-contracts.bio_utils``
        (D-MIGR-04 of master plan v3) so the FASTA parser in
        ``protea-sources`` can populate ``UniProtProteinRecord
        .sequence_hash`` without depending on PROTEA's ORM. The wrapper
        keeps every existing call site (``Sequence.compute_hash(seq)``)
        working unchanged.
        """
        from protea_contracts import compute_sequence_hash

        return compute_sequence_hash(seq)

    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)
        if self.sequence and not self.sequence_hash:
            self.sequence_hash = self.compute_hash(self.sequence)

    def __repr__(self) -> str:
        return f"<Sequence(id={self.id}, len={len(self.sequence)})>"
