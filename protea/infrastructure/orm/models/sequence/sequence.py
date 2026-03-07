from __future__ import annotations

import hashlib
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

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    # Relationships (1-N)
    proteins: Mapped[list[Protein]] = relationship("Protein", back_populates="sequence")

    @staticmethod
    def compute_hash(seq: str) -> str:
        return hashlib.md5(seq.encode("utf-8")).hexdigest()

    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)
        if self.sequence and not self.sequence_hash:
            self.sequence_hash = self.compute_hash(self.sequence)

    def __repr__(self) -> str:
        return f"<Sequence(id={self.id}, len={len(self.sequence)})>"
