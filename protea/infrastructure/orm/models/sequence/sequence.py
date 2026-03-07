# protein_information_system/sql/model/entities/sequence/sequence.py
from __future__ import annotations

import hashlib

from sqlalchemy import Column, DateTime, Integer, String, Text, func
from sqlalchemy.orm import relationship

from protea.infrastructure.orm.base import Base


class Sequence(Base):
    """
    Production-grade Sequence schema.

    Key points:
    - Stores raw amino-acid sequence.
    - Deduplicated by sequence_hash (MD5).
    - A Sequence can be referenced by MANY proteins (Sequence.proteins).
    """

    __tablename__ = "sequence"

    id = Column(Integer, primary_key=True, autoincrement=True)

    sequence = Column(Text, nullable=False)

    # MD5 hex digest (32 chars)
    sequence_hash = Column(String(32), unique=True, index=True, nullable=False)

    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    # Relationships (1-N)
    proteins = relationship("Protein", back_populates="sequence")

    @staticmethod
    def compute_hash(seq: str) -> str:
        return hashlib.md5(seq.encode("utf-8")).hexdigest()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.sequence and not self.sequence_hash:
            self.sequence_hash = self.compute_hash(self.sequence)

    def __repr__(self) -> str:
        return f"<Sequence(id={self.id}, len={len(self.sequence)})>"
