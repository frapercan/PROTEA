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
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from protea.infrastructure.orm.base import Base

if TYPE_CHECKING:
    from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
    from protea.infrastructure.orm.models.sequence.sequence import Sequence


class ResidueCode(Base):
    """Where the per-residue code of a sequence under a residue vocabulary lives (ADR-D51).

    The code itself is not in the database: 300 million residues times about 144 active
    features is over 40 billion values. It lives in a compressed shard in the artifact
    store, one shard per GPU batch, and this row says which shard, where in it, and the
    shard's sha256, so a reader verifies the bytes before trusting them.

    One row per (sequence, config). A batch inserts its rows only after its shard is in
    the store, so a row never points at a shard that does not exist, and a failed batch
    leaves nothing a retry would collide with.
    """

    __tablename__ = "residue_code"
    __table_args__ = (
        UniqueConstraint(
            "sequence_id",
            "embedding_config_id",
            name="uq_residue_code_sequence_config",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    sequence_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("sequence.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    embedding_config_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("embedding_config.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    #: Key of the shard in the artifact store.
    shard_key: Mapped[str] = mapped_column(String(512), nullable=False, index=True)
    shard_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    #: Position of the sequence inside the shard.
    position: Mapped[int] = mapped_column(Integer, nullable=False)
    residue_count: Mapped[int] = mapped_column(Integer, nullable=False)
    entry_count: Mapped[int] = mapped_column(BigInteger, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    sequence: Mapped[Sequence] = relationship("Sequence")
    embedding_config: Mapped[EmbeddingConfig] = relationship("EmbeddingConfig")
