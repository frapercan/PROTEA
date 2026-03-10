from __future__ import annotations

import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Any

from pgvector.sqlalchemy import Vector
from sqlalchemy import BigInteger, DateTime, ForeignKey, Integer, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from protea.infrastructure.orm.base import Base

if TYPE_CHECKING:
    from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
    from protea.infrastructure.orm.models.sequence.sequence import Sequence


class SequenceEmbedding(Base):
    """Stores a computed embedding for a sequence under a specific EmbeddingConfig.

    One row per (sequence, config, chunk_start). When chunking is disabled
    ``chunk_index_s=0`` and ``chunk_index_e=None`` (NULL in the DB), meaning
    the embedding covers the full sequence.  When chunking is enabled, each
    chunk produces a separate row identified by its start/end residue indices.

    Proteins sharing the same amino-acid sequence share one set of embedding
    rows per config (deduplicated at the ``Sequence`` level).

    Full traceability: ``embedding_config`` records the exact model,
    transformer layers, aggregation strategy, pooling, normalisation, and
    chunking parameters used.
    """

    __tablename__ = "sequence_embedding"
    __table_args__ = (
        UniqueConstraint(
            "sequence_id",
            "embedding_config_id",
            "chunk_index_s",
            name="uq_seq_embedding_seq_config_chunk",
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
    # chunk_index_s: start residue index (0-based, inclusive).
    # 0 when chunking is disabled (single embedding per sequence).
    chunk_index_s: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    # chunk_index_e: end residue index (exclusive).
    # NULL when chunking is disabled (covers the full sequence).
    chunk_index_e: Mapped[int | None] = mapped_column(Integer, nullable=True)
    embedding: Mapped[Any] = mapped_column(Vector, nullable=False)
    embedding_dim: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    sequence: Mapped[Sequence] = relationship("Sequence")
    embedding_config: Mapped[EmbeddingConfig] = relationship("EmbeddingConfig")
