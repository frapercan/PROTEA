from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import BigInteger, DateTime, ForeignKey, String, Text, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from protea.infrastructure.orm.base import Base

#: The kinds of weight the registry holds (ADR-D54). The re-ranker's boosters keep
#: their own registry, ``reranker_model``.
WEIGHT_KINDS = (
    "residue_vocabulary",
    "learned_encoder",
    "world_model",
    "reader",
    "generator",
    "method_bundle",
)


class ModelWeights(Base):
    """One file of fitted weights in the artifact store, registered with its hash (ADR-D54).

    The lab's weights enter the platform as files, and an operation finds one through
    an address in its payload (``vocabulary_artifact_uri``, ``encoder_artifact_uri``).
    Before this table nothing listed them: to launch an operation an operator had to
    know a key and trust the bytes behind it. A row says which file a key holds, by its
    sha256 and size, what it is, what it was fitted on and when it arrived, so the
    instrument can list weights and launch from them.

    ``uri`` is the artifact store key the operations take. An uploaded file gets a
    content-addressed key, ``model-weights/<kind>/<name>-<sha256[:16]><extension>``, which
    the operations' resolver verifies against the bytes it fetches. A file registered by
    reference keeps the key it already had, and its hash was checked against the store
    when it was registered.
    """

    __tablename__ = "model_weights"
    __table_args__ = (UniqueConstraint("uri", name="uq_model_weights_uri"),)

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    kind: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    format: Mapped[str] = mapped_column(String(32), nullable=False)
    uri: Mapped[str] = mapped_column(String(512), nullable=False)
    sha256: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    meta: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    trained_on_annotation_set_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("annotation_set.id", ondelete="SET NULL"),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    created_by: Mapped[str | None] = mapped_column(String(200), nullable=True)
