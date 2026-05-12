from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import BigInteger, DateTime, ForeignKey, Index, Integer, String, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from protea.infrastructure.orm.base import Base


class Dataset(Base):
    """A frozen re-ranker dataset published to the artifact store.

    One row per ``export_research_dataset`` (or ``dump_reranker_dataset``)
    run that completes successfully. The row is the durable handle the
    lab uses to pull an exact dump — lookups by ``name`` (human label)
    or ``id`` (UUID) both resolve here.

    ``train_uri`` / ``eval_uri`` / ``manifest_uri`` are opaque URIs in the
    backend scheme (``file://…``, ``s3://bucket/key``). The caller does
    not need to know the backend; :class:`ArtifactStore` resolves them.

    Content addressing: ``schema_sha`` fingerprints the feature set
    (must match the booster's ``feature_schema_sha`` at inference) and
    ``manifest_sha`` is the sha256 of the serialized manifest bytes —
    two independent dumps with identical provenance will collide on this
    field, making drift detectable.
    """

    __tablename__ = "dataset"
    __table_args__ = (
        # T3.5: list endpoints order by ``created_at DESC``.
        Index("ix_dataset_created_at", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)

    # Where the dump came from.
    operation: Mapped[str] = mapped_column(String(64), nullable=False)
    job_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("job.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    # Storage layout.
    storage_backend: Mapped[str] = mapped_column(String(32), nullable=False)
    key_prefix: Mapped[str] = mapped_column(String(512), nullable=False)
    train_uri: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    eval_uri: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    manifest_uri: Mapped[str] = mapped_column(String(1024), nullable=False)

    # Content fingerprints.
    schema_sha: Mapped[str] = mapped_column(String(16), nullable=False)
    manifest_sha: Mapped[str | None] = mapped_column(String(64), nullable=True)
    n_train_rows: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    n_eval_rows: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)

    # Dump parameters (also in manifest; duplicated here for indexable lookup).
    k: Mapped[int] = mapped_column(Integer, nullable=False)
    annotation_source: Mapped[str] = mapped_column(String(32), nullable=False)
    embedding_config_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("embedding_config.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    ontology_snapshot_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("ontology_snapshot.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    # List of "v{old}-v{new}" strings — one per training shard.
    train_snapshot_pairs: Mapped[list[str]] = mapped_column(
        JSONB, nullable=False, default=list
    )
    eval_snapshot_pair: Mapped[str | None] = mapped_column(String(64), nullable=True)

    # Reproducibility.
    producer_version: Mapped[str | None] = mapped_column(String(64), nullable=True)
    producer_git_sha: Mapped[str | None] = mapped_column(String(40), nullable=True)

    # Free-form extension hook (e.g. future ``features_hash``, dataset
    # tags, curator notes). Keep schema fields that actually gate
    # behaviour out of here.
    meta: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
