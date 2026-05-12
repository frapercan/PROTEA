from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, ForeignKey, Index, String, Text, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from protea.infrastructure.orm.base import Base


class RerankerModel(Base):
    """A trained LightGBM re-ranker model.

    The booster can be stored inline (``model_data``, legacy) or by
    reference (``artifact_uri``, preferred). Rows registered through
    ``scripts/register_reranker.py`` always point at the artifact store;
    older rows still serialize the booster inline.

    Provenance columns (``feature_schema_sha``, ``producer_version``,
    ``producer_git_sha``, ``spec_yaml``) let us reproduce and audit a
    model without re-running the lab. ``feature_schema_sha`` is
    load-bearing at inference time: the predict operation refuses to use
    a booster whose expected feature schema does not match the live
    pipeline (fallback to no-reranking).
    """

    __tablename__ = "reranker_model"
    __table_args__ = (
        # T3.5: ``latest reranker`` lookups order by ``created_at DESC``.
        Index("ix_reranker_model_created_at", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    prediction_set_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("prediction_set.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    evaluation_set_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("evaluation_set.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    category: Mapped[str] = mapped_column(String(10), nullable=False)
    aspect: Mapped[str | None] = mapped_column(String(3), nullable=True)

    # Legacy inline booster string. Nullable — new rows carry
    # ``artifact_uri`` and leave this NULL.
    model_data: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Artifact-store URI for the booster (``file://…`` or ``s3://…``).
    artifact_uri: Mapped[str | None] = mapped_column(String(512), nullable=True)

    # Feature-family-aware schema fingerprint (12 hex chars) from
    # ``protea_reranker_lab.contracts.compute_feature_schema_sha``.
    feature_schema_sha: Mapped[str | None] = mapped_column(String(16), nullable=True)

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

    producer_version: Mapped[str | None] = mapped_column(String(64), nullable=True)
    producer_git_sha: Mapped[str | None] = mapped_column(String(40), nullable=True)

    # External provenance — set when the booster was trained in
    # ``protea-reranker-lab`` (or any future offline trainer) rather than
    # by a PROTEA-internal operation. ``dataset_id`` points at the
    # ``Dataset`` row consumed by the lab run; ``external_source`` is a
    # free-form tag such as ``"protea-reranker-lab@<git-sha>"``.
    dataset_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("dataset.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    external_source: Mapped[str | None] = mapped_column(String(128), nullable=True)

    # Full ExperimentSpec YAML for reproducibility.
    spec_yaml: Mapped[str | None] = mapped_column(Text, nullable=True)

    metrics: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    feature_importance: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
