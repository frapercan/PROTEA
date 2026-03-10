from __future__ import annotations

import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import DateTime, Float, ForeignKey, Integer, String, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from protea.infrastructure.orm.base import Base

if TYPE_CHECKING:
    from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
    from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
    from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
    from protea.infrastructure.orm.models.embedding.go_prediction import GOPrediction
    from protea.infrastructure.orm.models.query.query_set import QuerySet


class PredictionSet(Base):
    """Groups GO predictions from a single prediction run.

    Records which ``EmbeddingConfig`` was used for similarity search and which
    ``AnnotationSet`` was used as the reference, providing complete traceability
    for every predicted GO term.
    """

    __tablename__ = "prediction_set"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    embedding_config_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("embedding_config.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    annotation_set_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("annotation_set.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    ontology_snapshot_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("ontology_snapshot.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    query_set_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("query_set.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    limit_per_entry: Mapped[int] = mapped_column(Integer, nullable=False)
    distance_threshold: Mapped[float | None] = mapped_column(Float, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    meta: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)

    embedding_config: Mapped[EmbeddingConfig] = relationship("EmbeddingConfig")
    annotation_set: Mapped[AnnotationSet] = relationship("AnnotationSet")
    ontology_snapshot: Mapped[OntologySnapshot] = relationship("OntologySnapshot")
    query_set: Mapped[QuerySet | None] = relationship("QuerySet")
    predictions: Mapped[list[GOPrediction]] = relationship(
        "GOPrediction", back_populates="prediction_set", cascade="all, delete-orphan"
    )
