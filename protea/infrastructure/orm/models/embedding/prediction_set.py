from __future__ import annotations

import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from protea.infrastructure.orm.base import Base

if TYPE_CHECKING:
    from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
    from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
    from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
    from protea.infrastructure.orm.models.embedding.go_prediction import GOPrediction
    from protea.infrastructure.orm.models.method.method import Method
    from protea.infrastructure.orm.models.query.query_set import QuerySet


class PredictionSet(Base):
    """Groups GO predictions from a single prediction run.

    Records which ``EmbeddingConfig`` was used for similarity search and which
    ``AnnotationSet`` was used as the reference, providing complete traceability
    for every predicted GO term.

    A set is produced by an embedding configuration, by a method (ADR-D60,
    decision 3), or by both: ``embedding_config_id`` is NULL for a method that
    is not a vote over one embedding, and ``method_id`` is NULL for every set
    written before methods had rows of their own. A set with neither names no
    producer and is refused by ``ck_prediction_set_has_a_producer``.
    """

    __tablename__ = "prediction_set"
    __table_args__ = (
        # T3.5: list endpoints order by ``created_at DESC``; Postgres
        # walks an ASC b-tree backwards just as efficiently.
        Index("ix_prediction_set_created_at", "created_at"),
        CheckConstraint(
            "embedding_config_id IS NOT NULL OR method_id IS NOT NULL",
            name="ck_prediction_set_has_a_producer",
        ),
        # A method's set has no retrieval depth; a configuration's keeps one.
        CheckConstraint(
            "embedding_config_id IS NULL OR limit_per_entry IS NOT NULL",
            name="ck_prediction_set_a_configuration_has_a_limit",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    embedding_config_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("embedding_config.id", ondelete="RESTRICT"),
        nullable=True,
        index=True,
    )
    #: The method that produced the set, when it has a row of its own.
    method_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("method.id", ondelete="RESTRICT", name="fk_prediction_set_method_id"),
        nullable=True,
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
    limit_per_entry: Mapped[int | None] = mapped_column(Integer, nullable=True)
    distance_threshold: Mapped[float | None] = mapped_column(Float, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    meta: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    #: What the row IS, in a sentence its writer derives (ADR-D58's long
    #: descriptor); NULL on rows written before the column, which read as
    #: "nothing says", never as a default.
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    #: The job that created the set, and the code it ran; NULL on rows written
    #: before the columns. The code is ``producer_git_sha`` and not
    #: ``code_revision``: ``meta["code_revision"]`` already names the revision that
    #: identifies the method (``protea.core.method_seal``), and one name for two
    #: fields would make the seal's flat partition ambiguous.
    job_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("job.id", ondelete="SET NULL", name="fk_prediction_set_job"),
        nullable=True,
    )
    producer_git_sha: Mapped[str | None] = mapped_column(String(64), nullable=True)

    embedding_config: Mapped[EmbeddingConfig | None] = relationship("EmbeddingConfig")
    method: Mapped[Method | None] = relationship("Method")
    annotation_set: Mapped[AnnotationSet] = relationship("AnnotationSet")
    ontology_snapshot: Mapped[OntologySnapshot] = relationship("OntologySnapshot")
    query_set: Mapped[QuerySet | None] = relationship("QuerySet")
    predictions: Mapped[list[GOPrediction]] = relationship(
        "GOPrediction", back_populates="prediction_set", cascade="all, delete-orphan"
    )
