from __future__ import annotations

import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import DateTime, ForeignKey, Index, String, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from protea.infrastructure.orm.base import Base

if TYPE_CHECKING:
    from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
    from protea.infrastructure.orm.models.job import Job


class EvaluationSet(Base):
    """Result of comparing two AnnotationSets to produce CAFA-style evaluation data.

    Stores metadata and statistics about the delta between an old and a new GOA
    annotation set.  The actual ground-truth rows are computed on-demand from
    the stored annotation sets and streamed directly to the client.

    ``old_annotation_set_id`` is the reference (training) snapshot.
    ``new_annotation_set_id`` is the evaluation (ground-truth) snapshot.
    Delta proteins are those that gained at least one new experimental annotation
    between old → new.  NK/LK classification and NOT-qualifier propagation are
    applied during both generation and export.
    """

    __tablename__ = "evaluation_set"
    __table_args__ = (
        # T3.5: list endpoints order by ``created_at DESC``.
        Index("ix_evaluation_set_created_at", "created_at"),
        # The (old, new) pair semantically identifies one evaluation
        # episode; two rows for the same pair are duplicates by
        # construction. Enforced at the DB level via
        # alembic revision ``b8e3f1a7c2d9_evaluation_set_pair_unique``.
        UniqueConstraint(
            "old_annotation_set_id",
            "new_annotation_set_id",
            name="uq_evaluation_set_old_new_annotation_set",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    old_annotation_set_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("annotation_set.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    new_annotation_set_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("annotation_set.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    job_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("job.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    stats: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    groundtruth_uri: Mapped[str | None] = mapped_column(String(512), nullable=True)

    old_annotation_set: Mapped[AnnotationSet] = relationship(
        "AnnotationSet", foreign_keys=[old_annotation_set_id]
    )
    new_annotation_set: Mapped[AnnotationSet] = relationship(
        "AnnotationSet", foreign_keys=[new_annotation_set_id]
    )
    job: Mapped[Job | None] = relationship("Job")
