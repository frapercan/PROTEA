from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    String,
    func,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from protea.core.explorations import LINK_ROLES, LINK_TARGETS, check_in
from protea.infrastructure.orm.base import Base


def _exactly_one(columns: tuple[str, ...]) -> str:
    """CHECK that exactly one of ``columns`` is set: the exclusive arc."""
    terms = " + ".join(f"(CASE WHEN {c} IS NOT NULL THEN 1 ELSE 0 END)" for c in columns)
    return f"{terms} = 1"


def _target(table: str) -> Mapped[uuid.UUID | None]:
    """One arm of the arc: a nullable foreign key that refuses to orphan the link."""
    return mapped_column(
        UUID(as_uuid=True), ForeignKey(f"{table}.id", ondelete="RESTRICT"), nullable=True
    )


class ExplorationLink(Base):
    """One object an exploration uses or produces, as an exclusive arc (ADR-D55).

    Exactly one target column is set, and every one is a foreign key with
    ``RESTRICT``. That is the point of the table as much as the arc is: today a
    window or a prediction set can be deleted and take its evaluation results
    with it by cascade, so identity is lost without anyone deciding to lose it.
    A target an exploration links cannot be deleted while the link exists.

    A store object is the one target that is not a row; it is linked by its key
    together with its sha256, so a key whose bytes changed is a different
    object. A temporary target cannot be linked without an expiry date, which is
    what retention collects on.
    """

    __tablename__ = "exploration_link"
    __table_args__ = (
        CheckConstraint(check_in("role", LINK_ROLES), name="ck_exploration_link_role"),
        CheckConstraint(_exactly_one(LINK_TARGETS), name="ck_exploration_link_one_target"),
        CheckConstraint(
            "(store_key IS NULL) = (store_sha256 IS NULL)",
            name="ck_exploration_link_store_key_with_its_sha",
        ),
        CheckConstraint(
            "role <> 'temporary' OR expires_at IS NOT NULL",
            name="ck_exploration_link_temporary_expires",
        ),
        CheckConstraint(
            "target_exploration_id IS NULL OR target_exploration_id <> exploration_id",
            name="ck_exploration_link_not_to_itself",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    exploration_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("exploration.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    role: Mapped[str] = mapped_column(String(16), nullable=False)
    #: The arm's name, for a link that belongs to one arm.
    arm: Mapped[str | None] = mapped_column(String(64), nullable=True)
    #: Whether the exploration created the target and answers for its retention.
    owned: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="false"
    )
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    size_bytes: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    job_id: Mapped[uuid.UUID | None] = _target("job")
    prediction_set_id: Mapped[uuid.UUID | None] = _target("prediction_set")
    evaluation_result_id: Mapped[uuid.UUID | None] = _target("evaluation_result")
    model_weights_id: Mapped[uuid.UUID | None] = _target("model_weights")
    embedding_config_id: Mapped[uuid.UUID | None] = _target("embedding_config")
    query_set_id: Mapped[uuid.UUID | None] = _target("query_set")
    annotation_set_id: Mapped[uuid.UUID | None] = _target("annotation_set")
    information_accretion_set_id: Mapped[uuid.UUID | None] = _target("information_accretion_set")
    external_reference_id: Mapped[uuid.UUID | None] = _target("external_reference")
    target_exploration_id: Mapped[uuid.UUID | None] = _target("exploration")
    store_key: Mapped[str | None] = mapped_column(String(512), nullable=True)
    store_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
