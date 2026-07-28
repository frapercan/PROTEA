from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, String, Text, false, func, text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, Session, mapped_column

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
        # T1.6 (ADR D10): parallel ``schema_sha_v2`` for booster-side
        # schema fingerprint. Inference compares the live family-aware
        # SHA against this column when present; legacy rows fall back
        # to ``feature_schema_sha``.
        Index("ix_reranker_model_schema_sha_v2", "schema_sha_v2"),
        # Serve selection: at most one ACTIVE booster per (category, aspect)
        # slot. Partial unique index over the active rows only so the table
        # can hold any number of inactive history rows. NULL aspect is
        # coalesced to '' so a single (category, NULL-aspect) active row is
        # also enforced (Postgres treats NULLs as distinct in plain unique
        # indexes; the COALESCE closes that gap).
        Index(
            "uq_reranker_model_active_slot",
            "category",
            text("COALESCE(aspect, '')"),
            unique=True,
            postgresql_where=text("is_active"),
        ),
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
    # T1.6 (ADR D10): parallel column carrying the canonical
    # ``protea_contracts.compute_schema_sha`` digest computed against the
    # booster's feature set. Nullable until backfill completes; the
    # backfill script derives it from the dataset's ``schema_sha_v2``
    # for rows linked to a ``Dataset``, or from
    # ``protea_contracts.compute_schema_sha`` applied to the legacy
    # column when the booster is detached.
    schema_sha_v2: Mapped[str | None] = mapped_column(Text, nullable=True)

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

    # Operator-controlled serve selection flag. When True this booster is the
    # one the live serving path picks for its (category, aspect) slot. Default
    # False: until an operator marks a row active, serve selection falls back
    # to the legacy latest-by-created_at pick, so the column is behaviour-
    # preserving on a fresh migration (every existing row is inactive).
    is_active: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=false(), default=False
    )


def active_or_latest_reranker(
    session: Session,
    *,
    category: str | None = None,
    aspect: str | None = None,
) -> RerankerModel | None:
    """Select the serve booster: the ACTIVE one first, else the latest.

    The serving path used to pick the single latest-by-``created_at`` booster,
    which silently drifts whenever any new model is registered. This helper
    makes the choice explicit and operator-controlled:

    * If a row matching the optional ``category`` / ``aspect`` filter has
      ``is_active=True``, return it (the most recent active one if, against the
      partial unique index, more than one ever co-exist).
    * Otherwise FALL BACK to the latest-by-``created_at`` row in the same slot.

    The fallback is what makes this drop-in safe: until an operator marks a
    model active, behaviour is identical to the previous latest-only pick.
    Returns ``None`` when no matching ``RerankerModel`` rows exist.
    """
    base = session.query(RerankerModel)
    if category is not None:
        base = base.filter(RerankerModel.category == category)
    if aspect is not None:
        base = base.filter(RerankerModel.aspect == aspect)
    active = (
        base.filter(RerankerModel.is_active.is_(True))
        .order_by(RerankerModel.created_at.desc())
        .first()
    )
    if active is not None:
        return active
    return base.order_by(RerankerModel.created_at.desc()).first()
