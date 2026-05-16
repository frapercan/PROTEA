"""``ExperimentRun`` ORM (T3.8 of master plan v3.2 §24 Fase 4).

The narrative anchor that F-EXP campaigns and the F8b Experiments page
hang their per-run metadata off of. Mirrors :class:`Job`'s narrative
trio (description / findings / tags from T3.9) but for the broader
research-run scope: a single ``ExperimentRun`` typically aggregates
multiple Jobs / EvaluationResults / RerankerModels under one name.

Linkage to those sibling rows (Job FK array, EvaluationResult join,
etc.) lives in follow-up tasks T4.7-T4.9 + the F-EXP campaign work
so this slice stays a clean additive ORM bring-up.
"""
from __future__ import annotations

import enum
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import DateTime, Enum, Index, Integer, Text, text
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from protea.core.utils import utcnow
from protea.infrastructure.orm.base import Base


class ExperimentRunStatus(enum.StrEnum):
    """Lifecycle states for an ``ExperimentRun``.

    Mirrors ``JobStatus`` semantically but with ``planned`` as the
    initial state (Jobs default to QUEUED; experiments often live as
    drafts before any compute kicks off) and ``abandoned`` instead of
    ``failed`` (a research run can be stopped without a hard error).
    """

    PLANNED = "planned"
    RUNNING = "running"
    DONE = "done"
    ABANDONED = "abandoned"


class ExperimentRun(Base):
    """Per-research-run narrative + provenance anchor."""

    __tablename__ = "experiment_run"

    id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    hypothesis: Mapped[str | None] = mapped_column(Text, nullable=True)
    findings: Mapped[str | None] = mapped_column(Text, nullable=True)

    status: Mapped[ExperimentRunStatus] = mapped_column(
        # ``values_callable`` makes SQLAlchemy persist + load the
        # *member values* (``"planned"``, ``"running"``, ...) instead of
        # the default member *names* (``"PLANNED"``, ...). The Postgres
        # enum type ``experiment_run_status`` is created by the
        # ``1e4aee32e677_add_experiment_run_table`` migration with the
        # lowercase labels, so without this hint inserts raise
        # ``InvalidTextRepresentation`` and reads raise ``LookupError``
        # (the bug FARM-EXP.1's backfill script worked around with raw
        # ``sqlalchemy.text``).
        Enum(
            ExperimentRunStatus,
            name="experiment_run_status",
            values_callable=lambda e: [m.value for m in e],
        ),
        nullable=False,
        default=ExperimentRunStatus.PLANNED,
    )

    config: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    provenance: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    tags: Mapped[list[str]] = mapped_column(ARRAY(Text), nullable=False, default=list)

    # ----------------------------------------------------------------
    # FARM-EXP.1 axis columns. Together they form the axis tuple
    # (plm, k, reranker_spec_id, feature_schema_sha, eval_set_name,
    # eval_set_manifest_sha, propagation, ensemble_spec) that the
    # F-EXP-RESET re-benchmark addresses every cell by. The
    # ``axis_tuple_shortid`` is the canonical 12-hex digest
    # ``sha256(canonical_json(axis_tuple))[:12]`` shared with
    # protea-reranker-lab via :mod:`protea.core.axis_tuple` (which
    # delegates to ``protea_contracts.axis_tuple_shortid`` once the
    # contracts release ships the helper).
    plm: Mapped[str | None] = mapped_column(Text, nullable=True)
    k: Mapped[int | None] = mapped_column(Integer, nullable=True)
    reranker_spec_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    feature_schema_sha: Mapped[str | None] = mapped_column(Text, nullable=True)
    eval_set_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    eval_set_manifest_sha: Mapped[str | None] = mapped_column(Text, nullable=True)
    propagation: Mapped[str | None] = mapped_column(Text, nullable=True)
    ensemble_spec: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    axis_tuple_shortid: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index("ix_experiment_run_name", "name", unique=True),
        Index("ix_experiment_run_status_created_at", "status", "created_at"),
        # Partial-unique index: rows with NULL shortid (legacy + partially
        # backfilled) can coexist; once shortid is populated it must be
        # globally unique. Mirrors the lab cell-catalog contract.
        Index(
            "uq_experiment_run_axis_tuple_shortid",
            "axis_tuple_shortid",
            unique=True,
            postgresql_where=text("axis_tuple_shortid IS NOT NULL"),
        ),
    )


__all__ = ["ExperimentRun", "ExperimentRunStatus"]
