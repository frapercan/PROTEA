from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    JSON,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from protea.core.explorations import (
    ARCHIVE_REASONS,
    DECISIONS,
    GRADES,
    KINDS,
    STATES,
    VERDICTS,
    check_in,
)
from protea.infrastructure.orm.base import Base

_JSONB = JSONB().with_variant(JSON(), "sqlite")


class Exploration(Base):
    """One falsifiable hypothesis, its window, its frozen rules and its review (ADR-D55).

    The state moves forward only: ``draft``, ``running``, ``reported``,
    ``decided``. Launching freezes the rules, which records their canonical
    sha256 together with the database time and who froze them, so the three are
    set together or not at all, and only a draft has none. A decided
    exploration carries the author's decision, and only a decided one does.

    Archived is a mark with a reason, not a state, and an exploration is never
    deleted: every table that points here does so with ``RESTRICT``.
    """

    __tablename__ = "exploration"
    __table_args__ = (
        UniqueConstraint("number", name="uq_exploration_number"),
        CheckConstraint(check_in("kind", KINDS), name="ck_exploration_kind"),
        CheckConstraint(check_in("state", STATES), name="ck_exploration_state"),
        CheckConstraint(
            check_in("archived_reason", ARCHIVE_REASONS, nullable=True),
            name="ck_exploration_archived_reason",
        ),
        CheckConstraint(check_in("grade", GRADES, nullable=True), name="ck_exploration_grade"),
        CheckConstraint(
            check_in("verdict", VERDICTS, nullable=True), name="ck_exploration_verdict"
        ),
        CheckConstraint(
            check_in("decision", DECISIONS, nullable=True), name="ck_exploration_decision"
        ),
        CheckConstraint(
            "(rules_sha256 IS NULL) = (rules_frozen_at IS NULL) "
            "AND (rules_frozen_at IS NULL) = (rules_frozen_by IS NULL)",
            name="ck_exploration_rules_frozen_together",
        ),
        CheckConstraint(
            "(state = 'draft') = (rules_sha256 IS NULL)",
            name="ck_exploration_only_a_draft_has_open_rules",
        ),
        CheckConstraint(
            "(state = 'decided') = (decision IS NOT NULL)",
            name="ck_exploration_only_decided_carries_a_decision",
        ),
        CheckConstraint(
            "(archived_reason IS NULL) = (archived_at IS NULL)",
            name="ck_exploration_archived_together",
        ),
        CheckConstraint(
            "launcher IS NULL OR launcher <> proposer",
            name="ck_exploration_launcher_is_not_the_proposer",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    #: The number a person says ("exploration 3"). Assigned by the server.
    number: Mapped[int] = mapped_column(Integer, nullable=False)
    title: Mapped[str] = mapped_column(String(200), nullable=False)
    question: Mapped[str] = mapped_column(Text, nullable=False)
    hypothesis: Mapped[str] = mapped_column(Text, nullable=False)
    kind: Mapped[str] = mapped_column(String(16), nullable=False)
    state: Mapped[str] = mapped_column(
        String(16), nullable=False, default="draft", server_default="draft"
    )
    archived_reason: Mapped[str | None] = mapped_column(String(16), nullable=True)
    archived_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    #: The one window the hypothesis is answered on.
    evaluation_set_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("evaluation_set.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    rules: Mapped[dict[str, Any]] = mapped_column(_JSONB, nullable=False, default=dict)
    rules_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    rules_frozen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    rules_frozen_by: Mapped[str | None] = mapped_column(String(200), nullable=True)
    #: Set when the candidate already had results in the window when the rules
    #: were frozen. Such an exploration is graded ``exploratory``.
    post_hoc: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="false"
    )
    grade: Mapped[str | None] = mapped_column(String(16), nullable=True)
    verdict: Mapped[str | None] = mapped_column(String(16), nullable=True)
    proposer: Mapped[str] = mapped_column(String(200), nullable=False)
    launcher: Mapped[str | None] = mapped_column(String(200), nullable=True)
    reviewer: Mapped[str | None] = mapped_column(String(200), nullable=True)
    decision: Mapped[str | None] = mapped_column(String(8), nullable=True)
    decision_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    decided_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    decided_by: Mapped[str | None] = mapped_column(String(200), nullable=True)
    #: The sha256 of the report the author decided on, whichever way the
    #: decision went, and the checklist version it was reviewed against.
    report_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    checklist_version: Mapped[str | None] = mapped_column(String(16), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
