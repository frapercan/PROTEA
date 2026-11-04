from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    DDL,
    JSON,
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    String,
    event,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from protea.core.explorations import ACTOR_TYPES, EVENT_TYPES, check_in
from protea.infrastructure.orm.base import Base

_JSONB = JSONB().with_variant(JSON(), "sqlite")

#: The function and trigger that make the log append-only, as the migration
#: creates them. Kept here as well so ``Base.metadata.create_all``, which the
#: postgres tests build their schema with, makes the same table the migration
#: does. ``tests/test_exploration_tables.py`` checks the two texts agree.
APPEND_ONLY_FUNCTION = """
CREATE OR REPLACE FUNCTION exploration_event_is_append_only() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'exploration_event is append only: % refused', TG_OP;
END;
$$
"""

APPEND_ONLY_TRIGGER = """
CREATE TRIGGER exploration_event_append_only
    BEFORE UPDATE OR DELETE ON exploration_event
    FOR EACH ROW EXECUTE FUNCTION exploration_event_is_append_only()
"""


class ExplorationEvent(Base):
    """One entry in an exploration's log (ADR-D55). Written once, never changed.

    A trigger refuses ``UPDATE`` and ``DELETE``, so a correction is a new event
    rather than an edit, and the log of an accepted exploration is the log it
    was accepted with. ``at`` is the database's time, not the caller's.

    ``actor_type`` separates the author's own session from an API key that
    carries the same role: the decision is refused to a key whatever its role.
    """

    __tablename__ = "exploration_event"
    __table_args__ = (
        CheckConstraint(check_in("actor_type", ACTOR_TYPES), name="ck_exploration_event_actor"),
        CheckConstraint(check_in("event", EVENT_TYPES), name="ck_exploration_event_type"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    exploration_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("exploration.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    actor: Mapped[str] = mapped_column(String(200), nullable=False)
    actor_type: Mapped[str] = mapped_column(String(8), nullable=False)
    event: Mapped[str] = mapped_column(String(24), nullable=False)
    #: The link an objection, a reply or a deletion is about, when it is about one.
    link_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("exploration_link.id", ondelete="RESTRICT"),
        nullable=True,
    )
    body: Mapped[dict[str, Any]] = mapped_column(_JSONB, nullable=False, default=dict)


event.listen(
    ExplorationEvent.__table__,
    "after_create",
    DDL(APPEND_ONLY_FUNCTION).execute_if(dialect="postgresql"),
)
event.listen(
    ExplorationEvent.__table__,
    "after_create",
    DDL(APPEND_ONLY_TRIGGER).execute_if(dialect="postgresql"),
)
