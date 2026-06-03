"""``AuthAudit`` ORM model — append-only audit log (FARM-AUTH.9).

Every security-relevant action in a PROTEA deployment writes one row to
this table. The table is append-only by convention: the helper in
``protea.api.auth.audit`` never issues UPDATE or DELETE against it.
Postgres row-level security or a DB-level trigger can enforce this at
the storage layer once the deployment hardens further; for now the
convention is enforced by the application layer.

Schema
------

* ``id``             UUID PK (default ``gen_random_uuid()``).
* ``occurred_at``    Timestamp with time zone; indexed for pagination.
* ``event_type``     Text NOT NULL; indexed for filter-by-action queries.
* ``actor_user_id``  FK to ``user.id``; nullable (anonymous signup rows
                     have no authenticated actor yet).
* ``target_user_id`` FK to ``user.id``; nullable (only set when the action
                     affects a specific second user, e.g. admin approval).
* ``client_ip_hash`` Text; SHA-256 of the raw client IP so the log carries
                     forensic value without storing the IP in plaintext.
* ``details``        JSONB; free-form supplementary payload (e.g. the
                     email on a signup event, the old/new role on a
                     role-change).

Event-type vocabulary (non-exhaustive; enforce in audit.py callers):
``login_ok``, ``login_fail``, ``logout``, ``signup``,
``admin_approve``, ``admin_revoke``, ``role_change``,
``password_change``, ``api_key_mint``, ``api_key_revoke``.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import DateTime, ForeignKey, Index, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from protea.core.utils import utcnow
from protea.infrastructure.orm.base import Base


class AuthAudit(Base):
    """One row per security-relevant event in this PROTEA deployment."""

    __tablename__ = "auth_audit"

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )

    occurred_at: Mapped[datetime] = mapped_column(
        # ``timezone=True`` persists TIMESTAMPTZ so the value survives a
        # pg_dump / pg_restore that changes the server TimeZone setting.
        DateTime(timezone=True),
        nullable=False,
        default=utcnow,
    )

    event_type: Mapped[str] = mapped_column(Text, nullable=False)

    actor_user_id: Mapped[UUID | None] = mapped_column(
        PG_UUID(as_uuid=True),
        # ForeignKey with ``ondelete="SET NULL"`` so that hard-deleting a
        # test user (if ever introduced) does not cascade-wipe the audit
        # log. In production User rows are deactivated, not deleted, so
        # this guard is belt-and-suspenders.
        ForeignKey("user.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    target_user_id: Mapped[UUID | None] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("user.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    client_ip_hash: Mapped[str | None] = mapped_column(Text, nullable=True)

    details: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    __table_args__ = (
        # Primary access pattern: paginated viewer scrolling backwards in
        # time; the DESC index keeps Postgres from doing a full-table sort.
        Index("ix_auth_audit_occurred_at", "occurred_at"),
        # Filter by event type (e.g. "show me all login_fail events").
        Index("ix_auth_audit_event_type", "event_type"),
    )

    def __repr__(self) -> str:
        return (
            f"<AuthAudit(id={self.id}, event_type={self.event_type!r},"
            f" actor={self.actor_user_id}, occurred_at={self.occurred_at})>"
        )


__all__ = ["AuthAudit"]
