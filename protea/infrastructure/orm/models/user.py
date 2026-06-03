"""``User`` ORM model — identity foundation for FARM-AUTH (ADR D37).

The ``user`` table is the per-deployment identity store that every
subsequent F-AUTH slice (bootstrap admin, login/signup, role gating,
session revocation, audit log) depends on. Each PROTEA deployment is
sovereign with its own User table: there is no cross-instance
identity plane.

Schema highlights
-----------------

* ``id`` is a UUID, not a serial, so user identifiers can be exchanged
  in URLs without leaking row counts and so the same value survives a
  ``pg_dump | pg_restore`` round-trip into a fresh database.
* ``email`` and ``username`` are independently unique. Email is the
  primary login handle; username is the display handle that appears
  on shared resources (job listings, audit log entries).
* ``password_hash`` stores the Argon2id PHC string produced by
  :mod:`protea.api.auth.passwords`. We deliberately do not call this
  column ``password`` so a careless ``log.info(user.password)`` cannot
  accidentally leak the column's purpose.
* ``role`` and ``status`` are Postgres enums with the same naming
  convention used by :class:`ExperimentRunStatus` — lowercase string
  values, with ``values_callable`` on the SQLAlchemy side so Python
  member *names* are not what hits the DB.
* ``intended_use`` is a free-text field captured at signup so an
  admin reviewing a pending account knows what the requester wants to
  do with their PROTEA quota.
* ``last_login_at`` and ``deactivated_at`` are observability +
  lifecycle hooks; login flows in FARM-AUTH.3 will write to them.

Role ordering
-------------

The four role values form a strict linear order:
``guest < researcher < operator < admin``. The ordering is enforced by
the ``require_role`` FastAPI dependency that lands in FARM-AUTH.4;
the database itself only stores the label as an enum value. We do not
embed the ordinal here because Postgres enums already guarantee a
fixed declaration order, and duplicating the ranking in two places
invites drift.

Status semantics
----------------

* ``pending`` — signup completed, awaiting admin approval. Cannot log in.
* ``active`` — approved; can authenticate and exercise role privileges.
* ``deactivated`` — admin-disabled. Cannot log in; rows are kept for
  audit-log foreign-key integrity rather than hard-deleted.
"""

from __future__ import annotations

import enum
from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import DateTime, Enum, Index, Text
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from protea.core.utils import utcnow
from protea.infrastructure.orm.base import Base


class UserRole(enum.StrEnum):
    """Role hierarchy. Stored as a Postgres enum; ranked elsewhere."""

    GUEST = "guest"
    RESEARCHER = "researcher"
    OPERATOR = "operator"
    ADMIN = "admin"


class UserStatus(enum.StrEnum):
    """Account lifecycle state."""

    PENDING = "pending"
    ACTIVE = "active"
    DEACTIVATED = "deactivated"


class User(Base):
    """A registered identity inside a single PROTEA deployment."""

    __tablename__ = "user"

    id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)

    email: Mapped[str] = mapped_column(Text, nullable=False)
    username: Mapped[str] = mapped_column(Text, nullable=False)
    display_name: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Argon2id PHC string from :func:`protea.api.auth.passwords.hash_password`.
    password_hash: Mapped[str] = mapped_column(Text, nullable=False)

    role: Mapped[UserRole] = mapped_column(
        # ``values_callable`` forces SQLAlchemy to persist the member
        # *value* (lowercase) instead of the member *name* (uppercase)
        # which is what bit the project on ``ExperimentRunStatus`` and
        # is documented under the "ExperimentRun enum bug" memory.
        Enum(
            UserRole,
            name="user_role",
            values_callable=lambda e: [m.value for m in e],
        ),
        nullable=False,
        default=UserRole.RESEARCHER,
    )
    status: Mapped[UserStatus] = mapped_column(
        Enum(
            UserStatus,
            name="user_status",
            values_callable=lambda e: [m.value for m in e],
        ),
        nullable=False,
        default=UserStatus.PENDING,
    )

    intended_use: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow
    )
    last_login_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    deactivated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index("uq_user_email", "email", unique=True),
        Index("uq_user_username", "username", unique=True),
        # FARM-AUTH.5 endpoint gating sweep will list pending users on
        # an admin dashboard; the partial index keeps that query cheap
        # without indexing every active row.
        Index(
            "ix_user_status_created_at",
            "status",
            "created_at",
        ),
    )

    def __repr__(self) -> str:
        return f"<User(id={self.id}, username={self.username!r}, role={self.role.value}, status={self.status.value})>"


__all__ = ["User", "UserRole", "UserStatus"]
