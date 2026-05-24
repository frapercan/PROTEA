"""``UserSession`` ORM model: persistent server-side session store (FARM-AUTH.8).

Replaces the in-process ``_SESSION_STORE`` dict that AUTH.3 introduced
with a durable ``user_session`` table so that:

* Logout actually invalidates the session across all server replicas.
* An admin can revoke every active session for a given user account.
* Sessions survive a process restart (no data loss on redeploy).

The name ``UserSession`` (not ``Session``) avoids shadowing
``sqlalchemy.orm.Session`` in import namespaces, which is a common
footgun when naming ORM models after SQLAlchemy internals.

Schema
------

* ``id``            UUID PK (``gen_random_uuid()``).
* ``user_id``       FK to ``user.id`` (CASCADE DELETE so test teardown
                    works; production users are never hard-deleted).
* ``token_hash``    SHA-256 hex digest of the raw JWT stored in the
                    cookie; UNIQUE so duplicate mints are rejected.
* ``created_at``    Timestamp with timezone; set once at insert.
* ``expires_at``    Timestamp with timezone; mirrors the ``exp`` JWT
                    claim so a DB query can find expired rows.
* ``last_seen_at``  Timestamp with timezone; updated on every ``/me``
                    hit so admins can see stale vs active sessions.
* ``revoked_at``    Nullable timestamp; non-null means the session has
                    been explicitly revoked (logout or admin revoke).
* ``user_agent``    Raw User-Agent header value; stored for forensic
                    audit purposes (which browser/service minted this
                    session).
* ``client_ip_hash`` SHA-256 hex digest of the raw client IP so the
                     table carries forensic value without storing PII.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import DateTime, ForeignKey, Index, Text
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from protea.core.utils import utcnow
from protea.infrastructure.orm.base import Base


class UserSession(Base):
    """One row per active or revoked browser/API session."""

    __tablename__ = "user_session"

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )

    user_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("user.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # SHA-256 hex of the raw JWT value placed in the cookie.
    # We never store the raw token so a DB dump does not expose live secrets.
    token_hash: Mapped[str] = mapped_column(Text, nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utcnow,
    )

    expires_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )

    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utcnow,
    )

    # Non-null once the session has been invalidated via logout or admin revoke.
    revoked_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    # Forensic fields: which device/IP created this session.
    user_agent: Mapped[str | None] = mapped_column(Text, nullable=True)
    client_ip_hash: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        # Unique token_hash: prevents duplicate rows for the same JWT and
        # lets find_session() use an indexed equality lookup.
        Index("uq_user_session_token_hash", "token_hash", unique=True),
        # Fast revocation sweep: all rows for a user (used by admin revoke).
        Index("ix_user_session_user_id", "user_id"),
        # TTL cleanup queries (optional background job can prune expired rows).
        Index("ix_user_session_expires_at", "expires_at"),
    )

    def __repr__(self) -> str:
        return (
            f"<UserSession(id={self.id}, user_id={self.user_id},"
            f" revoked_at={self.revoked_at})>"
        )


__all__ = ["UserSession"]
