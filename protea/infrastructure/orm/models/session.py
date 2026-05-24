"""``UserSession`` ORM model — server-side session table (FARM-AUTH.8).

Replaces the in-memory ``_SESSION_STORE`` dict introduced by FARM-AUTH.3
with a persistent Postgres table so that:

* logout is durable across process restarts,
* admin can revoke all sessions for a user (security incident response),
* last activity is observable per-session.

Schema highlights
-----------------

* ``id``             UUID PK (server-generated).
* ``user_id``        FK to ``user.id`` — every session belongs to exactly
                     one identity.
* ``token_hash``     SHA-256 of the raw cookie value, stored as hex.
                     Indexed for O(1) lookup on every request. We hash
                     instead of storing the plaintext JWT so a DB dump
                     alone cannot replay sessions.
* ``created_at``     Timestamptz set at INSERT; never updated.
* ``expires_at``     Timestamptz; mirrors the ``exp`` claim in the JWT so
                     the app can prune old rows without decoding the token.
* ``last_seen_at``   Timestamptz; updated on every authenticated ``/me``
                     call for liveness tracking.
* ``revoked_at``     Timestamptz NULL — set on logout or admin revocation.
                     A non-NULL value means the session is dead regardless
                     of ``expires_at``.
* ``user_agent``     Text NULL — raw User-Agent header at login; stored
                     for audit without indexing.
* ``client_ip_hash`` Text NULL — SHA-256 of the client IP; privacy-safe
                     proxy for geolocation anomaly detection.

Revocation semantics
--------------------

A session is considered valid when ALL of the following hold:

1. A row with the matching ``token_hash`` exists.
2. ``revoked_at IS NULL``.
3. ``expires_at > now()``.

Conditions 2 and 3 are checked in :func:`find_session`.
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
    """A server-side session row for one authenticated browser/client."""

    __tablename__ = "user_session"

    id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)

    user_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("user.id", ondelete="CASCADE"),
        nullable=False,
    )

    # SHA-256 hex of the raw JWT cookie value.
    token_hash: Mapped[str] = mapped_column(Text, nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow
    )
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow
    )
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # Audit metadata — stored but not indexed.
    user_agent: Mapped[str | None] = mapped_column(Text, nullable=True)
    # SHA-256 hex of the client IP address (privacy-safe).
    client_ip_hash: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        # Primary lookup: one row per token on every authenticated request.
        Index("ix_user_session_token_hash", "token_hash", unique=True),
        # Secondary: revoke-all-for-user admin operation.
        Index("ix_user_session_user_id", "user_id"),
    )

    def __repr__(self) -> str:
        return (
            f"<UserSession(id={self.id}, user_id={self.user_id}, "
            f"revoked_at={self.revoked_at})>"
        )


__all__ = ["UserSession"]
