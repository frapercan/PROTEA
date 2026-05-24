"""``EmailToken`` ORM model — one-time tokens for SMTP flows (FARM-AUTH.11).

Stores hashed one-time tokens issued for magic-link login and
password-reset flows. The raw token is generated server-side, emailed
to the user exactly once, and never stored. Only a SHA-256 hex digest
is persisted here so a database breach does not let an attacker replay
undelivered tokens.

Schema
------

* ``id``           UUID PK.
* ``user_id``      FK -> user(id) ON DELETE CASCADE. Deleting the user
                   automatically purges their pending tokens.
* ``token_hash``   SHA-256 hex digest of the raw random token (64 chars).
                   Indexed unique so lookup is O(1) with constant-time
                   comparison at the application layer.
* ``kind``         Text: one of ``'magic_link'`` or ``'password_reset'``.
* ``created_at``   Timestamp when the row was inserted.
* ``expires_at``   Timestamp when the token expires (15-minute TTL).
* ``consumed_at``  Timestamp when the token was used. Nullable: NULL means
                   the token has not been consumed yet. Once set the token
                   is rejected on any subsequent attempt, preventing replay.

Security notes
--------------

Tokens are generated with :func:`secrets.token_urlsafe` (32 bytes of
entropy) and hashed with SHA-256. Only the digest is stored; the raw
token travels only in the email body. Tokens expire after 15 minutes
and are single-use: the first successful consume sets ``consumed_at``
and any later attempt is rejected.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import DateTime, ForeignKey, Text
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from protea.core.utils import utcnow
from protea.infrastructure.orm.base import Base


class EmailToken(Base):
    """A hashed one-time token for magic-link or password-reset flows."""

    __tablename__ = "email_token"

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

    # SHA-256 hex digest of the raw random token. Unique so the lookup
    # path is a simple unique-index scan; constant-time comparison
    # happens in the application layer (hmac.compare_digest).
    token_hash: Mapped[str] = mapped_column(Text, nullable=False, unique=True)

    # 'magic_link' or 'password_reset'
    kind: Mapped[str] = mapped_column(Text, nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utcnow,
    )

    expires_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )

    consumed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        default=None,
    )

    # Indexes are declared in the alembic migration only (with IF NOT EXISTS
    # so re-running upgrade head against the shared CI pg never duplicates):
    #   - ix_email_token_token_hash (unique)
    #   - ix_email_token_user_id
    #   - ix_email_token_expires_at
    # Keeping them out of __table_args__ avoids the create_all collision
    # in tests that drop_all + create_all without dropping indexes first
    # (SQLAlchemy's metadata.create_all does not checkfirst on Index objects).

    def __repr__(self) -> str:
        return (
            f"<EmailToken(id={self.id}, kind={self.kind!r},"
            f" user_id={self.user_id}, consumed={self.consumed_at is not None})>"
        )


__all__ = ["EmailToken"]
