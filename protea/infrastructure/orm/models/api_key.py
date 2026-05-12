"""``ApiKey`` ORM model — first iteration of authn (T5.6a).

Stores hashed API keys for HTTP authentication of the most sensitive
PROTEA endpoints (``POST /jobs``, ``POST /datasets``, the reranker
import paths). The raw key is shown to the caller exactly once at
creation time; subsequent reads expose only the human label and the
8-character ``prefix`` (first chars of the raw key) so an operator can
identify a key in listings without ever giving the actual secret back.

Storage rules (security-load-bearing)
-------------------------------------

* ``key_hash``: sha256 hex digest of the raw key. Lookup is by
  ``prefix`` (indexed) followed by a constant-time hash comparison.
  The plaintext key never touches the database or any log line.
* ``revoked_at`` is a tombstone column: once set, the key is rejected
  by ``require_api_key`` even if the hash still matches. Revocation is
  irreversible (no resurrection endpoint) to avoid confusion.
* ``last_used_at`` is best-effort. Updates happen in a background task
  so they never block the originating request, and a missed update
  (e.g. process crash between auth and ``BackgroundTasks.run``) is
  acceptable: it is observability data, not a security gate.
"""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, Index, String, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from protea.infrastructure.orm.base import Base


class ApiKey(Base):
    """A hashed API key used to authenticate sensitive HTTP requests.

    The raw key is generated server-side, returned exactly once by
    ``POST /auth/api-keys``, and never stored. Persisted columns:

    * ``id`` — UUID primary key.
    * ``key_hash`` — sha256 hex digest of the raw key (64 chars).
    * ``prefix`` — first 8 chars of the raw key, used as a non-unique
      lookup index and as the display handle in listings.
    * ``name`` — human-readable label supplied by the caller.
    * ``created_at`` / ``revoked_at`` / ``last_used_at`` — lifecycle
      timestamps. ``revoked_at`` is a tombstone; ``last_used_at`` is
      best-effort observability.
    """

    __tablename__ = "api_key"
    __table_args__ = (
        # T3.5: ``GET /auth/api-keys`` orders by ``created_at DESC``.
        Index("ix_api_key_created_at", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    key_hash: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    prefix: Mapped[str] = mapped_column(String(8), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    revoked_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_used_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    def __repr__(self) -> str:
        state = "revoked" if self.revoked_at else "active"
        return f"<ApiKey({self.prefix}…, name={self.name!r}, {state})>"
