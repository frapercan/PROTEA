"""Anonymous-user IP-hash daily quota gate (FARM-AUTH.6).

Provides :func:`require_anon_quota`, a FastAPI dependency that:

* is a no-op when a logged-in principal is already present (session cookie
  or API key / Bearer token), so authenticated users bypass the gate entirely,
* computes a sha256 hash of the client IP (honouring X-Forwarded-For),
* looks up or creates the ``(ip_hash, today_utc)`` row in ``anon_quota``,
* increments the counter and returns on success,
* raises HTTP 429 with a ``Retry-After`` header (seconds until midnight UTC)
  when the daily limit is exhausted.

Fail-open policy: if the DB row cannot be read or written (transient error),
the request is allowed through and the error is logged at WARNING level.

Limit is read from ``Settings.anon_quota_per_day`` (env override
``PROTEA_ANON_QUOTA_PER_DAY``, default 5).
"""

from __future__ import annotations

import hashlib
import logging
import os
import uuid
from datetime import UTC, date, datetime, timedelta
from typing import Any

from fastapi import Depends, HTTPException, Request
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from protea.api.deps import get_session_factory
from protea.infrastructure.orm.models.anon_quota import AnonQuota
from protea.infrastructure.session import session_scope

logger = logging.getLogger(__name__)


def _client_ip(request: Request) -> str:
    """Extract the real client IP, honouring X-Forwarded-For.

    Precedence: X-Forwarded-For first entry, then request.client.host.
    """
    xff = request.headers.get("x-forwarded-for")
    if xff:
        return xff.split(",")[0].strip()
    if request.client:
        return request.client.host
    return "unknown"


def _hash_ip(ip: str) -> str:
    """Return sha256 hex digest of the client IP (64 chars)."""
    return hashlib.sha256(ip.encode("utf-8")).hexdigest()


def _seconds_until_midnight_utc() -> int:
    """Return seconds remaining until the next UTC midnight."""
    now = datetime.now(UTC)
    tomorrow = (now + timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    return max(1, int((tomorrow - now).total_seconds()))


def _is_authenticated(request: Request) -> bool:
    """Return True when the request carries any auth credentials.

    Checks for Bearer token, ApiKey header, or X-Api-Key header so that
    authenticated users bypass the anon quota gate regardless of role.
    """
    auth = request.headers.get("authorization", "").lower()
    if auth.startswith(("bearer ", "apikey ")):
        return True
    return bool(request.headers.get("x-api-key"))


def _quota_limit() -> int:
    """Read per-day quota from env PROTEA_ANON_QUOTA_PER_DAY (default 5)."""
    raw = os.getenv("PROTEA_ANON_QUOTA_PER_DAY", "5")
    try:
        v = int(raw)
        return max(1, v)
    except ValueError:
        return 5


def _increment_quota(
    factory: sessionmaker[Session],
    ip_hash: str,
    today: date,
    limit: int,
) -> None:
    """Upsert the quota row and raise 429 if the limit is exceeded.

    Uses a SELECT then INSERT/UPDATE pattern inside a single session.
    Fail-open: if an exception occurs, the error is logged but the
    request is allowed through.
    """
    try:
        with session_scope(factory) as session:
            row: AnonQuota | None = session.execute(
                select(AnonQuota).where(
                    AnonQuota.ip_hash == ip_hash,
                    AnonQuota.day == today,
                )
            ).scalar_one_or_none()

            if row is None:
                row = AnonQuota(
                    id=uuid.uuid4(),
                    ip_hash=ip_hash,
                    day=today,
                    count=0,
                    updated_at=datetime.now(UTC),
                )
                session.add(row)
                session.flush()

            if row.count >= limit:
                retry_after = _seconds_until_midnight_utc()
                raise HTTPException(
                    status_code=429,
                    detail={
                        "error": "anon_quota_exceeded",
                        "retry_after_seconds": retry_after,
                    },
                    headers={"Retry-After": str(retry_after)},
                )

            row.count += 1
            row.updated_at = datetime.now(UTC)
    except HTTPException:
        raise
    except Exception as exc:
        logger.warning("anon_quota increment failed (fail-open): %s", exc)


def require_anon_quota(
    request: Request,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> Any:
    """FastAPI dependency: enforce IP-hash daily quota for anonymous callers.

    Authenticated callers (any auth header present) bypass this gate
    entirely. For anonymous callers, the quota row is fetched or created
    and the counter is incremented; once the limit is reached the
    dependency raises HTTP 429 with a ``Retry-After`` header.

    Fail-open: DB errors are logged at WARNING and the request proceeds.
    """
    if _is_authenticated(request):
        return None

    ip = _client_ip(request)
    ip_hash = _hash_ip(ip)
    today = datetime.now(UTC).date()
    limit = _quota_limit()

    _increment_quota(factory, ip_hash, today, limit)
    return None


__all__ = [
    "require_anon_quota",
]
