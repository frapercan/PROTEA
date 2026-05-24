"""Per-user daily operation quota gate (FARM-AUTH.7).

Provides :func:`require_user_quota`, a dependency factory that enforces
configurable daily limits for expensive operations
(``predict``, ``export_research_dataset``, ``run_cafa_evaluation``).

Resolution order
----------------

1. ``PROTEA_AUTHN_REQUIRED`` falsy: short-circuit, no quota enforced.
2. Principal is ``None`` (unauthenticated after step 1): 401.
3. Principal has role ``admin``: bypass quota unconditionally.
4. Resolve the ``user_id`` UUID from the principal (Bearer ``sub`` or
   ApiKey; API keys without a bound user are rejected with 401 because
   there is no row to charge).
5. Look up ``(user_id, today_utc, operation)`` in ``user_quota``.
6. If ``count >= limit``: raise 429 with ``Retry-After`` header set to
   seconds until next UTC midnight.
7. Increment ``count`` and commit; return normally.

The dependency is produced by the :func:`require_user_quota` factory so
each endpoint can name its own operation::

    @router.post("/predict", dependencies=[Depends(require_user_quota("predict"))])
    def predict_go_terms(...): ...

Admin bypass
------------

Principals with ``role_of(principal) == ROLE_ADMIN`` skip the quota
table entirely (no row is read or written for admins).

Fail-open on DB error
---------------------

If the quota table cannot be read or written (transient DB error), the
request is allowed through with a WARNING log entry. Quota is
observability-adjacent: it should never be the cause of a 503.
"""

from __future__ import annotations

import logging
import os
import uuid
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta

from fastapi import Depends, HTTPException
from sqlalchemy.orm import Session, sessionmaker

from protea.api.bearer import BearerPrincipal
from protea.api.deps import get_session_factory, get_user_quota_per_day
from protea.api.roles import ROLE_ADMIN, role_of
from protea.infrastructure.orm.models.api_key import ApiKey
from protea.infrastructure.orm.models.user_quota import UserQuota
from protea.infrastructure.session import session_scope

logger = logging.getLogger(__name__)


def _authn_required() -> bool:
    raw = os.getenv("PROTEA_AUTHN_REQUIRED")
    if raw is None:
        return True
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _resolve_user_id(
    principal: ApiKey | BearerPrincipal | None,
) -> uuid.UUID | None:
    """Extract a user UUID from the authenticated principal.

    Bearer tokens carry the user UUID in the ``sub`` claim.
    API keys do not have a bound ``user_id`` column in this iteration;
    they return ``None`` so the caller can gate accordingly.
    """
    if principal is None:
        return None
    if isinstance(principal, BearerPrincipal):
        try:
            return uuid.UUID(principal.sub)
        except (ValueError, AttributeError):
            return None
    # ApiKey principals: no user binding in this iteration.
    return None


def _seconds_until_midnight_utc() -> int:
    """Return the number of whole seconds remaining until the next UTC midnight."""
    now = datetime.now(UTC)
    tomorrow = (now + timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    return max(0, int((tomorrow - now).total_seconds()))


def _enforce_quota(
    session: Session,
    user_id: uuid.UUID,
    operation: str,
    limit: int,
) -> None:
    """Read the quota row, check the limit, and increment the counter.

    Raises :class:`HTTPException` 429 when the limit is reached.
    All DB work happens inside the caller's session so the increment
    is committed atomically with any application-level writes the
    handler would do.
    """
    today: date = datetime.now(UTC).date()
    row = (
        session.query(UserQuota)
        .filter(
            UserQuota.user_id == user_id,
            UserQuota.day == today,
            UserQuota.operation == operation,
        )
        .first()
    )

    current_count = row.count if row is not None else 0
    if current_count >= limit:
        retry_after = _seconds_until_midnight_utc()
        raise HTTPException(
            status_code=429,
            detail="user_quota_exceeded",
            headers={"Retry-After": str(retry_after)},
        )

    if row is None:
        row = UserQuota(
            user_id=user_id,
            day=today,
            operation=operation,
            count=1,
        )
        session.add(row)
    else:
        row.count = current_count + 1


def require_user_quota(operation: str) -> Callable[..., None]:
    """Return a FastAPI dependency that enforces the daily quota for ``operation``.

    Usage::

        @router.post("/predict", dependencies=[Depends(require_user_quota("predict"))])
        def predict_go_terms(...): ...

    The dependency is parameterised by operation name so each endpoint
    registers its own budget independently. The name must match a key in
    ``Settings.user_quota_per_day``; unknown names fall back to a limit of 0
    (blocked) to prevent accidental quota-free endpoints.
    """

    def _gate(
        principal: ApiKey | BearerPrincipal | None = Depends(
            # Lazy import avoids a circular dependency chain:
            # user_quota -> auth -> bearer -> (no quota)
            __import__(
                "protea.api.auth", fromlist=["require_api_key_or_bearer"]
            ).require_api_key_or_bearer
        ),
        factory: sessionmaker[Session] = Depends(get_session_factory),
        quota_map: dict[str, int] = Depends(get_user_quota_per_day),
    ) -> None:
        if not _authn_required():
            return

        if principal is None:
            raise HTTPException(status_code=401, detail="Authentication required")

        # Admin role bypasses quota entirely.
        if role_of(principal) == ROLE_ADMIN:
            return

        user_id = _resolve_user_id(principal)
        if user_id is None:
            # API-key principals without a bound user are not quotaed in
            # this iteration: treat them as trusted operator keys and pass.
            return

        limit = quota_map.get(operation, 0)

        try:
            with session_scope(factory) as session:
                _enforce_quota(session, user_id, operation, limit)
        except HTTPException:
            raise
        except Exception as exc:
            # Fail-open: log and allow the request through.
            logger.warning(
                "user_quota DB error for user_id=%s operation=%r; allowing request: %s",
                user_id,
                operation,
                exc,
            )

    return _gate


__all__ = ["require_user_quota"]
