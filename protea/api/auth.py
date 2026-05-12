"""API key authentication primitives (T5.6a — first iteration).

This module owns:

* the constant-time helper functions that hash and verify a raw API key,
* the FastAPI dependency :func:`require_api_key`,
* the small set of env knobs that gate the dependency in dev.

Header format
-------------

Two equivalent header shapes are accepted (mirroring the conventions
used by most public APIs):

* ``Authorization: ApiKey <key>``
* ``X-Api-Key: <key>``

Both are checked; whichever arrives first wins. The dependency returns
the matched :class:`ApiKey` row so downstream handlers can audit the
caller (currently unused, but the hook is in place).

Hashing
-------

Keys are stored as sha256 hex digests. sha256 is fine here because the
raw key has 32 bytes of entropy already (192 bits in base64, well above
what an offline brute-force can hope to crack). We use
:func:`hmac.compare_digest` for the verification step to avoid timing
side-channels on the hash comparison.

Env knobs
---------

* ``PROTEA_AUTHN_REQUIRED`` (default ``true``) — when false, the
  dependency short-circuits and waves every request through. Useful for
  local development; production deployments must leave it set.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import os
import secrets
from datetime import UTC, datetime
from typing import Any

from fastapi import BackgroundTasks, Depends, Header, Request
from sqlalchemy.orm import Session, sessionmaker

from protea.api.deps import get_session_factory
from protea.infrastructure.orm.models.api_key import ApiKey
from protea.infrastructure.session import session_scope

logger = logging.getLogger(__name__)

# Header names.
_AUTHN_SCHEME = "ApiKey"  # ``Authorization: ApiKey <key>``

# Number of leading characters of the raw key used as the lookup
# ``prefix`` column. Matches the column width on ``ApiKey.prefix``.
PREFIX_LEN = 8

# Raw key is 32 bytes of randomness rendered as 43 url-safe chars.
# This gives 192 bits of entropy — plenty of head-room above the
# attacker-resource ceiling, and short enough to type into a CI secret
# field without truncation.
_RAW_KEY_BYTES = 32


def _authn_required() -> bool:
    """Read the ``PROTEA_AUTHN_REQUIRED`` env knob.

    Default-on so production stays safe by accident: an operator
    explicitly opts out for local dev with ``PROTEA_AUTHN_REQUIRED=false``.
    Truthy values: ``1``, ``true``, ``yes``, ``on`` (case-insensitive).
    """
    raw = os.getenv("PROTEA_AUTHN_REQUIRED")
    if raw is None:
        return True
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def generate_raw_key() -> str:
    """Generate a fresh random API key (43 url-safe chars, 192 bits).

    The returned string is the value handed to the caller exactly once.
    Hash + prefix are derived from it via :func:`hash_key` /
    :func:`prefix_of`.
    """
    return secrets.token_urlsafe(_RAW_KEY_BYTES)


def hash_key(raw: str) -> str:
    """Return the sha256 hex digest used for the ``key_hash`` column.

    Wrapper exists so the algorithm can be swapped later (Argon2id, for
    instance, if we ever move to short user-chosen secrets) without
    grepping the codebase.
    """
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def prefix_of(raw: str) -> str:
    """Return the first :data:`PREFIX_LEN` characters of ``raw``.

    Used as the display handle in API responses and as the indexed
    lookup column on the ``api_key`` table.
    """
    return raw[:PREFIX_LEN]


def _extract_raw_key(authorization: str | None, x_api_key: str | None) -> str | None:
    """Return the raw API key from either header, or ``None``.

    Header precedence: ``Authorization: ApiKey <key>`` wins over
    ``X-Api-Key: <key>``. The bearer-token scheme (``Bearer <jwt>``) is
    not handled here; it lands in T5.6b. Anything else on
    ``Authorization`` is ignored so that other middlewares can layer
    their own schemes without colliding with ours.
    """
    if authorization:
        token = authorization.strip()
        if token.lower().startswith(f"{_AUTHN_SCHEME.lower()} "):
            return token[len(_AUTHN_SCHEME) + 1 :].strip() or None
    if x_api_key:
        x = x_api_key.strip()
        if x:
            return x
    return None


def _raise_unauthorized(request: Request, detail: str) -> None:
    """Raise a 401 ``HTTPException`` with the ApiKey challenge header.

    The body is converted to RFC 7807 ``application/problem+json`` by
    the global handler in :mod:`protea.api.problem_details`; here we
    only need to attach the ``WWW-Authenticate`` header so generic
    HTTP clients know which scheme to retry under. ``request`` is
    kept on the signature for symmetry with future variants that may
    want to embed the path into the problem body directly.
    """
    del request  # currently unused; handler adds the path itself
    from fastapi import HTTPException

    raise HTTPException(
        status_code=401,
        detail=detail,
        headers={"WWW-Authenticate": _AUTHN_SCHEME},
    )


def _stamp_last_used(factory: sessionmaker[Session], key_id: Any) -> None:
    """Update ``last_used_at`` in a fire-and-forget background task.

    Errors are swallowed (logged at WARNING) so a transient DB hiccup
    never breaks the originating request. ``last_used_at`` is
    observability data: a missed update is acceptable.
    """
    try:
        with session_scope(factory) as session:
            row = session.get(ApiKey, key_id)
            if row is not None:
                row.last_used_at = datetime.now(UTC)
    except Exception as exc:  # pragma: no cover — pure observability path
        logger.warning("api_key last_used_at update failed: %s", exc)


def _lookup_and_validate(
    factory: sessionmaker[Session], raw: str
) -> tuple[ApiKey | None, str | None]:
    """Resolve a raw key to a detached :class:`ApiKey` snapshot.

    Returns ``(snapshot, error_detail)``. ``snapshot`` is ``None`` on
    failure with ``error_detail`` describing the reason; the caller
    converts that into the 401 response. Keeping the DB work out of
    :func:`require_api_key` keeps the dependency under the 60-LOC
    smell budget and isolates the session lifecycle.
    """
    prefix = prefix_of(raw)
    candidate_hash = hash_key(raw)

    with session_scope(factory) as session:
        rows = session.query(ApiKey).filter(ApiKey.prefix == prefix).all()
        matched = next(
            (r for r in rows if hmac.compare_digest(r.key_hash, candidate_hash)),
            None,
        )
        if matched is None:
            return None, "Invalid API key"
        if matched.revoked_at is not None:
            return None, "API key has been revoked"
        snapshot = ApiKey(
            id=matched.id,
            prefix=matched.prefix,
            name=matched.name,
            key_hash=matched.key_hash,
            created_at=matched.created_at,
            revoked_at=matched.revoked_at,
            last_used_at=matched.last_used_at,
        )
        return snapshot, None


def require_api_key(
    request: Request,
    background_tasks: BackgroundTasks,
    factory: sessionmaker[Session] = Depends(get_session_factory),
    authorization: str | None = Header(
        default=None,
        description=(
            "API-key challenge in the ``Authorization: ApiKey <key>`` "
            "form (T5.6a). Mutually exclusive with ``X-Api-Key``; "
            "whichever arrives first wins."
        ),
    ),
    x_api_key: str | None = Header(
        default=None,
        alias="X-Api-Key",
        description=(
            "Alternative API-key carrier (``X-Api-Key: <key>``). "
            "Equivalent to the ``Authorization: ApiKey`` header form."
        ),
    ),
) -> ApiKey | None:
    """FastAPI dependency that validates an API key on the request.

    Behaviour:

    1. If ``PROTEA_AUTHN_REQUIRED`` is falsy, return ``None`` — gate
       disabled (dev stack only).
    2. Read the raw key from ``Authorization: ApiKey <key>`` or
       ``X-Api-Key: <key>``. Missing → 401.
    3. Look up by the 8-char ``prefix`` (indexed) and compare hashes
       in constant time. Mismatch or revoked → 401.
    4. Schedule a background ``last_used_at`` update so the request
       is not blocked on the write.

    The matched :class:`ApiKey` snapshot is returned to the route
    handler for downstream audit. Routes that wire this as a
    router-level dependency typically ignore the return value.
    """
    if not _authn_required():
        return None

    raw = _extract_raw_key(authorization, x_api_key)
    if not raw:
        _raise_unauthorized(request, "Missing API key")
        return None  # pragma: no cover

    snapshot, error = _lookup_and_validate(factory, raw)
    if snapshot is None:
        _raise_unauthorized(request, error or "Invalid API key")
        return None  # pragma: no cover

    background_tasks.add_task(_stamp_last_used, factory, snapshot.id)
    return snapshot


__all__ = [
    "PREFIX_LEN",
    "generate_raw_key",
    "hash_key",
    "prefix_of",
    "require_api_key",
]
