"""``POST /auth/login`` — exchange an API key for a short-lived JWT.

FEAT-AUTH (WAVE-2 2026-05-23). The frontend trades a long-lived
``X-Api-Key`` for a Bearer JWT carrying the caller's role; the JWT
rides in a ``protea_session`` cookie that ``apps/web/middleware.ts``
and ``apps/web/lib/auth.ts`` read. HS256, signed with
``PROTEA_JWT_SECRET``. The role on the matched ``ApiKey`` row becomes
the ``role`` claim (NULL → ``viewer``).
"""

from __future__ import annotations

import hmac
import os
import time
from typing import Any

import jwt
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel, Field, field_validator
from sqlalchemy.orm import Session, sessionmaker

from protea.api.auth import hash_key, prefix_of
from protea.api.deps import get_session_factory
from protea.api.rate_limit import api_keys_limit, limiter
from protea.api.roles import ROLE_VIEWER, normalise_role
from protea.infrastructure.orm.models.api_key import ApiKey
from protea.infrastructure.session import session_scope

router = APIRouter(prefix="/auth", tags=["auth"])

_JWT_ALGORITHM = "HS256"
_DEFAULT_TTL_SECONDS = 3600
_MAX_TTL_SECONDS = 24 * 3600


def _read_secret() -> str | None:
    raw = os.getenv("PROTEA_JWT_SECRET")
    if raw is None or not raw.strip():
        return None
    return raw


class LoginRequest(BaseModel):
    """Body for ``POST /auth/login`` (raw key + optional TTL)."""

    model_config = {"extra": "forbid"}

    api_key: str = Field(..., min_length=8, description="Raw API key.")
    ttl_seconds: int = Field(
        default=_DEFAULT_TTL_SECONDS,
        ge=60,
        le=_MAX_TTL_SECONDS,
        description="JWT lifetime in seconds (capped at 24h, default 1h).",
    )

    @field_validator("api_key", mode="before")
    @classmethod
    def _strip(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("api_key must be a non-empty string")
        return v.strip()


def _lookup_role(factory: sessionmaker[Session], raw: str) -> tuple[str, str] | None:
    """Return ``(sub, role)`` for a valid raw key, or ``None``.

    ``sub`` is the ``ApiKey`` UUID. Revoked rows surface as misses so
    the failure mode is uniform with "unknown key".
    """
    prefix = prefix_of(raw)
    candidate_hash = hash_key(raw)
    with session_scope(factory) as session:
        rows = session.query(ApiKey).filter(ApiKey.prefix == prefix).all()
        matched = next(
            (r for r in rows if hmac.compare_digest(r.key_hash, candidate_hash)),
            None,
        )
        if matched is None or matched.revoked_at is not None:
            return None
        return str(matched.id), normalise_role(matched.role or ROLE_VIEWER)


@router.post("/login", summary="Exchange an API key for a session JWT")
@limiter.limit(api_keys_limit)
def login(
    request: Request,
    response: Response,
    body: LoginRequest,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Mint a short-lived bearer JWT (``sub``+``exp``+``iat``+``role``).

    The role is read from the matched ``ApiKey`` row (NULL → ``viewer``).
    """
    secret = _read_secret()
    if secret is None:
        raise HTTPException(
            status_code=503,
            detail="Bearer authentication is not configured on this server",
        )

    found = _lookup_role(factory, body.api_key)
    if found is None:
        raise HTTPException(status_code=401, detail="Invalid API key")
    sub, role = found

    now = int(time.time())
    payload = {
        "sub": sub,
        "iat": now,
        "exp": now + body.ttl_seconds,
        "role": role,
    }
    token = jwt.encode(payload, secret, algorithm=_JWT_ALGORITHM)
    return {
        "token": token,
        "token_type": "Bearer",
        "expires_in": body.ttl_seconds,
        "role": role,
        "sub": sub,
    }


__all__ = ["router"]
