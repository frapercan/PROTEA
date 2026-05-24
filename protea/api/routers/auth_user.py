"""``/auth/signup``, ``/auth/login``, ``/auth/logout``, ``GET /auth/me`` — User identity flow (FARM-AUTH.3).

Implements the four endpoints that wire User-based authentication end-to-end.
Session storage is an in-memory dict keyed by a random ``jti`` (JWT ID).
A real session-revocation table is deferred to FARM-AUTH.8; this slice
keeps it intentionally simple so subsequent slices can depend on a
working authenticated session without waiting for the full store.

Cookie contract (ADR D37)
-------------------------

* Name: ``protea_session``
* Value: a signed HS256 JWT (same secret as the API-key JWT: ``PROTEA_JWT_SECRET``)
* Attributes: ``HttpOnly``, ``Secure``, ``SameSite=strict``, ``Max-Age=2592000`` (30 days)
* Payload: ``{sub, jti, role, status, exp, iat}``

The ``jti`` (JWT ID) is a random UUID written to the in-memory session
store at login. Logout deletes the ``jti`` entry so a captured token
cannot be replayed after the user has signed out.

Status semantics (from ADR D37 + User.status enum)
---------------------------------------------------

* ``pending``     — signup done, admin approval required. Login returns 403.
* ``active``      — approved; authentication succeeds.
* ``deactivated`` — admin-disabled. Login returns 403.
"""

from __future__ import annotations

import logging
import os
import secrets
import time
from typing import Any
from uuid import UUID

import jwt
from fastapi import APIRouter, Cookie, Depends, HTTPException, Response
from pydantic import BaseModel, Field, field_validator
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from protea.api.auth.passwords import hash_password, verify_password
from protea.api.deps import get_session_factory
from protea.core.utils import utcnow
from protea.infrastructure.orm.models.user import User, UserRole, UserStatus
from protea.infrastructure.session import session_scope

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/auth", tags=["auth"])

# ---------------------------------------------------------------------------
# Session store — in-memory dict for FARM-AUTH.3; replaced in FARM-AUTH.8
# ---------------------------------------------------------------------------

# Maps jti (str UUID) -> user_id (str UUID). Login writes; logout deletes;
# /me verifies presence. Intentionally not thread-safe at the dict level —
# FastAPI/uvicorn runs in a single event-loop thread for requests and the
# dict ops are atomic at the CPython level. A real deployment would swap
# this for a DB-backed table (FARM-AUTH.8) or Redis.
_SESSION_STORE: dict[str, str] = {}

# ---------------------------------------------------------------------------
# JWT helpers
# ---------------------------------------------------------------------------

_JWT_ALGORITHM = "HS256"
_COOKIE_NAME = "protea_session"
_COOKIE_MAX_AGE = 30 * 24 * 3600  # 30 days in seconds


def _read_secret() -> str | None:
    raw = os.getenv("PROTEA_JWT_SECRET")
    if raw is None or not raw.strip():
        return None
    return raw


def _mint_session_jwt(user_id: str, role: str, status: str, jti: str, secret: str) -> str:
    """Produce the HS256 JWT stored in the session cookie."""
    now = int(time.time())
    payload: dict[str, Any] = {
        "sub": user_id,
        "jti": jti,
        "role": role,
        "status": status,
        "iat": now,
        "exp": now + _COOKIE_MAX_AGE,
    }
    return jwt.encode(payload, secret, algorithm=_JWT_ALGORITHM)


def _decode_session_jwt(token: str, secret: str) -> dict[str, Any]:
    """Decode and validate the session JWT; raises HTTPException on failure."""
    try:
        return jwt.decode(
            token,
            secret,
            algorithms=[_JWT_ALGORITHM],
            options={"require": ["exp", "iat", "sub", "jti"]},
        )
    except jwt.InvalidTokenError as exc:
        raise HTTPException(status_code=401, detail=f"Invalid session token: {exc}") from exc


# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------


class SignupRequest(BaseModel):
    """Body for ``POST /auth/signup``."""

    model_config = {
        "extra": "forbid",
        "json_schema_extra": {
            "example": {
                "email": "researcher@example.org",
                "username": "jsmith",
                "display_name": "Jane Smith",
                "password": "c0rrect-h0rse-battery-staple",
                "intended_use": "Benchmarking GO-term predictors for the CAFA challenge.",
            }
        },
    }

    email: str = Field(..., min_length=3, max_length=255, description="Login email address.")
    username: str = Field(..., min_length=1, max_length=100, description="Unique username handle.")
    display_name: str | None = Field(
        default=None,
        max_length=255,
        description="Human-readable display name (optional).",
    )
    password: str = Field(..., min_length=8, description="Plaintext password (min 8 characters).")
    intended_use: str | None = Field(
        default=None,
        description="Free-text note telling admins what the requester plans to use PROTEA for.",
    )

    @field_validator("email", mode="before")
    @classmethod
    def _normalise_email(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("email must be a non-empty string")
        return v.strip().lower()

    @field_validator("username", mode="before")
    @classmethod
    def _strip_username(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("username must be a non-empty string")
        return v.strip()


class LoginRequest(BaseModel):
    """Body for ``POST /auth/login``."""

    model_config = {"extra": "forbid"}

    email: str = Field(..., description="Registered email address.")
    password: str = Field(..., description="Account password.")

    @field_validator("email", mode="before")
    @classmethod
    def _normalise_email(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("email must be a non-empty string")
        return v.strip().lower()


# ---------------------------------------------------------------------------
# Cookie helpers
# ---------------------------------------------------------------------------


def _set_session_cookie(response: Response, token: str) -> None:
    """Write the ``protea_session`` cookie with ADR D37 security attributes."""
    response.set_cookie(
        key=_COOKIE_NAME,
        value=token,
        max_age=_COOKIE_MAX_AGE,
        httponly=True,
        secure=True,
        samesite="strict",
    )


def _clear_session_cookie(response: Response) -> None:
    """Delete the ``protea_session`` cookie."""
    response.delete_cookie(key=_COOKIE_NAME, httponly=True, secure=True, samesite="strict")


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post("/signup", status_code=201, summary="Register a new user account", operation_id="user_signup")
def signup(
    body: SignupRequest,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Create a User with ``status=pending``.

    The account is not activated until an admin approves it (ADR D37
    manual-approval flow). No session cookie is issued here: the client
    must wait for approval, then call ``POST /auth/login``.

    Returns ``{id, email, username, status}`` on success. Returns 409
    if the email or username is already registered.
    """
    user = User(
        email=body.email,
        username=body.username,
        display_name=body.display_name,
        password_hash=hash_password(body.password),
        role=UserRole.RESEARCHER,
        status=UserStatus.PENDING,
        intended_use=body.intended_use,
    )
    try:
        with session_scope(factory) as session:
            session.add(user)
            session.flush()
            user_id = str(user.id)
            user_email = user.email
            user_username = user.username
            user_status = user.status.value
    except IntegrityError as exc:
        detail = str(exc.orig) if exc.orig else str(exc)
        if "uq_user_email" in detail or "email" in detail.lower():
            raise HTTPException(status_code=409, detail="email_already_registered") from exc
        if "uq_user_username" in detail or "username" in detail.lower():
            raise HTTPException(status_code=409, detail="username_already_taken") from exc
        raise HTTPException(status_code=409, detail="duplicate_user") from exc

    return {
        "id": user_id,
        "email": user_email,
        "username": user_username,
        "status": user_status,
    }


@router.post("/login", status_code=200, summary="Authenticate with email and password", operation_id="user_login")
def login(
    response: Response,
    body: LoginRequest,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Verify credentials and issue a session cookie.

    * 200 + cookie — credentials valid, account active.
    * 401          — wrong email or wrong password (generic message to avoid user enumeration).
    * 403          — valid credentials but account not yet approved (``account_pending_approval``)
                     or has been deactivated (``account_deactivated``).

    ``last_login_at`` is updated on every successful login.
    """
    secret = _read_secret()
    if secret is None:
        raise HTTPException(
            status_code=503,
            detail="Bearer authentication is not configured on this server",
        )

    with session_scope(factory) as session:
        user = session.query(User).filter(User.email == body.email).first()
        if user is None or not verify_password(body.password, user.password_hash):
            raise HTTPException(status_code=401, detail="invalid_credentials")

        if user.status is UserStatus.PENDING:
            raise HTTPException(status_code=403, detail="account_pending_approval")
        if user.status is UserStatus.DEACTIVATED:
            raise HTTPException(status_code=403, detail="account_deactivated")

        user.last_login_at = utcnow()
        session.flush()

        jti = secrets.token_hex(16)
        user_id = str(user.id)
        role = user.role.value
        status = user.status.value
        email = user.email
        username = user.username
        display_name = user.display_name

    _SESSION_STORE[jti] = user_id
    token = _mint_session_jwt(user_id, role, status, jti, secret)
    _set_session_cookie(response, token)
    return {
        "id": user_id,
        "email": email,
        "username": username,
        "display_name": display_name,
        "role": role,
        "status": status,
    }


@router.post("/logout", status_code=204, summary="Invalidate the current session", operation_id="user_logout")
def logout(
    response: Response,
    protea_session: str | None = Cookie(default=None),
) -> None:
    """Clear the session cookie and invalidate the server-side session token.

    Returns 204 both when a valid session was found and when no cookie
    was present (idempotent logout).
    """
    if protea_session is not None:
        secret = _read_secret()
        if secret is not None:
            try:
                claims = _decode_session_jwt(protea_session, secret)
                jti = claims.get("jti")
                if isinstance(jti, str):
                    _SESSION_STORE.pop(jti, None)
            except HTTPException:
                # Malformed / expired token — still clear the cookie.
                pass
    _clear_session_cookie(response)


@router.get("/me", summary="Return the currently authenticated user", operation_id="user_me")
def me(
    protea_session: str | None = Cookie(default=None),
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Return ``{id, email, username, display_name, role, status}`` for the session user.

    Returns 401 when no valid session cookie is present or when the
    session has been invalidated via ``POST /auth/logout``.
    """
    if protea_session is None:
        raise HTTPException(status_code=401, detail="not_authenticated")

    secret = _read_secret()
    if secret is None:
        raise HTTPException(status_code=503, detail="Bearer authentication is not configured")

    claims = _decode_session_jwt(protea_session, secret)

    jti = claims.get("jti")
    if not isinstance(jti, str) or jti not in _SESSION_STORE:
        raise HTTPException(status_code=401, detail="session_revoked")

    user_id_str = claims.get("sub")
    if not isinstance(user_id_str, str):
        raise HTTPException(status_code=401, detail="invalid_session_payload")

    try:
        user_id = UUID(user_id_str)
    except ValueError as exc:
        raise HTTPException(status_code=401, detail="invalid_session_payload") from exc

    with session_scope(factory) as session:
        user = session.get(User, user_id)
        if user is None:
            raise HTTPException(status_code=401, detail="user_not_found")
        return {
            "id": str(user.id),
            "email": user.email,
            "username": user.username,
            "display_name": user.display_name,
            "role": user.role.value,
            "status": user.status.value,
        }


__all__ = ["router", "_SESSION_STORE"]
