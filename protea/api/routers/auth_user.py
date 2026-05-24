"""``/auth/signup``, ``/auth/login``, ``/auth/logout``, ``GET /auth/me`` — User identity flow (FARM-AUTH.3/8).

Implements the four endpoints that wire User-based authentication end-to-end,
plus the admin revoke-sessions endpoint added in FARM-AUTH.8.

Session storage was an in-memory dict in FARM-AUTH.3.  FARM-AUTH.8
replaces it with the ``user_session`` table so that:

* logout is durable across process restarts,
* admin can revoke all sessions for a user (security incident response),
* last activity is observable per-session.

Cookie contract (ADR D37)
-------------------------

* Name: ``protea_session``
* Value: a signed HS256 JWT (same secret as the API-key JWT: ``PROTEA_JWT_SECRET``)
* Attributes: ``HttpOnly``, ``Secure``, ``SameSite=strict``, ``Max-Age=2592000`` (30 days)
* Payload: ``{sub, jti, role, status, exp, iat}``

The ``jti`` (JWT ID) is a random hex string; its SHA-256 hash is stored
in ``user_session.token_hash``.  Lookup on every ``/me`` call is O(1) via
the unique index on ``token_hash``.

Status semantics (from ADR D37 + User.status enum)
---------------------------------------------------

* ``pending``     — signup done, admin approval required. Login returns 403.
* ``active``      — approved; authentication succeeds.
* ``deactivated`` — admin-disabled. Login returns 403.
"""

from __future__ import annotations

import hashlib
import logging
import os
import secrets
import time
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import jwt
from fastapi import APIRouter, Cookie, Depends, HTTPException, Request, Response
from pydantic import BaseModel, Field, field_validator
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from protea.api.auth.passwords import hash_password, verify_password
from protea.api.deps import get_session_factory
from protea.core.utils import utcnow
from protea.infrastructure.orm.models.session import UserSession
from protea.infrastructure.orm.models.user import User, UserRole, UserStatus
from protea.infrastructure.session import session_scope

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/auth", tags=["auth"])

# ---------------------------------------------------------------------------
# Backward-compat stub — kept so existing tests that import _SESSION_STORE
# directly still work.  The real session state is now in the DB.
# ---------------------------------------------------------------------------

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


def _token_hash(raw_token: str) -> str:
    """Return the SHA-256 hex digest of a raw JWT cookie value."""
    return hashlib.sha256(raw_token.encode()).hexdigest()


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
# Session DB helpers
# ---------------------------------------------------------------------------


def create_session(
    db: Session,
    *,
    user_id: UUID,
    raw_token: str,
    expires_at: datetime,
    user_agent: str | None = None,
    client_ip: str | None = None,
) -> UserSession:
    """Insert a new ``user_session`` row and return it.

    ``client_ip`` is hashed with SHA-256 before storage for privacy.
    ``raw_token`` is hashed with SHA-256 before storage so a DB dump
    cannot replay sessions.
    """
    client_ip_hash = hashlib.sha256(client_ip.encode()).hexdigest() if client_ip else None
    sess = UserSession(
        user_id=user_id,
        token_hash=_token_hash(raw_token),
        expires_at=expires_at,
        user_agent=user_agent,
        client_ip_hash=client_ip_hash,
    )
    db.add(sess)
    db.flush()
    return sess


def find_session(db: Session, *, raw_token: str) -> UserSession | None:
    """Look up a live session by the raw JWT value.

    Returns ``None`` when the token is unknown, revoked, or expired.
    """
    th = _token_hash(raw_token)
    row = db.query(UserSession).filter(UserSession.token_hash == th).first()
    if row is None:
        return None
    if row.revoked_at is not None:
        return None
    now = datetime.now(tz=UTC)
    if row.expires_at.replace(tzinfo=UTC) <= now:
        return None
    return row


def revoke_session(db: Session, *, raw_token: str) -> bool:
    """Mark a session as revoked by setting ``revoked_at``.

    Returns ``True`` when a live session was found and revoked,
    ``False`` when no matching session exists.
    """
    th = _token_hash(raw_token)
    row = db.query(UserSession).filter(UserSession.token_hash == th).first()
    if row is None or row.revoked_at is not None:
        return False
    row.revoked_at = utcnow()
    db.flush()
    return True


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


def _login_db_step(
    session: Session,
    *,
    email: str,
    password: str,
    secret: str,
    user_agent: str | None,
    client_ip: str | None,
) -> tuple[str, dict[str, Any]]:
    """Validate credentials, write last_login_at, mint JWT, insert session row.

    Returns ``(raw_token, user_payload)`` on success; raises HTTPException on
    any authentication failure so the caller just sets the cookie.
    """
    user = session.query(User).filter(User.email == email).first()
    if user is None or not verify_password(password, user.password_hash):
        raise HTTPException(status_code=401, detail="invalid_credentials")
    if user.status is UserStatus.PENDING:
        raise HTTPException(status_code=403, detail="account_pending_approval")
    if user.status is UserStatus.DEACTIVATED:
        raise HTTPException(status_code=403, detail="account_deactivated")

    user.last_login_at = utcnow()
    session.flush()

    jti = secrets.token_hex(16)
    raw_token = _mint_session_jwt(str(user.id), user.role.value, user.status.value, jti, secret)
    expires_at = datetime.fromtimestamp(int(time.time()) + _COOKIE_MAX_AGE, tz=UTC)
    create_session(session, user_id=user.id, raw_token=raw_token, expires_at=expires_at,
                   user_agent=user_agent, client_ip=client_ip)
    payload = {
        "id": str(user.id), "email": user.email, "username": user.username,
        "display_name": user.display_name, "role": user.role.value, "status": user.status.value,
    }
    return raw_token, payload


@router.post("/login", status_code=200, summary="Authenticate with email and password", operation_id="user_login")
def login(
    request: Request,
    response: Response,
    body: LoginRequest,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Verify credentials and issue a session cookie.

    * 200 + cookie — credentials valid, account active.
    * 401          — wrong email or wrong password (generic message to avoid user enumeration).
    * 403          — valid credentials but account not yet approved or deactivated.

    ``last_login_at`` is updated on every successful login.
    A ``user_session`` row is inserted to enable server-side revocation.
    """
    secret = _read_secret()
    if secret is None:
        raise HTTPException(status_code=503,
                            detail="Bearer authentication is not configured on this server")
    ua = request.headers.get("user-agent")
    client_ip = request.client.host if request.client else None
    with session_scope(factory) as session:
        raw_token, payload = _login_db_step(
            session, email=body.email, password=body.password, secret=secret,
            user_agent=ua, client_ip=client_ip,
        )
    _set_session_cookie(response, raw_token)
    return payload


@router.post("/logout", status_code=204, summary="Invalidate the current session", operation_id="user_logout")
def logout(
    response: Response,
    protea_session: str | None = Cookie(default=None),
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> None:
    """Clear the session cookie and revoke the server-side session.

    Returns 204 both when a valid session was found and when no cookie
    was present (idempotent logout).
    """
    if protea_session is not None:
        secret = _read_secret()
        if secret is not None:
            try:
                # Validate the JWT is well-formed before revoking.
                _decode_session_jwt(protea_session, secret)
                with session_scope(factory) as session:
                    revoke_session(session, raw_token=protea_session)
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
    session has been revoked via ``POST /auth/logout`` or admin revocation.
    Updates ``last_seen_at`` on the session row for liveness tracking.
    """
    if protea_session is None:
        raise HTTPException(status_code=401, detail="not_authenticated")

    secret = _read_secret()
    if secret is None:
        raise HTTPException(status_code=503, detail="Bearer authentication is not configured")

    claims = _decode_session_jwt(protea_session, secret)

    user_id_str = claims.get("sub")
    if not isinstance(user_id_str, str):
        raise HTTPException(status_code=401, detail="invalid_session_payload")

    try:
        user_id = UUID(user_id_str)
    except ValueError as exc:
        raise HTTPException(status_code=401, detail="invalid_session_payload") from exc

    with session_scope(factory) as session:
        sess_row = find_session(session, raw_token=protea_session)
        if sess_row is None:
            raise HTTPException(status_code=401, detail="session_revoked")

        # Update last_seen_at for liveness tracking.
        sess_row.last_seen_at = utcnow()
        session.flush()

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


@router.post(
    "/admin/revoke-sessions/{user_id}",
    status_code=200,
    summary="Revoke all active sessions for a user (admin only)",
    operation_id="admin_revoke_user_sessions",
)
def admin_revoke_sessions(
    user_id: str,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Mark all non-revoked sessions for ``user_id`` as revoked.

    This is the security-incident-response endpoint: call it when an
    account is believed to be compromised so that all active browser
    sessions are immediately invalidated.

    Authentication is intentionally not gated by a user session cookie
    (which would create a circular dependency) — callers must use the
    ``PROTEA_ADMIN_TOKEN`` header pattern (``Authorization: Bearer <token>``)
    enforced upstream by the admin role gate.  The endpoint lives under
    ``/auth/admin/`` so the API gateway can route it to admin-only callers.

    Returns ``{user_id, revoked_count}`` indicating how many sessions were
    invalidated. Returns 404 if the user does not exist.
    """
    try:
        uid = UUID(user_id)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="Invalid user_id format") from exc

    with session_scope(factory) as session:
        user = session.get(User, uid)
        if user is None:
            raise HTTPException(status_code=404, detail="user_not_found")

        now = utcnow()
        rows = (
            session.query(UserSession)
            .filter(
                UserSession.user_id == uid,
                UserSession.revoked_at.is_(None),
            )
            .all()
        )
        for row in rows:
            row.revoked_at = now
        session.flush()

    return {"user_id": user_id, "revoked_count": len(rows)}


__all__ = ["router", "_SESSION_STORE"]
