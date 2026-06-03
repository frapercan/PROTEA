"""``/auth/signup``, ``/auth/login``, ``/auth/logout``, ``GET /auth/me``: User identity flow (FARM-AUTH.3/AUTH.8).

AUTH.3 shipped the four core endpoints with an in-memory ``_SESSION_STORE``
dict. AUTH.8 replaces that dict with a persistent ``user_session`` table
so logout invalidates tokens server-wide across all replicas and an admin
can revoke a user's sessions at any time.

Cookie contract (ADR D37, with FARM-AUTH.10 amendment)
------------------------------------------------------

* Name: ``protea_session``
* Value: a signed HS256 JWT (same secret as the API-key JWT: ``PROTEA_JWT_SECRET``)
* Attributes: ``Secure``, ``SameSite=strict``, ``Max-Age=2592000`` (30 days)
* Payload: ``{sub, jti, role, status, exp, iat}``

Note on ``HttpOnly``
~~~~~~~~~~~~~~~~~~~~

D37 originally specified ``HttpOnly``. The entire client (``lib/auth.ts``,
``lib/api.ts``, ``useRole``, ``AuthChip``, sidebar admin gate, every
mutation in ``lib/api.ts``) reads ``document.cookie`` to surface the
session role and to mint the ``Authorization: Bearer`` header that the
API gate accepts; ``HttpOnly`` hides the cookie from JavaScript and
breaks all of those code paths (AuthChip stuck on anonymous after a
successful login, admin sidebar group invisible, every POST/PATCH/DELETE
silently 401s). The frontend has no equivalent to the cookie -> Bearer
plumbing on the server side, so the only way to preserve a "login that
persists across navigation" experience is to let JS read the cookie. The
realistic CSRF surface is already covered by ``SameSite=strict`` and the
transport surface by ``Secure``; the JWT is short-lived (30 days) and
server-revocable via the ``user_session`` table.

The ``jti`` (JWT ID) is a random UUID. On login a ``user_session`` row is
inserted with ``token_hash = sha256(raw_jwt)``. On logout the row's
``revoked_at`` is set to now(). ``/me`` looks up the row by token_hash,
rejects it when ``revoked_at`` is not null, and updates ``last_seen_at``.

Status semantics (from ADR D37 + User.status enum)
---------------------------------------------------

* ``pending``     - signup done, admin approval required. Login returns 403.
* ``active``      - approved; authentication succeeds.
* ``deactivated`` - admin-disabled. Login returns 403.
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

from protea.api.auth.audit import audit_event
from protea.api.auth.passwords import hash_password, verify_password
from protea.api.deps import get_session_factory
from protea.api.roles import ROLE_ADMIN, require_role
from protea.core.utils import utcnow
from protea.infrastructure.orm.models.user import User, UserRole, UserStatus
from protea.infrastructure.orm.models.user_session import UserSession
from protea.infrastructure.session import session_scope

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/auth", tags=["auth"])

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


def _hash_token(raw_token: str) -> str:
    """Return the SHA-256 hex digest of a raw JWT string."""
    return hashlib.sha256(raw_token.encode()).hexdigest()


# ---------------------------------------------------------------------------
# Session DB helpers
# ---------------------------------------------------------------------------


def create_session(
    session: Session,
    *,
    user_id: UUID,
    raw_token: str,
    expires_at: datetime,
    user_agent: str | None = None,
    client_ip_hash: str | None = None,
) -> UserSession:
    """Insert a new UserSession row and flush it within the caller's transaction."""
    row = UserSession(
        user_id=user_id,
        token_hash=_hash_token(raw_token),
        expires_at=expires_at,
        last_seen_at=utcnow(),
        user_agent=user_agent,
        client_ip_hash=client_ip_hash,
    )
    session.add(row)
    session.flush()
    return row


def find_session(session: Session, raw_token: str) -> UserSession | None:
    """Return the UserSession row for a token, or None if not found."""
    token_hash = _hash_token(raw_token)
    return (
        session.query(UserSession)
        .filter(UserSession.token_hash == token_hash)
        .first()
    )


def revoke_session(session: Session, raw_token: str) -> bool:
    """Mark the session as revoked; returns True when a live row was found."""
    row = find_session(session, raw_token)
    if row is None:
        return False
    if row.revoked_at is None:
        row.revoked_at = utcnow()
        session.flush()
    return True


def revoke_all_sessions(session: Session, user_id: UUID) -> int:
    """Revoke every non-revoked session for the given user; returns count."""
    now = utcnow()
    rows = (
        session.query(UserSession)
        .filter(UserSession.user_id == user_id, UserSession.revoked_at.is_(None))
        .all()
    )
    for row in rows:
        row.revoked_at = now
    session.flush()
    return len(rows)


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
    """Write the ``protea_session`` cookie with ADR D37 security attributes.

    ``httponly=False`` (FARM-AUTH.10 amendment): the chrome reads the
    JWT via ``document.cookie`` so the AuthChip, useRole hook, sidebar
    admin gate and every mutation in ``apps/web/lib/api.ts`` can
    re-mint the ``Authorization: Bearer`` header on each call. Switching
    this back to ``httponly=True`` strands the entire client (login
    appears to "not persist" because every cookie-derived surface
    silently falls back to anonymous). ``Secure`` + ``SameSite=strict``
    remain the transport / CSRF defences.
    """
    response.set_cookie(
        key=_COOKIE_NAME,
        value=token,
        max_age=_COOKIE_MAX_AGE,
        httponly=False,
        secure=True,
        samesite="strict",
    )


def _clear_session_cookie(response: Response) -> None:
    """Delete the ``protea_session`` cookie."""
    response.delete_cookie(key=_COOKIE_NAME, httponly=False, secure=True, samesite="strict")


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
            audit_event(session, "signup", actor_id=None, target_id=user_id, email=user_email)
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
    request: Request,
    response: Response,
    body: LoginRequest,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Verify credentials and issue a session cookie.

    * 200 + cookie - credentials valid, account active.
    * 401          - wrong email or wrong password (generic message to avoid user enumeration).
    * 403          - valid credentials but account not yet approved (``account_pending_approval``)
                     or has been deactivated (``account_deactivated``).

    On success a ``user_session`` row is inserted with the token hash so
    the session can be revoked server-side without waiting for JWT expiry.
    ``last_login_at`` is updated on every successful login.
    """
    secret = _read_secret()
    if secret is None:
        raise HTTPException(
            status_code=503,
            detail="Bearer authentication is not configured on this server",
        )

    with session_scope(factory) as session:
        user = _authenticate_or_raise(session, body)
        _check_account_status_or_raise(session, user)
        user.last_login_at = utcnow()
        token, claims = _issue_login_session(session, user, secret, request)
        audit_event(session, "login_ok", actor_id=claims["id"])
        session.flush()

    _set_session_cookie(response, token)
    return claims


def _authenticate_or_raise(session: Session, body: LoginRequest) -> User:
    user = session.query(User).filter(User.email == body.email).first()
    if user is None or not verify_password(body.password, user.password_hash):
        audit_event(session, "login_fail", actor_id=None, email=body.email)
        raise HTTPException(status_code=401, detail="invalid_credentials")
    return user


def _check_account_status_or_raise(session: Session, user: User) -> None:
    if user.status is UserStatus.PENDING:
        audit_event(session, "login_fail", actor_id=str(user.id), reason="account_pending_approval")
        raise HTTPException(status_code=403, detail="account_pending_approval")
    if user.status is UserStatus.DEACTIVATED:
        audit_event(session, "login_fail", actor_id=str(user.id), reason="account_deactivated")
        raise HTTPException(status_code=403, detail="account_deactivated")


def _issue_login_session(
    session: Session, user: User, secret: str, request: Request
) -> tuple[str, dict[str, Any]]:
    jti = secrets.token_hex(16)
    user_id = str(user.id)
    role = user.role.value
    status = user.status.value
    token = _mint_session_jwt(user_id, role, status, jti, secret)
    expires_at = datetime.fromtimestamp(int(time.time()) + _COOKIE_MAX_AGE, tz=UTC)
    ua = request.headers.get("user-agent")
    create_session(
        session,
        user_id=user.id,
        raw_token=token,
        expires_at=expires_at,
        user_agent=ua[:512] if ua else None,
    )
    claims = {
        "id": user_id,
        "email": user.email,
        "username": user.username,
        "display_name": user.display_name,
        "role": role,
        "status": status,
    }
    return token, claims


@router.post("/logout", status_code=204, summary="Invalidate the current session", operation_id="user_logout")
def logout(
    response: Response,
    protea_session: str | None = Cookie(default=None),
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> None:
    """Clear the session cookie and revoke the server-side session row.

    Returns 204 both when a valid session was found and when no cookie
    was present (idempotent logout). Sets ``revoked_at`` on the
    ``user_session`` row so a captured token cannot be replayed.
    """
    actor_id: str | None = None
    if protea_session is not None:
        secret = _read_secret()
        if secret is not None:
            try:
                claims = _decode_session_jwt(protea_session, secret)
                actor_id = claims.get("sub")
            except HTTPException:
                # Malformed / expired token: still clear the cookie.
                pass
            with session_scope(factory) as session:
                revoke_session(session, protea_session)
                audit_event(session, "logout", actor_id=actor_id)
    else:
        with session_scope(factory) as session:
            audit_event(session, "logout", actor_id=None)
    _clear_session_cookie(response)


@router.get("/me", summary="Return the currently authenticated user", operation_id="user_me")
def me(
    protea_session: str | None = Cookie(default=None),
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Return ``{id, email, username, display_name, role, status}`` for the session user.

    Returns 401 when no valid session cookie is present, when the JWT
    is expired/invalid, or when the session has been revoked via
    ``POST /auth/logout`` or the admin revoke endpoint.
    Updates ``last_seen_at`` on every successful call.
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
        # Validate the session row exists and is not revoked.
        sess_row = find_session(session, protea_session)
        if sess_row is None:
            raise HTTPException(status_code=401, detail="session_revoked")
        if sess_row.revoked_at is not None:
            raise HTTPException(status_code=401, detail="session_revoked")

        # Update last_seen_at so admins can distinguish stale from active sessions.
        sess_row.last_seen_at = utcnow()

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


# ---------------------------------------------------------------------------
# Admin endpoint: revoke all sessions for a user
# ---------------------------------------------------------------------------


@router.post(
    "/admin/revoke-sessions/{user_id}",
    status_code=200,
    summary="Revoke all active sessions for a user (admin only)",
    operation_id="admin_revoke_user_sessions",
    dependencies=[Depends(require_role(ROLE_ADMIN))],
)
def admin_revoke_sessions(
    user_id: str,
    factory: sessionmaker[Session] = Depends(get_session_factory),
    _principal: Any = Depends(require_role(ROLE_ADMIN)),
) -> dict[str, Any]:
    """Mark all non-revoked sessions for ``user_id`` as revoked.

    Requires admin role. The target user will be logged out of every
    active browser/API session on their next request. Idempotent: calling
    twice for the same user returns count=0 on the second call.

    Emits an ``admin_session_revoke`` audit event with the target user id.
    """
    try:
        target_uuid = UUID(user_id)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="invalid user_id format") from exc

    with session_scope(factory) as session:
        target_user = session.get(User, target_uuid)
        if target_user is None:
            raise HTTPException(status_code=404, detail="user_not_found")

        count = revoke_all_sessions(session, target_uuid)
        audit_event(
            session,
            "admin_session_revoke",
            target_id=user_id,
            revoked_count=count,
        )

    return {"user_id": user_id, "revoked_count": count}


__all__ = ["router", "create_session", "find_session", "revoke_session", "revoke_all_sessions"]
