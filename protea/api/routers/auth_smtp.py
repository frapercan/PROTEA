"""SMTP auth endpoints — magic-link login and password reset (FARM-AUTH.11).

All four endpoints are 404 when ``PROTEA_SMTP_ENABLED`` is unset / false.
They are surfaced only when an SMTP relay has been configured so that
deployments without email infrastructure cannot accidentally expose
incomplete flows.

Endpoint contract
-----------------

GET  /auth/smtp-enabled
    Returns ``{enabled: bool}``. Frontend uses this to show or hide the
    magic-link button on the login page. Always 200.

POST /auth/magic-link/request {email}
    Generates a magic-link token (15-min TTL) and emails it to the
    address if a matching active account exists. Always returns 204 so
    the caller cannot infer whether the address is registered.
    Returns 404 when SMTP is disabled.

GET  /auth/magic-link/consume?token=...
    Validates the token, marks it consumed, creates a session cookie, and
    redirects to /. Returns 400 on expired or already-consumed tokens.
    Returns 404 when SMTP is disabled.

POST /auth/password-reset/request {email}
    Same pattern as magic-link/request but for password-reset tokens.
    Returns 204 unconditionally (no user enumeration). 404 when disabled.

POST /auth/password-reset/consume {token, new_password}
    Validates the token, updates the password hash, marks the token
    consumed, returns 204. Returns 400 on bad / expired tokens.
    Returns 404 when SMTP is disabled.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import os
import secrets
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import jwt
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, Field, field_validator
from sqlalchemy.orm import Session, sessionmaker

from protea.api.auth.passwords import hash_password
from protea.api.auth.smtp import send_magic_link, send_password_reset
from protea.api.deps import get_session_factory
from protea.infrastructure.orm.models.email_token import EmailToken
from protea.infrastructure.orm.models.user import User, UserStatus
from protea.infrastructure.session import session_scope
from protea.infrastructure.settings import load_settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/auth", tags=["auth"])

_JWT_ALGORITHM = "HS256"
_COOKIE_NAME = "protea_session"
_COOKIE_MAX_AGE = 30 * 24 * 3600  # 30 days in seconds


# ---------------------------------------------------------------------------
# Settings helpers
# ---------------------------------------------------------------------------


def _load_settings():  # type: ignore[return]  # noqa: ANN201
    """Load settings from the project root.

    Uses the same path-discovery logic as ``create_app`` so the settings
    are always consistent with the running instance.
    """
    project_root = Path(__file__).resolve().parents[3]
    return load_settings(project_root)


def _smtp_enabled() -> bool:
    """Return True when SMTP has been explicitly enabled via env / settings."""
    return _load_settings().smtp_enabled


def _require_smtp() -> None:
    """Raise 404 if SMTP is not enabled.

    404 rather than 503 matches the admin-controlled feature gate pattern:
    from the client's perspective the endpoint simply does not exist when
    SMTP is not configured.
    """
    if not _smtp_enabled():
        raise HTTPException(status_code=404, detail="smtp_not_configured")


def _read_jwt_secret() -> str | None:
    raw = os.getenv("PROTEA_JWT_SECRET")
    if raw is None or not raw.strip():
        return None
    return raw


# ---------------------------------------------------------------------------
# Shared token-validation helper
# ---------------------------------------------------------------------------


def _hash_token(raw: str) -> str:
    return hashlib.sha256(raw.encode()).hexdigest()


def _consume_token(
    session: Session,
    raw: str,
    expected_kind: str,
) -> EmailToken:
    """Validate and consume a raw token.

    Raises ``HTTPException(400)`` on any of:
    * token digest not found in the DB,
    * kind mismatch,
    * already consumed (``consumed_at`` set),
    * expired (``expires_at`` in the past).

    On success marks ``consumed_at`` and flushes inside the caller's
    session. The caller is responsible for the final commit.
    """
    digest = _hash_token(raw)
    row: EmailToken | None = (
        session.query(EmailToken).filter(EmailToken.token_hash == digest).first()
    )
    if row is None or not hmac.compare_digest(row.token_hash, digest):
        raise HTTPException(status_code=400, detail="invalid_token")
    if row.kind != expected_kind:
        raise HTTPException(status_code=400, detail="invalid_token")
    if row.consumed_at is not None:
        raise HTTPException(status_code=400, detail="token_already_used")
    if datetime.now(UTC) > row.expires_at.replace(tzinfo=UTC):
        raise HTTPException(status_code=400, detail="token_expired")

    row.consumed_at = datetime.now(UTC)
    session.flush()
    return row


# ---------------------------------------------------------------------------
# Session cookie helpers (mirrors auth_user.py)
# ---------------------------------------------------------------------------


def _mint_session_jwt(user_id: str, role: str, status: str, jti: str, secret: str) -> str:
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


def _set_session_cookie(response: Response, token: str) -> None:
    response.set_cookie(
        key=_COOKIE_NAME,
        value=token,
        max_age=_COOKIE_MAX_AGE,
        httponly=True,
        secure=True,
        samesite="strict",
    )


# ---------------------------------------------------------------------------
# Request / response schemas
# ---------------------------------------------------------------------------


class MagicLinkRequestBody(BaseModel):
    """Body for POST /auth/magic-link/request."""

    model_config = {"extra": "forbid"}

    email: str = Field(..., description="Email address to send the magic link to.")

    @field_validator("email", mode="before")
    @classmethod
    def _normalise(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("email must be a non-empty string")
        return v.strip().lower()


class PasswordResetRequestBody(BaseModel):
    """Body for POST /auth/password-reset/request."""

    model_config = {"extra": "forbid"}

    email: str = Field(..., description="Email address registered to the account.")

    @field_validator("email", mode="before")
    @classmethod
    def _normalise(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("email must be a non-empty string")
        return v.strip().lower()


class PasswordResetConsumeBody(BaseModel):
    """Body for POST /auth/password-reset/consume."""

    model_config = {"extra": "forbid"}

    token: str = Field(..., description="Raw reset token from the email.")
    new_password: str = Field(..., min_length=8, description="New plaintext password.")


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get(
    "/smtp-enabled",
    summary="Check whether SMTP-based auth flows are enabled",
    operation_id="auth_smtp_enabled",
)
def smtp_enabled_check() -> dict[str, bool]:
    """Return ``{enabled: bool}`` — 200 regardless of SMTP configuration.

    The frontend queries this endpoint to decide whether to show the
    magic-link option on the login page. This endpoint is always 200;
    it never returns 404.
    """
    return {"enabled": _smtp_enabled()}


@router.post(
    "/magic-link/request",
    status_code=204,
    summary="Request a magic-link sign-in email",
    operation_id="auth_magic_link_request",
)
def magic_link_request(
    request: Request,
    body: MagicLinkRequestBody,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> None:
    """Send a magic-link email if a matching active account exists.

    Always returns 204 so the caller cannot infer whether the email is
    registered. Emails are sent only to ``active`` accounts; pending and
    deactivated accounts are silently skipped (same 204 response).

    Returns 404 when SMTP is not enabled.
    """
    _require_smtp()

    settings = _load_settings()
    base_url = str(request.base_url).rstrip("/")

    with session_scope(factory) as session:
        user: User | None = (
            session.query(User).filter(User.email == body.email).first()
        )
        if user is not None and user.status is UserStatus.ACTIVE:
            try:
                send_magic_link(settings, session, user, base_url)
            except Exception:
                # Log and swallow: the client always gets 204 to avoid
                # leaking whether the address is registered.
                logger.exception("magic_link send failed for email=%r", body.email)


@router.get(
    "/magic-link/consume",
    summary="Consume a magic-link token and create a session",
    operation_id="auth_magic_link_consume",
)
def magic_link_consume(
    response: Response,
    token: str = Query(..., description="Raw magic-link token from the email."),
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> RedirectResponse:
    """Validate a magic-link token, create a session cookie, redirect to /.

    * 302 (redirect to /)  -- token valid, session cookie set.
    * 400                  -- invalid, expired, or already-consumed token.
    * 404                  -- SMTP not enabled.
    * 503                  -- JWT secret not configured.
    """
    _require_smtp()

    secret = _read_jwt_secret()
    if secret is None:
        raise HTTPException(
            status_code=503,
            detail="Bearer authentication is not configured on this server",
        )

    import time
    from datetime import UTC, datetime
    from uuid import UUID as _UUID

    from protea.api.routers.auth_user import _COOKIE_MAX_AGE, create_session

    with session_scope(factory) as session:
        row = _consume_token(session, token, "magic_link")
        user: User | None = session.get(User, row.user_id)
        if user is None:
            raise HTTPException(status_code=400, detail="invalid_token")
        if user.status is not UserStatus.ACTIVE:
            raise HTTPException(status_code=403, detail="account_not_active")

        jti = secrets.token_hex(16)
        jwt_token = _mint_session_jwt(
            str(user.id),
            user.role.value,
            user.status.value,
            jti,
            secret,
        )
        expires_at = datetime.fromtimestamp(int(time.time()) + _COOKIE_MAX_AGE, tz=UTC)
        # Coerce to UUID so test stubs that store user.id as plain str still work
        # against the UserSession.user_id UUID column.
        user_uuid = user.id if isinstance(user.id, _UUID) else _UUID(str(user.id))
        create_session(
            session,
            user_id=user_uuid,
            raw_token=jwt_token,
            expires_at=expires_at,
        )

    redirect = RedirectResponse(url="/", status_code=302)
    _set_session_cookie(redirect, jwt_token)
    return redirect


@router.post(
    "/password-reset/request",
    status_code=204,
    summary="Request a password-reset email",
    operation_id="auth_password_reset_request",
)
def password_reset_request(
    request: Request,
    body: PasswordResetRequestBody,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> None:
    """Send a password-reset email if a matching active account exists.

    Always returns 204 (no user-enumeration). Returns 404 when SMTP is
    not enabled.
    """
    _require_smtp()

    settings = _load_settings()
    base_url = str(request.base_url).rstrip("/")

    with session_scope(factory) as session:
        user: User | None = (
            session.query(User).filter(User.email == body.email).first()
        )
        if user is not None and user.status is UserStatus.ACTIVE:
            try:
                send_password_reset(settings, session, user, base_url)
            except Exception:
                logger.exception("password_reset send failed for email=%r", body.email)


@router.post(
    "/password-reset/consume",
    status_code=204,
    summary="Consume a password-reset token and update the password",
    operation_id="auth_password_reset_consume",
)
def password_reset_consume(
    body: PasswordResetConsumeBody,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> None:
    """Validate a reset token and update the account's password hash.

    * 204 -- token valid, password updated.
    * 400 -- invalid, expired, or already-consumed token.
    * 404 -- SMTP not enabled.
    """
    _require_smtp()

    with session_scope(factory) as session:
        row = _consume_token(session, body.token, "password_reset")
        user: User | None = session.get(User, row.user_id)
        if user is None:
            raise HTTPException(status_code=400, detail="invalid_token")
        user.password_hash = hash_password(body.new_password)
        session.flush()


__all__ = ["router"]
