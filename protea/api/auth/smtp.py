"""SMTP helpers for optional email-driven auth flows (FARM-AUTH.11).

This module implements three public entry points consumed by the SMTP
auth router:

* :func:`send_mail` -- low-level SMTP dispatch via :mod:`smtplib`.
  Returns ``False`` immediately (no-op) when SMTP is disabled.
* :func:`send_magic_link` -- generates a magic-link token, inserts an
  :class:`~protea.infrastructure.orm.models.email_token.EmailToken` row,
  and sends the one-time login URL to the user.
* :func:`send_password_reset` -- same pattern for password-reset tokens.

No async SMTP library is needed. FastAPI runs token generation and DB
writes synchronously inside a regular thread (the router endpoints are
sync), and :mod:`smtplib` is straightforward stdlib. Keeping it sync
avoids an extra dependency and matches the existing auth router style.

Security notes
--------------

* Tokens are generated with :func:`secrets.token_urlsafe` (32 bytes of
  entropy = 256-bit security).
* Only the SHA-256 hex digest is stored in the DB. The raw token is
  included once in the email body and is never logged.
* TTL is 15 minutes. The ``expires_at`` column is checked on consume
  before anything else.
* Both ``send_magic_link`` and ``send_password_reset`` are called only
  when a user with the supplied email exists; callers MUST NOT reveal
  whether the lookup succeeded (constant-time response to the HTTP
  client).
"""

from __future__ import annotations

import hashlib
import logging
import secrets
import smtplib
from datetime import UTC, datetime, timedelta
from email.mime.text import MIMEText
from typing import TYPE_CHECKING

from sqlalchemy.orm import Session

from protea.infrastructure.orm.models.email_token import EmailToken

if TYPE_CHECKING:
    from protea.infrastructure.orm.models.user import User
    from protea.infrastructure.settings import Settings

logger = logging.getLogger(__name__)

_TOKEN_TTL_MINUTES = 15


def _hash_token(raw: str) -> str:
    """Return the SHA-256 hex digest of *raw*."""
    return hashlib.sha256(raw.encode()).hexdigest()


def send_mail(
    settings: Settings,
    to: str,
    subject: str,
    body: str,
) -> bool:
    """Send a plain-text email via SMTP.

    Parameters
    ----------
    settings:
        The resolved :class:`~protea.infrastructure.settings.Settings`
        instance. Uses ``smtp_host``, ``smtp_port``, ``smtp_user``,
        ``smtp_password``, and ``smtp_from_addr``.
    to:
        Recipient email address.
    subject:
        Email subject line.
    body:
        Plain-text email body.

    Returns
    -------
    bool
        ``True`` if the mail was accepted by the SMTP server.
        ``False`` if SMTP is disabled (``smtp_enabled=False``) -- the
        call is a no-op in that case and no exception is raised.

    Raises
    ------
    smtplib.SMTPException
        On any SMTP-level error when SMTP is enabled. Callers may catch
        and log; the router returns 503 to the client.
    """
    if not settings.smtp_enabled:
        return False

    from_addr = settings.smtp_from_addr or settings.smtp_user or "protea@localhost"
    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = to

    host = settings.smtp_host or "localhost"
    port = settings.smtp_port

    try:
        with smtplib.SMTP(host, port, timeout=10) as conn:
            conn.ehlo()
            if conn.has_extn("STARTTLS"):
                conn.starttls()
                conn.ehlo()
            if settings.smtp_user and settings.smtp_password:
                conn.login(settings.smtp_user, settings.smtp_password)
            conn.sendmail(from_addr, [to], msg.as_string())
        return True
    except smtplib.SMTPException:
        logger.exception("SMTP send failed to %r", to)
        raise


def _insert_token(
    session: Session,
    user: User,
    kind: str,
) -> str:
    """Generate a token, persist its hash, and return the raw token.

    The raw token is returned to the caller (for inclusion in the email
    body) and is never stored. Only the SHA-256 digest is written to the
    ``email_token`` table.
    """
    raw = secrets.token_urlsafe(32)
    digest = _hash_token(raw)
    now = datetime.now(UTC)
    row = EmailToken(
        user_id=user.id,
        token_hash=digest,
        kind=kind,
        created_at=now,
        expires_at=now + timedelta(minutes=_TOKEN_TTL_MINUTES),
        consumed_at=None,
    )
    session.add(row)
    session.flush()
    return raw


def send_magic_link(
    settings: Settings,
    session: Session,
    user: User,
    base_url: str,
) -> bool:
    """Generate a magic-link token and email it to *user*.

    Inserts an :class:`EmailToken` row with ``kind='magic_link'`` and
    sends an email containing the one-time login URL
    ``{base_url}/v1/auth/magic-link/consume?token=<raw>``.

    Parameters
    ----------
    settings:
        Resolved settings (must have ``smtp_enabled=True`` for anything
        to be sent).
    session:
        Open SQLAlchemy session; the token row is flushed inside this call.
    user:
        The :class:`~protea.infrastructure.orm.models.user.User` to
        email. The raw token is emailed to ``user.email``.
    base_url:
        The deployment root URL, e.g. ``https://protea.example.org``.
        The link is ``{base_url}/v1/auth/magic-link/consume?token=...``.

    Returns
    -------
    bool
        ``True`` if the mail was sent; ``False`` if SMTP is disabled.
    """
    if not settings.smtp_enabled:
        return False

    raw = _insert_token(session, user, "magic_link")
    link = f"{base_url.rstrip('/')}/v1/auth/magic-link/consume?token={raw}"

    subject = "Your PROTEA sign-in link"
    body = (
        f"Hello {user.display_name or user.username},\n\n"
        f"Click the link below to sign in to PROTEA. "
        f"The link expires in {_TOKEN_TTL_MINUTES} minutes and can only be used once.\n\n"
        f"{link}\n\n"
        f"If you did not request this link, you can safely ignore this message.\n"
    )
    return send_mail(settings, user.email, subject, body)


def send_password_reset(
    settings: Settings,
    session: Session,
    user: User,
    base_url: str,
) -> bool:
    """Generate a password-reset token and email it to *user*.

    Inserts an :class:`EmailToken` row with ``kind='password_reset'``
    and sends an email with the reset instructions.

    Parameters
    ----------
    settings:
        Resolved settings (must have ``smtp_enabled=True``).
    session:
        Open SQLAlchemy session; the token row is flushed inside this call.
    user:
        The user whose password should be reset.
    base_url:
        The deployment root URL.

    Returns
    -------
    bool
        ``True`` if the mail was sent; ``False`` if SMTP is disabled.
    """
    if not settings.smtp_enabled:
        return False

    raw = _insert_token(session, user, "password_reset")
    link = f"{base_url.rstrip('/')}/reset-password?token={raw}"

    subject = "Reset your PROTEA password"
    body = (
        f"Hello {user.display_name or user.username},\n\n"
        f"We received a request to reset the password for your PROTEA account.\n"
        f"Click the link below to choose a new password. "
        f"The link expires in {_TOKEN_TTL_MINUTES} minutes and can only be used once.\n\n"
        f"{link}\n\n"
        f"If you did not request a password reset, you can safely ignore this message.\n"
    )
    return send_mail(settings, user.email, subject, body)


__all__ = ["send_mail", "send_magic_link", "send_password_reset"]
