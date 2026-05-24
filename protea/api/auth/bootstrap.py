"""Bootstrap admin creation for FARM-AUTH.2 (ADR D37).

When ``PROTEA_BOOTSTRAP_ADMIN_EMAIL`` is set in the environment and no
``User`` row with ``role=admin`` exists yet, :func:`bootstrap_admin`
creates one during the FastAPI lifespan event that fires before the
first request. The function is also called by the ``protea-cli admin
add-user`` subcommand for break-glass use without a running API server.

Idempotency guarantee
---------------------

:func:`bootstrap_admin` checks whether an admin row already exists
*by email* before inserting. A second startup with the same env var is a
no-op: the function logs at INFO level and returns without touching the
database. This means it is safe to run in multi-replica deployments
where every replica fires the lifespan hook concurrently: whichever
replica wins the INSERT first, the others will observe the existing row
on re-check (or get a unique-constraint violation if two race within the
same millisecond, which is also handled gracefully).

Password handling
-----------------

If ``PROTEA_BOOTSTRAP_ADMIN_PASSWORD`` is set, that value is hashed with
Argon2id and stored. Otherwise a 32-character random token is generated
via :func:`secrets.token_urlsafe`, printed **once** to ``stderr``, and
then discarded, following the same ``stderr``-only convention used by
Django's ``createsuperuser --noinput``. The generated password is never
written to any log or database column in plaintext.
"""

from __future__ import annotations

import logging
import secrets
import sys

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from protea.api.auth.passwords import hash_password
from protea.infrastructure.orm.models.user import User, UserRole, UserStatus

_log = logging.getLogger(__name__)

# Minimum display-name fallback when bootstrapping from env vars.
_BOOTSTRAP_DISPLAY_NAME = "Bootstrap Admin"


def _find_admin_by_email(session: Session, email: str) -> User | None:
    """Return the User row matching *email*, or ``None`` if absent."""
    return session.scalar(select(User).where(User.email == email))


def _any_admin_exists(session: Session) -> bool:
    """Return ``True`` if at least one admin-role user exists."""
    return session.scalar(select(User).where(User.role == UserRole.ADMIN)) is not None


def bootstrap_admin(
    session: Session,
    email: str,
    *,
    password: str | None = None,
) -> tuple[User | None, bool]:
    """Ensure an admin user with *email* exists; create one if not.

    Parameters
    ----------
    session:
        An open SQLAlchemy ``Session``. The caller is responsible for
        committing after a successful return; this function does not
        commit so it can be composed inside a larger transaction or
        tested with a rollback.
    email:
        Email address for the admin account.
    password:
        Plaintext password to hash and store. When ``None``, a secure
        random password is generated and printed to ``stderr``.

    Returns
    -------
    (user, created):
        ``user`` is the ``User`` ORM object (existing or newly built).
        ``created`` is ``True`` when a new row was inserted, ``False``
        when the admin already existed and no changes were made.
    """
    existing = _find_admin_by_email(session, email)
    if existing is not None:
        _log.info(
            "bootstrap_admin: admin row already exists for %s — no-op",
            email,
        )
        return existing, False

    if password is None:
        generated = secrets.token_urlsafe(32)
        print(  # noqa: T201 — intentional: one-time credential to stderr
            f"[PROTEA bootstrap] Generated admin password for {email}: {generated}",
            file=sys.stderr,
            flush=True,
        )
        password = generated

    # Derive a stable username from the local-part of the email so the
    # unique constraint on ``username`` is satisfied without user input.
    username_candidate = email.split("@")[0]
    user = User(
        email=email,
        username=username_candidate,
        display_name=_BOOTSTRAP_DISPLAY_NAME,
        password_hash=hash_password(password),
        role=UserRole.ADMIN,
        status=UserStatus.ACTIVE,
    )
    session.add(user)
    try:
        session.flush()
    except IntegrityError:
        session.rollback()
        _log.warning(
            "bootstrap_admin: race condition detected for %s — re-checking",
            email,
        )
        session.begin()
        existing = _find_admin_by_email(session, email)
        if existing is not None:
            return existing, False
        raise

    _log.info("bootstrap_admin: created admin user %s (id=%s)", email, user.id)
    return user, True


__all__ = ["bootstrap_admin"]
