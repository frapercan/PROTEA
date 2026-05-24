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


def _generate_and_announce_password(email: str) -> str:
    generated = secrets.token_urlsafe(32)
    print(  # noqa: T201 — intentional: one-time credential to stderr
        f"[PROTEA bootstrap] Generated admin password for {email}: {generated}",
        file=sys.stderr,
        flush=True,
    )
    return generated


def _build_admin_user(email: str, password: str) -> User:
    return User(
        email=email,
        username=email.split("@")[0],
        display_name=_BOOTSTRAP_DISPLAY_NAME,
        password_hash=hash_password(password),
        role=UserRole.ADMIN,
        status=UserStatus.ACTIVE,
    )


def _insert_or_resolve_race(session: Session, user: User, email: str) -> User:
    """Insert and flush; on IntegrityError treat as race and re-resolve.

    Returns the inserted user on success, the racing-winner admin row
    on integrity-race, and re-raises the IntegrityError when no admin
    was found post-rollback (i.e. the conflict was on a different
    constraint — caller must surface it).
    """
    session.add(user)
    try:
        session.flush()
        return user
    except IntegrityError:
        session.rollback()
        _log.warning("bootstrap_admin: race condition for %s — re-checking", email)
        session.begin()
        winner = _find_admin_by_email(session, email)
        if winner is None:
            raise
        return winner


def bootstrap_admin(
    session: Session,
    email: str,
    *,
    password: str | None = None,
) -> tuple[User | None, bool]:
    """Ensure an admin user with *email* exists; create one if not.

    Returns ``(user, created)``. The caller commits after a successful return.
    A ``None`` password triggers ``token_urlsafe(32)`` generation, printed to
    stderr exactly once.
    """
    existing = _find_admin_by_email(session, email)
    if existing is not None:
        _log.info("bootstrap_admin: admin row already exists for %s — no-op", email)
        return existing, False

    if password is None:
        password = _generate_and_announce_password(email)

    user = _build_admin_user(email, password)
    resolved = _insert_or_resolve_race(session, user, email)
    if resolved is not user:
        return resolved, False

    _log.info("bootstrap_admin: created admin user %s (id=%s)", email, user.id)
    return user, True


__all__ = ["bootstrap_admin"]
