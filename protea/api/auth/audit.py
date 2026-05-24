"""Audit-log helpers for FARM-AUTH.9.

``audit_event`` is the single entry point for writing to ``auth_audit``.
It is deliberately fire-and-forget: any exception is caught, logged at
WARNING, and swallowed so that a transient DB error never surfaces as a
500 on a login or signup request.

Usage
-----

Call from within a request handler that already holds a live
``sqlalchemy.orm.Session``.  The session must be open (not committed and
closed) when this function is called; the insert is flushed inside the
call so it participates in the caller's transaction::

    with session_scope(factory) as session:
        # ... do the primary work ...
        audit_event(
            session,
            event_type="login_ok",
            actor_id=str(user.id),
            client_ip_hash=_hash_ip(request.client.host),
        )

The caller does NOT need to flush or commit separately; ``audit_event``
calls ``session.flush()`` itself so the row is visible in the same
transaction.

Event-type vocabulary (call-sites must use these exact strings):

* ``login_ok``       -- successful password authentication.
* ``login_fail``     -- bad credentials (actor_id is None).
* ``logout``         -- explicit sign-out.
* ``signup``         -- new account created (status=pending).
* ``admin_approve``  -- admin set status=active on target account.
* ``admin_revoke``   -- admin set status=deactivated on target account.
* ``role_change``    -- admin changed role on target account.
* ``password_change``-- user or admin changed a password.
* ``api_key_mint``   -- new API key created.
* ``api_key_revoke`` -- API key revoked.
"""

from __future__ import annotations

import logging
from typing import Any
from uuid import UUID

from sqlalchemy.orm import Session

from protea.infrastructure.orm.models.auth_audit import AuthAudit

logger = logging.getLogger(__name__)


def audit_event(
    session: Session,
    event_type: str,
    *,
    actor_id: str | UUID | None = None,
    target_id: str | UUID | None = None,
    client_ip_hash: str | None = None,
    **details: Any,
) -> None:
    """Insert one row into ``auth_audit``.

    Parameters
    ----------
    session:
        An open SQLAlchemy session. The row is flushed inside this call
        so it participates in the caller's transaction; the caller is
        responsible for the final commit.
    event_type:
        One of the event-type strings documented at the top of this module.
    actor_id:
        UUID (or str-UUID) of the user performing the action. Pass
        ``None`` for anonymous events such as a failed login where we do
        not know (and must not guess) the user identity.
    target_id:
        UUID (or str-UUID) of the user the action targets, if any. For
        example, an admin-approve action has an actor (the admin) and a
        target (the approved user).
    client_ip_hash:
        SHA-256 hex digest of the raw client IP address. Callers should
        hash the IP before passing it here; this module never hashes raw
        IPs itself so the PII decision stays at the call-site.
    **details:
        Arbitrary keyword arguments serialised as a JSON object into the
        ``details`` JSONB column. Keep values JSON-serialisable (str,
        int, bool, None, list, dict). UUIDs must be stringified before
        passing.
    """
    try:
        row = AuthAudit(
            event_type=event_type,
            actor_user_id=UUID(str(actor_id)) if actor_id is not None else None,
            target_user_id=UUID(str(target_id)) if target_id is not None else None,
            client_ip_hash=client_ip_hash,
            details=dict(details) if details else None,
        )
        session.add(row)
        session.flush()
    except Exception as exc:  # pragma: no cover
        logger.warning(
            "audit_event failed (event_type=%r, actor=%r): %s",
            event_type,
            actor_id,
            exc,
        )


__all__ = ["audit_event"]
