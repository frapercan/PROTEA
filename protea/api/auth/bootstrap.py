"""Mint the first operator API key on a database that has none.

THE BOOTSTRAP PROBLEM. ``POST /auth/api-keys`` is gated by
``require_role("admin")``, so on an empty database nobody can call it, and
the keys it mints carry no role (they read as ``viewer``). Its router leaves
the first key "to a manual SQL insert", and the bootstrap modes ADR-D37
describes for the first admin are not implemented. A hand-written INSERT
has to reproduce the hashing and the prefix by hand, and leaves no audit
row.

This module is that step, built from the same pieces the router uses:
:func:`protea.api.auth.generate_raw_key`, :func:`protea.api.auth.hash_key`,
:func:`protea.api.auth.prefix_of`, the :class:`ApiKey` row, the role names of
:mod:`protea.api.roles`, and :func:`protea.api.auth.audit.audit_event` with
the ``api_key_mint`` event ADR-D37 lists.

IT ONLY EVER MINTS THE FIRST ONE. It refuses while any unrevoked key already
carries ``operator`` or ``admin`` (read through
:func:`protea.api.roles.normalise_role`, exactly as the gate reads it), so it
cannot become a second door around the admin gate. A lost key is revoked
first (``revoked_at``), after which the bootstrap works again.

The raw key exists only in the returned value: the row holds its sha256 and
its 8-character prefix, and the audit row holds the prefix. The caller prints
it once (``scripts/bootstrap_operator_key.py``).
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from protea.api.auth import generate_raw_key, hash_key, prefix_of
from protea.api.auth.audit import audit_event
from protea.api.roles import ROLE_ADMIN, ROLE_OPERATOR, normalise_role
from protea.infrastructure.orm.models.api_key import ApiKey

#: Roles that already open the bootstrap's door: an unrevoked key holding one
#: of them means the database is past its first operator key.
_OPERATOR_CAPABLE = frozenset({ROLE_OPERATOR, ROLE_ADMIN})

#: Matches the ``ApiKey.name`` column and the router's own bound.
_NAME_MAX = 255


class OperatorKeyExistsError(RuntimeError):
    """An unrevoked operator or admin key exists, so this is not the first."""


@dataclass(frozen=True)
class MintedKey:
    """The new key's identity, plus the raw value the caller shows once."""

    key_id: uuid.UUID
    prefix: str
    name: str
    role: str
    raw_key: str


def _clean_name(name: str) -> str:
    cleaned = name.strip() if isinstance(name, str) else ""
    if not cleaned:
        raise ValueError("the key needs a non-empty name")
    if len(cleaned) > _NAME_MAX:
        raise ValueError(f"the key name is longer than {_NAME_MAX} characters")
    return cleaned


def _lock_api_keys(session: Session) -> None:
    """Serialise two bootstraps on Postgres, so both cannot see "none" and mint.

    SHARE ROW EXCLUSIVE conflicts with itself and with the INSERT of any
    other writer, and not with the SELECTs the API runs to check keys. The
    lock ends with the transaction. Other dialects (unit tests) skip it.
    """
    if session.get_bind().dialect.name == "postgresql":
        session.execute(text("LOCK TABLE api_key IN SHARE ROW EXCLUSIVE MODE"))


def mint_first_operator_key(session: Session, *, name: str, source: str) -> MintedKey:
    """Insert the first operator API key and return it with its raw value.

    ``name`` is the label key listings show. ``source`` says what minted the
    key and is recorded on the audit row. Raises :class:`ValueError` for an
    unusable name and :class:`OperatorKeyExistsError` when an unrevoked
    operator or admin key exists. Nothing is committed here: the caller owns
    the transaction, and must show the raw key only after it commits.
    """
    label = _clean_name(name)
    _lock_api_keys(session)
    roles = session.scalars(select(ApiKey.role).where(ApiKey.revoked_at.is_(None))).all()
    if any(normalise_role(role) in _OPERATOR_CAPABLE for role in roles):
        raise OperatorKeyExistsError(
            "an unrevoked operator or admin API key already exists, so this "
            "database is past its first one; a lost key is revoked (revoked_at) "
            "before bootstrapping again"
        )

    raw = generate_raw_key()
    row = ApiKey(
        id=uuid.uuid4(),
        key_hash=hash_key(raw),
        prefix=prefix_of(raw),
        name=label,
        role=ROLE_OPERATOR,
    )
    session.add(row)
    session.flush()
    audit_event(
        session,
        "api_key_mint",
        key_id=str(row.id),
        prefix=row.prefix,
        name=label,
        role=ROLE_OPERATOR,
        source=source,
    )
    return MintedKey(key_id=row.id, prefix=row.prefix, name=label, role=ROLE_OPERATOR, raw_key=raw)


__all__ = ["MintedKey", "OperatorKeyExistsError", "mint_first_operator_key"]
