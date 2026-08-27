"""``/auth/api-keys`` — manage API keys (T5.6a first iteration).

Three endpoints:

* ``POST /auth/api-keys`` — mint a new key. Returns the raw value
  exactly once. Subsequent reads only expose the prefix and the name.
* ``GET  /auth/api-keys`` — list keys (prefix + name + state, no
  secret). Used by an operator dashboard.
* ``DELETE /auth/api-keys/{id}`` — revoke a key (sets ``revoked_at``).
  Revocation is irreversible; deleting the row outright is out of
  scope for this iteration (audit trail).

The endpoints themselves are intentionally **not** guarded by
``require_api_key`` in this first iteration: the bootstrap problem
(how does the first operator get a key?) is left to a manual SQL
insert or to a follow-up admin-token gate in T5.6b. Production
deployments should front this router with oauth2-proxy (T5.6c) or a
similar trusted layer.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from pydantic import BaseModel, Field, field_validator
from sqlalchemy.orm import Session, sessionmaker

from protea.api.auth import generate_raw_key, hash_key, prefix_of
from protea.api.deps import get_session_factory
from protea.api.rate_limit import api_keys_limit, limiter
from protea.api.roles import ROLE_ADMIN, require_role
from protea.core.utils import utcnow
from protea.infrastructure.orm.models.api_key import ApiKey
from protea.infrastructure.session import session_scope

router = APIRouter(prefix="/auth/api-keys", tags=["auth"])


class CreateApiKeyRequest(BaseModel):
    """Body for ``POST /auth/api-keys``.

    The caller only chooses the human-readable label; entropy is
    generated server-side so we never trust a client-supplied value.
    """

    model_config = {
        "extra": "forbid",
        "json_schema_extra": {
            "example": {"name": "lab-runner-2026-05"},
        },
    }

    name: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description=(
            "Human-readable label shown in listings. Free-form (no "
            "validation beyond length); typically the service or "
            "operator the key belongs to."
        ),
    )

    @field_validator("name", mode="before")
    @classmethod
    def _strip_name(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("name must be a non-empty string")
        return v.strip()


def _to_summary(row: ApiKey) -> dict[str, Any]:
    """Serialise an ``ApiKey`` row for list / lookup responses.

    Never includes the hash or any value that could be used to
    impersonate the holder; only the prefix (for display) and the
    lifecycle timestamps.
    """
    return {
        "id": str(row.id),
        "prefix": row.prefix,
        "name": row.name,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "revoked_at": row.revoked_at.isoformat() if row.revoked_at else None,
        "last_used_at": row.last_used_at.isoformat() if row.last_used_at else None,
    }


@router.post(
    "",
    status_code=201,
    summary="Mint a new API key",
    dependencies=[Depends(require_role(ROLE_ADMIN))],
)
@limiter.limit(api_keys_limit)
def create_api_key(
    request: Request,
    response: Response,
    body: CreateApiKeyRequest,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Create a fresh API key and return its raw value **once**.

    Response shape::

        {
          "id": "<uuid>",
          "prefix": "abc12345",
          "name": "lab-runner-2026-05",
          "key": "<the only chance to copy this>",
          "created_at": "..."
        }

    The raw ``key`` field is the value the caller should store in their
    secret manager / CI. PROTEA stores only the sha256 hash + the
    8-char prefix; we cannot recover the value if it is lost (just mint
    another and revoke the misplaced one).
    """
    raw = generate_raw_key()
    row = ApiKey(
        key_hash=hash_key(raw),
        prefix=prefix_of(raw),
        name=body.name,
    )
    with session_scope(factory) as session:
        session.add(row)
        session.flush()
        snapshot = _to_summary(row)

    return {**snapshot, "key": raw}


@router.get("", summary="List API keys", dependencies=[Depends(require_role(ROLE_ADMIN))])
def list_api_keys(
    include_revoked: bool = Query(
        default=False,
        description=(
            "When false (default), revoked keys are hidden from the "
            "listing. Set true to audit the full history."
        ),
    ),
    limit: int = Query(
        default=50,
        ge=1,
        le=500,
        description="Max rows per page; capped at 500.",
    ),
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> list[dict[str, Any]]:
    """Return registered API keys newest-first.

    The response never includes the secret — only the prefix and the
    name. Use this endpoint to confirm a key was created or to look
    up the id of a key you want to revoke.
    """
    with session_scope(factory) as session:
        q = session.query(ApiKey)
        if not include_revoked:
            q = q.filter(ApiKey.revoked_at.is_(None))
        rows = q.order_by(ApiKey.created_at.desc()).limit(limit).all()
        return [_to_summary(r) for r in rows]


@router.delete(
    "/{key_id}", summary="Revoke an API key", dependencies=[Depends(require_role(ROLE_ADMIN))]
)
def revoke_api_key(
    key_id: UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Mark an API key as revoked.

    Sets ``revoked_at`` to the current UTC timestamp. Subsequent uses
    of the key are rejected by :func:`require_api_key` with a 401.
    The row is preserved (not deleted) so the audit trail of
    historical access stays intact.
    """
    with session_scope(factory) as session:
        row = session.get(ApiKey, key_id)
        if row is None:
            raise HTTPException(status_code=404, detail="API key not found")
        if row.revoked_at is not None:
            return _to_summary(row)
        row.revoked_at = utcnow()
        session.flush()
        return _to_summary(row)
