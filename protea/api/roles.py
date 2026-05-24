"""Role-based gates for FEAT-AUTH (WAVE-2 2026-05-23).

PROTEA recognises three roles, ordered by privilege:

* ``viewer`` (default) — read-only access; cannot mutate state.
* ``operator`` — can dispatch jobs (``POST /datasets``), run
  maintenance vacuums, and otherwise drive the pipeline.
* ``admin`` — destructive ops (``POST /admin/reset-db``); reserved
  for the operator persona holding the platform reins.

The role is carried by the JWT minted at ``POST /auth/api-key-login``; the
``ApiKey`` row holds the source-of-truth value (nullable, defaults
to ``viewer`` at decode time). Routes opt into a gate via the
:func:`require_role` dependency factory.
"""

from __future__ import annotations

from typing import Any

from fastapi import Depends, HTTPException

from protea.api.auth import require_api_key_or_bearer
from protea.api.bearer import BearerPrincipal
from protea.infrastructure.orm.models.api_key import ApiKey

ROLE_VIEWER = "viewer"
ROLE_OPERATOR = "operator"
ROLE_ADMIN = "admin"

_RANK: dict[str, int] = {
    ROLE_VIEWER: 0,
    ROLE_OPERATOR: 1,
    ROLE_ADMIN: 2,
}


def normalise_role(raw: Any) -> str:
    """Coerce a free-form role value to one of the canonical names.

    Anything unrecognised collapses to ``viewer`` so a malformed JWT
    or a stale key cannot accidentally escalate privileges; the gate
    treats ``viewer`` as the default-deny baseline.
    """
    if not isinstance(raw, str):
        return ROLE_VIEWER
    candidate = raw.strip().lower()
    if candidate in _RANK:
        return candidate
    return ROLE_VIEWER


def role_of(principal: ApiKey | BearerPrincipal | None) -> str:
    """Extract the effective role from an auth principal.

    ``None`` (gate disabled in dev) returns ``admin`` so local stacks
    keep working without a token. API keys read the column directly
    (legacy rows surface as ``viewer``); bearer tokens read the
    ``role`` JWT claim.
    """
    if principal is None:
        return ROLE_ADMIN
    if isinstance(principal, BearerPrincipal):
        return normalise_role(principal.claims.get("role"))
    return normalise_role(getattr(principal, "role", None))


def require_role(min_role: str):
    """Return a FastAPI dependency that enforces ``min_role`` or higher.

    Usage::

        @router.post("/x", dependencies=[Depends(require_role("operator"))])
        def x(): ...

    Always wraps :func:`require_api_key_or_bearer` so any route that
    opts into a role gate also gets authenticated; this keeps the
    two concerns in one place at the router declaration.
    """
    floor = _RANK[normalise_role(min_role)] if min_role in _RANK else _RANK[ROLE_VIEWER]
    if min_role not in _RANK:
        raise ValueError(f"unknown role: {min_role!r}")

    def _gate(
        principal: ApiKey | BearerPrincipal | None = Depends(require_api_key_or_bearer),
    ) -> ApiKey | BearerPrincipal | None:
        role = role_of(principal)
        if _RANK[role] < floor:
            raise HTTPException(
                status_code=403,
                detail=f"role '{role}' is insufficient (requires '{min_role}')",
            )
        return principal

    return _gate


__all__ = [
    "ROLE_ADMIN",
    "ROLE_OPERATOR",
    "ROLE_VIEWER",
    "normalise_role",
    "require_role",
    "role_of",
]
