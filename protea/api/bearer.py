"""Bearer JWT authentication (T5.6b — second auth iteration).

Adds an ``Authorization: Bearer <jwt>`` flow alongside the API-key
dependency from T5.6a. Both are accepted via the combined dependency
:func:`require_api_key_or_bearer`.

Algorithm
---------

* HS256 with a shared secret from ``PROTEA_JWT_SECRET``.
* Minimum payload: ``{sub, exp, iat}``. ``aud`` and ``iss`` are accepted
  if present; we do not validate them in this iteration.
* On startup, when ``PROTEA_AUTHN_REQUIRED=true`` AND
  ``PROTEA_JWT_SECRET`` is missing the API process must fail loudly —
  see :func:`assert_bearer_config` invoked from ``create_app``.

Why HS256 (not RS256)?
----------------------

PROTEA does not issue tokens itself in this slice (T5.6b is consumer
only). The thesis dev stack signs tokens out-of-band with a shared
secret and the secret is rotated manually. RS256 / OIDC lands in
T5.6c (post-defensa) together with the oauth2-proxy fronting layer.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any

import jwt
from fastapi import HTTPException, Request
from jwt.exceptions import InvalidTokenError

logger = logging.getLogger(__name__)

_BEARER_SCHEME = "Bearer"  # ``Authorization: Bearer <jwt>``
_JWT_ALGORITHM = "HS256"

# Env knobs (centralised so tests can monkeypatch them).
_ENV_SECRET = "PROTEA_JWT_SECRET"
_ENV_AUTHN_REQUIRED = "PROTEA_AUTHN_REQUIRED"


@dataclass(frozen=True)
class BearerPrincipal:
    """Subject + raw claims surfaced to handlers that want them.

    Mirrors the :class:`ApiKey` snapshot shape returned by
    :func:`require_api_key` so the combined dependency can hand back
    one or the other without callers having to discriminate at the
    type level.
    """

    sub: str
    claims: dict[str, Any]


def _authn_required() -> bool:
    """Mirror of :func:`protea.api.auth._authn_required` to avoid a circular import."""
    raw = os.getenv(_ENV_AUTHN_REQUIRED)
    if raw is None:
        return True
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _read_secret() -> str | None:
    """Return the configured HS256 secret or ``None`` if unset."""
    raw = os.getenv(_ENV_SECRET)
    if raw is None or not raw.strip():
        return None
    return raw


def assert_bearer_config() -> None:
    """Fail loudly on startup when auth is on and the secret is missing.

    Call from ``create_app`` BEFORE the routers are mounted so the
    process exits with a clear error message rather than 500-ing every
    bearer request at runtime. When ``PROTEA_AUTHN_REQUIRED=false`` we
    skip the check (dev stacks are allowed to operate without a secret;
    the gate short-circuits anyway).
    """
    if not _authn_required():
        return
    if _read_secret() is None:
        raise RuntimeError(
            "PROTEA_JWT_SECRET is not set but PROTEA_AUTHN_REQUIRED is on. "
            "Set the env var to a non-empty value or disable authn for "
            "local development (PROTEA_AUTHN_REQUIRED=false)."
        )


def extract_bearer_token(authorization: str | None) -> str | None:
    """Return the raw JWT from an ``Authorization: Bearer <jwt>`` header.

    Any other scheme (``ApiKey``, ``Basic``, ...) returns ``None`` so
    the caller can fall through to the next auth mechanism without
    swallowing tokens that belong to another dependency.
    """
    if not authorization:
        return None
    token = authorization.strip()
    prefix = f"{_BEARER_SCHEME.lower()} "
    if not token.lower().startswith(prefix):
        return None
    candidate = token[len(_BEARER_SCHEME) + 1 :].strip()
    return candidate or None


def decode_bearer_token(token: str) -> BearerPrincipal:
    """Validate signature + ``exp`` and return the principal.

    Raises :class:`HTTPException` 401 on every failure mode (expired,
    bad signature, missing required claim, malformed). The same status
    code is used for every cause so the API does not leak which part
    of the token was rejected.
    """
    secret = _read_secret()
    if secret is None:  # pragma: no cover — assert_bearer_config catches this earlier
        raise HTTPException(
            status_code=401,
            detail="Bearer authentication is not configured",
            headers={"WWW-Authenticate": _BEARER_SCHEME},
        )
    try:
        payload: dict[str, Any] = jwt.decode(
            token,
            secret,
            algorithms=[_JWT_ALGORITHM],
            options={"require": ["exp", "iat", "sub"]},
        )
    except InvalidTokenError as exc:
        raise HTTPException(
            status_code=401,
            detail=f"Invalid bearer token: {exc}",
            headers={"WWW-Authenticate": _BEARER_SCHEME},
        ) from exc

    sub = payload.get("sub")
    if not isinstance(sub, str) or not sub:
        raise HTTPException(
            status_code=401,
            detail="Bearer token missing 'sub' claim",
            headers={"WWW-Authenticate": _BEARER_SCHEME},
        )
    return BearerPrincipal(sub=sub, claims=payload)


def require_bearer(
    request: Request,
) -> BearerPrincipal | None:
    """Standalone bearer dep — used directly only for tests / dev token.

    Production routes use :func:`require_api_key_or_bearer` so either
    scheme is accepted. We read the header off the request directly
    (instead of a ``Header()`` arg) so the dep stays interchangeable
    with the combined variant.
    """
    if not _authn_required():
        return None
    token = extract_bearer_token(request.headers.get("authorization"))
    if token is None:
        raise HTTPException(
            status_code=401,
            detail="Missing bearer token",
            headers={"WWW-Authenticate": _BEARER_SCHEME},
        )
    return decode_bearer_token(token)


__all__ = [
    "BearerPrincipal",
    "assert_bearer_config",
    "decode_bearer_token",
    "extract_bearer_token",
    "require_bearer",
]
