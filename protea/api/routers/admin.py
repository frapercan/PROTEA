from __future__ import annotations

import os
import subprocess
from pathlib import Path

from fastapi import APIRouter, Header, HTTPException
from starlette.requests import Request

from protea.api.bearer import decode_bearer_token, extract_bearer_token
from protea.api.roles import ROLE_ADMIN, role_of
from protea.infrastructure.session import build_session_factory
from protea.infrastructure.settings import load_settings

router = APIRouter(prefix="/admin", tags=["admin"])

_PROJECT_ROOT = Path(__file__).resolve().parents[3]

_ADMIN_TOKEN = os.getenv("PROTEA_ADMIN_TOKEN", "")


def _try_admin_role_bearer(authorization: str | None) -> bool:
    """FEAT-AUTH: accept an admin-role session JWT alongside the legacy token.

    Returns True when the ``Authorization: Bearer <jwt>`` header decodes
    successfully AND carries ``role=admin``. Any failure (missing
    header, bad signature, missing claim, wrong role) returns False
    silently so the caller can fall back to the legacy token gate.
    """
    token = extract_bearer_token(authorization)
    if token is None:
        return False
    try:
        principal = decode_bearer_token(token)
    except HTTPException:
        return False
    return role_of(principal) == ROLE_ADMIN


def _require_admin_token(authorization: str | None) -> None:
    """Validate bearer token for destructive admin endpoints.

    Accepts either:

    * a session JWT (minted by ``/auth/login``) whose ``role`` claim
      is ``admin`` (FEAT-AUTH path), or
    * the legacy ``PROTEA_ADMIN_TOKEN`` env-var match (kept for the
      bootstrap flow until the api-keys-management UI lands).
    """
    if _try_admin_role_bearer(authorization):
        return
    if not _ADMIN_TOKEN:
        raise HTTPException(
            status_code=403,
            detail="Admin operations are disabled; set PROTEA_ADMIN_TOKEN env var to enable.",
        )
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Bearer token.")
    if authorization[7:] != _ADMIN_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid admin token.")


@router.post("/reset-db")
def reset_db(
    request: Request,
    authorization: str | None = Header(
        default=None,
        description=(
            "Bearer token matching ``PROTEA_ADMIN_TOKEN`` env var. "
            "Required: requests without it are rejected with 403."
        ),
    ),
) -> dict:
    """Drop and recreate the public schema, then re-apply all Alembic migrations."""
    _require_admin_token(authorization)
    settings = load_settings(_PROJECT_ROOT)

    # 1. Drop + recreate schema using a raw connection (outside SQLAlchemy pool)
    import psycopg

    with psycopg.connect(settings.db_url.replace("+psycopg", ""), autocommit=True) as conn:
        conn.execute("DROP SCHEMA public CASCADE")
        conn.execute("CREATE SCHEMA public")

    # 2. Re-apply migrations
    result = subprocess.run(
        ["poetry", "run", "alembic", "upgrade", "head"],
        cwd=str(_PROJECT_ROOT),
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return {"ok": False, "error": result.stderr}

    # 3. Rebuild the session factory on app state so new connections use the fresh schema
    request.app.state.session_factory = build_session_factory(settings.db_url)

    return {"ok": True}
