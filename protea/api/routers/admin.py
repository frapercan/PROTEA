from __future__ import annotations

import os
import subprocess
from pathlib import Path

from fastapi import APIRouter, Header, HTTPException
from starlette.requests import Request

from protea.infrastructure.session import build_session_factory
from protea.infrastructure.settings import load_settings

router = APIRouter(prefix="/admin", tags=["admin"])

_PROJECT_ROOT = Path(__file__).resolve().parents[3]

_ADMIN_TOKEN = os.getenv("PROTEA_ADMIN_TOKEN", "")


def _require_admin_token(authorization: str | None) -> None:
    """Validate bearer token for destructive admin endpoints."""
    if not _ADMIN_TOKEN:
        raise HTTPException(
            status_code=403,
            detail="Admin operations are disabled — set PROTEA_ADMIN_TOKEN env var to enable.",
        )
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Bearer token.")
    if authorization[7:] != _ADMIN_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid admin token.")


@router.post("/reset-db")
def reset_db(request: Request, authorization: str | None = Header(default=None)) -> dict:
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
