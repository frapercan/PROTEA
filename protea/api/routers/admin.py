from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Depends
from starlette.requests import Request

from protea.api.bearer import BearerPrincipal
from protea.api.roles import require_role
from protea.infrastructure.orm.models.api_key import ApiKey
from protea.infrastructure.session import build_session_factory
from protea.infrastructure.settings import load_settings

router = APIRouter(prefix="/admin", tags=["admin"])

_PROJECT_ROOT = Path(__file__).resolve().parents[3]


@router.post("/reset-db")
def reset_db(
    request: Request,
    _principal: Annotated[ApiKey | BearerPrincipal | None, Depends(require_role("admin"))],
) -> dict:
    """Drop and recreate the public schema, then re-apply all Alembic migrations.

    Requires an authenticated principal with the ``admin`` role (FARM-AUTH.4).
    """
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
