from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from starlette.requests import Request

from protea.api.bearer import BearerPrincipal
from protea.api.deps import get_amqp_url
from protea.api.roles import require_role
from protea.infrastructure.orm.models.api_key import ApiKey
from protea.infrastructure.session import build_session_factory
from protea.infrastructure.settings import load_settings
from protea.services.dlq_manager import dlq_purge, dlq_replay, dlq_summary

router = APIRouter(prefix="/admin", tags=["admin"])

_PROJECT_ROOT = Path(__file__).resolve().parents[3]

_AdminPrincipal = Annotated[
    ApiKey | BearerPrincipal | None,
    Depends(require_role("admin")),
]


@router.post("/reset-db")
def reset_db(
    request: Request,
    _principal: _AdminPrincipal,
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


# ---------------------------------------------------------------------------
# DLQ management (F-OPS-JOBS.3)
# ---------------------------------------------------------------------------


@router.get("/dlq/summary")
def get_dlq_summary(
    _principal: _AdminPrincipal,
    amqp_url: str = Depends(get_amqp_url),
    max_peek: int = Query(
        default=500,
        ge=1,
        le=5000,
        description="Upper bound on DLQ messages peeked for the summary; capped at 5000.",
    ),
) -> dict[str, Any]:
    """Grouped count of dead-letter messages by operation, source queue, and age.

    Peeks up to ``max_peek`` messages from ``protea.dead-letter`` without
    consuming them, groups them by ``{operation, first_death_queue, age_bucket}``,
    and re-queues all peeked messages before returning.  The DLQ depth is
    unchanged after this call.
    """
    try:
        return dlq_summary(amqp_url, max_peek=max_peek)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"DLQ unreachable: {exc}") from exc


class DlqReplayRequest(BaseModel):
    """Filter for DLQ messages to re-enqueue back onto their source queue."""

    operation: str | None = Field(
        default=None, description="Restrict to messages whose payload `operation` matches."
    )
    first_death_queue: str | None = Field(
        default=None,
        description="Restrict to messages originally dead-lettered from this queue.",
    )
    target_queue: str | None = Field(
        default=None,
        description="Override the destination queue. Defaults to the original source queue.",
    )
    dry_run: bool = Field(
        default=False,
        description="If true, count matches without re-enqueueing or acknowledging.",
    )
    max_messages: int = Field(
        default=1000, description="Upper bound on messages processed in one call."
    )


@router.post("/dlq/replay")
def replay_dlq(
    body: DlqReplayRequest,
    _principal: _AdminPrincipal,
    amqp_url: str = Depends(get_amqp_url),
) -> dict[str, Any]:
    """Re-enqueue DLQ messages matching the filter.

    Matching messages are published back to their original source queue (or
    ``target_queue`` if specified) and acked from the DLQ.  Non-matching
    messages remain in the DLQ.

    ``dry_run=True`` reports how many messages *would* be replayed without
    actually moving them.
    """
    try:
        return dlq_replay(
            amqp_url,
            operation_filter=body.operation,
            queue_filter=body.first_death_queue,
            target_queue=body.target_queue,
            dry_run=body.dry_run,
            max_messages=body.max_messages,
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"DLQ replay failed: {exc}") from exc


class DlqPurgeRequest(BaseModel):
    """Filter for DLQ messages to permanently discard."""

    operation: str | None = Field(
        default=None, description="Restrict to messages whose payload `operation` matches."
    )
    first_death_queue: str | None = Field(
        default=None,
        description="Restrict to messages originally dead-lettered from this queue.",
    )
    dry_run: bool = Field(
        default=False,
        description="If true, count matches without removing anything from the DLQ.",
    )
    max_messages: int = Field(
        default=10000, description="Upper bound on messages processed in one call."
    )


@router.post("/dlq/purge")
def purge_dlq(
    body: DlqPurgeRequest,
    _principal: _AdminPrincipal,
    amqp_url: str = Depends(get_amqp_url),
) -> dict[str, Any]:
    """Discard DLQ messages matching the filter.

    Matching messages are acked (permanently removed from the DLQ).
    Non-matching messages remain in the DLQ.

    ``dry_run=True`` reports how many messages *would* be purged without
    removing them.  Always prefer a dry-run first.
    """
    try:
        return dlq_purge(
            amqp_url,
            operation_filter=body.operation,
            queue_filter=body.first_death_queue,
            dry_run=body.dry_run,
            max_messages=body.max_messages,
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"DLQ purge failed: {exc}") from exc
