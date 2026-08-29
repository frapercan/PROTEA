from __future__ import annotations

import os
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

# Read-only observation of the dead-letter queue (no message mutation, no DB
# write) does not need the admin floor; operators legitimately use it to
# triage backlog before deciding whether to escalate to a destructive
# replay/purge.
_OperatorPrincipal = Annotated[
    ApiKey | BearerPrincipal | None,
    Depends(require_role("operator")),
]


def _authorize_reset_db(principal: ApiKey | BearerPrincipal | None) -> None:
    """Defense-in-depth gate for the irreversible ``DROP SCHEMA`` reset.

    The live DB has been wiped four times, so the destructive reset carries
    two extra checks on top of ``require_role("admin")``:

    1. Reject a ``None`` principal. The dev convenience that maps ``None`` to
       ``admin`` (``role_of(None) == ROLE_ADMIN``) is deliberately not
       accepted here, so a stack with auth disabled cannot drop the schema.
       A genuine admin is always an ``ApiKey`` or a ``BearerPrincipal``.
    2. Require the explicit opt-in sentinel ``PROTEA_ALLOW_DB_RESET=1``;
       return 403 when unset, even for a real admin.
    """
    if principal is None:
        raise HTTPException(
            status_code=401,
            detail=(
                "reset-db requires a real authenticated admin principal; "
                "the dev no-auth fallback is not accepted for this "
                "destructive operation"
            ),
        )
    if os.getenv("PROTEA_ALLOW_DB_RESET") != "1":
        raise HTTPException(
            status_code=403,
            detail=(
                "reset-db is disabled; set PROTEA_ALLOW_DB_RESET=1 on the API "
                "process to permit DROP SCHEMA public CASCADE"
            ),
        )


@router.post("/reset-db")
def reset_db(
    request: Request,
    _principal: _AdminPrincipal,
) -> dict:
    """Drop and recreate the public schema, then re-apply all Alembic migrations.

    Requires an authenticated ``admin`` principal (FARM-AUTH.4) plus the
    extra destructive-op guards in :func:`_authorize_reset_db`.
    """
    _authorize_reset_db(_principal)

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
    _principal: _OperatorPrincipal,
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
    """Filter for DLQ messages to re-enqueue back onto their source queue.

    WHY dry_run DEFAULTS TO TRUE. Every field here is optional, so ``{}`` is a
    valid body, and it used to mean "replay up to a thousand messages onto
    whatever queues they came from". The authentication gate is off in this
    deployment (``PROTEA_AUTHN_REQUIRED=false``, and ``role_of(None)`` returns
    admin), so one unauthenticated request with an empty body moved real work
    back onto live queues. The default is now the reading, and acting takes
    saying ``{"dry_run": false}`` on purpose. The response reports the same
    counts either way, so a caller that relied on the old default now learns
    what it would have done instead of doing it.
    """

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
        default=True,
        description=(
            "Count matches without re-enqueueing or acknowledging. Defaults to "
            "true: a request that says nothing gets an answer, not an action."
        ),
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
    """Filter for DLQ messages to permanently discard.

    Defaults to a dry run for the reason in :class:`DlqReplayRequest`, and more
    so: an empty body used to discard up to ten thousand messages with no
    filter and no undo. A dead letter is often the only surviving copy of the
    payload that produced it.
    """

    operation: str | None = Field(
        default=None, description="Restrict to messages whose payload `operation` matches."
    )
    first_death_queue: str | None = Field(
        default=None,
        description="Restrict to messages originally dead-lettered from this queue.",
    )
    dry_run: bool = Field(
        default=True,
        description=(
            "Count matches without removing anything. Defaults to true: this "
            "endpoint discards messages permanently and there is no undo."
        ),
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
