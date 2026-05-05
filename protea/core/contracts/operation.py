# protea/core/contracts/operation.py
from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol
from uuid import UUID

from pydantic import BaseModel, ConfigDict
from sqlalchemy.orm import Session

Level = Literal["info", "warning", "error"]
EmitFn = Callable[[str, str | None, dict[str, Any], Level], None]

_safe_emit_logger = logging.getLogger("protea.emit")


def make_safe_emit(raw_emit: EmitFn) -> EmitFn:
    """Wrap a raw EmitFn so failures are logged and never propagate.

    The platform-level emit may fail for transient reasons (DB
    connection lost mid-operation, JobEvent insert conflict, etc.).
    Operations should not crash because the audit trail hiccupped:
    the operation's primary work matters more than the event row.
    Failures are logged at ERROR with full traceback so they remain
    visible in observability without breaking the running job.
    """

    def wrapped(
        event: str,
        message: str | None = None,
        fields: dict[str, Any] | None = None,
        level: Level = "info",
    ) -> None:
        try:
            raw_emit(event, message, fields or {}, level)
        except Exception:
            _safe_emit_logger.error(
                "emit failed; operation continues. event=%s level=%s",
                event,
                level,
                exc_info=True,
            )

    return wrapped


@dataclass(frozen=True)
class OperationResult:
    """Return value of every Operation.execute() call.

    ``result`` is a free-form dict that gets stored in ``Job.meta`` and
    surfaced in the job detail view. ``progress_current`` / ``progress_total``
    are written back to the Job row so the UI can render a progress bar.

    ``deferred`` — if True, BaseWorker will NOT transition the job to SUCCEEDED.
    Use this for coordinator operations that delegate work to child jobs; the
    last child is responsible for marking the parent SUCCEEDED.

    ``publish_after_commit`` — list of (queue_name, job_id) pairs that BaseWorker
    will publish to RabbitMQ *after* the DB commit, guaranteeing workers always
    find the child job row before they try to claim it.
    """

    result: dict[str, Any] = field(default_factory=dict)
    progress_current: int | None = None
    progress_total: int | None = None
    deferred: bool = False
    publish_after_commit: list[tuple[str, UUID]] = field(default_factory=list)
    publish_operations: list[tuple[str, dict[str, Any]]] = field(default_factory=list)


class RetryLaterError(Exception):
    """Raised by an operation when it cannot run yet but should be retried.

    BaseWorker resets the job to QUEUED and the consumer re-publishes the
    message after ``delay_seconds``, leaving the GPU free for other work.
    """

    def __init__(self, reason: str, delay_seconds: int = 60) -> None:  # noqa: B042
        super().__init__(reason)
        self.delay_seconds = delay_seconds


class ProteaPayload(BaseModel):
    """Immutable, strictly-typed base class for all operation payloads.

    Subclass and declare fields using Pydantic annotations.  Validation runs
    automatically via ``model_validate(dict)`` — no manual parsing needed.
    ``strict=True`` prevents silent type coercion (e.g. ``"yes"`` is not a
    valid ``bool``).
    """

    model_config = ConfigDict(strict=True, frozen=True)


class Operation(Protocol):
    """Protocol that every domain operation must satisfy.

    Operations are pure domain logic: they receive an open SQLAlchemy session
    and an ``emit`` callback for structured event logging, and return an
    ``OperationResult``.  They must not manage sessions or queue connections.

    ``description`` is a short, static, human-readable explanation of what
    the operation does in general, surfaced in the jobs UI to give the
    operations history context.

    ``summarize_payload`` returns a 1-line dynamic summary of the most
    informative fields in the payload (e.g. "release 211 ← OBO 2022-07-01"),
    so each individual job in the history tells its own story.  Returning an
    empty string is acceptable for operations with no useful payload.
    """

    name: str
    description: str

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult: ...

    def summarize_payload(self, payload: dict[str, Any]) -> str: ...
