# protea/core/contracts/operation.py
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol
from uuid import UUID

from pydantic import BaseModel, ConfigDict
from sqlalchemy.orm import Session

Level = Literal["info", "warning", "error"]
EmitFn = Callable[[str, str | None, dict[str, Any], Level], None]


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
    """

    name: str

    def execute(self, session: Session, payload: dict[str, Any], *, emit: EmitFn) -> OperationResult:
        ...
