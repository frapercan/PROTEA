# protea/core/contracts/operation.py
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol

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
    """

    result: dict[str, Any] = field(default_factory=dict)
    progress_current: int | None = None
    progress_total: int | None = None


class ProteaPayload(BaseModel, frozen=True):
    """Immutable, strictly-typed base class for all operation payloads.

    Subclass and declare fields using Pydantic annotations.  Validation runs
    automatically via ``model_validate(dict)`` — no manual parsing needed.
    ``strict=True`` prevents silent type coercion (e.g. ``"yes"`` is not a
    valid ``bool``).
    """

    model_config = ConfigDict(strict=True)


class Operation(Protocol):
    """Protocol that every domain operation must satisfy.

    Operations are pure domain logic: they receive an open SQLAlchemy session
    and an ``emit`` callback for structured event logging, and return an
    ``OperationResult``.  They must not manage sessions or queue connections.
    """

    name: str

    def execute(self, session: Session, payload: dict[str, Any], *, emit: EmitFn) -> OperationResult:
        ...
