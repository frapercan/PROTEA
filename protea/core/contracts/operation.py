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
    result: dict[str, Any] = field(default_factory=dict)
    progress_current: int | None = None
    progress_total: int | None = None


class ProteaPayload(BaseModel, frozen=True):
    """Base class for all operation payloads. Subclass and add fields with Pydantic validators."""

    model_config = ConfigDict(strict=True)


class Operation(Protocol):
    name: str

    def execute(self, session: Session, payload: dict[str, Any], *, emit: EmitFn) -> OperationResult:
        ...
