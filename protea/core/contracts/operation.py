# protea/core/contracts/operation.py
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol

from sqlalchemy.orm import Session

Level = Literal["info", "warning", "error"]
EmitFn = Callable[[str, str | None, dict[str, Any], Level], None]


@dataclass(frozen=True)
class OperationResult:
    result: dict[str, Any] = field(default_factory=dict)
    progress_current: int | None = None
    progress_total: int | None = None


class PayloadModel(Protocol):
    @staticmethod
    def from_dict(d: dict[str, Any]) -> Any:
        ...


class Operation(Protocol):
    name: str

    def execute(self, session: Session, payload: dict[str, Any], *, emit: EmitFn) -> OperationResult:
        ...
