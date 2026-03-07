# protea/core/contracts/operation.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Literal, Optional, Protocol

from sqlalchemy.orm import Session

Level = Literal["info", "warning", "error"]
EmitFn = Callable[[str, Optional[str], Dict[str, Any], Level], None]


@dataclass(frozen=True)
class OperationResult:
    result: Dict[str, Any] = field(default_factory=dict)
    progress_current: Optional[int] = None
    progress_total: Optional[int] = None


class PayloadModel(Protocol):
    @staticmethod
    def from_dict(d: Dict[str, Any]) -> Any:
        ...


class Operation(Protocol):
    name: str

    def execute(self, session: Session, payload: Dict[str, Any], *, emit: EmitFn) -> OperationResult:
        ...
