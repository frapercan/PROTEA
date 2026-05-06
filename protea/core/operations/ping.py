# protea/core/operations/ping.py
from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, Operation, OperationResult


class PingOperation(Operation):
    name = "ping"
    description = "Smoke-test operation that emits two events and returns immediately."

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        emit("ping.start", "Ping received", {"payload_keys": list(payload.keys())}, "info")
        emit("ping.done", "Ping finished", {}, "info")
        return OperationResult(result={"ok": True})

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        return ""
