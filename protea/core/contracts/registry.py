# protea/core/contracts/registry.py
from __future__ import annotations

from typing import Dict

from protea.core.contracts.operation import Operation


class OperationRegistry:
    def __init__(self) -> None:
        self._ops: Dict[str, Operation] = {}

    def register(self, op: Operation) -> None:
        if op.name in self._ops:
            raise ValueError(f"Operation already registered: {op.name}")
        self._ops[op.name] = op

    def get(self, name: str) -> Operation:
        try:
            return self._ops[name]
        except KeyError as e:
            raise KeyError(f"Unknown operation: {name}") from e
