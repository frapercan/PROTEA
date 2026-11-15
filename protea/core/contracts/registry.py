# protea/core/contracts/registry.py
from __future__ import annotations

from protea.core.contracts.operation import Operation
from protea.core.operation_spec import OperationSpec


class OperationRegistry:
    """Operations by name, each with the spec it declared (ADR-D60, decision 6).

    Routing, the consumer a worker runs, the checks of ``POST /jobs`` and the stages
    the cluster may run as file stages all read the specs, so an operation without one
    cannot enter.
    """

    def __init__(self) -> None:
        self._ops: dict[str, Operation] = {}
        self._specs: dict[str, OperationSpec] = {}

    def register(self, op: Operation, spec: OperationSpec) -> None:
        """Register ``op`` under its name with the spec it declares.

        Refused: a name already registered, a missing spec, and a spec whose queue
        already carries the other kind of message, since one queue has one consumer.
        """
        if op.name in self._ops:
            raise ValueError(f"Operation already registered: {op.name}")
        if not isinstance(spec, OperationSpec):
            raise TypeError(
                f"Operation {op.name} declares no OperationSpec: every operation states "
                "its domain, kind, queue, purity, resource, family, publishes_to and "
                "status (ADR-D60, decision 6)"
            )
        clash = self._other_consumer_on(spec)
        if clash is not None:
            raise ValueError(
                f"Operation {op.name} is a {spec.kind} on {spec.queue}, where {clash} is a "
                f"{self._specs[clash].kind}: a queue carries Job ids or operation "
                "messages, never both"
            )
        self._ops[op.name] = op
        self._specs[op.name] = spec

    def _other_consumer_on(self, spec: OperationSpec) -> str | None:
        """A registered operation on ``spec``'s queue that needs the other consumer."""
        for name, other in self._specs.items():
            if other.queue == spec.queue and other.carries_jobs != spec.carries_jobs:
                return name
        return None

    def get(self, name: str) -> Operation:
        try:
            return self._ops[name]
        except KeyError as e:
            raise KeyError(f"Unknown operation: {name}") from e

    def spec(self, name: str) -> OperationSpec:
        """The spec ``name`` declared when it was registered."""
        try:
            return self._specs[name]
        except KeyError as e:
            raise KeyError(f"Unknown operation: {name}") from e

    def specs(self) -> dict[str, OperationSpec]:
        """Every registered operation's spec, by name (a copy)."""
        return dict(self._specs)
