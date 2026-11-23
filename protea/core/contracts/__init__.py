"""PROTEA core contracts package.

Re-exports the canonical abstractions so callers can import from one
well-known location::

    from protea.core.contracts import (
        Operation, OperationResult, EmitFn, ProteaPayload,
    )
"""

from protea.core.contracts.operation import (  # noqa: F401
    EmitFn,
    Operation,
    OperationResult,
    ProteaPayload,
    RetryLaterError,
    make_safe_emit,
)

__all__ = [
    "EmitFn",
    "make_safe_emit",
    "Operation",
    "OperationResult",
    "ProteaPayload",
    "RetryLaterError",
]
