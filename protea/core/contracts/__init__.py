"""PROTEA core contracts package.

Re-exports the canonical abstractions so callers can import from one
well-known location::

    from protea.core.contracts import (
        Operation, OperationResult, EmitFn, ProteaPayload,
        MultiStagePayload, StageArtifactStore, Coordinator, PipelineStage,
    )
"""

from protea.core.contracts.multistage import (  # noqa: F401
    Coordinator,
    MultiStagePayload,
    PipelineStage,
    StageArtifactStore,
    publish_next_stage,
)
from protea.core.contracts.operation import (  # noqa: F401
    EmitFn,
    Operation,
    OperationResult,
    ProteaPayload,
    RetryLaterError,
    make_safe_emit,
)

__all__ = [
    "Coordinator",
    "EmitFn",
    "make_safe_emit",
    "MultiStagePayload",
    "Operation",
    "OperationResult",
    "PipelineStage",
    "ProteaPayload",
    "publish_next_stage",
    "RetryLaterError",
    "StageArtifactStore",
]
