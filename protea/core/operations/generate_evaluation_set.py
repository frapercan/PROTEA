from __future__ import annotations

import uuid
from typing import Any

from pydantic import field_validator
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.core.evaluation import compute_evaluation_data
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet


class GenerateEvaluationSetPayload(ProteaPayload, frozen=True):
    old_annotation_set_id: str
    new_annotation_set_id: str

    @field_validator("old_annotation_set_id", "new_annotation_set_id", mode="before")
    @classmethod
    def must_be_non_empty(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("must be a non-empty string")
        return v.strip()


class GenerateEvaluationSetOperation:
    """Computes the CAFA evaluation delta between two GOA annotation sets.

    Applies experimental evidence code filtering, NOT-qualifier exclusion with
    GO DAG descendant propagation, and classifies delta proteins into NK/LK.

    Stores an EvaluationSet row with summary statistics.  The actual ground-truth
    rows are computed on-demand by the download endpoints using the same logic.
    """

    name = "generate_evaluation_set"

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = GenerateEvaluationSetPayload.model_validate(payload)

        old_set_id = uuid.UUID(p.old_annotation_set_id)
        new_set_id = uuid.UUID(p.new_annotation_set_id)

        old_set = session.get(AnnotationSet, old_set_id)
        if old_set is None:
            raise ValueError(f"AnnotationSet {old_set_id} not found")
        new_set = session.get(AnnotationSet, new_set_id)
        if new_set is None:
            raise ValueError(f"AnnotationSet {new_set_id} not found")
        if old_set.ontology_snapshot_id != new_set.ontology_snapshot_id:
            raise ValueError("Both annotation sets must use the same ontology snapshot")

        emit(
            "generate_evaluation_set.start",
            None,
            {
                "old_annotation_set_id": str(old_set_id),
                "new_annotation_set_id": str(new_set_id),
                "ontology_snapshot_id": str(old_set.ontology_snapshot_id),
            },
            "info",
        )

        emit("generate_evaluation_set.computing_delta", None, {}, "info")
        data = compute_evaluation_data(
            session,
            old_set_id,
            new_set_id,
            old_set.ontology_snapshot_id,
        )

        stats = data.stats()
        emit("generate_evaluation_set.delta_done", None, stats, "info")

        eval_set = EvaluationSet(
            old_annotation_set_id=old_set_id,
            new_annotation_set_id=new_set_id,
            stats=stats,
        )
        session.add(eval_set)
        session.flush()

        result = {"evaluation_set_id": str(eval_set.id), **stats}
        emit("generate_evaluation_set.done", None, result, "info")
        return OperationResult(result=result)
