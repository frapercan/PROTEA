from __future__ import annotations

import tempfile
import uuid
from pathlib import Path
from typing import Any

from pydantic import field_validator
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.core.evaluation import (
    compute_evaluation_data,
    compute_evaluation_data_reconciled,
    groundtruth_key_for,
    serialize_evaluation_data_to_parquet,
)
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
from protea.infrastructure.settings import load_settings
from protea.infrastructure.storage import get_artifact_store


class GenerateEvaluationSetPayload(ProteaPayload, frozen=True):
    old_annotation_set_id: str
    new_annotation_set_id: str
    pivot_ontology_snapshot_id: str | None = None

    @field_validator("old_annotation_set_id", "new_annotation_set_id", mode="before")
    @classmethod
    def must_be_non_empty(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("must be a non-empty string")
        return v.strip()

    @field_validator("pivot_ontology_snapshot_id", mode="before")
    @classmethod
    def pivot_opt_non_empty(cls, v):
        if v is None:
            return None
        if not isinstance(v, str) or not v.strip():
            raise ValueError("must be a non-empty string or null")
        return v.strip()


class GenerateEvaluationSetOperation:
    """Computes the CAFA evaluation delta between two GOA annotation sets.

    Applies experimental evidence code filtering, NOT-qualifier exclusion with
    GO DAG descendant propagation, and classifies delta proteins into NK/LK.

    Stores an EvaluationSet row with summary statistics.  The actual ground-truth
    rows are computed on-demand by the download endpoints using the same logic.
    """

    name = "generate_evaluation_set"
    description = (
        "Compute the CAFA delta between an old and a new GOA annotation set, "
        "split delta proteins into NK/LK and persist an EvaluationSet. "
        "Supports cross-OBO reconciliation via an optional pivot snapshot."
    )

    def summarize_payload(self, payload: dict[str, Any], *, session: Session | None = None) -> str:
        p = payload or {}
        old_raw = p.get("old_annotation_set_id")
        new_raw = p.get("new_annotation_set_id")
        if old_raw and new_raw and session is not None:
            try:
                old = session.get(AnnotationSet, uuid.UUID(str(old_raw)))
                new = session.get(AnnotationSet, uuid.UUID(str(new_raw)))
            except Exception:
                old = new = None
            if old is not None and new is not None:
                ov = old.source_version or str(old.id)[:8]
                nv = new.source_version or str(new.id)[:8]
                return f"{old.source}@{ov} → {new.source}@{nv}"
        if old_raw and new_raw:
            return f"{str(old_raw)[:8]} → {str(new_raw)[:8]}"
        return ""

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

        if p.pivot_ontology_snapshot_id is not None:
            pivot_id = uuid.UUID(p.pivot_ontology_snapshot_id)
            if session.get(OntologySnapshot, pivot_id) is None:
                raise ValueError(f"OntologySnapshot {pivot_id} not found")
        else:
            pivot_id = new_set.ontology_snapshot_id

        same_snapshot = (
            old_set.ontology_snapshot_id == new_set.ontology_snapshot_id == pivot_id
        )
        mode = "same_snapshot" if same_snapshot else "reconciled"

        emit(
            "generate_evaluation_set.start",
            None,
            {
                "old_annotation_set_id": str(old_set_id),
                "new_annotation_set_id": str(new_set_id),
                "old_ontology_snapshot_id": str(old_set.ontology_snapshot_id),
                "new_ontology_snapshot_id": str(new_set.ontology_snapshot_id),
                "pivot_ontology_snapshot_id": str(pivot_id),
                "mode": mode,
            },
            "info",
        )

        emit("generate_evaluation_set.computing_delta", None, {"mode": mode}, "info")
        if same_snapshot:
            data = compute_evaluation_data(session, old_set_id, new_set_id, pivot_id)
        else:
            data = compute_evaluation_data_reconciled(
                session,
                old_set_id,
                new_set_id,
                old_set.ontology_snapshot_id,
                new_set.ontology_snapshot_id,
                pivot_id,
            )

        stats = data.stats()
        stats["mode"] = mode
        stats["pivot_ontology_snapshot_id"] = str(pivot_id)
        emit("generate_evaluation_set.delta_done", None, stats, "info")

        eval_set = EvaluationSet(
            old_annotation_set_id=old_set_id,
            new_annotation_set_id=new_set_id,
            stats=stats,
        )
        session.add(eval_set)
        session.flush()

        # Persist the full ground-truth (nk/lk/pk/known/pk_known) to the artifact
        # store. Downstream consumers (the dump helper, cafaeval) read this
        # parquet via load_evaluation_data_for_set instead of recomputing.
        project_root = Path(__file__).resolve().parents[3]
        store = get_artifact_store(load_settings(project_root))
        key = groundtruth_key_for(eval_set.id)
        with tempfile.TemporaryDirectory(prefix="protea_eval_gt_") as tmp:
            local_path = Path(tmp) / "groundtruth.parquet"
            serialize_evaluation_data_to_parquet(data, local_path)
            uri = store.put(key, local_path)
        eval_set.groundtruth_uri = uri
        session.flush()
        emit(
            "generate_evaluation_set.groundtruth_persisted",
            None,
            {"evaluation_set_id": str(eval_set.id), "uri": uri, "key": key},
            "info",
        )

        result = {"evaluation_set_id": str(eval_set.id), "groundtruth_uri": uri, **stats}
        emit("generate_evaluation_set.done", None, result, "info")
        return OperationResult(result=result)
