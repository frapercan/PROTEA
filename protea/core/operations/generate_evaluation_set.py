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
    # ADR D40: bind the produced set to a rolling-origin protocol window.
    # ``"valid"`` (selection + threshold tuning) | ``"test"`` (report once)
    # | ``None`` (unbound). Defaults to None so existing callers are
    # unaffected.
    window_role: str | None = None

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

    @field_validator("window_role", mode="before")
    @classmethod
    def window_role_in_vocab(cls, v):
        if v is None:
            return None
        if not isinstance(v, str) or v not in ("valid", "test"):
            raise ValueError("must be one of 'valid', 'test', or null")
        return v


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
        old_set, new_set, pivot_id = self._resolve_eval_inputs(session, p, old_set_id, new_set_id)
        same_snapshot = (
            old_set.ontology_snapshot_id == new_set.ontology_snapshot_id == pivot_id
        )
        mode = "same_snapshot" if same_snapshot else "reconciled"

        reuse = self._maybe_reuse_existing(
            session, old_set_id, new_set_id, p.window_role, emit
        )
        if reuse is not None:
            return reuse

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
                session, old_set_id, new_set_id,
                old_set.ontology_snapshot_id, new_set.ontology_snapshot_id, pivot_id,
            )

        stats = data.stats()
        stats["mode"] = mode
        stats["pivot_ontology_snapshot_id"] = str(pivot_id)
        emit("generate_evaluation_set.delta_done", None, stats, "info")

        eval_set = EvaluationSet(
            old_annotation_set_id=old_set_id,
            new_annotation_set_id=new_set_id,
            stats=stats,
            window_role=p.window_role,
        )
        session.add(eval_set)
        session.flush()
        uri = self._persist_groundtruth(eval_set, data, emit)
        result = {"evaluation_set_id": str(eval_set.id), "groundtruth_uri": uri, **stats}
        emit("generate_evaluation_set.done", None, result, "info")
        return OperationResult(result=result)

    def _maybe_reuse_existing(
        self,
        session: Session,
        old_set_id: uuid.UUID,
        new_set_id: uuid.UUID,
        window_role: str | None,
        emit: EmitFn,
    ) -> OperationResult | None:
        """Return the existing EvaluationSet's result, or None if absent.

        Idempotency: if an EvaluationSet already exists for this
        ``(old, new)`` pair, return its summary instead of computing and
        inserting a duplicate. The DB-level UNIQUE constraint enforces
        this at the schema layer (alembic
        ``b8e3f1a7c2d9_evaluation_set_pair_unique``); this short-circuit
        avoids paying the delta compute cost on a re-submission and keeps
        the operation idempotent from the caller's perspective.

        ADR D40: a re-submission MAY carry a ``window_role`` to (re)bind an
        already-computed set to a protocol window. Designating the window
        is metadata only, so it is applied in place without recomputing
        the delta.
        """
        existing = (
            session.query(EvaluationSet)
            .filter_by(
                old_annotation_set_id=old_set_id,
                new_annotation_set_id=new_set_id,
            )
            .one_or_none()
        )
        if existing is None:
            return None
        self._rebind_window_role(session, existing, window_role, emit)
        stats = dict(existing.stats or {})
        result = {
            "evaluation_set_id": str(existing.id),
            "groundtruth_uri": existing.groundtruth_uri,
            **stats,
        }
        emit(
            "generate_evaluation_set.idempotent_reuse",
            None,
            {
                "evaluation_set_id": str(existing.id),
                "old_annotation_set_id": str(old_set_id),
                "new_annotation_set_id": str(new_set_id),
            },
            "info",
        )
        return OperationResult(result=result)

    @staticmethod
    def _rebind_window_role(
        session: Session,
        existing: EvaluationSet,
        window_role: str | None,
        emit: EmitFn,
    ) -> None:
        """Re-designate an existing set's protocol window in place (ADR D40).

        No-op unless a ``window_role`` is supplied and differs from the
        current one. Metadata only: the delta is never recomputed.
        """
        if window_role is None or existing.window_role == window_role:
            return
        existing.window_role = window_role
        session.flush()
        emit(
            "generate_evaluation_set.window_role_set",
            None,
            {"evaluation_set_id": str(existing.id), "window_role": window_role},
            "info",
        )

    def _resolve_eval_inputs(
        self,
        session: Session,
        p: GenerateEvaluationSetPayload,
        old_set_id: uuid.UUID,
        new_set_id: uuid.UUID,
    ) -> tuple[AnnotationSet, AnnotationSet, uuid.UUID]:
        """Validate the two annotation sets exist + resolve pivot snapshot."""
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
        return old_set, new_set, pivot_id

    def _persist_groundtruth(
        self,
        eval_set: EvaluationSet,
        data: Any,
        emit: EmitFn,
    ) -> str:
        """Serialise the full ground-truth parquet to the artifact store.

        Downstream consumers (the dump helper, cafaeval) read this
        parquet via ``load_evaluation_data_for_set`` instead of
        recomputing the delta.
        """
        project_root = Path(__file__).resolve().parents[3]
        store = get_artifact_store(load_settings(project_root))
        key = groundtruth_key_for(eval_set.id)
        with tempfile.TemporaryDirectory(prefix="protea_eval_gt_") as tmp:
            local_path = Path(tmp) / "groundtruth.parquet"
            serialize_evaluation_data_to_parquet(data, local_path)
            uri = store.put(key, local_path)
        eval_set.groundtruth_uri = uri
        emit(
            "generate_evaluation_set.groundtruth_persisted",
            None,
            {"evaluation_set_id": str(eval_set.id), "uri": uri, "key": key},
            "info",
        )
        return uri
