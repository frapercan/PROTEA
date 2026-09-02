from __future__ import annotations

import tempfile
import uuid
from pathlib import Path
from typing import Any, NamedTuple

from pydantic import field_validator
from sqlalchemy import text
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.core.evaluation import (
    compute_evaluation_data,
    compute_evaluation_data_reconciled,
    groundtruth_key_for,
    serialize_evaluation_data_to_parquet,
)
from protea.core.utils import contract_payload, job_id_from_payload
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
from protea.infrastructure.settings import load_settings
from protea.infrastructure.storage import get_artifact_store


class _Identity(NamedTuple):
    """The five fields that decide which delta a row holds.

    Kept as one object because they travel together: every lookup, every
    refusal and every insert needs all five, and a signature that took them
    loose is a signature somebody can call with four.
    """

    old_set_id: uuid.UUID
    new_set_id: uuid.UUID
    pivot_id: uuid.UUID
    old_native: uuid.UUID
    new_native: uuid.UUID


class GenerateEvaluationSetPayload(ProteaPayload, frozen=True):
    old_annotation_set_id: str
    new_annotation_set_id: str
    pivot_ontology_snapshot_id: str | None = None
    # Cross-OBO override: propagate each side's annotations under an EXPLICIT
    # ontology snapshot (its native DAG), decoupled from the annotation set's
    # stored ``ontology_snapshot_id``. The reconcile already resolves go_id text
    # and loads the native DAG by snapshot id, so these select the propagation
    # graph WITHOUT touching annotation rows. ``None`` falls back to each set's
    # stored snapshot (existing behaviour). Use when an annotation set is bound
    # to a wrong/too-new OBO (the phantom-gap: t0 propagated under a churned
    # graph that marks pre-window experimental annotations as new knowledge).
    old_native_snapshot_id: str | None = None
    new_native_snapshot_id: str | None = None
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

    @field_validator(
        "pivot_ontology_snapshot_id",
        "old_native_snapshot_id",
        "new_native_snapshot_id",
        mode="before",
    )
    @classmethod
    def snapshot_opt_non_empty(cls, v):
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
        p = GenerateEvaluationSetPayload.model_validate(contract_payload(payload))
        old_set_id = uuid.UUID(p.old_annotation_set_id)
        new_set_id = uuid.UUID(p.new_annotation_set_id)
        old_set, new_set, old_native, new_native, pivot_id = self._resolve_eval_inputs(
            session, p, old_set_id, new_set_id
        )
        mode = self._delta_mode(old_set, new_set, old_native, new_native, pivot_id)

        ident = _Identity(old_set_id, new_set_id, pivot_id, old_native, new_native)
        short = self._maybe_reuse_existing(session, ident, p.window_role, emit)
        if short is not None:
            return short

        data, stats = self._compute_delta(session, ident, mode, emit)
        self._refuse_empty_delta(session, ident, stats)

        eval_set = EvaluationSet(
            old_annotation_set_id=old_set_id,
            new_annotation_set_id=new_set_id,
            pivot_snapshot_id=pivot_id,
            old_native_snapshot_id=old_native,
            new_native_snapshot_id=new_native,
            stats=stats,
            window_role=p.window_role,
            job_id=job_id_from_payload(payload),
        )
        session.add(eval_set)
        session.flush()
        uri = self._persist_groundtruth(eval_set, data, emit)
        result = {"evaluation_set_id": str(eval_set.id), "groundtruth_uri": uri, **stats}
        emit("generate_evaluation_set.done", None, result, "info")
        return OperationResult(result=result)

    @staticmethod
    def _delta_mode(
        old_set: AnnotationSet,
        new_set: AnnotationSet,
        old_native: uuid.UUID,
        new_native: uuid.UUID,
        pivot_id: uuid.UUID,
    ) -> str:
        """Which delta path is VALID here, which is not the same as which was asked for.

        The fast path resolves annotations through ``go_term.id``, and that
        column is scoped to a snapshot: the same GO accession has one row per
        snapshot, so an id minted under one graph matches nothing under
        another. It is therefore only correct when both annotation sets are
        BOUND to the graph being used -- a property of the sets, not of the
        arguments.

        This used to test ``old_native == new_native == pivot_id``, which is a
        property of the arguments alone. The two agree whenever no override is
        passed, and diverge exactly when one is: asking for both sides under a
        pivot the sets are not bound to satisfied the old test, took the fast
        path, and matched none of the 11.2 million annotations of the 220 ->
        227 window. Every one was dropped by ``_load_experimental_annotations_by_ns``,
        which drops terms of unknown aspect without comment, and the operation
        stored a perfectly well-formed EvaluationSet holding zero of everything.

        So the test is on all five, and the sets' own bindings lead. The
        reconciled path resolves by ``go_id`` text and is correct in every case;
        the fast path is an optimisation, and an optimisation that can be wrong
        has to prove it is not.
        """
        bound_alike = old_set.ontology_snapshot_id == new_set.ontology_snapshot_id == pivot_id
        asked_alike = old_native == new_native == pivot_id
        return "same_snapshot" if bound_alike and asked_alike else "reconciled"

    def _compute_delta(
        self, session: Session, ident: _Identity, mode: str, emit: EmitFn
    ) -> tuple[Any, dict[str, Any]]:
        """Run the delta under this identity and return it with its stats.

        The two compute paths are one decision, taken once here: if all three
        snapshots coincide there is nothing to reconcile, and the cheaper
        same-snapshot path gives the same answer. Anything else needs each side
        closed under its own DAG and intersected with the pivot.
        """
        emit(
            "generate_evaluation_set.start",
            None,
            {
                "old_annotation_set_id": str(ident.old_set_id),
                "new_annotation_set_id": str(ident.new_set_id),
                "old_ontology_snapshot_id": str(ident.old_native),
                "new_ontology_snapshot_id": str(ident.new_native),
                "pivot_ontology_snapshot_id": str(ident.pivot_id),
                "mode": mode,
            },
            "info",
        )
        emit("generate_evaluation_set.computing_delta", None, {"mode": mode}, "info")
        if mode == "same_snapshot":
            data = compute_evaluation_data(
                session, ident.old_set_id, ident.new_set_id, ident.pivot_id
            )
        else:
            data = compute_evaluation_data_reconciled(
                session,
                ident.old_set_id,
                ident.new_set_id,
                ident.old_native,
                ident.new_native,
                ident.pivot_id,
            )
        stats = data.stats()
        stats["mode"] = mode
        stats["pivot_ontology_snapshot_id"] = str(ident.pivot_id)
        emit("generate_evaluation_set.delta_done", None, stats, "info")
        return data, stats

    @staticmethod
    def _refuse_empty_delta(session: Session, ident: _Identity, stats: dict[str, Any]) -> None:
        """Refuse a delta of nothing taken over corpora of something.

        An EvaluationSet with zero delta proteins is a legitimate result only
        when there was nothing to find. When the two corpora hold millions of
        annotations between them and the delta is empty, the terms did not
        resolve -- and nothing downstream can tell the two apart, because the
        row that comes out is well formed either way. That is how a silently
        empty ground truth reaches an evaluation and reports a clean zero.

        The check is cheap and it can fail, which is the only kind worth
        having: it counts the annotations the two sets actually hold and only
        raises when they are not empty either.
        """
        if int(stats.get("delta_proteins") or 0):
            return
        held = session.execute(
            text(
                "SELECT count(*) FROM protein_go_annotation "
                "WHERE annotation_set_id IN (:old_id, :new_id)"
            ),
            {"old_id": ident.old_set_id, "new_id": ident.new_set_id},
        ).scalar_one()
        if not held:
            return
        raise ValueError(
            f"the delta is empty but the two annotation sets hold {held} annotations. "
            "Their terms did not resolve under the graphs asked for "
            f"(pivot {str(ident.pivot_id)[:8]}, natives {str(ident.old_native)[:8]}/"
            f"{str(ident.new_native)[:8]}). Storing this would be a ground truth of "
            "nothing that reports a clean zero."
        )

    def _existing_set(self, session: Session, ident: _Identity) -> EvaluationSet | None:
        """The EvaluationSet already stored under this identity, if any.

        The identity is the five fields the delta depends on, not the two it
        used to be. A pair of annotation sets read under a different
        propagation graph is a different measurement: on GOA 220 -> 227 the
        choice moves the PK bucket by 21 percent of its annotations. Looking
        one up by the pair alone would serve a caller a set that answers a
        question they did not ask.
        """
        return (
            session.query(EvaluationSet)
            .filter_by(
                old_annotation_set_id=ident.old_set_id,
                new_annotation_set_id=ident.new_set_id,
                pivot_snapshot_id=ident.pivot_id,
                old_native_snapshot_id=ident.old_native,
                new_native_snapshot_id=ident.new_native,
            )
            .one_or_none()
        )

    def _maybe_reuse_existing(
        self,
        session: Session,
        ident: _Identity,
        window_role: str | None,
        emit: EmitFn,
    ) -> OperationResult | None:
        """Return the existing EvaluationSet's result, or None if absent.

        Idempotency: if an EvaluationSet already exists under this identity,
        return its summary instead of computing and inserting a duplicate. The
        DB-level UNIQUE constraint enforces it at the schema layer (alembic
        ``d4b8c2f10a37_evaluation_set_identity_includes_graphs``); this
        short-circuit avoids paying the delta compute cost on a re-submission.

        A caller asking for the same pair under a DIFFERENT propagation graph
        does not land here: no row matches, and the delta is computed. That is
        the point of widening the key. It used to raise instead, which made two
        legitimate measurements mutually exclusive.

        ADR D40: a re-submission MAY carry a ``window_role`` to (re)bind an
        already-computed set to a protocol window. Designating the window
        is metadata only, so it is applied in place without recomputing
        the delta.
        """
        existing = self._existing_set(session, ident)
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
                "old_annotation_set_id": str(ident.old_set_id),
                "new_annotation_set_id": str(ident.new_set_id),
                "pivot_snapshot_id": str(ident.pivot_id),
                "old_native_snapshot_id": str(ident.old_native),
                "new_native_snapshot_id": str(ident.new_native),
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
    ) -> tuple[AnnotationSet, AnnotationSet, uuid.UUID, uuid.UUID, uuid.UUID]:
        """Validate the two annotation sets exist + resolve the propagation snapshots.

        Returns ``(old_set, new_set, old_native, new_native, pivot)``. Each native
        defaults to its annotation set's stored ``ontology_snapshot_id`` but can be
        overridden explicitly (cross-OBO), and the pivot defaults to ``new_native``.
        """
        old_set = session.get(AnnotationSet, old_set_id)
        if old_set is None:
            raise ValueError(f"AnnotationSet {old_set_id} not found")
        new_set = session.get(AnnotationSet, new_set_id)
        if new_set is None:
            raise ValueError(f"AnnotationSet {new_set_id} not found")

        def _resolve_snapshot(raw: str | None, fallback: uuid.UUID) -> uuid.UUID:
            if raw is None:
                return fallback
            sid = uuid.UUID(raw)
            if session.get(OntologySnapshot, sid) is None:
                raise ValueError(f"OntologySnapshot {sid} not found")
            return sid

        old_native = _resolve_snapshot(p.old_native_snapshot_id, old_set.ontology_snapshot_id)
        new_native = _resolve_snapshot(p.new_native_snapshot_id, new_set.ontology_snapshot_id)
        pivot_id = _resolve_snapshot(p.pivot_ontology_snapshot_id, new_native)
        return old_set, new_set, old_native, new_native, pivot_id

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
