"""``PredictGOTermsBatchOperation`` and its execute-time helper context types.

Extracted from the monolithic ``predict_go_terms.py`` as part of T2B.6.
T2B.4 then lifted the reranker scoring path out of the
``_RerankerMixin`` hierarchy into the compositive
:class:`protea.core.operations.predict_go_terms._reranker_scorer.RerankerScorer`,
so the orchestrator now collaborates with the scorer through a
constructor-injected instance instead of through MRO.

F2C.5c (the orchestrator collapse) moves the unified-pool, aspect-
separated, post-KNN, and chunked-publish bulk-logic helpers out into
sibling modules (:mod:`._unified_path`, :mod:`._aspect_helpers`,
:mod:`._post_knn_pipeline`, :mod:`._store`). The orchestrator keeps
short delegate methods so existing unit tests that ``patch.object`` on
:class:`PredictGOTermsBatchOperation` keep working without churn, while
the class itself drops well below the master plan §3 500-LOC ceiling.
"""

from __future__ import annotations

import time
import uuid
from typing import Any, NamedTuple
from uuid import UUID

import numpy as np
from sqlalchemy import text
from sqlalchemy.orm import Session

from protea.core.code_revision import (
    ForeignRevisionError,
    code_revision,
    dependency_conflicts,
    dependency_revisions,
    is_identifying,
    revisions_conflict,
)
from protea.core.contracts.operation import EmitFn, OperationResult
from protea.core.feature_enricher import enrich_v6_features
from protea.core.operations._predict_go_terms_adapter import (
    AdapterResult,
    call_pipeline_predict,
)
from protea.core.operations.predict_go_terms import (
    _aspect_helpers as _aspect,
)
from protea.core.operations.predict_go_terms import (
    _post_knn_pipeline as _post_knn,
)
from protea.core.operations.predict_go_terms import (
    _store as _store_mod,
)
from protea.core.operations.predict_go_terms import (
    _unified_path as _unified,
)
from protea.core.pca_cache import _load_or_fit_pca_state
from protea.core.utils import contract_payload

# The unified-pool and post-KNN paths delegate to ``call_pipeline_predict``,
# ``_load_or_fit_pca_state``, and ``enrich_v6_features``. Re-export those
# symbols on this module so existing unit tests that patch the legacy
# ``protea.core.operations.predict_go_terms._batch_op.<name>`` paths keep
# working without churn (F2C.5c compatibility).
__all__ = (
    "PredictGOTermsBatchOperation",
    "_load_or_fit_pca_state",
    "call_pipeline_predict",
    "enrich_v6_features",
)
from protea.core.operations.predict_go_terms._batch_op_feature import _FeatureLoadingMixin
from protea.core.operations.predict_go_terms._batch_op_reference import _ReferenceMixin
from protea.core.operations.predict_go_terms._common import (
    AspectSeparatedKnnContext,
    PredictGOTermsBatchPayload,
    _UnifiedPredictContext,
)
from protea.core.operations.predict_go_terms._reranker_scorer import RerankerScorer
from protea.infrastructure.orm.models.job import Job, JobStatus


class _BatchExecCtx(NamedTuple):
    """Static identifiers for one ``PredictGOTermsBatchOperation.execute`` call."""

    p: PredictGOTermsBatchPayload
    parent_job_id: UUID
    prediction_set_id: uuid.UUID
    embedding_config_id: uuid.UUID
    annotation_set_id: uuid.UUID


class _QueryBatch(NamedTuple):
    """Per-batch query inputs used by KNN dispatch + v6 enrichment."""

    valid_accessions: list[str]
    query_embeddings: np.ndarray


class _KnnResult(NamedTuple):
    """KNN dispatch outcome shared between v6 enrichment and ancestor expansion.

    Carries the query batch alongside the predictions so the v6 enrichment
    helper can reuse it without an extra parameter.
    """

    prediction_dicts: list[dict[str, Any]]
    v6_ctx: dict[str, Any] | None
    query_batch: _QueryBatch


class PredictGOTermsBatchOperation(
    _ReferenceMixin,
    _FeatureLoadingMixin,
):
    """CPU batch worker: KNN search + GO annotation transfer for one query chunk.

    Reference embeddings and their GO annotations are loaded from DB on
    first access and cached at the process level (_REF_CACHE).  Subsequent
    batch messages reuse the cached reference without any DB round-trip.

    Result is published to ``protea.predictions.write`` for bulk DB
    insertion. The reranker scoring path is delegated to an injected
    :class:`RerankerScorer` collaborator (T2B.4). The unified-pool,
    aspect-separated, post-KNN and chunked-publish helpers live in
    sibling modules (F2C.5c orchestrator collapse).
    """

    name = "predict_go_terms_batch"
    description = (
        "CPU child job: KNN search and GO annotation transfer for one query "
        "chunk; result is forwarded to store_predictions."
    )

    def __init__(self, reranker_scorer: RerankerScorer | None = None) -> None:
        self._reranker_scorer = reranker_scorer or RerankerScorer(
            attach_aspect=self._attach_go_term_aspect,
            attach_category=self._attach_query_category,
        )

    def _attach_query_category(
        self,
        session: Session,
        annotation_set_id: uuid.UUID,
        prediction_dicts: list[dict[str, Any]],
    ) -> dict[str, int]:
        """Assign each candidate a CAFA category from the protein's t0 known terms.

        Bound here (not on the scorer) so the category loader reuses this op's
        ``_load_annotations_for`` chunked annotation reader, matching how
        ``_attach_go_term_aspect`` is injected.
        """
        from protea.core.operations.predict_go_terms._category_dispatch import (
            attach_query_category,
        )

        return attach_query_category(self, session, annotation_set_id, prediction_dicts)

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        p = payload or {}
        n = len(p.get("query_accessions") or [])
        return f"n={n}" if n else ""

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = PredictGOTermsBatchPayload.model_validate(contract_payload(payload))
        ctx = _BatchExecCtx(
            p=p,
            parent_job_id=UUID(p.parent_job_id),
            prediction_set_id=uuid.UUID(p.prediction_set_id),
            embedding_config_id=uuid.UUID(p.embedding_config_id),
            annotation_set_id=uuid.UUID(p.annotation_set_id),
        )
        if self._should_skip_for_parent(session, ctx.parent_job_id, emit):
            return OperationResult(result={"skipped": True})
        self._refuse_foreign_revision(session, ctx, emit)

        ref_data = self._ensure_reference_cache(session, ctx, emit)
        query_embeddings, valid_accessions = self._load_query_embeddings(
            session, p.query_accessions, ctx.embedding_config_id, p, emit
        )
        if not query_embeddings.size:
            return OperationResult(result={"predictions": 0})

        t0 = time.perf_counter()
        query_batch = _QueryBatch(
            valid_accessions=valid_accessions, query_embeddings=query_embeddings
        )
        knn_result = self._run_knn_path(session, ctx, query_batch, ref_data, emit)
        if knn_result is None:
            return OperationResult(result={"predictions": 0})

        prediction_dicts, reranker_stats = self._run_post_knn_pipeline(
            session, ctx, knn_result, ref_data, emit
        )

        self._emit_done(
            emit,
            valid_accessions=valid_accessions,
            prediction_dicts=prediction_dicts,
            reranker_stats=reranker_stats,
            started_at=t0,
        )
        store_messages = self._chunked_publish(
            parent_job_id=ctx.parent_job_id,
            prediction_set_id=ctx.prediction_set_id,
            prediction_dicts=prediction_dicts,
        )
        self._finalize_parent_if_last(session, ctx.parent_job_id, emit)
        return OperationResult(
            result={
                "predictions": len(prediction_dicts),
                "store_chunks": len(store_messages),
            },
            publish_operations=store_messages,
        )

    @staticmethod
    def _finalize_parent_if_last(session: Session, parent_job_id: UUID, emit: EmitFn) -> None:
        """Atomically bump ``meta.batches_completed`` and mark the parent
        ``predict_go_terms`` Job ``SUCCEEDED`` when the count reaches
        ``meta.expected_batches`` (FIX-PREDICT-COORD-CLAIM, 2026-06-01).

        Without this, the coord job returns ``deferred=True`` and stays
        ``RUNNING`` forever; the stale-job reaper eventually kills it as
        ``lease_expired`` even though every batch has succeeded.
        """
        row = session.execute(
            text(
                "UPDATE job SET "
                "  meta = jsonb_set("
                "    COALESCE(meta, '{}'::jsonb),"
                "    '{batches_completed}',"
                "    to_jsonb(COALESCE((meta->>'batches_completed')::int, 0) + 1)"
                "  ),"
                "  status = CASE"
                "    WHEN COALESCE((meta->>'batches_completed')::int, 0) + 1"
                "         >= COALESCE((meta->>'expected_batches')::int, 2147483647)"
                "    THEN 'SUCCEEDED'::job_status"
                "    ELSE status"
                "  END,"
                "  finished_at = CASE"
                "    WHEN COALESCE((meta->>'batches_completed')::int, 0) + 1"
                "         >= COALESCE((meta->>'expected_batches')::int, 2147483647)"
                "    THEN NOW()"
                "    ELSE finished_at"
                "  END,"
                # FIX-UI-PROVENANCE: on the terminal SUCCEEDED transition snap
                # progress_current up to progress_total so the UI shows 100%
                # instead of a stale sub-total left by the last batch emit.
                "  progress_current = CASE"
                "    WHEN COALESCE((meta->>'batches_completed')::int, 0) + 1"
                "         >= COALESCE((meta->>'expected_batches')::int, 2147483647)"
                "    THEN COALESCE(progress_total, progress_current)"
                "    ELSE progress_current"
                "  END "
                "WHERE id = :pid AND status = 'RUNNING'::job_status "
                "RETURNING (status = 'SUCCEEDED'::job_status) AS finalized, "
                "          (meta->>'batches_completed')::int AS done, "
                "          (meta->>'expected_batches')::int AS expected"
            ),
            {"pid": parent_job_id},
        ).first()
        if row is not None and row.finalized:
            emit(
                "predict_go_terms.coord_finalized",
                None,
                {"batches_completed": row.done, "expected_batches": row.expected},
                "info",
            )

    @staticmethod
    def _refuse_foreign_revision(session: Session, ctx: _BatchExecCtx, emit: EmitFn) -> None:
        """Refuse to write into a set that different code opened.

        Two comparisons, because the repository commit does not identify the
        code: PROTEA pins six sibling packages by git commit, and a node can
        hold the right tree with a stale sibling whose version string did not
        move, which is exactly what happened on 2026-08-29.

        Each comparison is one-sided on purpose. A conflict is only declared
        when both sides name something that can be checked out again, so a
        dirty tree, a checkout without git, or an installer that left no
        witness never fails a batch. It emits a warning instead, because
        "cannot be verified" and "verified equal" have to be different words in
        the job stream: the run this guard exists for reported success, and a
        guard that quietly passed when it could not look would have reported
        success too.
        """
        stored = session.execute(
            text("SELECT meta FROM prediction_set WHERE id = :sid"),
            {"sid": ctx.prediction_set_id},
        ).scalar()
        # Anything that is not an object carries no revision, which includes a
        # set that is not there. Reading the keys here rather than in SQL keeps
        # the two questions this asks in one place.
        meta = stored if isinstance(stored, dict) else {}
        recorded_revision = meta.get("code_revision")
        recorded_deps = meta.get("dependency_revisions")
        running_revision = code_revision()
        differing = dependency_conflicts(recorded_deps, dependency_revisions())

        if revisions_conflict(recorded_revision, running_revision) or differing:
            fields = {
                "recorded": recorded_revision,
                "running": running_revision,
                "dependencies": {k: list(v) for k, v in differing.items()},
            }
            emit("predict_go_terms_batch.foreign_revision", None, fields, "error")
            raise ForeignRevisionError(
                _foreign_revision_message(
                    ctx.prediction_set_id, recorded_revision, running_revision, differing
                )
            )

        if not (is_identifying(recorded_revision) and is_identifying(running_revision)):
            emit(
                "predict_go_terms_batch.revision_unverifiable",
                None,
                {"recorded": recorded_revision, "running": running_revision},
                "warning",
            )
        elif not recorded_deps:
            # The repository matched, so the set is not foreign, but it was
            # opened before dependency commits were recorded and a stale
            # sibling here would be invisible.
            emit(
                "predict_go_terms_batch.dependencies_unverifiable",
                None,
                {"recorded": recorded_revision},
                "warning",
            )

    @staticmethod
    def _should_skip_for_parent(session: Session, parent_job_id: UUID, emit: EmitFn) -> bool:
        """Skip the batch if its parent Job was cancelled or failed in flight."""
        parent = session.get(Job, parent_job_id)
        if parent is not None and parent.status in (JobStatus.CANCELLED, JobStatus.FAILED):
            emit(
                "predict_go_terms_batch.skipped",
                None,
                {"parent_job_id": str(parent_job_id)},
                "warning",
            )
            return True
        return False

    def _run_knn_path(
        self,
        session: Session,
        ctx: _BatchExecCtx,
        query_batch: _QueryBatch,
        ref_data: Any,
        emit: EmitFn,
    ) -> _KnnResult | None:
        """Dispatch the KNN path: aspect-separated vs unified-pool.

        Returns ``None`` for the unified path when the reference pool is
        empty (caller short-circuits with a no-op result).
        """
        if ctx.p.aspect_separated_knn:
            return self._run_aspect_separated_path(session, ctx, query_batch, ref_data)
        return self._run_unified_path(session, ctx, query_batch, ref_data, emit)

    def _emit_done(
        self,
        emit: EmitFn,
        *,
        valid_accessions: list[str],
        prediction_dicts: list[dict[str, Any]],
        reranker_stats: dict[str, Any] | None,
        started_at: float,
    ) -> None:
        """Emit the per-batch ``done`` audit event."""
        done_fields: dict[str, Any] = {
            "queries": len(valid_accessions),
            "predictions": len(prediction_dicts),
            "elapsed_seconds": time.perf_counter() - started_at,
        }
        if reranker_stats is not None:
            done_fields["reranker"] = reranker_stats
        emit("predict_go_terms_batch.done", None, done_fields, "info")

    # ------------------------------------------------------------------
    # Delegate methods: keep the public surface stable for unit tests
    # that ``patch.object`` these names, while the bulk logic lives in
    # the sibling helper modules (F2C.5c orchestrator collapse).
    # ------------------------------------------------------------------

    def _run_unified_path(
        self,
        session: Session,
        ctx: _BatchExecCtx,
        query_batch: _QueryBatch,
        ref_data: Any,
        emit: EmitFn,
    ) -> _KnnResult | None:
        return _unified.run_unified_path(self, session, ctx, query_batch, ref_data, emit)

    def _run_aspect_separated_path(
        self,
        session: Session,
        ctx: _BatchExecCtx,
        query_batch: _QueryBatch,
        ref_data: Any,
    ) -> _KnnResult:
        return _aspect.run_aspect_separated_path(self, session, ctx, query_batch, ref_data)

    def _run_aspect_separated_knn(
        self,
        session: Session,
        ctx: AspectSeparatedKnnContext,
    ) -> tuple[
        list[dict[str, Any]],
        dict[str, list[list[tuple[str, float]]]],
        dict[str, dict[str, list[dict[str, Any]]]],
        dict[tuple[str, str], dict[str, Any]],
    ]:
        return _aspect.run_aspect_separated_knn(self, session, ctx)

    def _unified_predict_via_pipeline(
        self,
        session: Session,
        ctx: _UnifiedPredictContext,
    ) -> AdapterResult:
        return _unified.unified_predict_via_pipeline(self, session, ctx)

    def _unified_load_annotations(
        self,
        session: Session,
        ctx: _UnifiedPredictContext,
    ) -> tuple[dict[str, list[dict[str, Any]]], set[str]]:
        return _unified.unified_load_annotations(self, session, ctx)

    def _unified_load_pair_inputs(
        self,
        session: Session,
        ctx: _UnifiedPredictContext,
        unique_neighbors: set[str],
    ) -> tuple[
        dict[str, str],
        dict[str, str],
        dict[str, int | None],
        dict[str, int | None],
    ]:
        return _unified.unified_load_pair_inputs(self, session, ctx, unique_neighbors)

    def _run_post_knn_pipeline(
        self,
        session: Session,
        ctx: _BatchExecCtx,
        knn_result: _KnnResult,
        ref_data: Any,
        emit: EmitFn,
    ) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
        return _post_knn.run_post_knn_pipeline(self, session, ctx, knn_result, ref_data, emit)

    def _apply_v6_features(
        self,
        session: Session,
        ctx: _BatchExecCtx,
        knn_result: _KnnResult,
        ref_data: Any,
        emit: EmitFn,
    ) -> None:
        _post_knn.apply_v6_features(self, session, ctx, knn_result, ref_data, emit)

    def _expand_to_ancestors(
        self,
        session: Session,
        p: PredictGOTermsBatchPayload,
        prediction_dicts: list[dict[str, Any]],
        emit: EmitFn,
    ) -> list[dict[str, Any]]:
        return _post_knn.expand_to_ancestors(self, session, p, prediction_dicts, emit)

    def _stamp_go_ids(
        self,
        session: Session,
        prediction_dicts: list[dict[str, Any]],
    ) -> dict[int, str]:
        return _post_knn.stamp_go_ids(session, prediction_dicts)

    def _resolve_synthetic_fks(
        self,
        session: Session,
        prediction_dicts: list[dict[str, Any]],
        int_to_str: dict[int, str],
        snapshot_id: uuid.UUID,
    ) -> list[dict[str, Any]]:
        return _post_knn.resolve_synthetic_fks(session, prediction_dicts, int_to_str, snapshot_id)

    def _chunked_publish(
        self,
        *,
        parent_job_id: UUID,
        prediction_set_id: uuid.UUID,
        prediction_dicts: list[dict[str, Any]],
    ) -> list[tuple[str, dict[str, Any]]]:
        return _store_mod.chunked_publish(
            parent_job_id=parent_job_id,
            prediction_set_id=prediction_set_id,
            prediction_dicts=prediction_dicts,
        )


def _foreign_revision_message(
    prediction_set_id: uuid.UUID,
    recorded: str | None,
    running: str | None,
    differing: dict[str, tuple[str, str]],
) -> str:
    """Name every difference, because the operator has to fix all of them.

    A message that stopped at the repository would send someone to check out a
    commit they already have.
    """
    parts = [f"prediction set {prediction_set_id} was opened by other code"]
    if recorded != running:
        parts.append(f"protea: opened by {recorded}, this worker runs {running}")
    for name, (was, now) in sorted(differing.items()):
        parts.append(f"{name}: opened by {was}, this worker runs {now}")
    return "; ".join(parts)
