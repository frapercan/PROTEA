"""``PredictGOTermsBatchOperation`` and its execute-time helper context types.

Extracted from the monolithic ``predict_go_terms.py`` as part of T2B.6.
T2B.4 then lifted the reranker scoring path out of the ``_RerankerMixin``
hierarchy and into the compositive
:class:`protea.core.operations.predict_go_terms._reranker_scorer.RerankerScorer`,
so the orchestrator now collaborates with the scorer through a
constructor-injected instance instead of through MRO.
"""

from __future__ import annotations

import time
import uuid
from typing import Any, NamedTuple
from uuid import UUID

import numpy as np
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, OperationResult
from protea.core.feature_enricher import KnnEnrichmentContext, enrich_v6_features
from protea.core.operations._predict_go_terms_adapter import (
    AdapterInputs,
    AdapterResult,
    call_pipeline_predict,
)
from protea.core.operations.predict_go_terms._aspect_helpers import (
    _build_aspect_adapter_inputs,
    call_pipeline_predict_aspect_separated,
)
from protea.core.operations.predict_go_terms._batch_op_feature import _FeatureLoadingMixin
from protea.core.operations.predict_go_terms._batch_op_reference import _ReferenceMixin
from protea.core.operations.predict_go_terms._common import (
    _WRITE_QUEUE,
    AspectSeparatedKnnContext,
    PredictGOTermsBatchPayload,
    _UnifiedPredictContext,
)
from protea.core.operations.predict_go_terms._reranker_scorer import RerankerScorer
from protea.core.pca_cache import _load_or_fit_pca_state
from protea.core.reranker import EMBEDDING_PCA_DIM
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
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

    Reference embeddings and their GO annotations are loaded from DB on first
    access and cached at the process level (_REF_CACHE).  Subsequent batch
    messages reuse the cached reference without any DB round-trip.

    Result is published to protea.predictions.write for bulk DB insertion.
    The reranker scoring path is delegated to an injected
    :class:`RerankerScorer` collaborator (T2B.4).
    """

    name = "predict_go_terms_batch"
    description = (
        "CPU child job: KNN search and GO annotation transfer for one query "
        "chunk; result is forwarded to store_predictions."
    )

    def __init__(self, reranker_scorer: RerankerScorer | None = None) -> None:
        self._reranker_scorer = reranker_scorer or RerankerScorer(
            attach_aspect=self._attach_go_term_aspect,
        )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        p = payload or {}
        n = len(p.get("query_accessions") or [])
        return f"n={n}" if n else ""

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = PredictGOTermsBatchPayload.model_validate(payload)
        ctx = _BatchExecCtx(
            p=p,
            parent_job_id=UUID(p.parent_job_id),
            prediction_set_id=uuid.UUID(p.prediction_set_id),
            embedding_config_id=uuid.UUID(p.embedding_config_id),
            annotation_set_id=uuid.UUID(p.annotation_set_id),
        )
        if self._should_skip_for_parent(session, ctx.parent_job_id, emit):
            return OperationResult(result={"skipped": True})

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
        return OperationResult(
            result={
                "predictions": len(prediction_dicts),
                "store_chunks": len(store_messages),
            },
            publish_operations=store_messages,
        )

    def _run_post_knn_pipeline(
        self,
        session: Session,
        ctx: _BatchExecCtx,
        knn_result: _KnnResult,
        ref_data: Any,
        emit: EmitFn,
    ) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
        """Apply v6 enrichment, ancestor expansion, and the reranker to the
        KNN candidates. Returns ``(prediction_dicts, reranker_stats)``.

        Ancestor expansion runs AFTER v6 so synthetic ancestor records
        inherit the leaf's anc2vec_/emb_pca_ values, mirroring what the
        dump helper emits; without that the lab booster sees a feature
        distribution it never trained on.
        """
        p = ctx.p
        if p.compute_v6_features and knn_result.v6_ctx is not None and knn_result.prediction_dicts:
            self._apply_v6_features(session, ctx, knn_result, ref_data, emit)
        prediction_dicts = knn_result.prediction_dicts
        if p.expand_votes_to_ancestors and prediction_dicts:
            prediction_dicts = self._expand_to_ancestors(session, p, prediction_dicts, emit)
        reranker_stats: dict[str, Any] | None = None
        if p.reranker_model_id and prediction_dicts:
            scorer = self._reranker_scorer
            reranker_stats = scorer.apply_if_aligned(session, prediction_dicts, p, emit)
        return prediction_dicts, reranker_stats

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

        Returns ``None`` for the unified path when the reference pool is empty
        (caller short-circuits with a no-op result).
        """
        if ctx.p.aspect_separated_knn:
            return self._run_aspect_separated_path(session, ctx, query_batch, ref_data)
        return self._run_unified_path(session, ctx, query_batch, ref_data, emit)

    def _run_aspect_separated_path(
        self,
        session: Session,
        ctx: _BatchExecCtx,
        query_batch: _QueryBatch,
        ref_data: Any,
    ) -> _KnnResult:
        """Aspect-separated KNN dispatch; one pass per GO aspect."""
        p = ctx.p
        (
            prediction_dicts,
            neighbors_by_aspect,
            go_map_by_aspect,
            pair_features,
        ) = self._run_aspect_separated_knn(
            session,
            AspectSeparatedKnnContext(
                valid_accessions=query_batch.valid_accessions,
                query_embeddings=query_batch.query_embeddings,
                ref_data_by_aspect=ref_data,
                annotation_set_id=ctx.annotation_set_id,
                prediction_set_id=ctx.prediction_set_id,
                payload=p,
            ),
        )
        v6_ctx: dict[str, Any] | None = None
        if p.compute_v6_features:
            v6_ctx = {
                "neighbors_by_aspect": neighbors_by_aspect,
                "go_map_by_aspect": go_map_by_aspect,
                "pair_features": pair_features,
            }
        return _KnnResult(prediction_dicts=prediction_dicts, v6_ctx=v6_ctx, query_batch=query_batch)

    def _run_unified_path(
        self,
        session: Session,
        ctx: _BatchExecCtx,
        query_batch: _QueryBatch,
        ref_data: Any,
        emit: EmitFn,
    ) -> _KnnResult | None:
        """Unified-pool KNN via ``protea_method.pipeline.predict``."""
        if not ref_data["embeddings"].size:
            emit("predict_go_terms_batch.no_references", None, {}, "warning")
            return None
        adapter_result = self._unified_predict_via_pipeline(
            session,
            _UnifiedPredictContext(
                p=ctx.p,
                annotation_set_id=ctx.annotation_set_id,
                prediction_set_id=ctx.prediction_set_id,
                valid_accessions=query_batch.valid_accessions,
                query_embeddings=query_batch.query_embeddings,
                ref_data=ref_data,
            ),
        )
        v6_ctx: dict[str, Any] | None = None
        if ctx.p.compute_v6_features:
            v6_ctx = {
                "neighbors_by_aspect": adapter_result.neighbors_by_aspect,
                "go_map_by_aspect": adapter_result.go_map_by_aspect,
                "pair_features": adapter_result.pair_features,
            }
        return _KnnResult(
            prediction_dicts=adapter_result.predictions,
            v6_ctx=v6_ctx,
            query_batch=query_batch,
        )

    def _apply_v6_features(
        self,
        session: Session,
        ctx: _BatchExecCtx,
        knn_result: _KnnResult,
        ref_data: Any,
        emit: EmitFn,
    ) -> None:
        """Run Anc2Vec / tax_voters / emb_pca enrichment on the prediction
        list in place. PCA is fitted (or loaded from cache) over the full
        unified embedding pool; for aspect-separated mode the per-aspect
        f32 arrays are concatenated first.
        """
        from protea.core.domain.aspect import ASPECT_CODES as _ASPECTS

        p = ctx.p
        v6_ctx = knn_result.v6_ctx
        assert v6_ctx is not None  # caller guards on this
        if p.aspect_separated_knn:
            pools = [
                ref_data[a]["embeddings_f32"]
                for a in _ASPECTS
                if ref_data[a].get("embeddings_f32") is not None
                and ref_data[a]["embeddings_f32"].size
            ]
            pca_pool = np.concatenate(pools, axis=0) if pools else np.empty((0,), dtype=np.float32)
        else:
            pca_pool = ref_data.get("embeddings_f32", np.empty((0,), dtype=np.float32))

        pca_state = _load_or_fit_pca_state(ctx.embedding_config_id, pca_pool)
        enrich_v6_features(
            knn_result.prediction_dicts,
            session=session,
            ctx=KnnEnrichmentContext(
                valid_accessions=knn_result.query_batch.valid_accessions,
                query_embeddings=knn_result.query_batch.query_embeddings,
                neighbors_by_aspect=v6_ctx["neighbors_by_aspect"],
                go_map_by_aspect=v6_ctx["go_map_by_aspect"],
                pair_features=v6_ctx["pair_features"],
                pca_state=pca_state,
            ),
            compute_taxonomy=p.compute_taxonomy,
        )
        emit(
            "predict_go_terms_batch.v6_features_done",
            None,
            {
                "pca_state_fit": pca_state is not None,
                "pca_dim": EMBEDDING_PCA_DIM if pca_state is not None else 0,
                "rows_enriched": len(knn_result.prediction_dicts),
            },
            "info",
        )

    def _unified_predict_via_pipeline(
        self,
        session: Session,
        ctx: _UnifiedPredictContext,
    ) -> AdapterResult:
        """Run the unified KNN path through ``protea_method.pipeline.predict``.

        Resolves the GO term metadata maps, sequences / taxonomy inputs
        (when the payload requests them), and the lazy ``go_map`` keyed
        on neighbours actually found, then delegates to
        :func:`call_pipeline_predict`. The adapter returns the legacy
        prediction shape PROTEA's downstream consumers expect, plus the
        ``pair_features`` and aspect maps the v6 enricher needs.
        """
        annotations, unique_neighbors = self._unified_load_annotations(session, ctx)
        ref_sequences, query_sequences, ref_tax_ids, query_tax_ids = self._unified_load_pair_inputs(
            session, ctx, unique_neighbors
        )
        go_id_map, go_aspect_map = self._load_go_term_metadata(session, annotations)
        return call_pipeline_predict(
            AdapterInputs(
                p=ctx.p,
                valid_accessions=ctx.valid_accessions,
                query_embeddings=ctx.query_embeddings,
                ref_data=ctx.ref_data,
                annotations=annotations,
                go_id_map=go_id_map,
                go_aspect_map=go_aspect_map,
                prediction_set_id=ctx.prediction_set_id,
                ref_sequences=ref_sequences,
                query_sequences=query_sequences,
                ref_tax_ids=ref_tax_ids,
                query_tax_ids=query_tax_ids,
            )
        )

    def _unified_load_annotations(
        self,
        session: Session,
        ctx: _UnifiedPredictContext,
    ) -> tuple[dict[str, list[dict[str, Any]]], set[str]]:
        """Pre-search KNN to resolve the unique-neighbour set, then load
        the lazy go-map only for those references."""
        from protea.core.knn_search import search_knn

        p = ctx.p
        use_cos = p.metric == "cosine"
        ref_embeddings_f32 = (
            ctx.ref_data["embeddings_f32_cos"] if use_cos else ctx.ref_data["embeddings_f32"]
        )
        neighbors = search_knn(
            ctx.query_embeddings,
            ref_embeddings_f32,
            ctx.ref_data["accessions"],
            k=p.limit_per_entry,
            distance_threshold=p.distance_threshold,
            backend=p.search_backend,
            metric=p.metric,
            pre_normalized=use_cos,
            faiss_index_type=p.faiss_index_type,
            faiss_nlist=p.faiss_nlist,
            faiss_nprobe=p.faiss_nprobe,
            faiss_hnsw_m=p.faiss_hnsw_m,
            faiss_hnsw_ef_search=p.faiss_hnsw_ef_search,
        )
        unique_neighbors: set[str] = {ref_acc for top_refs in neighbors for ref_acc, _ in top_refs}
        annotations = self._load_annotations_for(session, ctx.annotation_set_id, unique_neighbors)
        return annotations, unique_neighbors

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
        """Conditionally load sequences (alignments) and taxonomy ids."""
        p = ctx.p
        ref_sequences: dict[str, str] = {}
        query_sequences: dict[str, str] = {}
        ref_tax_ids: dict[str, int | None] = {}
        query_tax_ids: dict[str, int | None] = {}
        if p.compute_alignments:
            ref_sequences = self._load_sequences_for_proteins(session, unique_neighbors)
            query_sequences = self._load_sequences_for_queries(session, p, ctx.valid_accessions)
        if p.compute_taxonomy:
            ref_tax_ids = self._load_taxonomy_ids_for_proteins(session, unique_neighbors)
            query_tax_ids = self._load_taxonomy_ids_for_queries(session, p, ctx.valid_accessions)
        return ref_sequences, query_sequences, ref_tax_ids, query_tax_ids

    def _expand_to_ancestors(
        self,
        session: Session,
        p: PredictGOTermsBatchPayload,
        prediction_dicts: list[dict[str, Any]],
        emit: EmitFn,
    ) -> list[dict[str, Any]]:
        """Expand each leaf prediction to its ancestor closure.

        Mirrors what the offline dump helper emits so live predictions
        carry the same candidate distribution the booster trained on.
        """
        from protea.core.feature_enricher import (
            expand_predictions_to_ancestors,
            load_parent_map,
        )

        snapshot_id = uuid.UUID(p.ontology_snapshot_id)
        parent_map = load_parent_map(session, snapshot_id)
        int_to_str = self._stamp_go_ids(session, prediction_dicts)
        n_before = len(prediction_dicts)
        prediction_dicts = expand_predictions_to_ancestors(
            prediction_dicts,
            parent_map=parent_map,
            k_limit=p.limit_per_entry,
            ia_weights=None,
        )
        prediction_dicts = self._resolve_synthetic_fks(
            session, prediction_dicts, int_to_str, snapshot_id
        )
        emit(
            "predict_go_terms_batch.expanded_to_ancestors",
            None,
            {
                "rows_before": n_before,
                "rows_after": len(prediction_dicts),
                "expansion_ratio": (len(prediction_dicts) / n_before if n_before else 0.0),
            },
            "info",
        )
        return prediction_dicts

    def _stamp_go_ids(
        self,
        session: Session,
        prediction_dicts: list[dict[str, Any]],
    ) -> dict[int, str]:
        """Materialise ``go_id`` strings on each prediction by FK lookup.

        Returns the ``int -> str`` map so the synthetic-ancestor FK
        resolver can reuse it without re-querying.
        """
        from sqlalchemy import select

        unique_int_ids = {rec["go_term_id"] for rec in prediction_dicts if rec.get("go_term_id")}
        id_pairs = session.execute(
            select(GOTerm.id, GOTerm.go_id).where(GOTerm.id.in_(unique_int_ids))
        ).all()
        int_to_str = {gid: go_id for gid, go_id in id_pairs}
        for rec in prediction_dicts:
            gid = rec.get("go_term_id")
            if gid is not None and gid in int_to_str:
                rec["go_id"] = int_to_str[gid]
        return int_to_str

    def _resolve_synthetic_fks(
        self,
        session: Session,
        prediction_dicts: list[dict[str, Any]],
        int_to_str: dict[int, str],
        snapshot_id: uuid.UUID,
    ) -> list[dict[str, Any]]:
        """Stamp ``go_term_id`` on synthetic ancestor records via the snapshot."""
        from sqlalchemy import select

        leaf_strs = set(int_to_str.values())
        ancestor_strs = {
            rec["go_id"]
            for rec in prediction_dicts
            if rec.get("go_id") and rec["go_id"] not in leaf_strs
        }
        if not ancestor_strs:
            return prediction_dicts
        anc_pairs = session.execute(
            select(GOTerm.id, GOTerm.go_id).where(
                GOTerm.go_id.in_(ancestor_strs),
                GOTerm.ontology_snapshot_id == snapshot_id,
            )
        ).all()
        str_to_int = {go_id: gid for gid, go_id in anc_pairs}
        str_to_int.update({v: k for k, v in int_to_str.items()})
        return [
            {**rec, "go_term_id": str_to_int[rec["go_id"]]}
            for rec in prediction_dicts
            if rec.get("go_id") in str_to_int
        ]

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

    def _chunked_publish(
        self,
        *,
        parent_job_id: UUID,
        prediction_set_id: uuid.UUID,
        prediction_dicts: list[dict[str, Any]],
    ) -> list[tuple[str, dict[str, Any]]]:
        """Split predictions into RabbitMQ-sized chunks for the write queue.

        RabbitMQ caps message size at 128 MB; ancestor-expanded batches
        serialise to ~250-300 MB and silently land in the dead-letter
        queue. Splitting into ~10k-row chunks (~20-25 MB each) keeps
        the broker happy and lets the parent job's batch counter
        advance only on the final chunk via ``is_final_chunk``.
        """
        from protea.config.tuning import get_tuning

        store_chunk_size = get_tuning().operation.store_chunk_size
        chunks: list[list[dict[str, Any]]] = [
            prediction_dicts[s : s + store_chunk_size]
            for s in range(0, len(prediction_dicts), store_chunk_size)
        ] or [[]]
        return [
            (
                _WRITE_QUEUE,
                {
                    "operation": "store_predictions",
                    "job_id": str(parent_job_id),
                    "payload": {
                        "parent_job_id": str(parent_job_id),
                        "prediction_set_id": str(prediction_set_id),
                        "predictions": chunk,
                        "is_final_chunk": i == len(chunks) - 1,
                    },
                },
            )
            for i, chunk in enumerate(chunks)
        ]

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
        """Delegate aspect-separated KNN + vote tally to ``pipeline.predict``
        (``aspect_separated=True``). Returns the legacy 4-tuple shape v6 /
        ancestor / reranker consumers still expect. See F2C.5b notes."""
        neighbors_by_aspect, inputs = _build_aspect_adapter_inputs(self, session, ctx)
        result = call_pipeline_predict_aspect_separated(
            inputs,
            neighbors_by_aspect=neighbors_by_aspect,
        )
        return (
            result.predictions,
            result.neighbors_by_aspect,
            result.go_map_by_aspect,
            result.pair_features,
        )
