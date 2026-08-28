"""Unified-pool KNN dispatch helpers for ``PredictGOTermsBatchOperation``.

F2C.5c extracts the unified-pool wire (KNN pre-search, annotation
load, pair-input load, ``pipeline.predict`` call) out of the
orchestrator class so the orchestrator stays under the master plan
§3 class ceiling. Behaviour is unchanged; each helper takes the
orchestrator instance for DB-bound loader access plus an explicit
context object, and returns the same shape the inline method did
pre-extraction.

The orchestrator keeps short delegate methods so unit tests that
patch :meth:`PredictGOTermsBatchOperation._run_unified_path` and
friends keep working without churn.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy.orm import Session

from protea.core.alignment_cache import SessionAlignmentCache
from protea.core.contracts.operation import EmitFn
from protea.core.operations._predict_go_terms_adapter import (
    AdapterInputs,
    AdapterResult,
)
from protea.core.operations.predict_go_terms._common import (
    _UnifiedPredictContext,
)
from protea.core.operations.predict_go_terms._sequence_identity import (
    load_sequence_identities,
)

if TYPE_CHECKING:
    from protea.core.operations.predict_go_terms._batch_op import (
        PredictGOTermsBatchOperation,
        _BatchExecCtx,
        _KnnResult,
        _QueryBatch,
    )


def run_unified_path(
    op: PredictGOTermsBatchOperation,
    session: Session,
    ctx: _BatchExecCtx,
    query_batch: _QueryBatch,
    ref_data: Any,
    emit: EmitFn,
) -> _KnnResult | None:
    """Unified-pool KNN via ``protea_method.pipeline.predict``.

    Returns ``None`` when the reference pool is empty (caller
    short-circuits with a no-op result).
    """
    from protea.core.operations.predict_go_terms._batch_op import _KnnResult

    if not ref_data["embeddings"].size:
        emit("predict_go_terms_batch.no_references", None, {}, "warning")
        return None
    adapter_result = op._unified_predict_via_pipeline(
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


def unified_predict_via_pipeline(
    op: PredictGOTermsBatchOperation,
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
    # Look up ``call_pipeline_predict`` through ``_batch_op`` so unit
    # tests that monkeypatch the shim symbol path keep flowing through
    # this helper (F2C.5c compatibility).
    from protea.core.operations.predict_go_terms import _batch_op

    annotations, unique_neighbors = op._unified_load_annotations(session, ctx)
    ref_sequences, query_sequences, ref_tax_ids, query_tax_ids = op._unified_load_pair_inputs(
        session, ctx, unique_neighbors
    )
    go_id_map, go_aspect_map = op._load_go_term_metadata(session, annotations)
    return _batch_op.call_pipeline_predict(
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
            alignment_cache=SessionAlignmentCache(session),
            ref_sequence_identities=load_sequence_identities(session, unique_neighbors),
        )
    )


def unified_load_annotations(
    op: PredictGOTermsBatchOperation,
    session: Session,
    ctx: _UnifiedPredictContext,
) -> tuple[dict[str, list[dict[str, Any]]], set[str]]:
    """Pre-search KNN to resolve the unique-neighbour set, then load
    the lazy go-map only for those references."""
    from protea.core.knn_search import search_knn
    from protea.core.operations.predict_go_terms._self_neighbour import (
        search_k_for,
        without_self,
    )

    p = ctx.p
    use_cos = p.metric == "cosine"
    ref_embeddings_f32 = (
        ctx.ref_data["embeddings_f32_cos"] if use_cos else ctx.ref_data["embeddings_f32"]
    )
    # One more than asked for when the query may not be its own neighbour, so
    # that dropping the self hit below leaves limit_per_entry real donors rather
    # than one fewer. See _self_neighbour for the measurement that prompted it.
    exclude_self = bool(getattr(p, "exclude_self_neighbour", False))
    neighbors = search_knn(
        ctx.query_embeddings,
        ref_embeddings_f32,
        ctx.ref_data["accessions"],
        k=search_k_for(p.limit_per_entry, exclude_self),
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
    neighbors = without_self(
        neighbors, list(ctx.valid_accessions), p.limit_per_entry, exclude_self
    )
    unique_neighbors: set[str] = {ref_acc for top_refs in neighbors for ref_acc, _ in top_refs}
    annotations = op._load_annotations_for(session, ctx.annotation_set_id, unique_neighbors)
    return annotations, unique_neighbors


def unified_load_pair_inputs(
    op: PredictGOTermsBatchOperation,
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
        ref_sequences = op._load_sequences_for_proteins(session, unique_neighbors)
        query_sequences = op._load_sequences_for_queries(session, p, ctx.valid_accessions)
    if p.compute_taxonomy:
        ref_tax_ids = op._load_taxonomy_ids_for_proteins(session, unique_neighbors)
        query_tax_ids = op._load_taxonomy_ids_for_queries(session, p, ctx.valid_accessions)
    return ref_sequences, query_sequences, ref_tax_ids, query_tax_ids


__all__ = (
    "run_unified_path",
    "unified_load_annotations",
    "unified_load_pair_inputs",
    "unified_predict_via_pipeline",
)
