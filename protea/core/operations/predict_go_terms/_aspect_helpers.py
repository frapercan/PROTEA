"""Aspect-separated KNN pre-search + adapter input builder.

Extracted from the monolithic ``predict_go_terms.py`` as part of T2B.6.
Behaviour is unchanged.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import numpy as np
from sqlalchemy.orm import Session

from protea.core.alignment_cache import SessionAlignmentCache
from protea.core.disk_cache import AnnoCsr, _csr_lookup
from protea.core.domain.aspect import ASPECT_CODES as _ASPECTS
from protea.core.knn_search import search_knn
from protea.core.operations._predict_go_terms_adapter import (
    AdapterInputs,
    call_pipeline_predict_aspect_separated,
)
from protea.core.operations.predict_go_terms._common import (
    _UNIFIED_REF_KEY,
    AspectSeparatedKnnContext,
    PredictGOTermsBatchPayload,
)
from protea.core.operations.predict_go_terms._self_neighbour import (
    search_k_for,
    without_self,
)
from protea.core.operations.predict_go_terms._sequence_identity import (
    load_sequence_identities,
)

if TYPE_CHECKING:
    from protea.core.operations.predict_go_terms._batch_op import (
        PredictGOTermsBatchOperation,
    )


class _AspectKnnPreSearch:
    """Per-aspect KNN pre-search used before the ``pipeline.predict()`` call.

    The aspect-separated wire delegates the vote tally + row
    materialisation to ``protea_method.pipeline.predict``. PROTEA still
    runs a thin per-aspect KNN pass first so it can scope the
    annotation / sequence / taxonomy loads to refs that actually got
    hit. ``pipeline.predict`` then re-runs the partitioned KNN against
    the unified pool with bit-exact partitioning rules (a ref is in
    aspect ``a``'s bank iff one of its annotations resolves to ``a``).
    """

    @staticmethod
    def _knn_one_aspect(
        aspect: str,
        valid_accessions: list[str],
        query_embeddings: np.ndarray,
        ref_data_by_aspect: dict[str, dict[str, Any]],
        p: PredictGOTermsBatchPayload,
        use_cos: bool,
    ) -> tuple[str, list[list[tuple[str, float]]]]:
        """Run KNN for one GO aspect and return ``(aspect, neighbors_list)``."""
        aspect_refs = ref_data_by_aspect[aspect]
        if not aspect_refs["accessions"]:
            return aspect, [[] for _ in valid_accessions]
        ref_f32 = (
            aspect_refs["embeddings_f32_cos"] if use_cos else aspect_refs["embeddings_f32"]
        )
        # Per aspect, the same rule as the unified path: ask for one more when
        # the query may not be its own neighbour, then drop it. Aspect-separated
        # retrieval hits this harder, because a protein present in all three
        # aspect corpora retrieves itself three times.
        exclude_self = bool(getattr(p, "exclude_self_neighbour", False))
        result = search_knn(
            query_embeddings,
            ref_f32,
            aspect_refs["accessions"],
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
        return aspect, without_self(
            result, list(valid_accessions), p.limit_per_entry, exclude_self
        )

    @staticmethod
    def run(
        valid_accessions: list[str],
        query_embeddings: np.ndarray,
        ref_data_by_aspect: dict[str, dict[str, Any]],
        p: PredictGOTermsBatchPayload,
    ) -> tuple[dict[str, list[list[tuple[str, float]]]], set[str]]:
        """Return ``(neighbors_by_aspect, all_unique_neighbors)``.

        Per-aspect KNN searches run in parallel via ``ThreadPoolExecutor``
        when ``aspect_knn_workers > 1`` (default 3). numpy's BLAS routines
        release the GIL for matrix ops so the three searches overlap on CPU.
        """
        from protea.config.tuning import get_tuning

        n_workers = get_tuning().operation.aspect_knn_workers
        use_cos = p.metric == "cosine"

        def _run_one(asp: str) -> tuple[str, list[list[tuple[str, float]]]]:
            return _AspectKnnPreSearch._knn_one_aspect(
                asp, valid_accessions, query_embeddings, ref_data_by_aspect, p, use_cos
            )

        neighbors_by_aspect: dict[str, list[list[tuple[str, float]]]] = {}
        if n_workers > 1 and len(_ASPECTS) > 1:
            with ThreadPoolExecutor(max_workers=n_workers) as pool:
                for asp, result in pool.map(_run_one, _ASPECTS):
                    neighbors_by_aspect[asp] = result
        else:
            for aspect in _ASPECTS:
                asp, result = _run_one(aspect)
                neighbors_by_aspect[asp] = result

        all_unique_neighbors: set[str] = set()
        for top_refs_list in neighbors_by_aspect.values():
            for top_refs in top_refs_list:
                for ref_acc, _ in top_refs:
                    all_unique_neighbors.add(ref_acc)
        return neighbors_by_aspect, all_unique_neighbors


@dataclass(frozen=True)
class DonorSource:
    """Where donations come from, and which of them the policy admits.

    The two travel together everywhere a donation is read: the loader, the
    CSR builder and the on-disk cache key. Passing them as one says that a
    set without its policy is not a source, which is the mistake that let a
    protein admitted on one experimental annotation donate every annotation
    it had.
    """

    annotation_set_id: Any
    policy: Any = None


def _load_aspect_go_map_for_hits(
    op: PredictGOTermsBatchOperation,
    session: Session,
    aspect_ref: dict[str, Any],
    source: DonorSource,
    hits_in_aspect: set[str],
    aspect: str,
) -> dict[str, list[dict[str, Any]]]:
    """Resolve one aspect's ``ref_acc -> annotations`` map for hit refs.

    Prefers the in-memory CSR cache attached to ``aspect_ref`` by
    :meth:`PredictGOTermsBatchOperation._assemble_aspect_view`; falls
    back to a DB query with the aspect filter when the CSR is absent.
    """
    if "anno_gtids" in aspect_ref:
        return _csr_lookup(
            hits_in_aspect,
            aspect_ref["acc_to_anno_idx"],
            AnnoCsr(
                gtids=aspect_ref["anno_gtids"],
                quals=aspect_ref["anno_quals"],
                ecodes=aspect_ref["anno_ecodes"],
                offsets=aspect_ref["anno_offsets"],
            ),
        )
    return op._load_annotations_for(
        session, source.annotation_set_id, hits_in_aspect, aspect=aspect,
        donor_policy=source.policy,
    )


def _load_aspect_separated_annotations(
    op: PredictGOTermsBatchOperation,
    session: Session,
    ref_data_by_aspect: dict[str, dict[str, Any]],
    annotation_set_id: Any,
    all_unique_neighbors: set[str],
    donor_policy: Any = None,
) -> dict[str, list[dict[str, Any]]]:
    """Union per-aspect annotations into a single ``ref_acc -> [anns]`` dict.

    ``pipeline.predict()``'s ``_annotation_aggregates`` sums
    ``go_term_frequency`` and ``ref_annotation_density`` across the
    full annotation set; restricting to one aspect's annotations would
    under-count both PROTEA-style and break the bit-exact
    aggregate-stamping the booster ingested at training time. Reuses
    the per-aspect CSR cache when present.
    """
    annotations: dict[str, list[dict[str, Any]]] = {}
    for aspect in _ASPECTS:
        aspect_ref = ref_data_by_aspect[aspect]
        hits_in_aspect = all_unique_neighbors & set(aspect_ref["accessions"])
        if not hits_in_aspect:
            continue
        asp_go_map = _load_aspect_go_map_for_hits(
            op, session, aspect_ref,
            DonorSource(annotation_set_id, donor_policy), hits_in_aspect, aspect,
        )
        for ref_acc, anns in asp_go_map.items():
            annotations.setdefault(ref_acc, []).extend(anns)
    return annotations


def _build_aspect_adapter_inputs(
    op: PredictGOTermsBatchOperation,
    session: Session,
    ctx: AspectSeparatedKnnContext,
) -> tuple[dict[str, list[list[tuple[str, float]]]], AdapterInputs]:
    """Gather everything the aspect-separated adapter call needs.

    Runs the per-aspect KNN pre-search, loads hit-ref annotations
    across aspects, hydrates the feature-engineering inputs, and
    materialises the GO term metadata maps. Returns both the
    pre-search neighbours (so the pair-feature builder can walk them
    once) and the ``AdapterInputs`` tuple the adapter consumes.
    """
    p = ctx.payload
    neighbors_by_aspect, all_unique_neighbors = _AspectKnnPreSearch.run(
        ctx.valid_accessions, ctx.query_embeddings, ctx.ref_data_by_aspect, p,
    )
    annotations = _load_aspect_separated_annotations(
        op, session, ctx.ref_data_by_aspect, ctx.annotation_set_id, all_unique_neighbors,
        getattr(p, "donor_policy", None),
    )
    ref_sequences, query_sequences, ref_tax_ids, query_tax_ids = (
        op._load_feature_engineering_data(
            session, p, ctx.valid_accessions, all_unique_neighbors,
        )
    )
    go_id_map, go_aspect_map = op._load_go_term_metadata(session, annotations)
    return neighbors_by_aspect, AdapterInputs(
        p=p,
        valid_accessions=ctx.valid_accessions,
        query_embeddings=ctx.query_embeddings,
        ref_data=ctx.ref_data_by_aspect[_UNIFIED_REF_KEY],
        annotations=annotations,
        go_id_map=go_id_map,
        go_aspect_map=go_aspect_map,
        prediction_set_id=ctx.prediction_set_id,
        ref_sequences=ref_sequences,
        query_sequences=query_sequences,
        ref_tax_ids=ref_tax_ids,
        query_tax_ids=query_tax_ids,
        alignment_cache=SessionAlignmentCache(session),
        # The QUERIES go in too, not only the bank. The method excludes a
            # query from its own neighbourhood by SEQUENCE, so it has to be
            # able to recognise the query's own; with only the bank mapped it
            # refuses, which is correct and useless.
            ref_sequence_identities=load_sequence_identities(
                session, set(ctx.valid_accessions) | set(all_unique_neighbors)
            ),
    )


def run_aspect_separated_knn(
    op: PredictGOTermsBatchOperation,
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
    neighbors_by_aspect, inputs = _build_aspect_adapter_inputs(op, session, ctx)
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


def run_aspect_separated_path(
    op: PredictGOTermsBatchOperation,
    session: Session,
    ctx: Any,
    query_batch: Any,
    ref_data: Any,
) -> Any:
    """Aspect-separated KNN dispatch; one pass per GO aspect.

    Returns ``_KnnResult`` shaped output (imported lazily to avoid a
    circular import with ``_batch_op``).
    """
    from protea.core.operations.predict_go_terms._batch_op import _KnnResult

    p = ctx.p
    (
        prediction_dicts,
        neighbors_by_aspect,
        go_map_by_aspect,
        pair_features,
    ) = op._run_aspect_separated_knn(
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


# Re-export ``call_pipeline_predict_aspect_separated`` so the batch op's
# ``_run_aspect_separated_knn`` can import a single name from this module.
__all__ = (
    "_AspectKnnPreSearch",
    "_build_aspect_adapter_inputs",
    "_load_aspect_go_map_for_hits",
    "_load_aspect_separated_annotations",
    "call_pipeline_predict_aspect_separated",
    "run_aspect_separated_knn",
    "run_aspect_separated_path",
)
