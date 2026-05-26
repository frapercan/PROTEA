"""export_knn_batch: run KNN for one snapshot pair and write a temp artefact.

This is an ``OperationConsumer`` operation (no DB Job row per message).
It runs on ``protea.training.knn-batch`` workers.

Execution flow:
1. Load the reference embedding pool from disk cache for the
   ``(embedding_config_id, annotation_set_id)`` pair.
2. Load query embeddings for ``query_version`` from the DB.
3. Run KNN search using ``search_backend`` (faiss / numpy / torch).
4. Write ``(pair_id, top_k_indices, top_k_distances, ref_accessions)``
   as a ``.npz`` artefact to the coordinator's temp prefix:
   ``temp/coordinator/<coordinator_job_id>/knn/<pair_id>.npz``.
5. Emit ``pair_knn_done`` event.
6. Publish a follow-on ``export_features_batch`` message to
   ``protea.training.features``.
"""

from __future__ import annotations

import logging
import time
import uuid
from io import BytesIO
from pathlib import Path
from typing import Any

import numpy as np
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, OperationResult
from protea.core.operations.export_minijob._payloads import (
    ExportKnnBatchPayload,
)

_LOG = logging.getLogger(__name__)
_FEATURES_QUEUE = "protea.training.features"


class ExportKnnBatchOperation:
    """OperationConsumer: run KNN for one snapshot pair.

    No DB Job row is created per message (mirrors
    ``ComputeEmbeddingsBatchOperation``). The coordinator's Job row
    tracks overall progress via ``update_parent_progress``.
    """

    name = "export_knn_batch"
    description = (
        "GPU/CPU minijob: run KNN for one snapshot pair and write the "
        "result to a temp artifact-store prefix. Publishes a follow-on "
        "export_features_batch message."
    )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        p = payload or {}
        bits = []
        if p.get("pair_id"):
            bits.append(str(p["pair_id"]))
        if p.get("output_name"):
            bits.append(str(p["output_name"]))
        return " · ".join(bits)

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = ExportKnnBatchPayload.model_validate(payload)
        t0 = time.perf_counter()
        emit(
            "export_knn_batch.start",
            None,
            {"pair_id": p.pair_id, "pair_type": p.pair_type},
            "info",
        )
        knn_temp_uri, n_queries = _run_knn_phase(session, p, emit)
        elapsed = time.perf_counter() - t0
        emit(
            "export_knn_batch.pair_knn_done",
            None,
            {
                "pair_id": p.pair_id,
                "pair_type": p.pair_type,
                "n_queries": n_queries,
                "elapsed_seconds": elapsed,
                "knn_temp_uri": knn_temp_uri,
            },
            "info",
        )
        return OperationResult(
            result={
                "pair_id": p.pair_id,
                "pair_type": p.pair_type,
                "knn_temp_uri": knn_temp_uri,
                "n_queries": n_queries,
                "elapsed_seconds": elapsed,
            },
            publish_operations=[(_FEATURES_QUEUE, _build_features_msg(p, knn_temp_uri))],
        )


def _run_knn_phase(
    session: Session,
    p: ExportKnnBatchPayload,
    emit: EmitFn,
) -> tuple[str, int]:
    """Load embeddings, run KNN, write artefact; returns (uri, n_queries)."""
    ref_emb, ref_accessions = _load_ref_pool(p.embedding_config_id, p.annotation_set_id, emit)
    query_emb, query_accessions = _load_query_embeddings(
        session, p.embedding_config_id, p.annotation_set_id, emit
    )
    indices, distances = _run_knn(query_emb, ref_emb, p.k, p.search_backend, emit)
    temp_key = _knn_temp_key(p.coordinator_job_id, p.pair_id)
    uri = _write_knn_artefact(temp_key, indices, distances, query_accessions, ref_accessions)
    return uri, len(query_accessions)


def _build_features_msg(p: ExportKnnBatchPayload, knn_temp_uri: str) -> dict[str, Any]:
    """Build the publish dict for the follow-on features minijob."""
    return {
        "coordinator_job_id": p.coordinator_job_id,
        "pair_id": p.pair_id,
        "pair_type": p.pair_type,
        "query_version": p.query_version,
        "ref_version": p.ref_version,
        "embedding_config_id": p.embedding_config_id,
        "annotation_set_id": p.annotation_set_id,
        "ontology_snapshot_id": p.ontology_snapshot_id,
        "output_name": p.output_name,
        "knn_temp_uri": knn_temp_uri,
        "k": p.k,
        "search_backend": p.search_backend,
        "annotation_source": p.annotation_source,
        "compute_alignments": p.compute_alignments,
        "compute_taxonomy": p.compute_taxonomy,
        "expand_votes_to_ancestors": p.expand_votes_to_ancestors,
        "use_embedding_pca": p.use_embedding_pca,
        "total_expected_pairs": p.total_expected_pairs,
    }


def _knn_temp_key(coordinator_job_id: str, pair_id: str) -> str:
    """Return the artifact-store key for a KNN temp result."""
    return f"temp/coordinator/{coordinator_job_id}/knn/{pair_id}.npz"


def _load_ref_pool(
    embedding_config_id: str,
    annotation_set_id: str,
    emit: EmitFn,
) -> tuple[np.ndarray, list[str]]:
    """Load the reference embedding pool from disk cache.

    Raises ``RuntimeError`` if the cache is missing so the worker can
    NACK and retry after the cache is pre-warmed.
    """
    from protea.core.disk_cache import _disk_cache_paths

    cfg_uuid = uuid.UUID(embedding_config_id)
    ann_uuid = uuid.UUID(annotation_set_id)
    emb_path, acc_path = _disk_cache_paths(cfg_uuid, ann_uuid)
    if not emb_path.exists() or not acc_path.exists():
        raise RuntimeError(
            f"Reference pool cache missing for config={embedding_config_id} "
            f"ann={annotation_set_id}. Pre-warm the cache before dispatching "
            "export_knn_batch messages."
        )
    emit("export_knn_batch.load_ref_pool", None, {"emb_path": str(emb_path)}, "info")
    ref_emb = np.load(str(emb_path), mmap_mode="r").astype(np.float32)
    ref_accessions: list[str] = np.load(str(acc_path), allow_pickle=True).tolist()
    return ref_emb, ref_accessions


def _load_query_embeddings(
    session: Session,
    embedding_config_id: str,
    annotation_set_id: str,
    emit: EmitFn,
) -> tuple[np.ndarray, list[str]]:
    """Load per-protein embeddings for proteins in the annotation set."""
    import uuid as _uuid

    from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet

    cfg_uuid = _uuid.UUID(embedding_config_id)
    ann_uuid = _uuid.UUID(annotation_set_id)

    ann_set = session.query(AnnotationSet).filter(AnnotationSet.id == ann_uuid).first()
    if ann_set is None:
        raise ValueError(f"AnnotationSet {annotation_set_id} not found")

    emit("export_knn_batch.load_query_embeddings", None, {"annotation_set_id": annotation_set_id}, "info")
    rows = _fetch_embeddings_for_ann_set(session, ann_uuid, cfg_uuid)
    if not rows:
        raise ValueError(
            f"No embeddings for config={embedding_config_id} in annotation set {annotation_set_id}"
        )
    query_accessions = [row[0] for row in rows]
    query_emb = np.stack([np.array(row[1], dtype=np.float32) for row in rows], axis=0)
    emit("export_knn_batch.load_query_done", None, {"n_queries": len(query_accessions)}, "info")
    return query_emb, query_accessions


def _fetch_embeddings_for_ann_set(
    session: Session,
    ann_uuid: Any,
    cfg_uuid: Any,
) -> list[Any]:
    """Fetch (accession, embedding) rows for proteins in the annotation set."""
    from sqlalchemy import and_, distinct

    from protea.infrastructure.orm.models.annotation.protein_go_annotation import (
        ProteinGOAnnotation,
    )
    from protea.infrastructure.orm.models.embedding.sequence_embedding import SequenceEmbedding
    from protea.infrastructure.orm.models.protein.protein import Protein
    from protea.infrastructure.orm.models.sequence.sequence import Sequence

    ann_accessions = {
        row[0]
        for row in session.query(distinct(ProteinGOAnnotation.protein_accession))
        .filter(ProteinGOAnnotation.annotation_set_id == ann_uuid)
        .all()
    }
    return (
        session.query(Protein.accession, SequenceEmbedding.embedding)
        .join(Sequence, Sequence.id == Protein.sequence_id)
        .join(
            SequenceEmbedding,
            and_(
                SequenceEmbedding.sequence_id == Sequence.id,
                SequenceEmbedding.embedding_config_id == cfg_uuid,
            ),
        )
        .filter(Protein.accession.in_(ann_accessions))
        .all()
    )


def _run_knn(
    query_emb: np.ndarray,
    ref_emb: np.ndarray,
    k: int,
    backend: str,
    emit: EmitFn,
) -> tuple[np.ndarray, np.ndarray]:
    """Run KNN search and return ``(indices, distances)``."""
    from protea.core.knn_search import search_knn

    emit("export_knn_batch.knn_start", None, {"backend": backend, "k": k}, "info")
    ref_accessions = [str(i) for i in range(len(ref_emb))]
    results = search_knn(
        query_emb, ref_emb, ref_accessions, k=k, distance_threshold=None, backend=backend
    )
    n_queries = len(results)
    indices = np.full((n_queries, k), -1, dtype=np.int32)
    distances = np.full((n_queries, k), float("nan"), dtype=np.float32)
    for q_idx, neighbors in enumerate(results):
        for rank, (acc_str, dist) in enumerate(neighbors[:k]):
            indices[q_idx, rank] = int(acc_str)
            distances[q_idx, rank] = float(dist)
    emit("export_knn_batch.knn_done", None, {"n_queries": n_queries, "k": k}, "info")
    return indices, distances


def _write_knn_artefact(
    temp_key: str,
    indices: np.ndarray,
    distances: np.ndarray,
    query_accessions: list[str],
    ref_accessions: list[str],
) -> str:
    """Write KNN result to the configured artifact store; return the URI."""
    from protea.infrastructure.settings import load_settings
    from protea.infrastructure.storage import get_artifact_store

    project_root = Path(__file__).resolve().parents[5]
    settings = load_settings(project_root)
    store = get_artifact_store(settings)
    buf = BytesIO()
    np.savez_compressed(
        buf,
        indices=indices,
        distances=distances,
        query_accessions=np.array(query_accessions),
        ref_accessions=np.array(ref_accessions),
    )
    buf.seek(0)
    return store.put(temp_key, buf.getvalue())
