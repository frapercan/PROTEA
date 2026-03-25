from __future__ import annotations

import os
import time
import uuid
from pathlib import Path
from typing import Annotated, Any
from uuid import UUID

import numpy as np
from pydantic import Field, field_validator
from sqlalchemy import update as sa_update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.core.feature_engineering import compute_alignment, compute_taxonomy
from protea.core.knn_search import search_knn
from protea.core.utils import utcnow
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
from protea.infrastructure.orm.models.annotation.protein_go_annotation import ProteinGOAnnotation
from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
from protea.infrastructure.orm.models.embedding.go_prediction import GOPrediction
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.embedding.sequence_embedding import SequenceEmbedding
from protea.infrastructure.orm.models.job import Job, JobEvent, JobStatus
from protea.infrastructure.orm.models.protein.protein import Protein
from protea.infrastructure.orm.models.query.query_set import QuerySet, QuerySetEntry
from protea.infrastructure.orm.models.sequence.sequence import Sequence

PositiveInt = Annotated[int, Field(gt=0)]

_ANNOTATION_CHUNK_SIZE = 10_000
_BATCH_QUEUE = "protea.predictions.batch"
_WRITE_QUEUE = "protea.predictions.write"
# Rows fetched per round-trip when streaming reference embeddings from PostgreSQL.
# At 1280 dims × 2 bytes (float16) × 2000 rows = ~5 MB per chunk — keeps Python
# object pressure negligible while amortising cursor round-trips.
_STREAM_CHUNK_SIZE = 2_000

# GO aspect single-character codes used in GOTerm.aspect
_ASPECTS = ("P", "F", "C")  # biological_process, molecular_function, cellular_component

# ---------------------------------------------------------------------------
# Process-level reference cache
# Keyed by (embedding_config_id_str, annotation_set_id_str).
# Value: {"accessions": list[str], "embeddings": np.ndarray (float16)}
# GO annotations are NOT cached — loaded lazily per batch for the unique
# neighbors actually found, avoiding ~5-10 GB of Python dicts in memory.
# Embeddings stored as float16 (half of float32) — converted to float32
# at KNN time with negligible accuracy loss for cosine similarity.
# Limited to 1 entry — evicts previous reference on config change.
# ---------------------------------------------------------------------------
_REF_CACHE: dict[tuple[str, str, bool], dict[str, Any]] = {}
_REF_CACHE_MAX = 1

# ---------------------------------------------------------------------------
# Disk cache for reference embeddings
# Survives worker restarts — avoids re-fetching GB of vectors from PostgreSQL.
# Files: {cache_dir}/{emb_config_id}__{ann_set_id}_embeddings.npy
#         {cache_dir}/{emb_config_id}__{ann_set_id}_accessions.npy
# Invalidation: annotation sets are immutable once loaded, so the cache is
# valid as long as the file exists. Delete files manually to force a reload.
# ---------------------------------------------------------------------------
_DISK_CACHE_DIR = Path(os.environ.get("PROTEA_REF_CACHE_DIR", "data/ref_cache"))


def _disk_cache_paths(
    embedding_config_id: uuid.UUID,
    annotation_set_id: uuid.UUID,
) -> tuple[Path, Path]:
    """Return (embeddings_path, accessions_path) for the unified reference cache."""
    key = f"{embedding_config_id}__{annotation_set_id}"
    return (
        _DISK_CACHE_DIR / f"{key}_embeddings.npy",
        _DISK_CACHE_DIR / f"{key}_accessions.npy",
    )


def _aspect_index_path(
    embedding_config_id: uuid.UUID,
    annotation_set_id: uuid.UUID,
    aspect: str,
) -> Path:
    """Return the path for the per-aspect index array (int32 indices into the unified cache)."""
    key = f"{embedding_config_id}__{annotation_set_id}"
    return _DISK_CACHE_DIR / f"{key}__{aspect}_indices.npy"


def _anno_disk_cache_paths(
    embedding_config_id: uuid.UUID,
    annotation_set_id: uuid.UUID,
    aspect: str,
) -> tuple[Path, Path, Path, Path]:
    """Return (gtids, quals, ecodes, offsets) paths for the annotation CSR cache."""
    key = f"{embedding_config_id}__{annotation_set_id}__{aspect}"
    base = _DISK_CACHE_DIR
    return (
        base / f"{key}_anno_gtids.npy",
        base / f"{key}_anno_quals.npy",
        base / f"{key}_anno_ecodes.npy",
        base / f"{key}_anno_offsets.npy",
    )


def _build_anno_csr(
    accessions: list[str],
    go_map: dict[str, list[dict[str, Any]]],
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Build a CSR-style annotation structure for the given accession list.

    Returns (go_term_ids, qualifiers, evidence_codes, offsets) where
    annotations for accessions[i] are at indices offsets[i]:offsets[i+1].
    """
    all_gtids: list[int] = []
    all_quals: list[Any] = []
    all_ecodes: list[Any] = []
    offsets: list[int] = [0]
    for acc in accessions:
        for ann in go_map.get(acc, []):
            all_gtids.append(ann["go_term_id"])
            all_quals.append(ann.get("qualifier"))
            all_ecodes.append(ann.get("evidence_code"))
        offsets.append(len(all_gtids))
    return (
        np.array(all_gtids, dtype=np.int32),
        np.array(all_quals, dtype=object),
        np.array(all_ecodes, dtype=object),
        np.array(offsets, dtype=np.int32),
    )


def _load_anno_csr_from_disk(
    embedding_config_id: uuid.UUID,
    annotation_set_id: uuid.UUID,
    aspect: str,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray] | None:
    """Load annotation CSR arrays from disk. Returns None on miss or error."""
    gtids_p, quals_p, ecodes_p, offsets_p = _anno_disk_cache_paths(
        embedding_config_id, annotation_set_id, aspect
    )
    if not all(p.exists() for p in (gtids_p, quals_p, ecodes_p, offsets_p)):
        return None
    try:
        return (
            np.load(gtids_p),
            np.load(quals_p, allow_pickle=True),
            np.load(ecodes_p, allow_pickle=True),
            np.load(offsets_p),
        )
    except Exception:
        return None


def _save_anno_csr_to_disk(
    embedding_config_id: uuid.UUID,
    annotation_set_id: uuid.UUID,
    aspect: str,
    gtids: np.ndarray,
    quals: np.ndarray,
    ecodes: np.ndarray,
    offsets: np.ndarray,
) -> None:
    gtids_p, quals_p, ecodes_p, offsets_p = _anno_disk_cache_paths(
        embedding_config_id, annotation_set_id, aspect
    )
    gtids_p.parent.mkdir(parents=True, exist_ok=True)
    np.save(gtids_p, gtids)
    np.save(quals_p, quals)
    np.save(ecodes_p, ecodes)
    np.save(offsets_p, offsets)


def _csr_lookup(
    query_accessions: set[str],
    accessions: list[str],
    acc_to_anno_idx: dict[str, int],
    gtids: np.ndarray,
    quals: np.ndarray,
    ecodes: np.ndarray,
    offsets: np.ndarray,
) -> dict[str, list[dict[str, Any]]]:
    """Return a go_map for query_accessions using the preloaded CSR annotation cache."""
    go_map: dict[str, list[dict[str, Any]]] = {}
    for acc in query_accessions:
        idx = acc_to_anno_idx.get(acc)
        if idx is None:
            continue
        start, end = int(offsets[idx]), int(offsets[idx + 1])
        if start >= end:
            continue
        go_map[acc] = [
            {
                "go_term_id": int(gtids[j]),
                "qualifier": quals[j] if quals[j] is not None else None,
                "evidence_code": ecodes[j] if ecodes[j] is not None else None,
            }
            for j in range(start, end)
        ]
    return go_map


def _load_from_disk_cache(
    embedding_config_id: uuid.UUID,
    annotation_set_id: uuid.UUID,
) -> dict[str, Any] | None:
    emb_path, acc_path = _disk_cache_paths(embedding_config_id, annotation_set_id)
    if not emb_path.exists() or not acc_path.exists():
        return None
    try:
        embeddings = np.load(emb_path)
        accessions = list(np.load(acc_path))
        return {"accessions": accessions, "embeddings": embeddings}
    except Exception:
        return None


def _save_to_disk_cache(
    embedding_config_id: uuid.UUID,
    annotation_set_id: uuid.UUID,
    accessions: list[str],
    embeddings: np.ndarray,
) -> None:
    emb_path, acc_path = _disk_cache_paths(embedding_config_id, annotation_set_id)
    emb_path.parent.mkdir(parents=True, exist_ok=True)
    np.save(emb_path, embeddings)
    np.save(acc_path, np.array(accessions))


# ---------------------------------------------------------------------------
# Payloads
# ---------------------------------------------------------------------------


class PredictGOTermsPayload(ProteaPayload, frozen=True):
    """Payload for the predict_go_terms coordinator job."""

    embedding_config_id: str
    annotation_set_id: str
    ontology_snapshot_id: str
    query_accessions: list[str] | None = None
    query_set_id: str | None = None
    limit_per_entry: PositiveInt = 5
    distance_threshold: float | None = None
    batch_size: PositiveInt = 1024

    # Search backend
    search_backend: str = "numpy"
    metric: str = "cosine"
    faiss_index_type: str = "Flat"
    faiss_nlist: int = 100
    faiss_nprobe: int = 10
    faiss_hnsw_m: int = 32
    faiss_hnsw_ef_search: int = 64

    # Feature engineering (opt-in)
    compute_alignments: bool = False
    compute_taxonomy: bool = False
    compute_reranker_features: bool = False

    # Per-aspect KNN indices (opt-in)
    # When True, three separate KNN indices are built — one per GO aspect (P/F/C).
    # Each index contains only reference proteins annotated in that aspect, and only
    # annotations of that aspect are transferred from matched neighbors.
    # This guarantees that every query protein receives BPO, MFO, and CCO candidates
    # even if its nearest neighbors in a unified index happen to be annotated only in
    # one or two aspects (a common cause of BPO recall ceilings).
    # Memory cost: 3× the reference embedding array; search time: 3 KNN calls per batch.
    aspect_separated_knn: bool = True

    @field_validator(
        "embedding_config_id", "annotation_set_id", "ontology_snapshot_id", mode="before"
    )
    @classmethod
    def must_be_non_empty(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("must be a non-empty string")
        return v.strip()


class PredictGOTermsBatchPayload(ProteaPayload, frozen=True):
    """Payload for one KNN batch dispatched by the coordinator."""

    embedding_config_id: str
    annotation_set_id: str
    prediction_set_id: str
    parent_job_id: str
    query_accessions: list[str]
    query_set_id: str | None = None
    limit_per_entry: PositiveInt = 5
    distance_threshold: float | None = None
    search_backend: str = "numpy"
    metric: str = "cosine"
    faiss_index_type: str = "Flat"
    faiss_nlist: int = 100
    faiss_nprobe: int = 10
    faiss_hnsw_m: int = 32
    faiss_hnsw_ef_search: int = 64
    compute_alignments: bool = False
    compute_taxonomy: bool = False
    compute_reranker_features: bool = False
    aspect_separated_knn: bool = True


class StorePredictionsPayload(ProteaPayload, frozen=True):
    """Payload carrying serialized prediction dicts to the write worker."""

    parent_job_id: str
    prediction_set_id: str
    predictions: list[dict[str, Any]]


# ---------------------------------------------------------------------------
# Coordinator
# ---------------------------------------------------------------------------


class PredictGOTermsOperation:
    """Coordinator: validates, creates PredictionSet, dispatches N batch messages.

    Pipeline:
    1. Validate EmbeddingConfig / AnnotationSet / OntologySnapshot.
    2. Load query accessions that have embeddings (no embedding data — keeps
       the coordinator session light).
    3. Create PredictionSet.
    4. Partition accessions into batches and publish to protea.predictions.batch.

    The actual KNN search and GO transfer happen inside PredictGOTermsBatchOperation.
    """

    name = "predict_go_terms"

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = PredictGOTermsPayload.model_validate(payload)
        parent_job_id = UUID(payload["_job_id"])

        embedding_config_id = uuid.UUID(p.embedding_config_id)
        annotation_set_id = uuid.UUID(p.annotation_set_id)
        ontology_snapshot_id = uuid.UUID(p.ontology_snapshot_id)

        config = session.get(EmbeddingConfig, embedding_config_id)
        if config is None:
            raise ValueError(f"EmbeddingConfig {p.embedding_config_id} not found")
        if session.get(AnnotationSet, annotation_set_id) is None:
            raise ValueError(f"AnnotationSet {p.annotation_set_id} not found")
        if session.get(OntologySnapshot, ontology_snapshot_id) is None:
            raise ValueError(f"OntologySnapshot {p.ontology_snapshot_id} not found")

        emit(
            "predict_go_terms.start",
            None,
            {
                "embedding_config_id": p.embedding_config_id,
                "model_name": config.model_name,
                "annotation_set_id": p.annotation_set_id,
                "limit_per_entry": p.limit_per_entry,
                "search_backend": p.search_backend,
            },
            "info",
        )

        query_accessions = self._load_query_accessions(session, p, embedding_config_id, emit)
        if not query_accessions:
            emit("predict_go_terms.no_queries", None, {}, "warning")
            return OperationResult(result={"batches": 0, "queries": 0})

        prediction_set = PredictionSet(
            embedding_config_id=embedding_config_id,
            annotation_set_id=annotation_set_id,
            ontology_snapshot_id=ontology_snapshot_id,
            query_set_id=uuid.UUID(p.query_set_id) if p.query_set_id else None,
            limit_per_entry=p.limit_per_entry,
            distance_threshold=p.distance_threshold,
            meta={},
        )
        session.add(prediction_set)
        session.flush()

        batches = [
            query_accessions[i : i + p.batch_size]
            for i in range(0, len(query_accessions), p.batch_size)
        ]
        n_batches = len(batches)

        emit(
            "predict_go_terms.dispatching",
            None,
            {
                "queries": len(query_accessions),
                "batches": n_batches,
                "prediction_set_id": str(prediction_set.id),
            },
            "info",
        )

        operations: list[tuple[str, dict[str, Any]]] = []
        for batch_accs in batches:
            operations.append(
                (
                    _BATCH_QUEUE,
                    {
                        "operation": "predict_go_terms_batch",
                        "job_id": str(parent_job_id),
                        "payload": {
                            "embedding_config_id": p.embedding_config_id,
                            "annotation_set_id": p.annotation_set_id,
                            "prediction_set_id": str(prediction_set.id),
                            "parent_job_id": str(parent_job_id),
                            "query_accessions": batch_accs,
                            "query_set_id": p.query_set_id,
                            "limit_per_entry": p.limit_per_entry,
                            "distance_threshold": p.distance_threshold,
                            "search_backend": p.search_backend,
                            "metric": p.metric,
                            "faiss_index_type": p.faiss_index_type,
                            "faiss_nlist": p.faiss_nlist,
                            "faiss_nprobe": p.faiss_nprobe,
                            "faiss_hnsw_m": p.faiss_hnsw_m,
                            "faiss_hnsw_ef_search": p.faiss_hnsw_ef_search,
                            "compute_alignments": p.compute_alignments,
                            "compute_taxonomy": p.compute_taxonomy,
                            "compute_reranker_features": p.compute_reranker_features,
                            "aspect_separated_knn": p.aspect_separated_knn,
                        },
                    },
                )
            )

        return OperationResult(
            result={
                "batches": n_batches,
                "queries": len(query_accessions),
                "prediction_set_id": str(prediction_set.id),
            },
            progress_current=0,
            progress_total=n_batches,
            deferred=True,
            publish_operations=operations,
        )

    def _load_query_accessions(
        self,
        session: Session,
        p: PredictGOTermsPayload,
        embedding_config_id: uuid.UUID,
        emit: EmitFn,
    ) -> list[str]:
        """Load accessions for query proteins that have an embedding."""
        emit("predict_go_terms.load_queries_start", None, {}, "info")

        if p.query_set_id:
            query_set_id = uuid.UUID(p.query_set_id)
            if session.get(QuerySet, query_set_id) is None:
                raise ValueError(f"QuerySet {p.query_set_id} not found")
            rows = (
                session.query(QuerySetEntry.accession)
                .join(
                    SequenceEmbedding,
                    (SequenceEmbedding.sequence_id == QuerySetEntry.sequence_id)
                    & (SequenceEmbedding.embedding_config_id == embedding_config_id),
                )
                .filter(QuerySetEntry.query_set_id == query_set_id)
                .all()
            )
        else:
            q = (
                session.query(Protein.accession)
                .join(Protein.sequence)
                .join(
                    SequenceEmbedding,
                    (SequenceEmbedding.sequence_id == Protein.sequence_id)
                    & (SequenceEmbedding.embedding_config_id == embedding_config_id),
                )
            )
            if p.query_accessions:
                q = q.filter(Protein.accession.in_(p.query_accessions))
            rows = q.all()

        accessions = [r[0] for r in rows]
        emit("predict_go_terms.load_queries_done", None, {"queries": len(accessions)}, "info")
        return accessions


# ---------------------------------------------------------------------------
# Batch worker
# ---------------------------------------------------------------------------


class PredictGOTermsBatchOperation:
    """CPU batch worker: KNN search + GO annotation transfer for one query chunk.

    Reference embeddings and their GO annotations are loaded from DB on first
    access and cached at the process level (_REF_CACHE).  Subsequent batch
    messages reuse the cached reference without any DB round-trip.

    Result is published to protea.predictions.write for bulk DB insertion.
    """

    name = "predict_go_terms_batch"

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = PredictGOTermsBatchPayload.model_validate(payload)
        parent_job_id = UUID(p.parent_job_id)
        prediction_set_id = uuid.UUID(p.prediction_set_id)
        embedding_config_id = uuid.UUID(p.embedding_config_id)
        annotation_set_id = uuid.UUID(p.annotation_set_id)

        # Skip if parent was cancelled/failed
        parent = session.get(Job, parent_job_id)
        if parent is not None and parent.status in (JobStatus.CANCELLED, JobStatus.FAILED):
            emit(
                "predict_go_terms_batch.skipped",
                None,
                {"parent_job_id": str(parent_job_id)},
                "warning",
            )
            return OperationResult(result={"skipped": True})

        # --- reference cache (load once per process per config+annotation_set+mode) ---
        # The cache key includes aspect_separated_knn so that switching modes on the
        # same worker process does not serve stale data from a previous run.
        cache_key = (p.embedding_config_id, p.annotation_set_id, p.aspect_separated_knn)
        if cache_key not in _REF_CACHE:
            # Evict oldest entry when cache is full to free numpy arrays from memory.
            if len(_REF_CACHE) >= _REF_CACHE_MAX:
                evict_key = next(iter(_REF_CACHE))
                del _REF_CACHE[evict_key]
            emit(
                "predict_go_terms_batch.loading_reference",
                None,
                {
                    "embedding_config_id": p.embedding_config_id,
                    "annotation_set_id": p.annotation_set_id,
                    "aspect_separated_knn": p.aspect_separated_knn,
                },
                "info",
            )
            if p.aspect_separated_knn:
                _REF_CACHE[cache_key] = self._load_reference_data_per_aspect(
                    session, embedding_config_id, annotation_set_id, emit
                )
            else:
                _REF_CACHE[cache_key] = self._load_reference_data(
                    session, embedding_config_id, annotation_set_id, emit
                )

        # --- query embeddings for this batch ---
        query_embeddings, valid_accessions = self._load_query_embeddings(
            session, p.query_accessions, embedding_config_id, p, emit
        )
        if not query_embeddings.size:
            return OperationResult(result={"predictions": 0})

        t0 = time.perf_counter()

        if p.aspect_separated_knn:
            prediction_dicts = self._run_aspect_separated_knn(
                session,
                valid_accessions,
                query_embeddings,
                _REF_CACHE[cache_key],
                annotation_set_id,
                prediction_set_id,
                p,
            )
        else:
            ref_data = _REF_CACHE[cache_key]
            if not ref_data["embeddings"].size:
                emit("predict_go_terms_batch.no_references", None, {}, "warning")
                return OperationResult(result={"predictions": 0})

            # --- KNN: convert float16 cache → float32 for search ---
            ref_embeddings_f32 = ref_data["embeddings"].astype(np.float32)
            neighbors = search_knn(
                query_embeddings,
                ref_embeddings_f32,
                ref_data["accessions"],
                k=p.limit_per_entry,
                distance_threshold=p.distance_threshold,
                backend=p.search_backend,
                metric=p.metric,
                faiss_index_type=p.faiss_index_type,
                faiss_nlist=p.faiss_nlist,
                faiss_nprobe=p.faiss_nprobe,
                faiss_hnsw_m=p.faiss_hnsw_m,
                faiss_hnsw_ef_search=p.faiss_hnsw_ef_search,
            )

            # --- lazy GO annotation load: only for neighbors actually found ---
            unique_neighbors: set[str] = set()
            for top_refs in neighbors:
                for ref_acc, _ in top_refs:
                    unique_neighbors.add(ref_acc)
            go_map = self._load_annotations_for(session, annotation_set_id, unique_neighbors)

            # --- feature engineering sequences / taxonomy (opt-in) ---
            ref_sequences: dict[str, str] = {}
            query_sequences: dict[str, str] = {}
            ref_tax_ids: dict[str, int | None] = {}
            query_tax_ids: dict[str, int | None] = {}

            if p.compute_alignments:
                ref_sequences = self._load_sequences_for_proteins(session, unique_neighbors)
                query_sequences = self._load_sequences_for_queries(session, p, valid_accessions)

            if p.compute_taxonomy:
                ref_tax_ids = self._load_taxonomy_ids_for_proteins(session, unique_neighbors)
                query_tax_ids = self._load_taxonomy_ids_for_queries(session, p, valid_accessions)

            ref_data_with_annotations = {
                "accessions": ref_data["accessions"],
                "embeddings": ref_embeddings_f32,
                "go_map": go_map,
            }
            prediction_dicts = self._predict_batch(
                valid_accessions,
                query_embeddings,
                ref_data_with_annotations,
                prediction_set_id,
                p,
                neighbors=neighbors,
                ref_sequences=ref_sequences,
                query_sequences=query_sequences,
                ref_tax_ids=ref_tax_ids,
                query_tax_ids=query_tax_ids,
            )

        elapsed = time.perf_counter() - t0

        emit(
            "predict_go_terms_batch.done",
            None,
            {
                "queries": len(valid_accessions),
                "predictions": len(prediction_dicts),
                "elapsed_seconds": elapsed,
            },
            "info",
        )

        return OperationResult(
            result={"predictions": len(prediction_dicts)},
            publish_operations=[
                (
                    _WRITE_QUEUE,
                    {
                        "operation": "store_predictions",
                        "job_id": str(parent_job_id),
                        "payload": {
                            "parent_job_id": str(parent_job_id),
                            "prediction_set_id": str(prediction_set_id),
                            "predictions": prediction_dicts,
                        },
                    },
                )
            ],
        )

    # ── helpers ──────────────────────────────────────────────────────────────

    def _load_reference_data(
        self,
        session: Session,
        embedding_config_id: uuid.UUID,
        annotation_set_id: uuid.UUID,
        emit: EmitFn,
    ) -> dict[str, Any]:
        """Load reference accessions and embeddings (float16) into the process cache.

        Checks the disk cache first (survives worker restarts). On miss, fetches
        from PostgreSQL and writes the result to disk for future restarts.

        GO annotations are NOT loaded here — they are fetched lazily per batch
        for only the unique neighbors found by KNN, saving several GB of RAM.
        Embeddings are stored as float16 (half the memory of float32); they are
        cast to float32 at search time with negligible accuracy loss.
        """
        emit("predict_go_terms_batch.load_references_start", None, {}, "info")

        cached = _load_from_disk_cache(embedding_config_id, annotation_set_id)
        if cached is not None:
            emit(
                "predict_go_terms_batch.load_references_done",
                None,
                {
                    "references": len(cached["accessions"]),
                    "embeddings_mb": round(cached["embeddings"].nbytes / 1024 / 1024),
                    "source": "disk_cache",
                },
                "info",
            )
            return cached

        annotated_accessions_sq = (
            session.query(ProteinGOAnnotation.protein_accession)
            .filter(ProteinGOAnnotation.annotation_set_id == annotation_set_id)
            .distinct()
            .subquery()
        )
        base_q = (
            session.query(Protein.accession, SequenceEmbedding.embedding)
            .join(
                SequenceEmbedding,
                (SequenceEmbedding.sequence_id == Protein.sequence_id)
                & (SequenceEmbedding.embedding_config_id == embedding_config_id),
            )
            .join(
                annotated_accessions_sq,
                Protein.accession == annotated_accessions_sq.c.protein_accession,
            )
        )

        # Count first so we can pre-allocate the numpy array and never build a
        # list-of-lists in Python.  Without pre-allocation, .all() on 400k rows
        # materialises ~14 GB of Python float objects and hits swap.
        total = base_q.count()
        if total == 0:
            return {"accessions": [], "embeddings": np.empty((0,), dtype=np.float16)}

        # Determine embedding dimension from a single row.
        first_emb = base_q.limit(1).one()[1]
        dim = len(first_emb)

        # Pre-allocate float16 array; fill row-by-row via yield_per so the
        # cursor fetches _STREAM_CHUNK_SIZE rows at a time — peak Python-object
        # memory stays at ~chunk_size × dim × 28 bytes ≈ tens of MB, not 14 GB.
        embeddings = np.empty((total, dim), dtype=np.float16)
        accessions: list[str] = []
        for i, (acc, emb) in enumerate(base_q.yield_per(_STREAM_CHUNK_SIZE)):
            embeddings[i] = emb
            accessions.append(acc)

        _save_to_disk_cache(embedding_config_id, annotation_set_id, accessions, embeddings)

        emit(
            "predict_go_terms_batch.load_references_done",
            None,
            {
                "references": len(accessions),
                "embeddings_mb": round(embeddings.nbytes / 1024 / 1024),
                "source": "database",
            },
            "info",
        )

        return {"accessions": accessions, "embeddings": embeddings}

    def _load_reference_data_per_aspect(
        self,
        session: Session,
        embedding_config_id: uuid.UUID,
        annotation_set_id: uuid.UUID,
        emit: EmitFn,
    ) -> dict[str, dict[str, Any]]:
        """Build per-aspect views over the single unified reference cache.

        Strategy — one array, three index slices:

        1. Load (or build) the **unified** reference embeddings exactly as
           :meth:`_load_reference_data` does — a single 1 GB float16 array shared
           across all three aspects.  No embeddings are duplicated on disk or in RAM.
        2. For each aspect (P / F / C) load (or build) a tiny **index array** — a
           1-D int32 array of row positions inside the unified array that correspond
           to proteins annotated in that aspect.  Index arrays are ~2 MB each and
           are built with a lightweight accession-only query (no embedding data fetched).
        3. Return per-aspect sub-arrays as numpy fancy-index results (a copy in
           float16, ~300 MB per aspect at most).

        Disk layout::

            {key}_embeddings.npy            ← unified, ~1 GB float16  (shared)
            {key}_accessions.npy            ← unified accession list   (shared)
            {key}__P_indices.npy            ← int32 row indices, ~2 MB
            {key}__F_indices.npy
            {key}__C_indices.npy
            {key}__P_anno_gtids.npy         ← CSR annotation cache per aspect
            {key}__P_anno_quals.npy
            {key}__P_anno_ecodes.npy
            {key}__P_anno_offsets.npy
            {key}__F_anno_*.npy
            {key}__C_anno_*.npy
        """
        emit(
            "predict_go_terms_batch.load_references_per_aspect_start",
            None,
            {
                "embedding_config_id": str(embedding_config_id),
                "annotation_set_id": str(annotation_set_id),
            },
            "info",
        )

        # ── step 1: unified embeddings (reuses existing disk cache or builds it once) ──
        unified = self._load_reference_data(session, embedding_config_id, annotation_set_id, emit)
        if not unified["accessions"]:
            return {
                asp: {"accessions": [], "embeddings": np.empty((0,), dtype=np.float16)}
                for asp in _ASPECTS
            }

        acc_to_idx: dict[str, int] = {acc: i for i, acc in enumerate(unified["accessions"])}

        # ── step 2: per-aspect index arrays ──────────────────────────────────────────
        result: dict[str, dict[str, Any]] = {}
        total_refs = 0

        # Determine which aspects still need DB queries (index or annotation cache missing)
        missing_aspects = [
            asp
            for asp in _ASPECTS
            if not _aspect_index_path(embedding_config_id, annotation_set_id, asp).exists()
            or _load_anno_csr_from_disk(embedding_config_id, annotation_set_id, asp) is None
        ]

        # Single-pass query for ALL missing aspects: fetch full annotation rows
        # (accession, aspect, go_term_id, qualifier, evidence_code) in one table scan.
        # This replaces both the old index-only query and all per-batch IN queries.
        aspect_to_accset: dict[str, set[str]] = {asp: set() for asp in missing_aspects}
        aspect_to_go_map: dict[str, dict[str, list[dict[str, Any]]]] = {
            asp: {} for asp in missing_aspects
        }
        if missing_aspects:
            rows = (
                session.query(
                    ProteinGOAnnotation.protein_accession,
                    GOTerm.aspect,
                    ProteinGOAnnotation.go_term_id,
                    ProteinGOAnnotation.qualifier,
                    ProteinGOAnnotation.evidence_code,
                )
                .join(ProteinGOAnnotation.go_term)
                .filter(
                    ProteinGOAnnotation.annotation_set_id == annotation_set_id,
                    GOTerm.aspect.in_(missing_aspects),
                    (
                        ProteinGOAnnotation.qualifier.is_(None)
                        | ~ProteinGOAnnotation.qualifier.like("%NOT%")
                    ),
                )
                .yield_per(50_000)
            )
            for acc, asp, go_term_id, qualifier, evidence_code in rows:
                if asp in aspect_to_accset:
                    aspect_to_accset[asp].add(acc)
                    aspect_to_go_map[asp].setdefault(acc, []).append({
                        "go_term_id": go_term_id,
                        "qualifier": qualifier,
                        "evidence_code": evidence_code,
                    })

            for asp in missing_aspects:
                # Save embedding index array
                idx_path = _aspect_index_path(embedding_config_id, annotation_set_id, asp)
                indices = np.array(
                    [acc_to_idx[acc] for acc in aspect_to_accset[asp] if acc in acc_to_idx],
                    dtype=np.int32,
                )
                idx_path.parent.mkdir(parents=True, exist_ok=True)
                np.save(idx_path, indices)

                # Save annotation CSR cache — zero DB queries during batch processing
                asp_accessions = [unified["accessions"][i] for i in indices]
                gtids, quals, ecodes, offsets = _build_anno_csr(
                    asp_accessions, aspect_to_go_map[asp]
                )
                _save_anno_csr_to_disk(
                    embedding_config_id, annotation_set_id, asp, gtids, quals, ecodes, offsets
                )

        for aspect in _ASPECTS:
            idx_path = _aspect_index_path(embedding_config_id, annotation_set_id, aspect)
            indices = np.load(idx_path)
            source = "disk_cache" if aspect not in missing_aspects else "database"

            aspect_accessions = [unified["accessions"][i] for i in indices]
            aspect_embeddings = unified["embeddings"][indices]  # float16 copy, ~300 MB max

            anno_csr = _load_anno_csr_from_disk(embedding_config_id, annotation_set_id, aspect)
            anno_data: dict[str, Any] = {}
            if anno_csr is not None:
                gtids, quals, ecodes, offsets = anno_csr
                anno_data = {
                    "anno_gtids": gtids,
                    "anno_quals": quals,
                    "anno_ecodes": ecodes,
                    "anno_offsets": offsets,
                    "acc_to_anno_idx": {acc: i for i, acc in enumerate(aspect_accessions)},
                }

            result[aspect] = {
                "accessions": aspect_accessions,
                "embeddings": aspect_embeddings,
                **anno_data,
            }
            total_refs += len(indices)
            emit(
                "predict_go_terms_batch.load_references_per_aspect_done",
                None,
                {
                    "aspect": aspect,
                    "references": len(indices),
                    "source": source,
                },
                "info",
            )

        emit(
            "predict_go_terms_batch.load_references_per_aspect_all_done",
            None,
            {
                "total_references": total_refs,
            },
            "info",
        )
        return result

    def _run_aspect_separated_knn(
        self,
        session: Session,
        valid_accessions: list[str],
        query_embeddings: np.ndarray,
        ref_data_by_aspect: dict[str, dict[str, Any]],
        annotation_set_id: uuid.UUID,
        prediction_set_id: uuid.UUID,
        p: PredictGOTermsBatchPayload,
    ) -> list[dict[str, Any]]:
        """Run three independent KNN searches (one per GO aspect) and merge results.

        For each aspect ``a`` in (P, F, C):
        1. Build a KNN index from the aspect-filtered reference embeddings.
        2. Find the ``limit_per_entry`` nearest neighbors for every query.
        3. Load only aspect-``a`` GO annotations for those neighbors.
        4. Transfer those annotations as predictions.

        This guarantees that every query protein can receive BPO, MFO, and CCO
        candidates even if its globally nearest neighbors happen to carry
        annotations in only one or two aspects — the dominant cause of the BPO
        recall ceiling observed with a unified index.

        Feature engineering (alignments / taxonomy) is computed for the union of
        neighbors across all aspects to avoid redundant work on shared neighbors.
        """
        # Collect all unique neighbors across aspects so feature engineering
        # is computed once per pair regardless of how many aspects reference it.
        neighbors_by_aspect: dict[str, list[list[tuple[str, float]]]] = {}
        all_unique_neighbors: set[str] = set()

        for aspect in _ASPECTS:
            aspect_refs = ref_data_by_aspect[aspect]
            if not aspect_refs["accessions"]:
                neighbors_by_aspect[aspect] = [[] for _ in valid_accessions]
                continue

            ref_f32 = aspect_refs["embeddings"].astype(np.float32)
            aspect_neighbors = search_knn(
                query_embeddings,
                ref_f32,
                aspect_refs["accessions"],
                k=p.limit_per_entry,
                distance_threshold=p.distance_threshold,
                backend=p.search_backend,
                metric=p.metric,
                faiss_index_type=p.faiss_index_type,
                faiss_nlist=p.faiss_nlist,
                faiss_nprobe=p.faiss_nprobe,
                faiss_hnsw_m=p.faiss_hnsw_m,
                faiss_hnsw_ef_search=p.faiss_hnsw_ef_search,
            )
            neighbors_by_aspect[aspect] = aspect_neighbors
            for top_refs in aspect_neighbors:
                for ref_acc, _ in top_refs:
                    all_unique_neighbors.add(ref_acc)

        # Feature engineering — computed over the union of all neighbors
        ref_sequences: dict[str, str] = {}
        query_sequences: dict[str, str] = {}
        ref_tax_ids: dict[str, int | None] = {}
        query_tax_ids: dict[str, int | None] = {}

        if p.compute_alignments:
            ref_sequences = self._load_sequences_for_proteins(session, all_unique_neighbors)
            query_sequences = self._load_sequences_for_queries(session, p, valid_accessions)

        if p.compute_taxonomy:
            ref_tax_ids = self._load_taxonomy_ids_for_proteins(session, all_unique_neighbors)
            query_tax_ids = self._load_taxonomy_ids_for_queries(session, p, valid_accessions)

        # Build predictions per aspect, merging into a single list.
        # seen_terms is keyed per query protein to deduplicate across aspects.
        predictions: list[dict[str, Any]] = []
        seen_per_query: dict[str, set[int]] = {acc: set() for acc in valid_accessions}
        pair_features: dict[tuple[str, str], dict[str, Any]] = {}

        compute_rr = p.compute_reranker_features

        # Pre-compute per-query reranker stats across all aspects
        rr_distance_std_per_query: dict[str, float] = {}
        rr_vote_count_per_query: dict[str, dict[int, int]] = {}
        rr_k_position_per_query: dict[str, dict[int, int]] = {}
        # go_term_frequency and ref_annotation_density are computed per-aspect below
        all_go_term_freq: dict[int, int] = {}
        all_ref_ann_density: dict[str, int] = {}

        if compute_rr:
            for q_idx, q_acc in enumerate(valid_accessions):
                rr_vote_count_per_query[q_acc] = {}
                rr_k_position_per_query[q_acc] = {}
                all_distances = []
                for aspect in _ASPECTS:
                    aspect_neighbors = neighbors_by_aspect[aspect]
                    if q_idx < len(aspect_neighbors):
                        for _, d in aspect_neighbors[q_idx]:
                            all_distances.append(d)
                rr_distance_std_per_query[q_acc] = (
                    float(np.std(all_distances)) if len(all_distances) > 1 else 0.0
                )

        for aspect in _ASPECTS:
            unique_neighbors_aspect: set[str] = set()
            for top_refs in neighbors_by_aspect[aspect]:
                for ref_acc, _ in top_refs:
                    unique_neighbors_aspect.add(ref_acc)

            aspect_ref = ref_data_by_aspect[aspect]
            if "anno_gtids" in aspect_ref:
                go_map = _csr_lookup(
                    unique_neighbors_aspect,
                    aspect_ref["accessions"],
                    aspect_ref["acc_to_anno_idx"],
                    aspect_ref["anno_gtids"],
                    aspect_ref["anno_quals"],
                    aspect_ref["anno_ecodes"],
                    aspect_ref["anno_offsets"],
                )
            else:
                go_map = self._load_annotations_for(
                    session, annotation_set_id, unique_neighbors_aspect, aspect=aspect
                )

            # Pre-compute reranker aggregates for this aspect's go_map
            if compute_rr:
                for acc, anns in go_map.items():
                    if acc not in all_ref_ann_density:
                        all_ref_ann_density[acc] = 0
                    all_ref_ann_density[acc] += len(anns)
                    for ann in anns:
                        gtid = ann["go_term_id"]
                        all_go_term_freq[gtid] = all_go_term_freq.get(gtid, 0) + 1

                # vote_count and k_position per query per aspect
                for q_idx, q_acc in enumerate(valid_accessions):
                    vc = rr_vote_count_per_query.setdefault(q_acc, {})
                    kp = rr_k_position_per_query.setdefault(q_acc, {})
                    aspect_neighbors = neighbors_by_aspect[aspect]
                    if q_idx < len(aspect_neighbors):
                        for k_pos, (ref_acc, _) in enumerate(aspect_neighbors[q_idx], 1):
                            for ann in go_map.get(ref_acc, []):
                                gtid = ann["go_term_id"]
                                vc[gtid] = vc.get(gtid, 0) + 1
                                if gtid not in kp:
                                    kp[gtid] = k_pos

            for q_acc, top_refs in zip(valid_accessions, neighbors_by_aspect[aspect], strict=False):
                seen_terms = seen_per_query[q_acc]

                for ref_acc, distance in top_refs:
                    pair_key = (q_acc, ref_acc)
                    if pair_key not in pair_features:
                        feats: dict[str, Any] = {}
                        if p.compute_alignments:
                            q_seq = query_sequences.get(q_acc, "")
                            r_seq = ref_sequences.get(ref_acc, "")
                            if q_seq and r_seq:
                                feats.update(compute_alignment(q_seq, r_seq))
                        if p.compute_taxonomy:
                            q_tid = query_tax_ids.get(q_acc)
                            r_tid = ref_tax_ids.get(ref_acc)
                            feats.update(compute_taxonomy(q_tid, r_tid))
                            feats["query_taxonomy_id"] = q_tid
                            feats["ref_taxonomy_id"] = r_tid
                        pair_features[pair_key] = feats

                    feats = pair_features[pair_key]

                    for ann in go_map.get(ref_acc, []):
                        go_term_id = ann["go_term_id"]
                        if go_term_id in seen_terms:
                            continue
                        seen_terms.add(go_term_id)
                        pred: dict[str, Any] = {
                            "prediction_set_id": str(prediction_set_id),
                            "protein_accession": q_acc,
                            "go_term_id": go_term_id,
                            "ref_protein_accession": ref_acc,
                            "distance": distance,
                        }
                        if ann.get("qualifier"):
                            pred["qualifier"] = ann["qualifier"]
                        if ann.get("evidence_code"):
                            pred["evidence_code"] = ann["evidence_code"]
                        if compute_rr:
                            pred["vote_count"] = rr_vote_count_per_query.get(q_acc, {}).get(go_term_id, 1)
                            pred["k_position"] = rr_k_position_per_query.get(q_acc, {}).get(go_term_id, 1)
                            pred["go_term_frequency"] = all_go_term_freq.get(go_term_id, 0)
                            pred["ref_annotation_density"] = all_ref_ann_density.get(ref_acc, 0)
                            pred["neighbor_distance_std"] = rr_distance_std_per_query.get(q_acc, 0.0)
                        for key in (
                            "identity_nw",
                            "similarity_nw",
                            "alignment_score_nw",
                            "gaps_pct_nw",
                            "alignment_length_nw",
                            "identity_sw",
                            "similarity_sw",
                            "alignment_score_sw",
                            "gaps_pct_sw",
                            "alignment_length_sw",
                            "length_query",
                            "length_ref",
                            "query_taxonomy_id",
                            "ref_taxonomy_id",
                            "taxonomic_lca",
                            "taxonomic_distance",
                            "taxonomic_common_ancestors",
                            "taxonomic_relation",
                        ):
                            val = feats.get(key)
                            if val is not None:
                                pred[key] = val
                        predictions.append(pred)

        return predictions

    def _load_annotations_for(
        self,
        session: Session,
        annotation_set_id: uuid.UUID,
        accessions: set[str],
        aspect: str | None = None,
    ) -> dict[str, list[dict[str, Any]]]:
        """Load GO annotations for the given accessions, chunked to avoid param limits.

        Only non-negated annotations are loaded: rows with a NOT qualifier (e.g.
        ``'NOT'``, ``'NOT|involved_in'``) assert that the protein does *not* have
        the annotated function and must never be transferred as positive predictions.
        Although NOT annotations are rare in GOA/QuickGO (~0.1 % of rows), including
        them would introduce false positives that are silently penalised by cafaeval
        without any obvious trace in the prediction artefacts.

        When ``aspect`` is given (``'P'``, ``'F'``, or ``'C'``), only annotations
        whose GO term belongs to that aspect are returned.  This is used by the
        per-aspect KNN mode so that BPO-index neighbors transfer only BPO terms,
        MFO-index neighbors transfer only MFO terms, etc.  The join to ``go_term``
        is added only when needed to keep the no-aspect path as fast as before.
        """
        go_map: dict[str, list[dict[str, Any]]] = {}
        accessions_list = list(accessions)

        for i in range(0, len(accessions_list), _ANNOTATION_CHUNK_SIZE):
            chunk = accessions_list[i : i + _ANNOTATION_CHUNK_SIZE]
            q = session.query(
                ProteinGOAnnotation.protein_accession,
                ProteinGOAnnotation.go_term_id,
                ProteinGOAnnotation.qualifier,
                ProteinGOAnnotation.evidence_code,
            ).filter(
                ProteinGOAnnotation.annotation_set_id == annotation_set_id,
                ProteinGOAnnotation.protein_accession.in_(chunk),
                # Exclude NOT-qualified annotations (e.g. 'NOT', 'NOT|involved_in').
                # qualifier IS NULL must be preserved explicitly because SQL LIKE
                # returns NULL for NULL inputs, which would silently drop those rows.
                (
                    ProteinGOAnnotation.qualifier.is_(None)
                    | ~ProteinGOAnnotation.qualifier.like("%NOT%")
                ),
            )
            if aspect is not None:
                # Join go_term only when aspect filtering is requested to avoid
                # an unnecessary join on the common (non-aspect-separated) path.
                q = q.join(ProteinGOAnnotation.go_term).filter(GOTerm.aspect == aspect)
            rows = q.all()
            for acc, go_term_id, qualifier, evidence_code in rows:
                go_map.setdefault(acc, []).append(
                    {
                        "go_term_id": go_term_id,
                        "qualifier": qualifier,
                        "evidence_code": evidence_code,
                    }
                )

        return go_map

    def _load_query_embeddings(
        self,
        session: Session,
        query_accessions: list[str],
        embedding_config_id: uuid.UUID,
        p: PredictGOTermsBatchPayload,
        emit: EmitFn,
    ) -> tuple[np.ndarray, list[str]]:
        """Load embeddings for this batch's query accessions.

        Returns (embeddings, valid_accessions) — only accessions that actually
        have an embedding are included.
        """
        if p.query_set_id:
            query_set_id = uuid.UUID(p.query_set_id)
            rows = (
                session.query(QuerySetEntry.accession, SequenceEmbedding.embedding)
                .join(
                    SequenceEmbedding,
                    (SequenceEmbedding.sequence_id == QuerySetEntry.sequence_id)
                    & (SequenceEmbedding.embedding_config_id == embedding_config_id),
                )
                .filter(
                    QuerySetEntry.query_set_id == query_set_id,
                    QuerySetEntry.accession.in_(query_accessions),
                )
                .all()
            )
        else:
            rows = (
                session.query(Protein.accession, SequenceEmbedding.embedding)
                .join(Protein.sequence)
                .join(
                    SequenceEmbedding,
                    (SequenceEmbedding.sequence_id == Protein.sequence_id)
                    & (SequenceEmbedding.embedding_config_id == embedding_config_id),
                )
                .filter(Protein.accession.in_(query_accessions))
                .all()
            )

        if not rows:
            return np.empty((0,)), []

        valid_accessions = [r[0] for r in rows]
        embeddings = np.array([list(r[1]) for r in rows], dtype=np.float32)
        return embeddings, valid_accessions

    def _predict_batch(
        self,
        query_accessions: list[str],
        query_embeddings: np.ndarray,
        ref_data: dict[str, Any],
        prediction_set_id: uuid.UUID,
        p: PredictGOTermsBatchPayload,
        *,
        neighbors: list[list[tuple[str, float]]] | None = None,
        ref_sequences: dict[str, str] | None = None,
        query_sequences: dict[str, str] | None = None,
        ref_tax_ids: dict[str, int | None] | None = None,
        query_tax_ids: dict[str, int | None] | None = None,
    ) -> list[dict[str, Any]]:
        """Build serializable prediction dicts from KNN results.

        ``ref_data`` must have keys ``accessions``, ``embeddings``, and ``go_map``.
        If ``neighbors`` is provided (pre-computed by execute()), KNN is skipped.
        Returns compact dicts — None-valued optional fields are omitted to reduce
        message size.
        """
        ref_sequences = ref_sequences or {}
        query_sequences = query_sequences or {}
        ref_tax_ids = ref_tax_ids or {}
        query_tax_ids = query_tax_ids or {}

        if neighbors is None:
            ref_emb = ref_data["embeddings"]
            if ref_emb.dtype != np.float32:
                ref_emb = ref_emb.astype(np.float32)
            neighbors = search_knn(
                query_embeddings,
                ref_emb,
                ref_data["accessions"],
                k=p.limit_per_entry,
                distance_threshold=p.distance_threshold,
                backend=p.search_backend,
                metric=p.metric,
                faiss_index_type=p.faiss_index_type,
                faiss_nlist=p.faiss_nlist,
                faiss_nprobe=p.faiss_nprobe,
                faiss_hnsw_m=p.faiss_hnsw_m,
                faiss_hnsw_ef_search=p.faiss_hnsw_ef_search,
            )

        go_map = ref_data["go_map"]
        predictions: list[dict[str, Any]] = []

        # Pre-compute reranker aggregates if requested
        compute_rr = p.compute_reranker_features
        go_term_freq: dict[int, int] = {}
        ref_ann_density: dict[str, int] = {}
        if compute_rr:
            for acc, anns in go_map.items():
                ref_ann_density[acc] = len(anns)
                for ann in anns:
                    gtid = ann["go_term_id"]
                    go_term_freq[gtid] = go_term_freq.get(gtid, 0) + 1

        for q_acc, top_refs in zip(query_accessions, neighbors, strict=False):
            seen_terms: set[int] = set()
            pair_features: dict[str, dict[str, Any]] = {}

            # Reranker: pre-compute per-query stats
            rr_distance_std: float | None = None
            rr_vote_count: dict[int, int] = {}
            rr_k_position: dict[int, int] = {}
            if compute_rr and top_refs:
                rr_distance_std = float(np.std([d for _, d in top_refs])) if len(top_refs) > 1 else 0.0
                for k_pos, (ref_acc, _) in enumerate(top_refs, 1):
                    for ann in go_map.get(ref_acc, []):
                        gtid = ann["go_term_id"]
                        rr_vote_count[gtid] = rr_vote_count.get(gtid, 0) + 1
                        if gtid not in rr_k_position:
                            rr_k_position[gtid] = k_pos

            for ref_acc, distance in top_refs:
                if ref_acc not in pair_features:
                    features: dict[str, Any] = {}

                    if p.compute_alignments:
                        q_seq = query_sequences.get(q_acc, "")
                        r_seq = ref_sequences.get(ref_acc, "")
                        if q_seq and r_seq:
                            features.update(compute_alignment(q_seq, r_seq))

                    if p.compute_taxonomy:
                        q_tid = query_tax_ids.get(q_acc)
                        r_tid = ref_tax_ids.get(ref_acc)
                        tax = compute_taxonomy(q_tid, r_tid)
                        features.update(tax)
                        features["query_taxonomy_id"] = q_tid
                        features["ref_taxonomy_id"] = r_tid

                    pair_features[ref_acc] = features

                features = pair_features[ref_acc]

                for ann in go_map.get(ref_acc, []):
                    go_term_id = ann["go_term_id"]
                    if go_term_id in seen_terms:
                        continue
                    seen_terms.add(go_term_id)
                    # Only include non-None optional fields to keep message compact
                    pred: dict[str, Any] = {
                        "prediction_set_id": str(prediction_set_id),
                        "protein_accession": q_acc,
                        "go_term_id": go_term_id,
                        "ref_protein_accession": ref_acc,
                        "distance": distance,
                    }
                    if ann.get("qualifier"):
                        pred["qualifier"] = ann["qualifier"]
                    if ann.get("evidence_code"):
                        pred["evidence_code"] = ann["evidence_code"]
                    if compute_rr:
                        pred["vote_count"] = rr_vote_count.get(go_term_id, 1)
                        pred["k_position"] = rr_k_position.get(go_term_id, 1)
                        pred["go_term_frequency"] = go_term_freq.get(go_term_id, 0)
                        pred["ref_annotation_density"] = ref_ann_density.get(ref_acc, 0)
                        pred["neighbor_distance_std"] = rr_distance_std
                    for key in (
                        "identity_nw",
                        "similarity_nw",
                        "alignment_score_nw",
                        "gaps_pct_nw",
                        "alignment_length_nw",
                        "identity_sw",
                        "similarity_sw",
                        "alignment_score_sw",
                        "gaps_pct_sw",
                        "alignment_length_sw",
                        "length_query",
                        "length_ref",
                        "query_taxonomy_id",
                        "ref_taxonomy_id",
                        "taxonomic_lca",
                        "taxonomic_distance",
                        "taxonomic_common_ancestors",
                        "taxonomic_relation",
                    ):
                        val = features.get(key)
                        if val is not None:
                            pred[key] = val
                    predictions.append(pred)

        return predictions

    # ── feature-engineering helpers ───────────────────────────────────────────

    def _load_sequences_for_proteins(
        self, session: Session, accessions: set[str]
    ) -> dict[str, str]:
        result: dict[str, str] = {}
        acc_list = list(accessions)
        for i in range(0, len(acc_list), _ANNOTATION_CHUNK_SIZE):
            chunk = acc_list[i : i + _ANNOTATION_CHUNK_SIZE]
            rows = (
                session.query(Protein.accession, Sequence.sequence)
                .join(Protein.sequence)
                .filter(Protein.accession.in_(chunk))
                .all()
            )
            for acc, seq in rows:
                result[acc] = seq
        return result

    def _load_sequences_for_queries(
        self,
        session: Session,
        p: PredictGOTermsBatchPayload,
        accessions: list[str],
    ) -> dict[str, str]:
        if p.query_set_id:
            query_set_id = uuid.UUID(p.query_set_id)
            rows = (
                session.query(QuerySetEntry.accession, Sequence.sequence)
                .join(QuerySetEntry.sequence)
                .filter(QuerySetEntry.query_set_id == query_set_id)
                .all()
            )
            return {acc: seq for acc, seq in rows}
        return self._load_sequences_for_proteins(session, set(accessions))

    def _load_taxonomy_ids_for_proteins(
        self, session: Session, accessions: set[str]
    ) -> dict[str, int | None]:
        result: dict[str, int | None] = {}
        acc_list = list(accessions)
        for i in range(0, len(acc_list), _ANNOTATION_CHUNK_SIZE):
            chunk = acc_list[i : i + _ANNOTATION_CHUNK_SIZE]
            rows = (
                session.query(Protein.accession, Protein.taxonomy_id)
                .filter(Protein.accession.in_(chunk))
                .all()
            )
            for acc, tid in rows:
                result[acc] = int(tid) if tid else None
        return result

    def _load_taxonomy_ids_for_queries(
        self,
        session: Session,
        p: PredictGOTermsBatchPayload,
        accessions: list[str],
    ) -> dict[str, int | None]:
        acc_set = set(accessions)
        result: dict[str, int | None] = {acc: None for acc in acc_set}
        acc_list = list(acc_set)
        for i in range(0, len(acc_list), _ANNOTATION_CHUNK_SIZE):
            chunk = acc_list[i : i + _ANNOTATION_CHUNK_SIZE]
            rows = (
                session.query(Protein.accession, Protein.taxonomy_id)
                .filter(Protein.accession.in_(chunk))
                .all()
            )
            for acc, tid in rows:
                result[acc] = int(tid) if tid else None
        return result


# ---------------------------------------------------------------------------
# Write worker
# ---------------------------------------------------------------------------


class StorePredictionsOperation:
    """Write worker: bulk-inserts GOPrediction rows and updates parent job progress.

    Receives serialized prediction dicts from PredictGOTermsBatchOperation,
    inserts them into the DB, and atomically increments the parent Job's
    progress counter.  When the last batch is stored the parent Job is closed
    as SUCCEEDED.
    """

    name = "store_predictions"

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = StorePredictionsPayload.model_validate(payload)
        parent_job_id = UUID(p.parent_job_id)
        prediction_set_id = uuid.UUID(p.prediction_set_id)

        parent = session.get(Job, parent_job_id)
        if parent is not None and parent.status in (JobStatus.CANCELLED, JobStatus.FAILED):
            emit(
                "store_predictions.skipped", None, {"parent_job_id": str(parent_job_id)}, "warning"
            )
            return OperationResult(result={"skipped": True})

        if p.predictions:
            session.execute(
                pg_insert(GOPrediction).on_conflict_do_nothing(),
                [
                    {
                        "prediction_set_id": prediction_set_id,
                        "protein_accession": pred["protein_accession"],
                        "go_term_id": pred["go_term_id"],
                        "ref_protein_accession": pred["ref_protein_accession"],
                        "distance": pred["distance"],
                        "qualifier": pred.get("qualifier"),
                        "evidence_code": pred.get("evidence_code"),
                        "identity_nw": pred.get("identity_nw"),
                        "similarity_nw": pred.get("similarity_nw"),
                        "alignment_score_nw": pred.get("alignment_score_nw"),
                        "gaps_pct_nw": pred.get("gaps_pct_nw"),
                        "alignment_length_nw": pred.get("alignment_length_nw"),
                        "identity_sw": pred.get("identity_sw"),
                        "similarity_sw": pred.get("similarity_sw"),
                        "alignment_score_sw": pred.get("alignment_score_sw"),
                        "gaps_pct_sw": pred.get("gaps_pct_sw"),
                        "alignment_length_sw": pred.get("alignment_length_sw"),
                        "length_query": pred.get("length_query"),
                        "length_ref": pred.get("length_ref"),
                        "query_taxonomy_id": pred.get("query_taxonomy_id"),
                        "ref_taxonomy_id": pred.get("ref_taxonomy_id"),
                        "taxonomic_lca": pred.get("taxonomic_lca"),
                        "taxonomic_distance": pred.get("taxonomic_distance"),
                        "taxonomic_common_ancestors": pred.get("taxonomic_common_ancestors"),
                        "taxonomic_relation": pred.get("taxonomic_relation"),
                        "vote_count": pred.get("vote_count"),
                        "k_position": pred.get("k_position"),
                        "go_term_frequency": pred.get("go_term_frequency"),
                        "ref_annotation_density": pred.get("ref_annotation_density"),
                        "neighbor_distance_std": pred.get("neighbor_distance_std"),
                    }
                    for pred in p.predictions
                ],
            )

        emit(
            "store_predictions.done",
            None,
            {
                "predictions_inserted": len(p.predictions),
                "parent_job_id": str(parent_job_id),
            },
            "info",
        )

        self._update_parent_progress(session, parent_job_id, emit)

        return OperationResult(result={"predictions_inserted": len(p.predictions)})

    def _update_parent_progress(self, session: Session, parent_job_id: UUID, emit: EmitFn) -> None:
        row = session.execute(
            sa_update(Job)
            .where(Job.id == parent_job_id, Job.status == JobStatus.RUNNING)
            .values(progress_current=Job.progress_current + 1)
            .returning(Job.progress_current, Job.progress_total)
        ).fetchone()

        if row is None or row.progress_current != row.progress_total:
            return

        closed = session.execute(
            sa_update(Job)
            .where(Job.id == parent_job_id, Job.status == JobStatus.RUNNING)
            .values(status=JobStatus.SUCCEEDED, finished_at=utcnow())
            .returning(Job.id)
        ).fetchone()

        if closed:
            session.add(
                JobEvent(
                    job_id=parent_job_id,
                    event="job.succeeded",
                    fields={"via": "last_batch_stored"},
                    level="info",
                )
            )
            emit(
                "store_predictions.parent_succeeded",
                None,
                {"parent_job_id": str(parent_job_id)},
                "info",
            )
