from __future__ import annotations

import time
import uuid
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
_WRITE_QUEUE  = "protea.predictions.write"

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
_REF_CACHE: dict[tuple[str, str], dict[str, Any]] = {}
_REF_CACHE_MAX = 1


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

    @field_validator("embedding_config_id", "annotation_set_id", "ontology_snapshot_id", mode="before")
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

        embedding_config_id  = uuid.UUID(p.embedding_config_id)
        annotation_set_id    = uuid.UUID(p.annotation_set_id)
        ontology_snapshot_id = uuid.UUID(p.ontology_snapshot_id)

        config = session.get(EmbeddingConfig, embedding_config_id)
        if config is None:
            raise ValueError(f"EmbeddingConfig {p.embedding_config_id} not found")
        if session.get(AnnotationSet, annotation_set_id) is None:
            raise ValueError(f"AnnotationSet {p.annotation_set_id} not found")
        if session.get(OntologySnapshot, ontology_snapshot_id) is None:
            raise ValueError(f"OntologySnapshot {p.ontology_snapshot_id} not found")

        emit("predict_go_terms.start", None, {
            "embedding_config_id": p.embedding_config_id,
            "model_name": config.model_name,
            "annotation_set_id": p.annotation_set_id,
            "limit_per_entry": p.limit_per_entry,
            "search_backend": p.search_backend,
        }, "info")

        query_accessions = self._load_query_accessions(
            session, p, embedding_config_id, emit
        )
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
            query_accessions[i: i + p.batch_size]
            for i in range(0, len(query_accessions), p.batch_size)
        ]
        n_batches = len(batches)

        emit("predict_go_terms.dispatching", None, {
            "queries": len(query_accessions),
            "batches": n_batches,
            "prediction_set_id": str(prediction_set.id),
        }, "info")

        operations: list[tuple[str, dict[str, Any]]] = []
        for batch_accs in batches:
            operations.append((_BATCH_QUEUE, {
                "operation": "predict_go_terms_batch",
                "job_id":    str(parent_job_id),
                "payload": {
                    "embedding_config_id":  p.embedding_config_id,
                    "annotation_set_id":    p.annotation_set_id,
                    "prediction_set_id":    str(prediction_set.id),
                    "parent_job_id":        str(parent_job_id),
                    "query_accessions":     batch_accs,
                    "query_set_id":         p.query_set_id,
                    "limit_per_entry":      p.limit_per_entry,
                    "distance_threshold":   p.distance_threshold,
                    "search_backend":       p.search_backend,
                    "metric":               p.metric,
                    "faiss_index_type":     p.faiss_index_type,
                    "faiss_nlist":          p.faiss_nlist,
                    "faiss_nprobe":         p.faiss_nprobe,
                    "faiss_hnsw_m":         p.faiss_hnsw_m,
                    "faiss_hnsw_ef_search": p.faiss_hnsw_ef_search,
                    "compute_alignments":   p.compute_alignments,
                    "compute_taxonomy":     p.compute_taxonomy,
                },
            }))

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
        emit("predict_go_terms.load_queries_done", None,
             {"queries": len(accessions)}, "info")
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
        parent_job_id     = UUID(p.parent_job_id)
        prediction_set_id = uuid.UUID(p.prediction_set_id)
        embedding_config_id = uuid.UUID(p.embedding_config_id)
        annotation_set_id   = uuid.UUID(p.annotation_set_id)

        # Skip if parent was cancelled/failed
        parent = session.get(Job, parent_job_id)
        if parent is not None and parent.status in (JobStatus.CANCELLED, JobStatus.FAILED):
            emit("predict_go_terms_batch.skipped", None,
                 {"parent_job_id": str(parent_job_id)}, "warning")
            return OperationResult(result={"skipped": True})

        # --- reference cache (load once per process per config+annotation_set) ---
        cache_key = (p.embedding_config_id, p.annotation_set_id)
        if cache_key not in _REF_CACHE:
            # Evict oldest entry when cache is full to free numpy arrays from memory.
            if len(_REF_CACHE) >= _REF_CACHE_MAX:
                evict_key = next(iter(_REF_CACHE))
                del _REF_CACHE[evict_key]
            emit("predict_go_terms_batch.loading_reference", None, {
                "embedding_config_id": p.embedding_config_id,
                "annotation_set_id": p.annotation_set_id,
            }, "info")
            _REF_CACHE[cache_key] = self._load_reference_data(
                session, embedding_config_id, annotation_set_id, emit
            )

        ref_data = _REF_CACHE[cache_key]

        if not ref_data["embeddings"].size:
            emit("predict_go_terms_batch.no_references", None, {}, "warning")
            return OperationResult(result={"predictions": 0})

        # --- query embeddings for this batch ---
        query_embeddings, valid_accessions = self._load_query_embeddings(
            session, p.query_accessions, embedding_config_id, p, emit
        )
        if not query_embeddings.size:
            return OperationResult(result={"predictions": 0})

        # --- KNN: convert float16 cache → float32 for search ---
        t0 = time.perf_counter()
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
            ref_sequences   = self._load_sequences_for_proteins(session, unique_neighbors)
            query_sequences = self._load_sequences_for_queries(session, p, valid_accessions)

        if p.compute_taxonomy:
            ref_tax_ids   = self._load_taxonomy_ids_for_proteins(session, unique_neighbors)
            query_tax_ids = self._load_taxonomy_ids_for_queries(session, p, valid_accessions)

        # --- assemble ref_data with lazily-loaded annotations for _predict_batch ---
        ref_data_with_annotations = {
            "accessions": ref_data["accessions"],
            "embeddings": ref_embeddings_f32,
            "go_map":     go_map,
        }
        prediction_dicts = self._predict_batch(
            valid_accessions, query_embeddings, ref_data_with_annotations, prediction_set_id, p,
            neighbors=neighbors,
            ref_sequences=ref_sequences,
            query_sequences=query_sequences,
            ref_tax_ids=ref_tax_ids,
            query_tax_ids=query_tax_ids,
        )
        elapsed = time.perf_counter() - t0

        emit("predict_go_terms_batch.done", None, {
            "queries": len(valid_accessions),
            "predictions": len(prediction_dicts),
            "elapsed_seconds": elapsed,
        }, "info")

        return OperationResult(
            result={"predictions": len(prediction_dicts)},
            publish_operations=[(_WRITE_QUEUE, {
                "operation": "store_predictions",
                "job_id":    str(parent_job_id),
                "payload": {
                    "parent_job_id":     str(parent_job_id),
                    "prediction_set_id": str(prediction_set_id),
                    "predictions":       prediction_dicts,
                },
            })],
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

        GO annotations are NOT loaded here — they are fetched lazily per batch
        for only the unique neighbors found by KNN, saving several GB of RAM.
        Embeddings are stored as float16 (half the memory of float32); they are
        cast to float32 at search time with negligible accuracy loss.
        """
        emit("predict_go_terms_batch.load_references_start", None, {}, "info")

        rows = (
            session.query(
                Protein.accession,
                SequenceEmbedding.embedding,
            )
            .join(Protein.sequence)
            .join(
                SequenceEmbedding,
                (SequenceEmbedding.sequence_id == Protein.sequence_id)
                & (SequenceEmbedding.embedding_config_id == embedding_config_id),
            )
            .filter(
                Protein.accession.in_(
                    session.query(ProteinGOAnnotation.protein_accession)
                    .filter(ProteinGOAnnotation.annotation_set_id == annotation_set_id)
                    .distinct()
                )
            )
            .all()
        )

        if not rows:
            return {"accessions": [], "embeddings": np.empty((0,), dtype=np.float16)}

        accessions = [r[0] for r in rows]
        # float16: half the memory of float32, sufficient precision for cosine KNN
        embeddings = np.array([list(r[1]) for r in rows], dtype=np.float16)

        emit("predict_go_terms_batch.load_references_done", None, {
            "references": len(accessions),
            "embeddings_mb": round(embeddings.nbytes / 1024 / 1024),
        }, "info")

        return {"accessions": accessions, "embeddings": embeddings}

    def _load_annotations_for(
        self,
        session: Session,
        annotation_set_id: uuid.UUID,
        accessions: set[str],
    ) -> dict[str, list[dict[str, Any]]]:
        """Load GO annotations for the given accessions, chunked to avoid param limits."""
        go_map: dict[str, list[dict[str, Any]]] = {}
        accessions_list = list(accessions)

        for i in range(0, len(accessions_list), _ANNOTATION_CHUNK_SIZE):
            chunk = accessions_list[i: i + _ANNOTATION_CHUNK_SIZE]
            rows = (
                session.query(
                    ProteinGOAnnotation.protein_accession,
                    ProteinGOAnnotation.go_term_id,
                    ProteinGOAnnotation.qualifier,
                    ProteinGOAnnotation.evidence_code,
                )
                .filter(
                    ProteinGOAnnotation.annotation_set_id == annotation_set_id,
                    ProteinGOAnnotation.protein_accession.in_(chunk),
                )
                .all()
            )
            for acc, go_term_id, qualifier, evidence_code in rows:
                go_map.setdefault(acc, []).append({
                    "go_term_id": go_term_id,
                    "qualifier": qualifier,
                    "evidence_code": evidence_code,
                })

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
        ref_sequences   = ref_sequences   or {}
        query_sequences = query_sequences or {}
        ref_tax_ids     = ref_tax_ids     or {}
        query_tax_ids   = query_tax_ids   or {}

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

        for q_acc, top_refs in zip(query_accessions, neighbors, strict=False):
            seen_terms: set[int] = set()
            pair_features: dict[str, dict[str, Any]] = {}

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
                        "prediction_set_id":     str(prediction_set_id),
                        "protein_accession":     q_acc,
                        "go_term_id":            go_term_id,
                        "ref_protein_accession": ref_acc,
                        "distance":              distance,
                    }
                    if ann.get("qualifier"):
                        pred["qualifier"] = ann["qualifier"]
                    if ann.get("evidence_code"):
                        pred["evidence_code"] = ann["evidence_code"]
                    for key in (
                        "identity_nw", "similarity_nw", "alignment_score_nw",
                        "gaps_pct_nw", "alignment_length_nw",
                        "identity_sw", "similarity_sw", "alignment_score_sw",
                        "gaps_pct_sw", "alignment_length_sw",
                        "length_query", "length_ref",
                        "query_taxonomy_id", "ref_taxonomy_id",
                        "taxonomic_lca", "taxonomic_distance",
                        "taxonomic_common_ancestors", "taxonomic_relation",
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
            chunk = acc_list[i: i + _ANNOTATION_CHUNK_SIZE]
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
            chunk = acc_list[i: i + _ANNOTATION_CHUNK_SIZE]
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
            chunk = acc_list[i: i + _ANNOTATION_CHUNK_SIZE]
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
        parent_job_id     = UUID(p.parent_job_id)
        prediction_set_id = uuid.UUID(p.prediction_set_id)

        parent = session.get(Job, parent_job_id)
        if parent is not None and parent.status in (JobStatus.CANCELLED, JobStatus.FAILED):
            emit("store_predictions.skipped", None,
                 {"parent_job_id": str(parent_job_id)}, "warning")
            return OperationResult(result={"skipped": True})

        if p.predictions:
            session.execute(
                pg_insert(GOPrediction).on_conflict_do_nothing(),
                [
                    {
                        "prediction_set_id":     prediction_set_id,
                        "protein_accession":     pred["protein_accession"],
                        "go_term_id":            pred["go_term_id"],
                        "ref_protein_accession": pred["ref_protein_accession"],
                        "distance":              pred["distance"],
                        "qualifier":             pred.get("qualifier"),
                        "evidence_code":         pred.get("evidence_code"),
                        "identity_nw":           pred.get("identity_nw"),
                        "similarity_nw":         pred.get("similarity_nw"),
                        "alignment_score_nw":    pred.get("alignment_score_nw"),
                        "gaps_pct_nw":           pred.get("gaps_pct_nw"),
                        "alignment_length_nw":   pred.get("alignment_length_nw"),
                        "identity_sw":           pred.get("identity_sw"),
                        "similarity_sw":         pred.get("similarity_sw"),
                        "alignment_score_sw":    pred.get("alignment_score_sw"),
                        "gaps_pct_sw":           pred.get("gaps_pct_sw"),
                        "alignment_length_sw":   pred.get("alignment_length_sw"),
                        "length_query":          pred.get("length_query"),
                        "length_ref":            pred.get("length_ref"),
                        "query_taxonomy_id":     pred.get("query_taxonomy_id"),
                        "ref_taxonomy_id":       pred.get("ref_taxonomy_id"),
                        "taxonomic_lca":         pred.get("taxonomic_lca"),
                        "taxonomic_distance":    pred.get("taxonomic_distance"),
                        "taxonomic_common_ancestors": pred.get("taxonomic_common_ancestors"),
                        "taxonomic_relation":    pred.get("taxonomic_relation"),
                    }
                    for pred in p.predictions
                ],
            )

        emit("store_predictions.done", None, {
            "predictions_inserted": len(p.predictions),
            "parent_job_id": str(parent_job_id),
        }, "info")

        self._update_parent_progress(session, parent_job_id, emit)

        return OperationResult(result={"predictions_inserted": len(p.predictions)})

    def _update_parent_progress(
        self, session: Session, parent_job_id: UUID, emit: EmitFn
    ) -> None:
        row = session.execute(
            sa_update(Job)
            .where(Job.id == parent_job_id, Job.status == JobStatus.RUNNING)
            .values(progress_current=Job.progress_current + 1)
            .returning(Job.progress_current, Job.progress_total)
        ).fetchone()

        if row is None or row.progress_current < row.progress_total:
            return

        closed = session.execute(
            sa_update(Job)
            .where(Job.id == parent_job_id, Job.status == JobStatus.RUNNING)
            .values(status=JobStatus.SUCCEEDED, finished_at=utcnow())
            .returning(Job.id)
        ).fetchone()

        if closed:
            session.add(JobEvent(
                job_id=parent_job_id,
                event="job.succeeded",
                fields={"via": "last_batch_stored"},
                level="info",
            ))
            emit("store_predictions.parent_succeeded", None,
                 {"parent_job_id": str(parent_job_id)}, "info")
