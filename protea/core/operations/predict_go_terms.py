from __future__ import annotations

import time
import uuid
from typing import Annotated, Any

import numpy as np
from pydantic import Field, field_validator
from sqlalchemy import text
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.core.knn_search import search_knn
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
from protea.infrastructure.orm.models.annotation.protein_go_annotation import ProteinGOAnnotation
from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
from protea.infrastructure.orm.models.embedding.go_prediction import GOPrediction
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.embedding.sequence_embedding import SequenceEmbedding
from protea.infrastructure.orm.models.protein.protein import Protein
from protea.infrastructure.orm.models.query.query_set import QuerySet, QuerySetEntry

PositiveInt = Annotated[int, Field(gt=0)]


class PredictGOTermsPayload(ProteaPayload, frozen=True):
    """Payload for embedding-based GO term prediction.

    Transfers GO annotations from the K nearest reference proteins in embedding
    space to each query protein. Reference proteins are those present in
    ``annotation_set_id`` AND having a pre-computed embedding for the given
    ``embedding_config_id``.

    ``distance_threshold`` (optional) discards neighbors beyond a cosine
    distance cutoff. ``limit_per_entry`` caps the number of reference proteins
    per query before GO expansion.
    """

    embedding_config_id: str
    annotation_set_id: str
    ontology_snapshot_id: str
    query_accessions: list[str] | None = None
    query_set_id: str | None = None
    limit_per_entry: PositiveInt = 5
    distance_threshold: float | None = None
    batch_size: PositiveInt = 256

    # Search backend
    search_backend: str = "numpy"        # "numpy" | "faiss"
    metric: str = "cosine"               # "cosine" | "l2"
    faiss_index_type: str = "Flat"       # "Flat" | "IVFFlat" | "HNSW"
    faiss_nlist: int = 100               # IVFFlat: Voronoi cells
    faiss_nprobe: int = 10               # IVFFlat: cells visited at query time
    faiss_hnsw_m: int = 32               # HNSW: connections per node
    faiss_hnsw_ef_search: int = 64       # HNSW: beam width at query time

    @field_validator("embedding_config_id", "annotation_set_id", "ontology_snapshot_id", mode="before")
    @classmethod
    def must_be_non_empty(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("must be a non-empty string")
        return v.strip()


class PredictGOTermsOperation:
    """Predicts GO terms by nearest-neighbor transfer in embedding space.

    Pipeline:
    1. Load reference embeddings (proteins with annotations + embeddings).
    2. Load query embeddings (proteins with embeddings to be annotated).
    3. For each query batch: compute cosine distances to all references.
    4. Select top-K reference proteins per query (ChunkHit → ProteinHit).
    5. Transfer GO annotations from neighbors (ProteinHit → GOPrediction).
    6. Store results grouped in a new ``PredictionSet``.
    """

    name = "predict_go_terms"

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = PredictGOTermsPayload.model_validate(payload)

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

        t0 = time.perf_counter()
        emit("predict_go_terms.start", None, {
            "embedding_config_id": p.embedding_config_id,
            "model_name": config.model_name,
            "annotation_set_id": p.annotation_set_id,
            "limit_per_entry": p.limit_per_entry,
            "search_backend": p.search_backend,
            "metric": p.metric,
            "faiss_index_type": p.faiss_index_type if p.search_backend == "faiss" else None,
        }, "info")

        ref_data = self._load_reference_data(session, p, embedding_config_id, annotation_set_id, emit)
        if not ref_data["embeddings"].size:
            emit("predict_go_terms.no_references", None, {}, "warning")
            return OperationResult(result={"predictions_inserted": 0})

        query_data = self._load_query_data(session, p, embedding_config_id, emit)
        if not query_data["embeddings"].size:
            emit("predict_go_terms.no_queries", None, {}, "warning")
            return OperationResult(result={"predictions_inserted": 0})

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
        emit("predict_go_terms.prediction_set_created", None,
             {"prediction_set_id": str(prediction_set.id)}, "info")

        total_inserted = 0
        n_queries = len(query_data["accessions"])
        n_batches = max(1, -(-n_queries // p.batch_size))  # ceil division

        for i in range(0, n_queries, p.batch_size):
            # Keep the DB connection alive during long numpy computation.
            session.execute(text("SELECT 1"))

            batch_accessions = query_data["accessions"][i: i + p.batch_size]
            batch_embeddings = query_data["embeddings"][i: i + p.batch_size]

            predictions = self._predict_batch(
                batch_accessions, batch_embeddings,
                ref_data, prediction_set.id, p,
            )
            if predictions:
                session.add_all(predictions)
                session.flush()
                total_inserted += len(predictions)

            batch_num = i // p.batch_size + 1
            emit("predict_go_terms.batch_done", None, {
                "batch": batch_num,
                "total_batches": n_batches,
                "predictions_inserted": total_inserted,
                "_progress_current": batch_num,
                "_progress_total": n_batches,
            }, "info")

        elapsed = time.perf_counter() - t0
        result = {
            "prediction_set_id": str(prediction_set.id),
            "predictions_inserted": total_inserted,
            "queries": n_queries,
            "references": len(ref_data["accessions"]),
            "elapsed_seconds": elapsed,
        }
        emit("predict_go_terms.done", None, result, "info")
        return OperationResult(
            result=result,
            progress_current=n_batches,
            progress_total=n_batches,
        )

    # ── helpers ──────────────────────────────────────────────────────────────

    def _load_reference_data(
        self,
        session: Session,
        p: PredictGOTermsPayload,
        embedding_config_id: uuid.UUID,
        annotation_set_id: uuid.UUID,
        emit: EmitFn,
    ) -> dict[str, Any]:
        """Load accessions, embeddings, and GO annotations for reference proteins."""
        emit("predict_go_terms.load_references_start", None, {}, "info")

        # Proteins that have annotations in this set AND an embedding for this config
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
            return {"accessions": [], "embeddings": np.empty((0,))}

        accessions = [r[0] for r in rows]
        embeddings = np.array([list(r[1]) for r in rows], dtype=np.float32)

        # GO annotations — restricted to proteins that actually have embeddings.
        # This avoids loading millions of annotation rows for proteins that would
        # never appear as neighbors (annotation sets can be far larger than the
        # embedding set).
        ann_rows = (
            session.query(
                ProteinGOAnnotation.protein_accession,
                ProteinGOAnnotation.go_term_id,
                ProteinGOAnnotation.qualifier,
                ProteinGOAnnotation.evidence_code,
            )
            .filter(
                ProteinGOAnnotation.annotation_set_id == annotation_set_id,
                ProteinGOAnnotation.protein_accession.in_(accessions),
            )
            .all()
        )
        go_map: dict[str, list[dict[str, Any]]] = {}
        for acc, go_term_id, qualifier, evidence_code in ann_rows:
            go_map.setdefault(acc, []).append({
                "go_term_id": go_term_id,
                "qualifier": qualifier,
                "evidence_code": evidence_code,
            })

        emit("predict_go_terms.load_references_done", None, {
            "references": len(accessions),
        }, "info")
        return {"accessions": accessions, "embeddings": embeddings, "go_map": go_map}

    def _load_query_data(
        self,
        session: Session,
        p: PredictGOTermsPayload,
        embedding_config_id: uuid.UUID,
        emit: EmitFn,
    ) -> dict[str, Any]:
        """Load accessions + embeddings for proteins to be annotated.

        When ``query_set_id`` is provided, accessions and sequences are read
        from ``QuerySetEntry``. Otherwise the ``Protein`` table is used,
        optionally filtered by ``query_accessions``.
        """
        emit("predict_go_terms.load_queries_start", None, {}, "info")

        if p.query_set_id:
            query_set_id = uuid.UUID(p.query_set_id)
            if session.get(QuerySet, query_set_id) is None:
                raise ValueError(f"QuerySet {p.query_set_id} not found")

            rows = (
                session.query(QuerySetEntry.accession, SequenceEmbedding.embedding)
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
                session.query(Protein.accession, SequenceEmbedding.embedding)
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

        if not rows:
            return {"accessions": [], "embeddings": np.empty((0,))}

        accessions = [r[0] for r in rows]
        embeddings = np.array([list(r[1]) for r in rows], dtype=np.float32)

        emit("predict_go_terms.load_queries_done", None,
             {"queries": len(accessions)}, "info")
        return {"accessions": accessions, "embeddings": embeddings}

    def _predict_batch(
        self,
        query_accessions: list[str],
        query_embeddings: np.ndarray,
        ref_data: dict[str, Any],
        prediction_set_id: uuid.UUID,
        p: PredictGOTermsPayload,
    ) -> list[GOPrediction]:
        """Search neighbours and transfer GO annotations."""
        ref_embeddings: np.ndarray = ref_data["embeddings"]
        ref_accessions: list[str] = ref_data["accessions"]
        go_map: dict[str, list[dict[str, Any]]] = ref_data["go_map"]

        neighbors = search_knn(
            query_embeddings, ref_embeddings, ref_accessions,
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

        predictions: list[GOPrediction] = []
        for q_acc, top_refs in zip(query_accessions, neighbors):
            seen_terms: set[int] = set()
            for ref_acc, distance in top_refs:
                for ann in go_map.get(ref_acc, []):
                    go_term_id = ann["go_term_id"]
                    if go_term_id in seen_terms:
                        continue
                    seen_terms.add(go_term_id)
                    predictions.append(GOPrediction(
                        prediction_set_id=prediction_set_id,
                        protein_accession=q_acc,
                        go_term_id=go_term_id,
                        ref_protein_accession=ref_acc,
                        distance=distance,
                        qualifier=ann.get("qualifier"),
                        evidence_code=ann.get("evidence_code"),
                    ))

        return predictions
