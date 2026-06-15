"""Coordinator operation that fans out predict_go_terms_batch messages.

Extracted from the monolithic ``predict_go_terms.py`` as part of T2B.6.
"""

from __future__ import annotations

import uuid
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, OperationResult
from protea.core.operations.predict_go_terms._common import (
    _BATCH_QUEUE,
    PredictGOTermsPayload,
    _RerankerBinding,
)
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.embedding.reranker_model import RerankerModel
from protea.infrastructure.orm.models.embedding.sequence_embedding import SequenceEmbedding
from protea.infrastructure.orm.models.protein.protein import Protein
from protea.infrastructure.orm.models.query.query_set import QuerySet, QuerySetEntry


class PredictGOTermsOperation:
    """Coordinator: validates, creates PredictionSet, dispatches N batch messages.

    Pipeline:

    1. Validate ``EmbeddingConfig``, ``AnnotationSet`` and
       ``OntologySnapshot``.
    2. Load query accessions that have embeddings (no embedding data; keeps
       the coordinator session light).
    3. Create the ``PredictionSet``.
    4. Partition accessions into batches and publish to
       ``protea.predictions.batch``.

    The actual KNN search and GO transfer happen inside
    ``PredictGOTermsBatchOperation``.
    """

    name = "predict_go_terms"
    description = (
        "Coordinator: create a PredictionSet and partition query proteins into "
        "KNN batches dispatched to predict_go_terms_batch workers."
    )

    def summarize_payload(self, payload: dict[str, Any], *, session: Session | None = None) -> str:
        p = payload or {}
        bits: list[str] = []

        cfg_id_raw = p.get("embedding_config_id")
        if cfg_id_raw and session is not None:
            try:
                cfg = session.get(EmbeddingConfig, uuid.UUID(str(cfg_id_raw)))
            except Exception:
                cfg = None
            if cfg is not None:
                model_label = cfg.display_name or cfg.model_name or str(cfg.id)[:8]
                bits.append(f"{model_label} ({cfg.model_backend})")
        elif cfg_id_raw:
            bits.append(f"cfg={str(cfg_id_raw)[:8]}")

        ann_id_raw = p.get("annotation_set_id")
        if ann_id_raw and session is not None:
            try:
                ann = session.get(AnnotationSet, uuid.UUID(str(ann_id_raw)))
            except Exception:
                ann = None
            if ann is not None:
                label = f"{ann.source}@{ann.source_version}" if ann.source_version else ann.source
                bits.append(f"ref={label}")
        elif ann_id_raw:
            bits.append(f"ann={str(ann_id_raw)[:8]}")

        if p.get("query_set_id"):
            bits.append(f"qs={str(p['query_set_id'])[:8]}")
        if p.get("limit_per_entry"):
            bits.append(f"k={p['limit_per_entry']}")
        if p.get("search_backend"):
            backend = p["search_backend"]
            if backend == "faiss" and p.get("faiss_index_type"):
                backend = f"faiss/{p['faiss_index_type']}"
            bits.append(backend)
        if p.get("aspect_separated_knn"):
            bits.append("aspect-knn")
        if p.get("compute_alignments"):
            bits.append("+align")
        if p.get("compute_taxonomy"):
            bits.append("+tax")
        return " · ".join(bits)

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = PredictGOTermsPayload.model_validate(payload)
        parent_job_id = UUID(payload["_job_id"])
        embedding_config_id, annotation_set_id, ontology_snapshot_id, config = (
            self._validate_inputs(session, p)
        )
        self._emit_start(emit, p, config.model_name)
        binding = self._resolve_reranker_binding(session, p, emit)

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
        self._emit_dispatching(emit, len(query_accessions), n_batches, prediction_set.id)
        session.execute(
            text(
                "UPDATE job SET meta = jsonb_set("
                "  jsonb_set(COALESCE(meta, '{}'::jsonb), '{expected_batches}', to_jsonb(:n)),"
                "  '{batches_completed}', to_jsonb(0)"
                ") WHERE id = :jid"
            ),
            {"n": n_batches, "jid": parent_job_id},
        )
        operations = [
            (_BATCH_QUEUE, self._build_batch_message(p, prediction_set.id, parent_job_id, accs, binding))
            for accs in batches
        ]
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

    @staticmethod
    def _emit_start(emit: EmitFn, p: PredictGOTermsPayload, model_name: str) -> None:
        """Emit the ``predict_go_terms.start`` event with the resolved config."""
        emit(
            "predict_go_terms.start",
            None,
            {
                "embedding_config_id": p.embedding_config_id,
                "model_name": model_name,
                "annotation_set_id": p.annotation_set_id,
                "limit_per_entry": p.limit_per_entry,
                "search_backend": p.search_backend,
            },
            "info",
        )

    @staticmethod
    def _emit_dispatching(
        emit: EmitFn, queries: int, batches: int, prediction_set_id: uuid.UUID
    ) -> None:
        """Emit the ``predict_go_terms.dispatching`` event before the workers fan out."""
        emit(
            "predict_go_terms.dispatching",
            None,
            {
                "queries": queries,
                "batches": batches,
                "prediction_set_id": str(prediction_set_id),
            },
            "info",
        )

    @staticmethod
    def _validate_inputs(
        session: Session, p: PredictGOTermsPayload
    ) -> tuple[uuid.UUID, uuid.UUID, uuid.UUID, EmbeddingConfig]:
        """Resolve and validate the three FK UUIDs; return the EmbeddingConfig row."""
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
        return embedding_config_id, annotation_set_id, ontology_snapshot_id, config

    @staticmethod
    def _resolve_reranker_binding(
        session: Session, p: PredictGOTermsPayload, emit: EmitFn
    ) -> _RerankerBinding | None:
        """Look up the RerankerModel row, validate its artifact + schema fields."""
        if not p.reranker_model_id:
            return None
        reranker_row = session.get(RerankerModel, uuid.UUID(p.reranker_model_id))
        if reranker_row is None:
            raise ValueError(f"RerankerModel {p.reranker_model_id} not found")
        if not reranker_row.artifact_uri:
            raise ValueError(
                f"RerankerModel {p.reranker_model_id} has no artifact_uri; "
                "register it via scripts/register_reranker.py"
            )
        if not reranker_row.feature_schema_sha:
            raise ValueError(
                f"RerankerModel {p.reranker_model_id} has no feature_schema_sha; "
                "cannot validate feature alignment at inference time"
            )
        emit(
            "predict_go_terms.reranker_bound",
            None,
            {
                "reranker_model_id": p.reranker_model_id,
                "reranker_name": reranker_row.name,
                "feature_schema_sha": reranker_row.feature_schema_sha,
            },
            "info",
        )
        return _RerankerBinding(
            artifact_uri=reranker_row.artifact_uri,
            feature_schema_sha=reranker_row.feature_schema_sha,
        )

    @staticmethod
    def _build_batch_message(
        p: PredictGOTermsPayload,
        prediction_set_id: uuid.UUID,
        parent_job_id: UUID,
        batch_accs: list[str],
        binding: _RerankerBinding | None,
    ) -> dict[str, Any]:
        """Serialise one batch into the predict_go_terms_batch dispatch payload."""
        return {
            "operation": "predict_go_terms_batch",
            "job_id": str(parent_job_id),
            "payload": {
                "embedding_config_id": p.embedding_config_id,
                "annotation_set_id": p.annotation_set_id,
                "ontology_snapshot_id": p.ontology_snapshot_id,
                "prediction_set_id": str(prediction_set_id),
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
                "compute_v6_features": p.compute_v6_features,
                "compute_self_prior": p.compute_self_prior,
                "expand_votes_to_ancestors": p.expand_votes_to_ancestors,
                "aspect_separated_knn": p.aspect_separated_knn,
                "reranker_model_id": p.reranker_model_id,
                "reranker_artifact_uri": binding.artifact_uri if binding else None,
                "reranker_feature_schema_sha": binding.feature_schema_sha if binding else None,
            },
        }

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
        emit(
            "predict_go_terms.load_queries_done",
            None,
            {"queries": len(accessions)},
            "info",
        )
        return accessions
