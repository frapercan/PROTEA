from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, NamedTuple
from uuid import UUID

import numpy as np
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from protea.core.annotation_intern import intern_string
from protea.core.contracts.operation import EmitFn, OperationResult
from protea.core.contracts.parent_progress import update_parent_progress
from protea.core.disk_cache import (
    AnnoCsr,
    _aspect_index_path,
    _build_anno_csr,
    _csr_lookup,
    _derive_reference_views,
    _load_anno_csr_from_disk,
    _load_from_disk_cache,
    _save_anno_csr_to_disk,
    _save_to_disk_cache,
)
from protea.core.domain.aspect import ASPECT_CODES as _ASPECTS
from protea.core.feature_engineering import compute_alignment, compute_taxonomy
from protea.core.feature_enricher import NEW_V6_FEATURE_KEYS as _NEW_V6_FEATURE_KEYS
from protea.core.feature_enricher import KnnEnrichmentContext, enrich_v6_features
from protea.core.knn_search import search_knn
from protea.core.operations._predict_go_terms_adapter import AdapterResult
from protea.core.pca_cache import (
    _load_or_fit_pca_state,
)
from protea.core.reranker import (
    EMBEDDING_PCA_DIM,
    apply_reranker,
    infer_active_feature_families,
    load_reranker,
)
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
from protea.infrastructure.orm.models.annotation.protein_go_annotation import ProteinGOAnnotation
from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
from protea.infrastructure.orm.models.embedding.go_prediction import GOPrediction
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.embedding.reranker_model import RerankerModel
from protea.infrastructure.orm.models.embedding.sequence_embedding import SequenceEmbedding
from protea.infrastructure.orm.models.job import Job, JobStatus
from protea.infrastructure.orm.models.protein.protein import Protein
from protea.infrastructure.orm.models.query.query_set import QuerySet, QuerySetEntry
from protea.infrastructure.orm.models.sequence.sequence import Sequence
from protea.infrastructure.settings import load_settings
from protea.infrastructure.storage import get_artifact_store

# Annotation and stream chunk sizes are configured via OperationTuning
# (annotation_chunk_size, stream_chunk_size) and resolved at call time
# inside the helpers below. At 1280 dims x 2 bytes (float16) x 2000 rows
# the streaming reference query fetches ~5 MB per cursor round-trip,
# keeping Python object pressure negligible.

_BATCH_QUEUE = "protea.predictions.batch"
_WRITE_QUEUE = "protea.predictions.write"


@dataclass(frozen=True)
class _RerankerBinding:
    """Resolved RerankerModel artifact pointer + feature schema fingerprint."""

    artifact_uri: str
    feature_schema_sha: str

# GO aspect single-character codes used in GOTerm.aspect — imported above
# from the canonical protea.core.domain.aspect module.

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

# ---------------------------------------------------------------------------
# v6 reranker feature constants
# ---------------------------------------------------------------------------

_STORE_FLOAT_KEYS: tuple[str, ...] = (
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
    "vote_count",
    "k_position",
    "go_term_frequency",
    "ref_annotation_density",
    "neighbor_distance_std",
    "neighbor_vote_fraction",
    "neighbor_min_distance",
    "neighbor_mean_distance",
    *_NEW_V6_FEATURE_KEYS,
)


@dataclass(frozen=True)
class _UnifiedPredictContext:
    """Inputs for ``PredictGOTermsBatchOperation._unified_predict_via_pipeline``.

    Bundles the per-batch ids and the cached reference pool so the
    helper signature stays under flake8-bugbear's parameter ceiling.
    """

    p: PredictGOTermsBatchPayload
    annotation_set_id: uuid.UUID
    prediction_set_id: uuid.UUID
    valid_accessions: list[str]
    query_embeddings: np.ndarray
    ref_data: dict[str, Any]


@dataclass(frozen=True)
@dataclass
class _AspectKnnAccumulator:
    """Mutable per-batch state threaded through the aspect-separated KNN loop.

    Holds the prediction list, dedup tracking, pair feature cache, per-aspect
    go_maps, and the reranker aggregate counters that need to span all three
    aspects (e.g. ``go_term_frequency`` is summed across the union of votes).
    """

    predictions: list[dict[str, Any]]
    seen_per_query: dict[str, set[int]]
    pair_features: dict[tuple[str, str], dict[str, Any]]
    go_map_by_aspect: dict[str, dict[str, list[dict[str, Any]]]]
    rr_distance_std_per_query: dict[str, float]
    rr_vote_count_per_query: dict[str, dict[int, int]]
    rr_k_position_per_query: dict[str, dict[int, int]]
    rr_vote_min_d_per_query: dict[str, dict[int, float]]
    rr_vote_sum_d_per_query: dict[str, dict[int, float]]
    all_go_term_freq: dict[int, int]
    all_ref_ann_density: dict[str, int]


class _FeatureInputs(NamedTuple):
    """Sequence + taxonomy lookups feeding the per-pair feature computation."""

    ref_sequences: dict[str, str]
    query_sequences: dict[str, str]
    ref_tax_ids: dict[str, Any]
    query_tax_ids: dict[str, Any]


class _PredCellInputs(NamedTuple):
    """One (query, ref, annotation) cell handed to the prediction-dict builder."""

    q_acc: str
    ref_acc: str
    distance: float
    ann: dict[str, Any]
    feats: dict[str, Any]


_PAIR_FEATURE_KEYS: tuple[str, ...] = (
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
)


class AspectSeparatedKnnContext:
    """Inputs for ``PredictGOTermsBatchOperation._run_aspect_separated_knn``.

    Same family as :class:`BatchPredictContext` but for the
    aspect-separated KNN path: one independent index + neighbor set per
    GO aspect (P/F/C). ``ref_data_by_aspect`` is keyed by aspect char
    rather than the unified ``ref_data`` dict that ``_predict_batch``
    consumes; ``annotation_set_id`` is required because aspect-scoped
    GO annotation lookups happen inside the helper.
    """

    valid_accessions: list[str]
    query_embeddings: np.ndarray
    ref_data_by_aspect: dict[str, dict[str, Any]]
    annotation_set_id: uuid.UUID
    prediction_set_id: uuid.UUID
    payload: PredictGOTermsBatchPayload


def _clean_float(value: Any) -> Any:
    """Return ``None`` for NaN / non-finite floats, pass-through otherwise.

    Postgres stores NaN as a real value in double precision columns, but
    LightGBM treats NULL as missing (its native NA handling) while NaN can
    trip numeric safeguards downstream. Keeping NaN out of the DB avoids
    both footguns — feature columns read as ``None`` → pandas NA → LightGBM
    missing.
    """
    if value is None:
        return None
    if isinstance(value, float):
        if value != value or value in (float("inf"), float("-inf")):
            return None
    return value


def _row_from_prediction(
    pred: dict[str, Any],
    prediction_set_id: uuid.UUID,
) -> dict[str, Any]:
    """Build a GOPrediction INSERT row from a predict-side prediction dict."""
    row: dict[str, Any] = {
        "prediction_set_id": prediction_set_id,
        "protein_accession": pred["protein_accession"],
        "go_term_id": pred["go_term_id"],
        "ref_protein_accession": pred["ref_protein_accession"],
        "distance": pred["distance"],
        "qualifier": pred.get("qualifier"),
        "evidence_code": pred.get("evidence_code"),
        "taxonomic_relation": pred.get("taxonomic_relation"),
    }
    for key in _STORE_FLOAT_KEYS:
        row[key] = _clean_float(pred.get(key))
    return row


# ---------------------------------------------------------------------------
# Payloads
# ---------------------------------------------------------------------------
# T1.5 of master plan v3: payloads now live in protea-contracts.
# Re-export here so existing imports of these classes from this module
# keep working; new code should import from ``protea_contracts``.

from protea_contracts import (  # noqa: E402
    PredictGOTermsBatchPayload,
    PredictGOTermsPayload,
    StorePredictionsPayload,
)

# ---------------------------------------------------------------------------
# Coordinator
# ---------------------------------------------------------------------------


class PredictGOTermsOperation:
    """Coordinator: validates, creates PredictionSet, dispatches N batch messages.

    Pipeline:

    1. Validate ``EmbeddingConfig``, ``AnnotationSet`` and
       ``OntologySnapshot``.
    2. Load query accessions that have embeddings (no embedding data — keeps
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
                f"RerankerModel {p.reranker_model_id} has no artifact_uri — "
                "register it via scripts/register_reranker.py"
            )
        if not reranker_row.feature_schema_sha:
            raise ValueError(
                f"RerankerModel {p.reranker_model_id} has no feature_schema_sha — "
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
        emit("predict_go_terms.load_queries_done", None, {"queries": len(accessions)}, "info")
        return accessions


# ---------------------------------------------------------------------------
# Batch worker
# ---------------------------------------------------------------------------


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


class PredictGOTermsBatchOperation:
    """CPU batch worker: KNN search + GO annotation transfer for one query chunk.

    Reference embeddings and their GO annotations are loaded from DB on first
    access and cached at the process level (_REF_CACHE).  Subsequent batch
    messages reuse the cached reference without any DB round-trip.

    Result is published to protea.predictions.write for bulk DB insertion.
    """

    name = "predict_go_terms_batch"
    description = (
        "CPU child job: KNN search and GO annotation transfer for one query "
        "chunk; result is forwarded to store_predictions."
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

        if (
            p.compute_v6_features
            and knn_result.v6_ctx is not None
            and knn_result.prediction_dicts
        ):
            self._apply_v6_features(session, ctx, knn_result, ref_data, emit)

        # Ancestor expansion — required for the lab booster's candidate
        # distribution. Runs AFTER v6 enrichment so synthetic ancestor
        # records inherit the leaf's anc2vec_/emb_pca_ values, mirroring
        # what the dump helper emits.
        prediction_dicts = knn_result.prediction_dicts
        if p.expand_votes_to_ancestors and prediction_dicts:
            prediction_dicts = self._expand_to_ancestors(session, p, prediction_dicts, emit)

        reranker_stats: dict[str, Any] | None = None
        if p.reranker_model_id and prediction_dicts:
            reranker_stats = self._apply_reranker_if_aligned(session, prediction_dicts, p, emit)

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

    @staticmethod
    def _should_skip_for_parent(
        session: Session, parent_job_id: UUID, emit: EmitFn
    ) -> bool:
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

    def _ensure_reference_cache(
        self, session: Session, ctx: _BatchExecCtx, emit: EmitFn
    ) -> Any:
        """Load (or reuse) the per-process reference embedding cache for this
        ``(embedding_config, annotation_set, aspect_separated_knn)`` triple.

        The cache key includes ``aspect_separated_knn`` so switching modes on
        the same worker does not serve stale data from a previous run. When
        the cache is full, the oldest entry is evicted to free numpy memory.
        """
        p = ctx.p
        cache_key = (p.embedding_config_id, p.annotation_set_id, p.aspect_separated_knn)
        if cache_key not in _REF_CACHE:
            from protea.config.tuning import get_tuning

            cache_max = get_tuning().worker.ref_cache_max
            if len(_REF_CACHE) >= cache_max:
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
                    session, ctx.embedding_config_id, ctx.annotation_set_id, emit
                )
            else:
                _REF_CACHE[cache_key] = self._load_reference_data(
                    session, ctx.embedding_config_id, ctx.annotation_set_id, emit
                )
        return _REF_CACHE[cache_key]

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
        """Aspect-separated KNN dispatch — one pass per GO aspect."""
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
        return _KnnResult(
            prediction_dicts=prediction_dicts, v6_ctx=v6_ctx, query_batch=query_batch
        )

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
        unified embedding pool — for aspect-separated mode the per-aspect
        f32 arrays are concatenated first.
        """
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
            pca_pool = (
                np.concatenate(pools, axis=0) if pools else np.empty((0,), dtype=np.float32)
            )
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

    # ── helpers ──────────────────────────────────────────────────────────────

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
        from protea.core.operations._predict_go_terms_adapter import (
            AdapterInputs,
            call_pipeline_predict,
        )

        annotations, unique_neighbors = self._unified_load_annotations(session, ctx)
        ref_sequences, query_sequences, ref_tax_ids, query_tax_ids = (
            self._unified_load_pair_inputs(session, ctx, unique_neighbors)
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
        unique_neighbors: set[str] = {
            ref_acc for top_refs in neighbors for ref_acc, _ in top_refs
        }
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

    def _load_go_term_metadata(
        self,
        session: Session,
        annotations: dict[str, list[dict[str, Any]]],
    ) -> tuple[dict[int, str], dict[int, str]]:
        """Build ``(go_id_map, go_aspect_map)`` for every gtid in play."""
        gtids_in_play: set[int] = {
            int(ann["go_term_id"]) for anns in annotations.values() for ann in anns
        }
        go_id_map: dict[int, str] = {}
        go_aspect_map: dict[int, str] = {}
        if not gtids_in_play:
            return go_id_map, go_aspect_map
        from protea.config.tuning import get_tuning

        chunk_size = get_tuning().operation.annotation_chunk_size
        ids_list = list(gtids_in_play)
        for i in range(0, len(ids_list), chunk_size):
            chunk = ids_list[i : i + chunk_size]
            rows = (
                session.query(GOTerm.id, GOTerm.go_id, GOTerm.aspect)
                .filter(GOTerm.id.in_(chunk))
                .all()
            )
            for gid, go_str, aspect in rows:
                go_id_map[gid] = go_str
                go_aspect_map[gid] = aspect or ""
        return go_id_map, go_aspect_map

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

        unique_int_ids = {
            rec["go_term_id"] for rec in prediction_dicts if rec.get("go_term_id")
        }
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

    def _resolve_live_schema_sha(
        self,
        p: PredictGOTermsBatchPayload,
        emit: EmitFn,
    ) -> str | None:
        """Compute the live feature-schema SHA via ``protea_contracts``.

        Returns the SHA on success or ``None`` when the contracts module
        is unavailable (the booster is skipped silently in that case so
        production images without the dev dep can still serve KNN
        distance ordering).
        """
        try:
            from protea_contracts import compute_feature_schema_sha
        except Exception as exc:
            emit(
                "reranker.skipped",
                None,
                {
                    "reason": "contracts_unavailable",
                    "reranker_model_id": p.reranker_model_id,
                    "error": str(exc),
                },
                "warning",
            )
            return None
        live_families = infer_active_feature_families(
            compute_alignments=p.compute_alignments,
            compute_taxonomy=p.compute_taxonomy,
            compute_v6_features=p.compute_v6_features,
        )
        return compute_feature_schema_sha(live_families)

    def _score_with_reranker(
        self,
        session: Session,
        prediction_dicts: list[dict[str, Any]],
        p: PredictGOTermsBatchPayload,
    ) -> Any:
        """Load the booster, score predictions in-place, return the score array."""
        import pandas as pd

        project_root = Path(__file__).resolve().parents[2]
        settings = load_settings(project_root)
        store = get_artifact_store(settings)
        booster = load_reranker(
            p.reranker_artifact_uri,
            feature_schema_sha=p.reranker_feature_schema_sha,
            store=store,
        )
        self._attach_go_term_aspect(session, prediction_dicts)
        df = pd.DataFrame(prediction_dicts)
        scores = apply_reranker(df, booster)
        for rec, score in zip(prediction_dicts, scores.tolist(), strict=True):
            rec["reranker_score"] = float(score)
        return scores

    def _apply_reranker_if_aligned(
        self,
        session: Session,
        prediction_dicts: list[dict[str, Any]],
        p: PredictGOTermsBatchPayload,
        emit: EmitFn,
    ) -> dict[str, Any] | None:
        """Score ``prediction_dicts`` with the configured reranker.

        The booster is skipped (never crashed) whenever any precondition
        fails: missing artifact context, contracts module unavailable,
        or live schema SHA != expected. On success ``reranker_score``
        lands on every prediction dict in memory (not persisted) and
        the method returns per-batch summary stats.
        """
        if not (p.reranker_artifact_uri and p.reranker_feature_schema_sha):
            emit(
                "reranker.skipped",
                None,
                {"reason": "missing_artifact_context", "reranker_model_id": p.reranker_model_id},
                "warning",
            )
            return None
        live_sha = self._resolve_live_schema_sha(p, emit)
        if live_sha is None:
            return None
        if live_sha != p.reranker_feature_schema_sha:
            emit(
                "reranker.schema_mismatch",
                None,
                {
                    "reranker_model_id": p.reranker_model_id,
                    "expected_sha": p.reranker_feature_schema_sha,
                    "live_sha": live_sha,
                },
                "error",
            )
            return {
                "applied": False,
                "skipped_reason": "schema_mismatch",
                "expected_sha": p.reranker_feature_schema_sha,
                "live_sha": live_sha,
            }
        scores = self._score_with_reranker(session, prediction_dicts, p)
        if scores.size == 0:
            return {"applied": True, "rows": 0}
        return {
            "applied": True,
            "rows": int(scores.size),
            "score_min": float(scores.min()),
            "score_max": float(scores.max()),
            "score_mean": float(scores.mean()),
            "feature_schema_sha": live_sha,
        }

    def _attach_go_term_aspect(
        self,
        session: Session,
        prediction_dicts: list[dict[str, Any]],
    ) -> None:
        """Look up ``GOTerm.aspect`` for every unique ``go_term_id`` and
        write it back onto each prediction dict so the reranker's
        categorical feature is populated.
        """
        unique_ids = {
            rec["go_term_id"] for rec in prediction_dicts if rec.get("go_term_id") is not None
        }
        if not unique_ids:
            return
        aspect_by_id: dict[int, str] = dict(
            session.query(GOTerm.id, GOTerm.aspect).filter(GOTerm.id.in_(unique_ids)).all()
        )
        for rec in prediction_dicts:
            gid = rec.get("go_term_id")
            if gid is not None and gid in aspect_by_id:
                rec["aspect"] = aspect_by_id[gid]

    def _load_reference_data(
        self,
        session: Session,
        embedding_config_id: uuid.UUID,
        annotation_set_id: uuid.UUID,
        emit: EmitFn,
    ) -> dict[str, Any]:
        """Load reference accessions and embeddings (float16) into the process cache.

        Checks the disk cache first (survives worker restarts). On miss,
        fetches from PostgreSQL and writes the result to disk for future
        restarts. GO annotations are NOT loaded here; they are fetched
        lazily per batch for only the unique neighbours found by KNN,
        saving several GB of RAM.
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
            return _derive_reference_views(cached["accessions"], cached["embeddings"])
        accessions, embeddings = self._stream_reference_pool(
            session, embedding_config_id, annotation_set_id
        )
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
        return _derive_reference_views(accessions, embeddings)

    def _stream_reference_pool(
        self,
        session: Session,
        embedding_config_id: uuid.UUID,
        annotation_set_id: uuid.UUID,
    ) -> tuple[list[str], np.ndarray]:
        """Stream the ``(accession, embedding)`` rows for all annotated proteins.

        Pre-allocates a float16 numpy array sized to the row count so
        peak Python-object memory stays bounded by the cursor chunk;
        without pre-allocation a ``.all()`` on 400k rows materialises
        ~14 GB of Python float objects.
        """
        annotated_sq = (
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
            .join(annotated_sq, Protein.accession == annotated_sq.c.protein_accession)
        )
        total = base_q.count()
        if total == 0:
            return [], np.empty((0,), dtype=np.float16)
        # pgvector HalfVector exposes .dimensions() / .to_numpy() after the
        # 2026-04-11 halfvec migration; pull dimension from the first row.
        dim = base_q.limit(1).one()[1].dimensions()
        from protea.config.tuning import get_tuning

        stream_chunk = get_tuning().operation.stream_chunk_size
        embeddings = np.empty((total, dim), dtype=np.float16)
        accessions: list[str] = []
        for i, (acc, emb) in enumerate(base_q.yield_per(stream_chunk)):
            embeddings[i] = emb.to_numpy()
            accessions.append(acc)
        return accessions, embeddings

    def _load_reference_data_per_aspect(
        self,
        session: Session,
        embedding_config_id: uuid.UUID,
        annotation_set_id: uuid.UUID,
        emit: EmitFn,
    ) -> dict[str, dict[str, Any]]:
        """Build per-aspect reference views as numpy slices over a single unified cache.

        One ~1 GB float16 embedding array + three int32 index arrays (~2 MB each)
        live on disk. Every per-aspect view is a fancy-index slice into the unified
        array — no embedding duplication. See :meth:`_query_and_persist_aspect_caches`
        for the on-disk layout and the (one-shot) DB scan path.
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

        unified = self._load_reference_data(session, embedding_config_id, annotation_set_id, emit)
        if not unified["accessions"]:
            return {
                asp: _derive_reference_views([], np.empty((0,), dtype=np.float16))
                for asp in _ASPECTS
            }

        acc_to_idx: dict[str, int] = {acc: i for i, acc in enumerate(unified["accessions"])}
        missing_aspects = self._find_missing_aspects(embedding_config_id, annotation_set_id)
        if missing_aspects:
            self._query_and_persist_aspect_caches(
                session,
                missing_aspects,
                unified["accessions"],
                acc_to_idx,
                embedding_config_id,
                annotation_set_id,
            )

        result, total_refs = self._assemble_all_aspect_views(
            unified, missing_aspects, embedding_config_id, annotation_set_id, emit
        )
        emit(
            "predict_go_terms_batch.load_references_per_aspect_all_done",
            None,
            {"total_references": total_refs},
            "info",
        )
        return result

    def _assemble_all_aspect_views(
        self,
        unified: dict[str, Any],
        missing_aspects: list[str],
        embedding_config_id: uuid.UUID,
        annotation_set_id: uuid.UUID,
        emit: EmitFn,
    ) -> tuple[dict[str, dict[str, Any]], int]:
        """Slice the unified cache into per-aspect views, emit one ``done`` event
        per aspect, and return ``(views, total_refs)``."""
        result: dict[str, dict[str, Any]] = {}
        total_refs = 0
        for aspect in _ASPECTS:
            view, n_refs = self._assemble_aspect_view(
                aspect, unified, embedding_config_id, annotation_set_id
            )
            result[aspect] = view
            total_refs += n_refs
            emit(
                "predict_go_terms_batch.load_references_per_aspect_done",
                None,
                {
                    "aspect": aspect,
                    "references": n_refs,
                    "source": "database" if aspect in missing_aspects else "disk_cache",
                },
                "info",
            )
        return result, total_refs

    @staticmethod
    def _find_missing_aspects(
        embedding_config_id: uuid.UUID, annotation_set_id: uuid.UUID
    ) -> list[str]:
        """Return aspects whose index file or annotation CSR is absent on disk —
        these still need the DB query path to repopulate the on-disk cache."""
        return [
            asp
            for asp in _ASPECTS
            if not _aspect_index_path(embedding_config_id, annotation_set_id, asp).exists()
            or _load_anno_csr_from_disk(embedding_config_id, annotation_set_id, asp) is None
        ]

    @staticmethod
    def _query_and_persist_aspect_caches(
        session: Session,
        missing_aspects: list[str],
        unified_accessions: list[str],
        acc_to_idx: dict[str, int],
        embedding_config_id: uuid.UUID,
        annotation_set_id: uuid.UUID,
    ) -> None:
        """Single-pass DB scan + per-aspect on-disk persistence for the missing aspects.

        Collects ``(aspect_to_accset, aspect_to_go_map)`` via
        :meth:`_collect_aspect_annotations`, then writes the index array +
        annotation CSR for every missing aspect. Downstream batches read the
        CSR straight from disk — zero per-batch IN queries.
        """
        aspect_to_accset, aspect_to_go_map = (
            PredictGOTermsBatchOperation._collect_aspect_annotations(
                session, missing_aspects, annotation_set_id
            )
        )
        for asp in missing_aspects:
            idx_path = _aspect_index_path(embedding_config_id, annotation_set_id, asp)
            indices = np.array(
                [acc_to_idx[acc] for acc in aspect_to_accset[asp] if acc in acc_to_idx],
                dtype=np.int32,
            )
            idx_path.parent.mkdir(parents=True, exist_ok=True)
            np.save(idx_path, indices)
            asp_accessions = [unified_accessions[i] for i in indices]
            anno_csr = _build_anno_csr(asp_accessions, aspect_to_go_map[asp])
            _save_anno_csr_to_disk(embedding_config_id, annotation_set_id, asp, anno_csr)

    @staticmethod
    def _collect_aspect_annotations(
        session: Session,
        missing_aspects: list[str],
        annotation_set_id: uuid.UUID,
    ) -> tuple[
        dict[str, set[str]],
        dict[str, dict[str, list[dict[str, Any]]]],
    ]:
        """Single-pass query that fetches every ``ProteinGOAnnotation`` row for
        the missing aspects in one table scan, returning per-aspect accession
        sets + per-protein GO maps. Skips ``NOT`` qualifiers."""
        aspect_to_accset: dict[str, set[str]] = {asp: set() for asp in missing_aspects}
        aspect_to_go_map: dict[str, dict[str, list[dict[str, Any]]]] = {
            asp: {} for asp in missing_aspects
        }
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
            if asp not in aspect_to_accset:
                continue
            aspect_to_accset[asp].add(acc)
            aspect_to_go_map[asp].setdefault(acc, []).append(
                {
                    "go_term_id": go_term_id,
                    # Flyweight — see ``protea.core.annotation_intern``.
                    "qualifier": intern_string(qualifier),
                    "evidence_code": intern_string(evidence_code),
                }
            )
        return aspect_to_accset, aspect_to_go_map

    @staticmethod
    def _assemble_aspect_view(
        aspect: str,
        unified: dict[str, Any],
        embedding_config_id: uuid.UUID,
        annotation_set_id: uuid.UUID,
    ) -> tuple[dict[str, Any], int]:
        """Slice the unified embeddings by the aspect's on-disk index array and
        attach the CSR annotation cache. Returns ``(view, n_refs)``."""
        idx_path = _aspect_index_path(embedding_config_id, annotation_set_id, aspect)
        indices = np.load(idx_path)
        aspect_accessions = [unified["accessions"][i] for i in indices]
        view: dict[str, Any] = {
            "accessions": aspect_accessions,
            "embeddings": unified["embeddings"][indices],
            "embeddings_f32": unified["embeddings_f32"][indices],
            "embeddings_f32_cos": unified["embeddings_f32_cos"][indices],
        }
        anno_csr = _load_anno_csr_from_disk(embedding_config_id, annotation_set_id, aspect)
        if anno_csr is not None:
            gtids, quals, ecodes, offsets = anno_csr
            view.update(
                {
                    "anno_gtids": gtids,
                    "anno_quals": quals,
                    "anno_ecodes": ecodes,
                    "anno_offsets": offsets,
                    "acc_to_anno_idx": {acc: i for i, acc in enumerate(aspect_accessions)},
                }
            )
        return view, len(indices)

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
        """Run three independent KNN searches (one per GO aspect) and merge results.

        For each aspect (P/F/C): build a KNN index from the aspect-filtered
        references, take the top ``limit_per_entry`` neighbours of every
        query, load aspect-scoped GO annotations, and transfer them as
        predictions. Guarantees BPO + MFO + CCO candidates even when the
        globally nearest neighbours all happen to carry one or two aspects.

        Feature engineering (alignments / taxonomy) runs on the union of
        neighbours across aspects to avoid redundant work.
        """
        p = ctx.payload
        neighbors_by_aspect, all_unique_neighbors = self._run_knn_per_aspect(
            ctx.valid_accessions, ctx.query_embeddings, ctx.ref_data_by_aspect, p
        )
        feature_inputs = _FeatureInputs(
            *self._load_feature_engineering_data(
                session, p, ctx.valid_accessions, all_unique_neighbors
            )
        )
        acc = self._init_aspect_accumulator(ctx.valid_accessions, neighbors_by_aspect, p)

        for aspect in _ASPECTS:
            unique_neighbors_aspect: set[str] = set()
            for top_refs in neighbors_by_aspect[aspect]:
                for ref_acc, _ in top_refs:
                    unique_neighbors_aspect.add(ref_acc)
            go_map = self._load_aspect_go_map(
                session,
                ctx.ref_data_by_aspect[aspect],
                ctx.annotation_set_id,
                unique_neighbors_aspect,
                aspect,
            )
            acc.go_map_by_aspect[aspect] = go_map
            if p.compute_reranker_features:
                self._update_reranker_aggregates(
                    acc, ctx.valid_accessions, neighbors_by_aspect, go_map, aspect
                )
            self._build_aspect_predictions(
                ctx, acc, neighbors_by_aspect[aspect], go_map, feature_inputs
            )

        return (
            acc.predictions,
            neighbors_by_aspect,
            acc.go_map_by_aspect,
            acc.pair_features,
        )

    @staticmethod
    def _init_aspect_accumulator(
        valid_accessions: list[str],
        neighbors_by_aspect: dict[str, list[list[tuple[str, float]]]],
        p: PredictGOTermsBatchPayload,
    ) -> _AspectKnnAccumulator:
        """Build the per-batch accumulator and pre-compute the per-query
        ``neighbor_distance_std`` aggregate (uses the union of distances
        across all three aspects)."""
        acc = _AspectKnnAccumulator(
            predictions=[],
            seen_per_query={a: set() for a in valid_accessions},
            pair_features={},
            go_map_by_aspect={},
            rr_distance_std_per_query={},
            rr_vote_count_per_query={},
            rr_k_position_per_query={},
            rr_vote_min_d_per_query={},
            rr_vote_sum_d_per_query={},
            all_go_term_freq={},
            all_ref_ann_density={},
        )
        if not p.compute_reranker_features:
            return acc
        for q_idx, q_acc in enumerate(valid_accessions):
            acc.rr_vote_count_per_query[q_acc] = {}
            acc.rr_k_position_per_query[q_acc] = {}
            acc.rr_vote_min_d_per_query[q_acc] = {}
            acc.rr_vote_sum_d_per_query[q_acc] = {}
            all_distances: list[float] = []
            for aspect in _ASPECTS:
                aspect_neighbors = neighbors_by_aspect[aspect]
                if q_idx < len(aspect_neighbors):
                    all_distances.extend(d for _, d in aspect_neighbors[q_idx])
            acc.rr_distance_std_per_query[q_acc] = (
                float(np.std(all_distances)) if len(all_distances) > 1 else 0.0
            )
        return acc

    def _load_aspect_go_map(
        self,
        session: Session,
        aspect_ref: dict[str, Any],
        annotation_set_id: uuid.UUID,
        unique_neighbors_aspect: set[str],
        aspect: str,
    ) -> dict[str, list[dict[str, Any]]]:
        """Resolve the per-aspect ``ref_acc → annotations`` map: in-memory CSR
        cache when available, fall back to a DB query keyed on the aspect."""
        if "anno_gtids" in aspect_ref:
            return _csr_lookup(
                unique_neighbors_aspect,
                aspect_ref["acc_to_anno_idx"],
                AnnoCsr(
                    gtids=aspect_ref["anno_gtids"],
                    quals=aspect_ref["anno_quals"],
                    ecodes=aspect_ref["anno_ecodes"],
                    offsets=aspect_ref["anno_offsets"],
                ),
            )
        return self._load_annotations_for(
            session, annotation_set_id, unique_neighbors_aspect, aspect=aspect
        )

    @staticmethod
    def _update_reranker_aggregates(
        acc: _AspectKnnAccumulator,
        valid_accessions: list[str],
        neighbors_by_aspect: dict[str, list[list[tuple[str, float]]]],
        go_map: dict[str, list[dict[str, Any]]],
        aspect: str,
    ) -> None:
        """Fold this aspect's go_map into the running reranker counters:
        ``go_term_frequency``, ``ref_annotation_density``, ``vote_count``,
        ``k_position``, ``vote_min_distance``, ``vote_sum_distance``."""
        for ref_acc, anns in go_map.items():
            acc.all_ref_ann_density[ref_acc] = (
                acc.all_ref_ann_density.get(ref_acc, 0) + len(anns)
            )
            for ann in anns:
                gtid = ann["go_term_id"]
                acc.all_go_term_freq[gtid] = acc.all_go_term_freq.get(gtid, 0) + 1
        aspect_neighbors = neighbors_by_aspect[aspect]
        for q_idx, q_acc in enumerate(valid_accessions):
            if q_idx >= len(aspect_neighbors):
                continue
            vc = acc.rr_vote_count_per_query[q_acc]
            kp = acc.rr_k_position_per_query[q_acc]
            min_d = acc.rr_vote_min_d_per_query[q_acc]
            sum_d = acc.rr_vote_sum_d_per_query[q_acc]
            for k_pos, (ref_acc, dist) in enumerate(aspect_neighbors[q_idx], 1):
                for ann in go_map.get(ref_acc, []):
                    gtid = ann["go_term_id"]
                    vc[gtid] = vc.get(gtid, 0) + 1
                    if gtid not in kp:
                        kp[gtid] = k_pos
                    prev_min = min_d.get(gtid)
                    if prev_min is None or dist < prev_min:
                        min_d[gtid] = dist
                    sum_d[gtid] = sum_d.get(gtid, 0.0) + dist

    def _build_aspect_predictions(
        self,
        ctx: AspectSeparatedKnnContext,
        acc: _AspectKnnAccumulator,
        aspect_top_refs: list[list[tuple[str, float]]],
        go_map: dict[str, list[dict[str, Any]]],
        feature_inputs: _FeatureInputs,
    ) -> None:
        """Walk this aspect's per-query neighbours and emit one prediction
        dict per (query, ref, go_term) tuple — deduped against
        ``acc.seen_per_query`` so a term carried by multiple aspects only
        yields one row in the final list."""
        p = ctx.payload
        for q_acc, top_refs in zip(ctx.valid_accessions, aspect_top_refs, strict=False):
            seen_terms = acc.seen_per_query[q_acc]
            for ref_acc, distance in top_refs:
                feats = self._resolve_pair_features(
                    acc.pair_features, p, q_acc, ref_acc, feature_inputs
                )
                for ann in go_map.get(ref_acc, []):
                    go_term_id = ann["go_term_id"]
                    if go_term_id in seen_terms:
                        continue
                    seen_terms.add(go_term_id)
                    cell = _PredCellInputs(
                        q_acc=q_acc,
                        ref_acc=ref_acc,
                        distance=distance,
                        ann=ann,
                        feats=feats,
                    )
                    acc.predictions.append(self._build_prediction_dict(ctx, acc, cell))

    @staticmethod
    def _resolve_pair_features(
        pair_features: dict[tuple[str, str], dict[str, Any]],
        p: PredictGOTermsBatchPayload,
        q_acc: str,
        ref_acc: str,
        feature_inputs: _FeatureInputs,
    ) -> dict[str, Any]:
        """Return (and cache) per-(query, ref) pair features. Computes
        alignments + taxonomy once per pair across the aspect loop."""
        pair_key = (q_acc, ref_acc)
        if pair_key in pair_features:
            return pair_features[pair_key]
        feats: dict[str, Any] = {}
        if p.compute_alignments:
            q_seq = feature_inputs.query_sequences.get(q_acc, "")
            r_seq = feature_inputs.ref_sequences.get(ref_acc, "")
            if q_seq and r_seq:
                feats.update(compute_alignment(q_seq, r_seq))
        if p.compute_taxonomy:
            q_tid = feature_inputs.query_tax_ids.get(q_acc)
            r_tid = feature_inputs.ref_tax_ids.get(ref_acc)
            feats.update(compute_taxonomy(q_tid, r_tid))
            feats["query_taxonomy_id"] = q_tid
            feats["ref_taxonomy_id"] = r_tid
        pair_features[pair_key] = feats
        return feats

    @staticmethod
    def _build_prediction_dict(
        ctx: AspectSeparatedKnnContext,
        acc: _AspectKnnAccumulator,
        cell: _PredCellInputs,
    ) -> dict[str, Any]:
        """Construct one prediction dict — base fields + reranker stats +
        non-null pair features. Output schema matches the unified-pool path."""
        ann = cell.ann
        go_term_id = ann["go_term_id"]
        pred: dict[str, Any] = {
            "prediction_set_id": str(ctx.prediction_set_id),
            "protein_accession": cell.q_acc,
            "go_term_id": go_term_id,
            "ref_protein_accession": cell.ref_acc,
            "distance": cell.distance,
        }
        if ann.get("qualifier"):
            pred["qualifier"] = ann["qualifier"]
        if ann.get("evidence_code"):
            pred["evidence_code"] = ann["evidence_code"]
        if ctx.payload.compute_reranker_features:
            PredictGOTermsBatchOperation._stamp_reranker_features(pred, ctx, acc, cell)
        for key in _PAIR_FEATURE_KEYS:
            val = cell.feats.get(key)
            if val is not None:
                pred[key] = val
        return pred

    @staticmethod
    def _stamp_reranker_features(
        pred: dict[str, Any],
        ctx: AspectSeparatedKnnContext,
        acc: _AspectKnnAccumulator,
        cell: _PredCellInputs,
    ) -> None:
        """Fill the per-prediction reranker stats — vote / position / frequency
        / density / distance aggregates — from the running ``acc`` counters."""
        p = ctx.payload
        go_term_id = cell.ann["go_term_id"]
        q_acc = cell.q_acc
        vc_val = acc.rr_vote_count_per_query.get(q_acc, {}).get(go_term_id, 1)
        pred["vote_count"] = vc_val
        pred["k_position"] = acc.rr_k_position_per_query.get(q_acc, {}).get(go_term_id, 1)
        pred["go_term_frequency"] = acc.all_go_term_freq.get(go_term_id, 0)
        pred["ref_annotation_density"] = acc.all_ref_ann_density.get(cell.ref_acc, 0)
        pred["neighbor_distance_std"] = acc.rr_distance_std_per_query.get(q_acc, 0.0)
        pred["neighbor_vote_fraction"] = (
            vc_val / p.limit_per_entry if p.limit_per_entry else None
        )
        min_d_map = acc.rr_vote_min_d_per_query.get(q_acc, {})
        sum_d_map = acc.rr_vote_sum_d_per_query.get(q_acc, {})
        pred["neighbor_min_distance"] = min_d_map.get(go_term_id)
        if vc_val > 0 and go_term_id in sum_d_map:
            pred["neighbor_mean_distance"] = sum_d_map[go_term_id] / vc_val

    def _run_knn_per_aspect(
        self,
        valid_accessions: list[str],
        query_embeddings: np.ndarray,
        ref_data_by_aspect: dict[str, dict[str, Any]],
        p: PredictGOTermsBatchPayload,
    ) -> tuple[dict[str, list[list[tuple[str, float]]]], set[str]]:
        """Run one independent KNN search per GO aspect and accumulate
        the union of all neighbors across aspects.

        Returns ``(neighbors_by_aspect, all_unique_neighbors)`` — feature
        engineering downstream is computed once per pair regardless of how
        many aspects reference it, so the union is the right call surface.
        """
        neighbors_by_aspect: dict[str, list[list[tuple[str, float]]]] = {}
        all_unique_neighbors: set[str] = set()

        use_cos = p.metric == "cosine"
        for aspect in _ASPECTS:
            aspect_refs = ref_data_by_aspect[aspect]
            if not aspect_refs["accessions"]:
                neighbors_by_aspect[aspect] = [[] for _ in valid_accessions]
                continue

            ref_f32 = (
                aspect_refs["embeddings_f32_cos"] if use_cos else aspect_refs["embeddings_f32"]
            )
            aspect_neighbors = search_knn(
                query_embeddings,
                ref_f32,
                aspect_refs["accessions"],
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
            neighbors_by_aspect[aspect] = aspect_neighbors
            for top_refs in aspect_neighbors:
                for ref_acc, _ in top_refs:
                    all_unique_neighbors.add(ref_acc)

        return neighbors_by_aspect, all_unique_neighbors

    def _load_feature_engineering_data(
        self,
        session: Session,
        p: PredictGOTermsBatchPayload,
        valid_accessions: list[str],
        all_unique_neighbors: set[str],
    ) -> tuple[
        dict[str, str],
        dict[str, str],
        dict[str, int | None],
        dict[str, int | None],
    ]:
        """Load sequences and taxonomy IDs for downstream feature engineering.

        Each tuple slot is empty when the corresponding flag
        (``compute_alignments`` / ``compute_taxonomy``) is False, so the
        caller can pass them straight into the per-pair feature builder
        without further conditionals.

        Returns ``(ref_sequences, query_sequences, ref_tax_ids, query_tax_ids)``.
        """
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

        return ref_sequences, query_sequences, ref_tax_ids, query_tax_ids

    def _load_annotations_for(
        self,
        session: Session,
        annotation_set_id: uuid.UUID,
        accessions: set[str],
        aspect: str | None = None,
    ) -> dict[str, list[dict[str, Any]]]:
        """Load GO annotations for the given accessions, chunked to avoid param limits.

        Only non-negated annotations are loaded: rows with a NOT qualifier
        (e.g. ``'NOT'``, ``'NOT|involved_in'``) assert that the protein does
        *not* have the annotated function and must never be transferred as
        positive predictions. When ``aspect`` is provided (``'P'`` / ``'F'``
        / ``'C'``), only annotations whose GO term belongs to that aspect are
        returned (per-aspect KNN mode).
        """
        from protea.config.tuning import get_tuning

        chunk_size = get_tuning().operation.annotation_chunk_size
        go_map: dict[str, list[dict[str, Any]]] = {}
        accessions_list = list(accessions)
        for i in range(0, len(accessions_list), chunk_size):
            chunk = accessions_list[i : i + chunk_size]
            rows = self._fetch_annotation_chunk(session, annotation_set_id, chunk, aspect)
            for acc, go_term_id, qualifier, evidence_code in rows:
                go_map.setdefault(acc, []).append(
                    {
                        "go_term_id": go_term_id,
                        # Flyweight — qualifier / evidence_code take ~5-10
                        # distinct values across millions of rows; interning
                        # collapses every duplicate to one shared string.
                        "qualifier": intern_string(qualifier),
                        "evidence_code": intern_string(evidence_code),
                    }
                )
        return go_map

    def _fetch_annotation_chunk(
        self,
        session: Session,
        annotation_set_id: uuid.UUID,
        chunk: list[str],
        aspect: str | None,
    ) -> list[Any]:
        """Query one accession chunk for non-negated annotations.

        Returns the raw rows; the join to ``go_term`` is added only when
        aspect filtering is requested so the common (non-aspect-separated)
        path stays as fast as before. ``qualifier IS NULL`` is preserved
        explicitly because SQL ``LIKE`` returns NULL for NULL inputs and
        would otherwise drop those rows.
        """
        q = session.query(
            ProteinGOAnnotation.protein_accession,
            ProteinGOAnnotation.go_term_id,
            ProteinGOAnnotation.qualifier,
            ProteinGOAnnotation.evidence_code,
        ).filter(
            ProteinGOAnnotation.annotation_set_id == annotation_set_id,
            ProteinGOAnnotation.protein_accession.in_(chunk),
            (
                ProteinGOAnnotation.qualifier.is_(None)
                | ~ProteinGOAnnotation.qualifier.like("%NOT%")
            ),
        )
        if aspect is not None:
            q = q.join(ProteinGOAnnotation.go_term).filter(GOTerm.aspect == aspect)
        return q.all()

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
        # Rows return pgvector HalfVector instances (halfvec column since 2026-04-11).
        embeddings = np.array([r[1].to_list() for r in rows], dtype=np.float32)
        return embeddings, valid_accessions

    # ── v6 reranker features ─────────────────────────────────────────────────

    # ── feature-engineering helpers ───────────────────────────────────────────

    def _load_sequences_for_proteins(
        self, session: Session, accessions: set[str]
    ) -> dict[str, str]:
        from protea.config.tuning import get_tuning

        chunk_size = get_tuning().operation.annotation_chunk_size
        result: dict[str, str] = {}
        acc_list = list(accessions)
        for i in range(0, len(acc_list), chunk_size):
            chunk = acc_list[i : i + chunk_size]
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
        from protea.config.tuning import get_tuning

        chunk_size = get_tuning().operation.annotation_chunk_size
        result: dict[str, int | None] = {}
        acc_list = list(accessions)
        for i in range(0, len(acc_list), chunk_size):
            chunk = acc_list[i : i + chunk_size]
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
        from protea.config.tuning import get_tuning

        chunk_size = get_tuning().operation.annotation_chunk_size
        acc_set = set(accessions)
        result: dict[str, int | None] = {acc: None for acc in acc_set}
        acc_list = list(acc_set)
        for i in range(0, len(acc_list), chunk_size):
            chunk = acc_list[i : i + chunk_size]
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
    description = (
        "CPU child job: bulk-insert GOPrediction rows from a batch and "
        "atomically advance the parent predict_go_terms progress counter."
    )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        p = payload or {}
        n = len(p.get("predictions") or [])
        return f"n={n}" if n else ""

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
                [_row_from_prediction(pred, prediction_set_id) for pred in p.predictions],
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

        if p.is_final_chunk:
            self._update_parent_progress(session, parent_job_id, emit)

        return OperationResult(result={"predictions_inserted": len(p.predictions)})

    def _update_parent_progress(self, session: Session, parent_job_id: UUID, emit: EmitFn) -> None:
        update_parent_progress(
            session,
            parent_job_id,
            emit,
            event_name="store_predictions.parent_succeeded",
        )
