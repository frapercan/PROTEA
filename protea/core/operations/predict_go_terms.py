from __future__ import annotations

import time
import uuid
from pathlib import Path
from typing import Any
from uuid import UUID

import numpy as np
from sqlalchemy import update as sa_update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from protea.core.annotation_intern import intern_string
from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.core.contracts.parent_progress import update_parent_progress
from protea.core.disk_cache import (
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
from protea.core.feature_enricher import enrich_v6_features
from protea.core.knn_search import search_knn
from protea.core.pca_cache import (
    _load_or_fit_pca_state,
)
from protea.core.reranker import (
    EMBEDDING_PCA_DIM,
    apply_reranker,
    infer_active_feature_families,
    load_reranker,
)
from protea.core.utils import utcnow
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
from protea.infrastructure.orm.models.annotation.protein_go_annotation import ProteinGOAnnotation
from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
from protea.infrastructure.orm.models.embedding.go_prediction import GOPrediction
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.embedding.reranker_model import RerankerModel
from protea.infrastructure.orm.models.embedding.sequence_embedding import SequenceEmbedding
from protea.infrastructure.orm.models.job import Job, JobEvent, JobStatus
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

        reranker_artifact_uri: str | None = None
        reranker_feature_schema_sha: str | None = None
        if p.reranker_model_id:
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
            reranker_artifact_uri = reranker_row.artifact_uri
            reranker_feature_schema_sha = reranker_row.feature_schema_sha
            emit(
                "predict_go_terms.reranker_bound",
                None,
                {
                    "reranker_model_id": p.reranker_model_id,
                    "reranker_name": reranker_row.name,
                    "feature_schema_sha": reranker_feature_schema_sha,
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
                            "ontology_snapshot_id": p.ontology_snapshot_id,
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
                            "compute_v6_features": p.compute_v6_features,
                            "expand_votes_to_ancestors": p.expand_votes_to_ancestors,
                            "aspect_separated_knn": p.aspect_separated_knn,
                            "reranker_model_id": p.reranker_model_id,
                            "reranker_artifact_uri": reranker_artifact_uri,
                            "reranker_feature_schema_sha": reranker_feature_schema_sha,
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

        v6_ctx: dict[str, Any] | None = None

        if p.aspect_separated_knn:
            (
                prediction_dicts,
                neighbors_by_aspect,
                go_map_by_aspect,
                pair_features,
            ) = self._run_aspect_separated_knn(
                session,
                valid_accessions,
                query_embeddings,
                _REF_CACHE[cache_key],
                annotation_set_id,
                prediction_set_id,
                p,
            )
            if p.compute_v6_features:
                v6_ctx = {
                    "neighbors_by_aspect": neighbors_by_aspect,
                    "go_map_by_aspect": go_map_by_aspect,
                    "pair_features": pair_features,
                }
        else:
            ref_data = _REF_CACHE[cache_key]
            if not ref_data["embeddings"].size:
                emit("predict_go_terms_batch.no_references", None, {}, "warning")
                return OperationResult(result={"predictions": 0})

            # --- KNN: use precomputed f32 (cosine-normalised if metric == cosine) ---
            use_cos = p.metric == "cosine"
            ref_embeddings_f32 = (
                ref_data["embeddings_f32_cos"] if use_cos else ref_data["embeddings_f32"]
            )
            neighbors = search_knn(
                query_embeddings,
                ref_embeddings_f32,
                ref_data["accessions"],
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
            prediction_dicts, neighbors, pair_features = self._predict_batch(
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
            if p.compute_v6_features:
                # Unified mode: collapse to a single synthetic aspect key so the
                # enricher can partition GO terms via the aspect map.
                v6_ctx = {
                    "neighbors_by_aspect": {"": neighbors},
                    "go_map_by_aspect": {"": go_map},
                    "pair_features": pair_features,
                }

        # --- v6 feature enrichment (Anc2Vec + tax_voters + emb_pca) ---------
        if p.compute_v6_features and v6_ctx is not None and prediction_dicts:
            ref_unified = _REF_CACHE[cache_key]
            # For aspect-separated mode, the cache is a per-aspect dict —
            # concatenate f32 embeddings to fit PCA on the full pool.
            if p.aspect_separated_knn:
                pools = [
                    ref_unified[a]["embeddings_f32"]
                    for a in _ASPECTS
                    if ref_unified[a].get("embeddings_f32") is not None
                    and ref_unified[a]["embeddings_f32"].size
                ]
                pca_pool = (
                    np.concatenate(pools, axis=0)
                    if pools
                    else np.empty((0,), dtype=np.float32)
                )
            else:
                pca_pool = ref_unified.get("embeddings_f32", np.empty((0,), dtype=np.float32))

            pca_state = _load_or_fit_pca_state(embedding_config_id, pca_pool)
            enrich_v6_features(
                prediction_dicts,
                session=session,
                valid_accessions=valid_accessions,
                query_embeddings=query_embeddings,
                neighbors_by_aspect=v6_ctx["neighbors_by_aspect"],
                go_map_by_aspect=v6_ctx["go_map_by_aspect"],
                pair_features=v6_ctx["pair_features"],
                pca_state=pca_state,
                compute_taxonomy=p.compute_taxonomy,
            )
            emit(
                "predict_go_terms_batch.v6_features_done",
                None,
                {
                    "pca_state_fit": pca_state is not None,
                    "pca_dim": EMBEDDING_PCA_DIM if pca_state is not None else 0,
                    "rows_enriched": len(prediction_dicts),
                },
                "info",
            )

        # Ancestor expansion — required for the lab booster's candidate
        # distribution. Runs AFTER v6 enrichment so synthetic ancestor
        # records inherit the leaf's anc2vec_/emb_pca_ values, mirroring
        # what the dump helper emits.
        if p.expand_votes_to_ancestors and prediction_dicts:
            from sqlalchemy import select

            from protea.core.feature_enricher import (
                expand_predictions_to_ancestors,
                load_parent_map,
            )
            # predict_go_terms keys candidates by integer ``go_term_id``;
            # the expansion helper (and parent_map) operate on string GO
            # accessions (``"GO:0006357"``). Materialise the map once for
            # this batch's candidate set, then add ``go_id`` to each record
            # before expanding. After expansion, synthetic ancestor records
            # need ``go_term_id`` resolved back so the bulk insert can use
            # the FK — pull both directions from ``go_term`` in one query.
            parent_map = load_parent_map(session, uuid.UUID(p.ontology_snapshot_id))
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

            n_before = len(prediction_dicts)
            prediction_dicts = expand_predictions_to_ancestors(
                prediction_dicts,
                parent_map=parent_map,
                k_limit=p.limit_per_entry,
                ia_weights=None,
            )

            # Synthetic ancestors get a ``go_id`` string but no ``go_term_id``
            # (the helper just clones the leaf record). Resolve the FK so
            # store_predictions can insert the row.
            ancestor_strs = {
                rec["go_id"] for rec in prediction_dicts
                if rec.get("go_id") and rec["go_id"] not in {v for v in int_to_str.values()}
            }
            if ancestor_strs:
                anc_pairs = session.execute(
                    select(GOTerm.id, GOTerm.go_id).where(
                        GOTerm.go_id.in_(ancestor_strs),
                        GOTerm.ontology_snapshot_id == uuid.UUID(p.ontology_snapshot_id),
                    )
                ).all()
                str_to_int = {go_id: gid for gid, go_id in anc_pairs}
                str_to_int.update({v: k for k, v in int_to_str.items()})
                # Drop ancestors that don't exist in this snapshot — predict
                # cannot store rows without a valid go_term FK.
                prediction_dicts = [
                    {**rec, "go_term_id": str_to_int[rec["go_id"]]}
                    for rec in prediction_dicts
                    if rec.get("go_id") in str_to_int
                ]

            emit(
                "predict_go_terms_batch.expanded_to_ancestors",
                None,
                {
                    "rows_before": n_before,
                    "rows_after": len(prediction_dicts),
                    "expansion_ratio": (
                        len(prediction_dicts) / n_before if n_before else 0.0
                    ),
                },
                "info",
            )

        reranker_stats: dict[str, Any] | None = None
        if p.reranker_model_id and prediction_dicts:
            reranker_stats = self._apply_reranker_if_aligned(
                session, prediction_dicts, p, emit
            )

        elapsed = time.perf_counter() - t0

        done_fields: dict[str, Any] = {
            "queries": len(valid_accessions),
            "predictions": len(prediction_dicts),
            "elapsed_seconds": elapsed,
        }
        if reranker_stats is not None:
            done_fields["reranker"] = reranker_stats
        emit(
            "predict_go_terms_batch.done",
            None,
            done_fields,
            "info",
        )

        # RabbitMQ caps message size at 128 MB; ancestor-expanded batches
        # serialise to ~250-300 MB and silently land in the dead-letter
        # queue. Split into ≤10k-row chunks (~20-25 MB each) so the write
        # worker actually receives them and broker memory pressure stays low
        # even when many batches publish concurrently. Only the final chunk
        # advances the coordinator's batch counter (``is_final_chunk=True``)
        # so the parent job doesn't mark itself succeeded after the first
        # batch's chunks finish.
        from protea.config.tuning import get_tuning

        store_chunk_size = get_tuning().operation.store_chunk_size
        chunks: list[list[dict[str, Any]]] = [
            prediction_dicts[s:s + store_chunk_size]
            for s in range(0, len(prediction_dicts), store_chunk_size)
        ] or [[]]
        store_messages: list[tuple[str, dict[str, Any]]] = []
        for i, chunk in enumerate(chunks):
            store_messages.append(
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
            )

        return OperationResult(
            result={
                "predictions": len(prediction_dicts),
                "store_chunks": len(store_messages),
            },
            publish_operations=store_messages,
        )

    # ── helpers ──────────────────────────────────────────────────────────────

    def _apply_reranker_if_aligned(
        self,
        session: Session,
        prediction_dicts: list[dict[str, Any]],
        p: PredictGOTermsBatchPayload,
        emit: EmitFn,
    ) -> dict[str, Any] | None:
        """Score ``prediction_dicts`` with the configured reranker.

        The booster is skipped (never crashed) whenever any of the load-
        bearing preconditions fails:

        * ``artifact_uri`` / ``feature_schema_sha`` missing in the payload
          (coordinator bug — should not happen, but we log and continue).
        * ``protea_reranker_lab.contracts`` is not importable (production
          image without the dev dep).
        * ``live_sha != expected_sha`` (feature set diverged since
          training — silently fall back to KNN distance ordering).

        On success the ``reranker_score`` float ends up on every prediction
        dict in memory (not persisted — ``GOPrediction`` has no column for
        it yet) and the method returns per-batch summary stats for the
        ``predict_go_terms_batch.done`` event.
        """
        if not (p.reranker_artifact_uri and p.reranker_feature_schema_sha):
            emit(
                "reranker.skipped",
                None,
                {"reason": "missing_artifact_context", "reranker_model_id": p.reranker_model_id},
                "warning",
            )
            return None

        try:
            # T1.8 boundary validation: live sha computed via the canonical
            # protea_contracts implementation (single source of truth).
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
        live_sha = compute_feature_schema_sha(live_families)
        if live_sha != p.reranker_feature_schema_sha:
            emit(
                "reranker.schema_mismatch",
                None,
                {
                    "reranker_model_id": p.reranker_model_id,
                    "expected_sha": p.reranker_feature_schema_sha,
                    "live_sha": live_sha,
                    "live_families": live_families,
                },
                "error",
            )
            return {
                "applied": False,
                "skipped_reason": "schema_mismatch",
                "expected_sha": p.reranker_feature_schema_sha,
                "live_sha": live_sha,
            }

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
        unique_ids = {rec["go_term_id"] for rec in prediction_dicts if rec.get("go_term_id") is not None}
        if not unique_ids:
            return
        aspect_by_id: dict[int, str] = dict(
            session.query(GOTerm.id, GOTerm.aspect)
            .filter(GOTerm.id.in_(unique_ids))
            .all()
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
            return _derive_reference_views(cached["accessions"], cached["embeddings"])

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
            return _derive_reference_views([], np.empty((0,), dtype=np.float16))

        # Determine embedding dimension from a single row.  Rows come back as
        # pgvector HalfVector instances after the 2026-04-11 halfvec migration —
        # they expose .dimensions() and .to_numpy() but not len() / __array__.
        first_emb = base_q.limit(1).one()[1]
        dim = first_emb.dimensions()

        # Pre-allocate float16 array; fill row-by-row via yield_per so the
        # cursor fetches stream_chunk_size rows at a time, peak Python-object
        # memory stays at ~chunk_size x dim x 28 bytes ~= tens of MB, not 14 GB.
        from protea.config.tuning import get_tuning

        stream_chunk = get_tuning().operation.stream_chunk_size
        embeddings = np.empty((total, dim), dtype=np.float16)
        accessions: list[str] = []
        for i, (acc, emb) in enumerate(base_q.yield_per(stream_chunk)):
            embeddings[i] = emb.to_numpy()
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

        return _derive_reference_views(accessions, embeddings)

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
                asp: _derive_reference_views([], np.empty((0,), dtype=np.float16))
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
                    aspect_to_go_map[asp].setdefault(acc, []).append(
                        {
                            "go_term_id": go_term_id,
                            # Flyweight — see ``protea.core.annotation_intern``.
                            "qualifier": intern_string(qualifier),
                            "evidence_code": intern_string(evidence_code),
                        }
                    )

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
            aspect_embeddings_f32 = unified["embeddings_f32"][indices]
            aspect_embeddings_f32_cos = unified["embeddings_f32_cos"][indices]

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
                "embeddings_f32": aspect_embeddings_f32,
                "embeddings_f32_cos": aspect_embeddings_f32_cos,
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
    ) -> tuple[
        list[dict[str, Any]],
        dict[str, list[list[tuple[str, float]]]],
        dict[str, dict[str, list[dict[str, Any]]]],
        dict[tuple[str, str], dict[str, Any]],
    ]:
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
        # ── 1. KNN per aspect ─────────────────────────────────────────
        neighbors_by_aspect, all_unique_neighbors = self._run_knn_per_aspect(
            valid_accessions, query_embeddings, ref_data_by_aspect, p
        )

        # ── 2. Load feature-engineering inputs over the union of neighbors ──
        ref_sequences, query_sequences, ref_tax_ids, query_tax_ids = (
            self._load_feature_engineering_data(
                session, p, valid_accessions, all_unique_neighbors
            )
        )

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
        # Consensus features: per (q_acc, gtid) min and sum of distances across
        # the neighbors that voted for that term; mean is sum / vote_count.
        rr_vote_min_d_per_query: dict[str, dict[int, float]] = {}
        rr_vote_sum_d_per_query: dict[str, dict[int, float]] = {}
        # go_term_frequency and ref_annotation_density are computed per-aspect below
        all_go_term_freq: dict[int, int] = {}
        all_ref_ann_density: dict[str, int] = {}
        # Track the per-aspect go_maps so the v6 feature enricher can see the
        # full set of voting-neighbor annotations without re-querying.
        go_map_by_aspect: dict[str, dict[str, list[dict[str, Any]]]] = {}

        if compute_rr:
            for q_idx, q_acc in enumerate(valid_accessions):
                rr_vote_count_per_query[q_acc] = {}
                rr_k_position_per_query[q_acc] = {}
                rr_vote_min_d_per_query[q_acc] = {}
                rr_vote_sum_d_per_query[q_acc] = {}
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

            go_map_by_aspect[aspect] = go_map

            # Pre-compute reranker aggregates for this aspect's go_map
            if compute_rr:
                for acc, anns in go_map.items():
                    if acc not in all_ref_ann_density:
                        all_ref_ann_density[acc] = 0
                    all_ref_ann_density[acc] += len(anns)
                    for ann in anns:
                        gtid = ann["go_term_id"]
                        all_go_term_freq[gtid] = all_go_term_freq.get(gtid, 0) + 1

                # vote_count, k_position and consensus per query per aspect
                for q_idx, q_acc in enumerate(valid_accessions):
                    vc = rr_vote_count_per_query.setdefault(q_acc, {})
                    kp = rr_k_position_per_query.setdefault(q_acc, {})
                    min_d = rr_vote_min_d_per_query.setdefault(q_acc, {})
                    sum_d = rr_vote_sum_d_per_query.setdefault(q_acc, {})
                    aspect_neighbors = neighbors_by_aspect[aspect]
                    if q_idx < len(aspect_neighbors):
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
                            vc_val = rr_vote_count_per_query.get(q_acc, {}).get(go_term_id, 1)
                            pred["vote_count"] = vc_val
                            pred["k_position"] = rr_k_position_per_query.get(q_acc, {}).get(
                                go_term_id, 1
                            )
                            pred["go_term_frequency"] = all_go_term_freq.get(go_term_id, 0)
                            pred["ref_annotation_density"] = all_ref_ann_density.get(ref_acc, 0)
                            pred["neighbor_distance_std"] = rr_distance_std_per_query.get(
                                q_acc, 0.0
                            )
                            pred["neighbor_vote_fraction"] = (
                                vc_val / p.limit_per_entry if p.limit_per_entry else None
                            )
                            min_d_map = rr_vote_min_d_per_query.get(q_acc, {})
                            sum_d_map = rr_vote_sum_d_per_query.get(q_acc, {})
                            pred["neighbor_min_distance"] = min_d_map.get(go_term_id)
                            if vc_val > 0 and go_term_id in sum_d_map:
                                pred["neighbor_mean_distance"] = sum_d_map[go_term_id] / vc_val
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

        return predictions, neighbors_by_aspect, go_map_by_aspect, pair_features

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
                aspect_refs["embeddings_f32_cos"]
                if use_cos
                else aspect_refs["embeddings_f32"]
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
        from protea.config.tuning import get_tuning

        chunk_size = get_tuning().operation.annotation_chunk_size
        go_map: dict[str, list[dict[str, Any]]] = {}
        accessions_list = list(accessions)

        for i in range(0, len(accessions_list), chunk_size):
            chunk = accessions_list[i : i + chunk_size]
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
                        # Flyweight — qualifier / evidence_code take ~5-10 distinct
                        # values across millions of rows; interning collapses every
                        # duplicate to one shared string instance.
                        "qualifier": intern_string(qualifier),
                        "evidence_code": intern_string(evidence_code),
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
        # Rows return pgvector HalfVector instances (halfvec column since 2026-04-11).
        embeddings = np.array([r[1].to_list() for r in rows], dtype=np.float32)
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
    ) -> tuple[
        list[dict[str, Any]],
        list[list[tuple[str, float]]],
        dict[tuple[str, str], dict[str, Any]],
    ]:
        """Build serializable prediction dicts from KNN results.

        ``ref_data`` must have keys ``accessions``, ``embeddings``, and ``go_map``.
        If ``neighbors`` is provided (pre-computed by execute()), KNN is skipped.
        Returns ``(predictions, neighbors, pair_features)`` — the last two are
        used by ``_enrich_with_v6_features`` when ``compute_v6_features`` is on.
        ``pair_features`` is keyed by ``(query_accession, ref_accession)``.
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
        # Global (q_acc, ref_acc) keying lets callers reuse the pair features
        # for post-hoc v6 feature enrichment without recomputing taxonomy.
        pair_features: dict[tuple[str, str], dict[str, Any]] = {}

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

            # Reranker: pre-compute per-query stats
            rr_distance_std: float | None = None
            rr_vote_count: dict[int, int] = {}
            rr_k_position: dict[int, int] = {}
            rr_vote_min_d: dict[int, float] = {}
            rr_vote_sum_d: dict[int, float] = {}
            if compute_rr and top_refs:
                rr_distance_std = (
                    float(np.std([d for _, d in top_refs])) if len(top_refs) > 1 else 0.0
                )
                for k_pos, (ref_acc, dist) in enumerate(top_refs, 1):
                    for ann in go_map.get(ref_acc, []):
                        gtid = ann["go_term_id"]
                        rr_vote_count[gtid] = rr_vote_count.get(gtid, 0) + 1
                        if gtid not in rr_k_position:
                            rr_k_position[gtid] = k_pos
                        prev_min = rr_vote_min_d.get(gtid)
                        if prev_min is None or dist < prev_min:
                            rr_vote_min_d[gtid] = dist
                        rr_vote_sum_d[gtid] = rr_vote_sum_d.get(gtid, 0.0) + dist

            for ref_acc, distance in top_refs:
                pair_key = (q_acc, ref_acc)
                if pair_key not in pair_features:
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

                    pair_features[pair_key] = features

                features = pair_features[pair_key]

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
                        vc_val = rr_vote_count.get(go_term_id, 1)
                        pred["vote_count"] = vc_val
                        pred["k_position"] = rr_k_position.get(go_term_id, 1)
                        pred["go_term_frequency"] = go_term_freq.get(go_term_id, 0)
                        pred["ref_annotation_density"] = ref_ann_density.get(ref_acc, 0)
                        pred["neighbor_distance_std"] = rr_distance_std
                        pred["neighbor_vote_fraction"] = (
                            vc_val / p.limit_per_entry if p.limit_per_entry else None
                        )
                        pred["neighbor_min_distance"] = rr_vote_min_d.get(go_term_id)
                        if vc_val > 0 and go_term_id in rr_vote_sum_d:
                            pred["neighbor_mean_distance"] = rr_vote_sum_d[go_term_id] / vc_val
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

        return predictions, neighbors, pair_features

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
