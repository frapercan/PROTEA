"""Train LightGBM re-rankers from temporal holdout pairs.

Provides two operations:

* ``train_reranker`` — single pair (old → new annotation set).
* ``train_reranker_auto`` — automated multi-split training: generates
  consecutive pairs from a list of GOA version numbers, concatenates all
  labeled data, trains one combined model, and evaluates on a held-out
  test split.

Both operations run entirely in-process (no RabbitMQ coordination).
"""

from __future__ import annotations

import gc
import shutil
import tempfile
import time
import uuid
from pathlib import Path
from typing import Annotated, Any

import numpy as np
import pandas as pd
from pydantic import Field, field_validator
from sqlalchemy import text
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.core.evaluation import compute_evaluation_data
from protea.core.feature_engineering import compute_alignment, compute_taxonomy
from protea.core.knn_search import search_knn
from protea.core.metrics import compute_cafa_metrics
from protea.core.reranker import (
    ALL_FEATURES,
    LABEL_COLUMN,
    model_to_string,
)
from protea.core.reranker import (
    predict as reranker_predict,
)
from protea.core.reranker import (
    train as reranker_train,
)
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
from protea.infrastructure.orm.models.embedding.reranker_model import RerankerModel
from protea.infrastructure.orm.models.embedding.sequence_embedding import (
    SequenceEmbedding,
)
from protea.infrastructure.orm.models.protein.protein import Protein
from protea.infrastructure.orm.models.sequence.sequence import Sequence

PositiveInt = Annotated[int, Field(gt=0)]

_ASPECTS = ("P", "F", "C")
_ANNOTATION_CHUNK_SIZE = 10_000
_STREAM_CHUNK_SIZE = 2_000


# ---------------------------------------------------------------------------
# Payload
# ---------------------------------------------------------------------------


class TrainRerankerPayload(ProteaPayload, frozen=True):
    """Payload for the train_reranker operation."""

    name: str
    old_annotation_set_id: str
    new_annotation_set_id: str
    embedding_config_id: str
    ontology_snapshot_id: str

    # Evaluation category
    category: str = "nk"

    # KNN parameters
    limit_per_entry: PositiveInt = 5
    distance_threshold: float | None = None
    search_backend: str = "numpy"
    metric: str = "cosine"
    faiss_index_type: str = "Flat"
    faiss_nlist: int = 100
    faiss_nprobe: int = 10

    # LightGBM parameters
    num_boost_round: int = 1000
    early_stopping_rounds: int = 50
    val_fraction: float = 0.2
    neg_pos_ratio: float | None = None

    # Feature computation
    compute_alignments: bool = False
    compute_taxonomy: bool = False

    # Per-aspect model (None = all aspects)
    aspect: str | None = None

    @field_validator(
        "old_annotation_set_id",
        "new_annotation_set_id",
        "embedding_config_id",
        "ontology_snapshot_id",
        "name",
        mode="before",
    )
    @classmethod
    def must_be_non_empty(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("must be a non-empty string")
        return v.strip()

    @field_validator("category", mode="before")
    @classmethod
    def valid_category(cls, v: str) -> str:
        if v not in ("nk", "lk", "pk"):
            raise ValueError("category must be nk, lk, or pk")
        return v


# ---------------------------------------------------------------------------
# Operation
# ---------------------------------------------------------------------------

_ASPECT_MAP = {"bpo": "P", "mfo": "F", "cco": "C"}


class TrainRerankerOperation:
    """Trains a LightGBM re-ranker from a single temporal holdout pair.

    Pipeline (all in-process, no RabbitMQ coordination):
    1. Validate inputs.
    2. Compute evaluation delta (old → new annotation set).
    3. Load reference embeddings (proteins annotated in old set).
    4. Load query embeddings (delta proteins with embeddings).
    5. Run per-aspect KNN + GO term transfer.
    6. Label predictions against delta.
    7. Train LightGBM.
    8. Compute baseline Fmax (distance-based) and re-ranker Fmax.
    9. Store RerankerModel in DB.
    """

    name = "train_reranker"

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = TrainRerankerPayload.model_validate(payload)
        t0 = time.perf_counter()

        old_set_id = uuid.UUID(p.old_annotation_set_id)
        new_set_id = uuid.UUID(p.new_annotation_set_id)
        emb_config_id = uuid.UUID(p.embedding_config_id)
        ontology_snapshot_id = uuid.UUID(p.ontology_snapshot_id)

        # ── 1. Validate ──────────────────────────────────────────────────
        self._validate(session, p, old_set_id, new_set_id, emb_config_id, ontology_snapshot_id)

        emit(
            "train_reranker.start",
            None,
            {
                "name": p.name,
                "old_annotation_set_id": p.old_annotation_set_id,
                "new_annotation_set_id": p.new_annotation_set_id,
                "category": p.category,
                "limit_per_entry": p.limit_per_entry,
            },
            "info",
        )

        # ── 2. Evaluation delta ──────────────────────────────────────────
        emit("train_reranker.computing_delta", None, {}, "info")
        eval_data = compute_evaluation_data(
            session, old_set_id, new_set_id, ontology_snapshot_id
        )
        ground_truth: dict[str, set[str]] = getattr(eval_data, p.category)
        gt_pairs: set[tuple[str, str]] = set()
        for protein, go_ids in ground_truth.items():
            for go_id in go_ids:
                gt_pairs.add((protein, go_id))

        emit(
            "train_reranker.delta_computed",
            None,
            {
                **eval_data.stats(),
                "gt_pairs": len(gt_pairs),
            },
            "info",
        )

        if not gt_pairs:
            raise ValueError(
                f"No ground truth found for category '{p.category}' "
                f"between annotation sets {old_set_id} and {new_set_id}"
            )

        # ── 3. GO term mappings ──────────────────────────────────────────
        go_id_map, aspect_map = self._load_go_maps(session, ontology_snapshot_id)

        # ── 4. Load reference embeddings per aspect ──────────────────────
        emit("train_reranker.loading_references", None, {}, "info")
        ref_by_aspect = self._load_reference_per_aspect(
            session, emb_config_id, old_set_id, emit
        )

        # ── 5. Load query embeddings ─────────────────────────────────────
        query_accessions = list(ground_truth.keys())
        emit(
            "train_reranker.loading_queries",
            None,
            {"delta_proteins": len(query_accessions)},
            "info",
        )
        query_emb, valid_queries = self._load_query_embeddings(
            session, query_accessions, emb_config_id
        )
        emit(
            "train_reranker.queries_loaded",
            None,
            {"with_embeddings": len(valid_queries)},
            "info",
        )

        if not valid_queries:
            raise ValueError("No delta proteins have embeddings")

        # ── 6. KNN + GO transfer + label ─────────────────────────────────
        # Load sequences / taxonomy before releasing the DB connection
        qs: dict[str, str] | None = None
        rs: dict[str, str] | None = None
        qt: dict[str, int | None] | None = None
        rt: dict[str, int | None] | None = None
        if p.compute_alignments or p.compute_taxonomy:
            all_ref_accs: set[str] = set()
            for asp in _ASPECTS:
                all_ref_accs.update(ref_by_aspect[asp]["accessions"])
            query_set = set(valid_queries)
            if p.compute_alignments:
                emit("train_reranker.loading_sequences", None, {}, "info")
                qs = self._load_sequences(session, query_set)
                rs = self._load_sequences(session, all_ref_accs)
            if p.compute_taxonomy:
                emit("train_reranker.loading_taxonomy", None, {}, "info")
                qt = self._load_taxonomy_ids(session, query_set)
                rt = self._load_taxonomy_ids(session, all_ref_accs)

        # Release DB connection before CPU-heavy phase
        session.expire_all()

        emit("train_reranker.running_knn", None, {}, "info")
        labeled_preds = self._knn_transfer_and_label(
            session,
            valid_queries,
            query_emb,
            ref_by_aspect,
            go_id_map,
            aspect_map,
            gt_pairs,
            p,
            query_sequences=qs,
            ref_sequences=rs,
            query_tax_ids=qt,
            ref_tax_ids=rt,
        )

        emit(
            "train_reranker.knn_done",
            None,
            {
                "total_predictions": len(labeled_preds),
                "positives": sum(1 for r in labeled_preds if r["label"] == 1),
                "negatives": sum(1 for r in labeled_preds if r["label"] == 0),
            },
            "info",
        )

        if not labeled_preds:
            raise ValueError("KNN produced no predictions for delta proteins")

        # ── 7. Train LightGBM ────────────────────────────────────────────
        emit("train_reranker.training", None, {}, "info")
        df = pd.DataFrame(labeled_preds)

        # Aspect filter if requested
        aspect_filter = _ASPECT_MAP.get(p.aspect) if p.aspect else None
        if aspect_filter:
            df = df[df["aspect"] == aspect_filter]

        train_result = reranker_train(
            df,
            num_boost_round=p.num_boost_round,
            early_stopping_rounds=p.early_stopping_rounds,
            val_fraction=p.val_fraction,
            neg_pos_ratio=p.neg_pos_ratio,
        )

        emit(
            "train_reranker.trained",
            None,
            train_result.metrics,
            "info",
        )

        # ── 8. Compute baseline vs re-ranker Fmax ────────────────────────
        emit("train_reranker.evaluating", None, {}, "info")
        metrics_result = self._compute_comparison_metrics(
            df, train_result, eval_data, p.category
        )

        emit(
            "train_reranker.evaluated",
            None,
            {
                "baseline_fmax": metrics_result["baseline_fmax"],
                "reranker_fmax": metrics_result["reranker_fmax"],
                "fmax_improvement": metrics_result["fmax_improvement"],
            },
            "info",
        )

        # ── 9. Store RerankerModel ────────────────────────────────────────
        full_metrics = {
            **train_result.metrics,
            **metrics_result,
            "category": p.category,
            "old_annotation_set_id": str(old_set_id),
            "new_annotation_set_id": str(new_set_id),
            "embedding_config_id": str(emb_config_id),
            "limit_per_entry": p.limit_per_entry,
            "search_backend": p.search_backend,
            "n_query_proteins": len(valid_queries),
            "n_predictions": len(labeled_preds),
            "elapsed_seconds": round(time.perf_counter() - t0, 1),
        }

        model = RerankerModel(
            name=p.name,
            prediction_set_id=None,
            evaluation_set_id=None,
            category=p.category,
            aspect=p.aspect,
            model_data=model_to_string(train_result.model),
            metrics=full_metrics,
            feature_importance=train_result.feature_importance,
        )
        session.add(model)
        session.flush()

        result = {
            "reranker_model_id": str(model.id),
            "name": p.name,
            **full_metrics,
        }
        emit("train_reranker.done", None, result, "info")
        return OperationResult(result=result)

    # ── validation ────────────────────────────────────────────────────────

    def _validate(
        self,
        session: Session,
        p: TrainRerankerPayload,
        old_set_id: uuid.UUID,
        new_set_id: uuid.UUID,
        emb_config_id: uuid.UUID,
        ontology_snapshot_id: uuid.UUID,
    ) -> None:
        if session.get(AnnotationSet, old_set_id) is None:
            raise ValueError(f"AnnotationSet {old_set_id} not found")
        if session.get(AnnotationSet, new_set_id) is None:
            raise ValueError(f"AnnotationSet {new_set_id} not found")
        if session.get(EmbeddingConfig, emb_config_id) is None:
            raise ValueError(f"EmbeddingConfig {emb_config_id} not found")
        existing = (
            session.query(RerankerModel)
            .filter(RerankerModel.name == p.name)
            .first()
        )
        if existing is not None:
            raise ValueError(f"RerankerModel with name '{p.name}' already exists")

    # ── GO term mappings ──────────────────────────────────────────────────

    def _load_go_maps(
        self, session: Session, snapshot_id: uuid.UUID
    ) -> tuple[dict[int, str], dict[int, str]]:
        """Load {go_term.id: go_id} and {go_term.id: aspect} for the snapshot."""
        rows = session.execute(
            text("SELECT id, go_id, aspect FROM go_term WHERE ontology_snapshot_id = :snap_id"),
            {"snap_id": snapshot_id},
        ).fetchall()
        id_map = {db_id: go_id for db_id, go_id, _ in rows}
        aspect_map = {db_id: aspect for db_id, _, aspect in rows if aspect}
        return id_map, aspect_map

    # ── bulk embedding preload (used by train_reranker_auto) ─────────────

    def _preload_all_embeddings(
        self,
        session: Session,
        emb_config_id: uuid.UUID,
        emit: EmitFn,
    ) -> tuple[np.ndarray, list[str], dict[str, int]]:
        """Load ALL embeddings once into memory.

        Returns (embeddings_f16, accessions, acc_to_idx).
        This avoids reloading 527K vectors from PostgreSQL on every split.
        """
        conn = session.connection()

        count_row = conn.execute(text(
            "SELECT COUNT(*), "
            "       (SELECT vector_dims(se2.embedding) "
            "          FROM sequence_embedding se2 "
            "         WHERE se2.embedding_config_id = :ecid LIMIT 1) "
            "  FROM protein p "
            "  JOIN sequence_embedding se "
            "    ON se.sequence_id = p.sequence_id "
            "   AND se.embedding_config_id = :ecid"
        ), {"ecid": emb_config_id}).one()
        total, dim = int(count_row[0]), int(count_row[1]) if count_row[1] else 960

        emit(
            "train_reranker_auto.preloading_embeddings",
            None,
            {"total": total, "dim": dim},
            "info",
        )

        embeddings = np.empty((total, dim), dtype=np.float16)
        accessions: list[str] = []
        result_proxy = conn.execute(text(
            "SELECT p.accession, se.embedding "
            "  FROM protein p "
            "  JOIN sequence_embedding se "
            "    ON se.sequence_id = p.sequence_id "
            "   AND se.embedding_config_id = :ecid"
        ), {"ecid": emb_config_id}).yield_per(_STREAM_CHUNK_SIZE)

        for i, (acc, emb_str) in enumerate(result_proxy):
            if isinstance(emb_str, str):
                emb_arr = np.fromstring(emb_str.strip("[]"), sep=",", dtype=np.float16)
            else:
                emb_arr = np.array(emb_str, dtype=np.float16)
            embeddings[i] = emb_arr
            accessions.append(acc)

        acc_to_idx = {acc: i for i, acc in enumerate(accessions)}

        emit(
            "train_reranker_auto.embeddings_preloaded",
            None,
            {"total": len(accessions), "dim": dim, "memory_mb": round(embeddings.nbytes / 1024 / 1024, 1)},
            "info",
        )

        return embeddings, accessions, acc_to_idx

    def _build_reference_from_cache(
        self,
        session: Session,
        annotation_set_id: uuid.UUID,
        all_embeddings: np.ndarray,
        all_accessions: list[str],
        acc_to_idx: dict[str, int],
        emit: EmitFn,
    ) -> dict[str, dict[str, Any]]:
        """Build per-aspect reference data using preloaded embeddings.

        Only loads annotations from the DB (fast, small rows), then filters
        the preloaded embedding matrix in memory.
        """
        conn = session.connection()
        dim = all_embeddings.shape[1] if all_embeddings.ndim == 2 else 960

        ann_rows = conn.execute(text(
            "SELECT pga.protein_accession, gt.aspect, pga.go_term_id, "
            "       pga.qualifier, pga.evidence_code "
            "  FROM protein_go_annotation pga "
            "  JOIN go_term gt ON gt.id = pga.go_term_id "
            " WHERE pga.annotation_set_id = :asid "
            "   AND gt.aspect IN ('P', 'F', 'C') "
            "   AND (pga.qualifier IS NULL OR pga.qualifier NOT LIKE '%%NOT%%')"
        ), {"asid": annotation_set_id}).yield_per(50_000)

        aspect_accs: dict[str, set[str]] = {a: set() for a in _ASPECTS}
        aspect_go_map: dict[str, dict[str, list[dict[str, Any]]]] = {a: {} for a in _ASPECTS}
        for acc, asp, go_term_id, qualifier, evidence_code in ann_rows:
            if asp in aspect_accs and acc in acc_to_idx:
                aspect_accs[asp].add(acc)
                aspect_go_map[asp].setdefault(acc, []).append({
                    "go_term_id": go_term_id,
                    "qualifier": qualifier,
                    "evidence_code": evidence_code,
                })

        result: dict[str, dict[str, Any]] = {}
        for asp in _ASPECTS:
            indices = np.array(
                [acc_to_idx[a] for a in aspect_accs[asp]],
                dtype=np.int32,
            )
            asp_accessions = [all_accessions[i] for i in indices]
            asp_embeddings = all_embeddings[indices] if len(indices) > 0 else np.empty((0, dim), dtype=np.float16)
            result[asp] = {
                "accessions": asp_accessions,
                "embeddings": asp_embeddings,
                "go_map": aspect_go_map[asp],
            }
            emit(
                "train_reranker.aspect_loaded",
                None,
                {"aspect": asp, "references": len(indices)},
                "info",
            )

        return result

    # ── reference embeddings per aspect ───────────────────────────────────

    def _load_reference_per_aspect(
        self,
        session: Session,
        emb_config_id: uuid.UUID,
        annotation_set_id: uuid.UUID,
        emit: EmitFn,
    ) -> dict[str, dict[str, Any]]:
        """Load per-aspect reference data: accessions, embeddings, annotations.

        Returns {aspect: {accessions, embeddings (float16), go_map}}.

        Uses raw SQL + server-side cursor to avoid SQLAlchemy identity map
        overhead (540k ORM rows would consume ~20GB of Python objects).
        """
        conn = session.connection()

        # Step 1: count + dimension
        count_row = conn.execute(text(
            "SELECT COUNT(*), "
            "       (SELECT vector_dims(se2.embedding) "
            "          FROM sequence_embedding se2 "
            "         WHERE se2.embedding_config_id = :ecid LIMIT 1) "
            "  FROM protein p "
            "  JOIN sequence_embedding se "
            "    ON se.sequence_id = p.sequence_id "
            "   AND se.embedding_config_id = :ecid "
            " WHERE p.accession IN ("
            "   SELECT DISTINCT protein_accession "
            "     FROM protein_go_annotation "
            "    WHERE annotation_set_id = :asid"
            " )"
        ), {"ecid": emb_config_id, "asid": annotation_set_id}).one()
        total, dim = int(count_row[0]), int(count_row[1]) if count_row[1] else 960

        if total == 0:
            return {asp: {"accessions": [], "embeddings": np.empty((0,), dtype=np.float16), "go_map": {}} for asp in _ASPECTS}

        # Step 2: stream embeddings via raw SQL — no ORM objects kept
        embeddings = np.empty((total, dim), dtype=np.float16)
        accessions: list[str] = []
        result_proxy = conn.execute(text(
            "SELECT p.accession, se.embedding "
            "  FROM protein p "
            "  JOIN sequence_embedding se "
            "    ON se.sequence_id = p.sequence_id "
            "   AND se.embedding_config_id = :ecid "
            " WHERE p.accession IN ("
            "   SELECT DISTINCT protein_accession "
            "     FROM protein_go_annotation "
            "    WHERE annotation_set_id = :asid"
            " )"
        ), {"ecid": emb_config_id, "asid": annotation_set_id}).yield_per(_STREAM_CHUNK_SIZE)

        for i, (acc, emb_str) in enumerate(result_proxy):
            # pgvector returns text like '[0.1,0.2,...]'; parse to numpy
            if isinstance(emb_str, str):
                emb_arr = np.fromstring(emb_str.strip("[]"), sep=",", dtype=np.float16)
            else:
                emb_arr = np.array(emb_str, dtype=np.float16)
            embeddings[i] = emb_arr
            accessions.append(acc)

        acc_to_idx = {acc: i for i, acc in enumerate(accessions)}

        emit(
            "train_reranker.references_loaded",
            None,
            {"total_references": len(accessions), "dim": dim},
            "info",
        )

        # Step 3: load annotations per aspect (also raw SQL)
        ann_rows = conn.execute(text(
            "SELECT pga.protein_accession, gt.aspect, pga.go_term_id, "
            "       pga.qualifier, pga.evidence_code "
            "  FROM protein_go_annotation pga "
            "  JOIN go_term gt ON gt.id = pga.go_term_id "
            " WHERE pga.annotation_set_id = :asid "
            "   AND gt.aspect IN ('P', 'F', 'C') "
            "   AND (pga.qualifier IS NULL OR pga.qualifier NOT LIKE '%%NOT%%')"
        ), {"asid": annotation_set_id}).yield_per(50_000)

        aspect_accs: dict[str, set[str]] = {a: set() for a in _ASPECTS}
        aspect_go_map: dict[str, dict[str, list[dict[str, Any]]]] = {a: {} for a in _ASPECTS}
        for acc, asp, go_term_id, qualifier, evidence_code in ann_rows:
            if asp in aspect_accs:
                aspect_accs[asp].add(acc)
                aspect_go_map[asp].setdefault(acc, []).append({
                    "go_term_id": go_term_id,
                    "qualifier": qualifier,
                    "evidence_code": evidence_code,
                })

        # Step 4: build per-aspect views
        result: dict[str, dict[str, Any]] = {}
        for asp in _ASPECTS:
            indices = np.array(
                [acc_to_idx[a] for a in aspect_accs[asp] if a in acc_to_idx],
                dtype=np.int32,
            )
            asp_accessions = [accessions[i] for i in indices]
            asp_embeddings = embeddings[indices] if len(indices) > 0 else np.empty((0, dim), dtype=np.float16)
            result[asp] = {
                "accessions": asp_accessions,
                "embeddings": asp_embeddings,
                "go_map": aspect_go_map[asp],
            }
            emit(
                "train_reranker.aspect_loaded",
                None,
                {"aspect": asp, "references": len(indices)},
                "info",
            )

        return result

    # ── query embeddings ──────────────────────────────────────────────────

    def _load_query_embeddings(
        self,
        session: Session,
        accessions: list[str],
        emb_config_id: uuid.UUID,
    ) -> tuple[np.ndarray, list[str]]:
        """Load embeddings for delta proteins. Returns (embeddings_f32, valid_accessions)."""
        all_valid: list[str] = []
        all_emb: list[list[float]] = []
        for i in range(0, len(accessions), _ANNOTATION_CHUNK_SIZE):
            chunk = accessions[i : i + _ANNOTATION_CHUNK_SIZE]
            rows = (
                session.query(Protein.accession, SequenceEmbedding.embedding)
                .join(
                    SequenceEmbedding,
                    (SequenceEmbedding.sequence_id == Protein.sequence_id)
                    & (SequenceEmbedding.embedding_config_id == emb_config_id),
                )
                .filter(Protein.accession.in_(chunk))
                .all()
            )
            for acc, emb in rows:
                all_valid.append(acc)
                all_emb.append(list(emb))

        if not all_valid:
            return np.empty((0,)), []
        return np.array(all_emb, dtype=np.float32), all_valid

    # ── Sequence / taxonomy loaders ───────────────────────────────────────

    def _load_sequences(
        self, session: Session, accessions: set[str],
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

    def _load_taxonomy_ids(
        self, session: Session, accessions: set[str],
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

    # ── KNN + transfer + label ────────────────────────────────────────────

    def _knn_transfer_and_label(
        self,
        session: Session,
        valid_queries: list[str],
        query_emb: np.ndarray,
        ref_by_aspect: dict[str, dict[str, Any]],
        go_id_map: dict[int, str],
        aspect_map: dict[int, str],
        gt_pairs: set[tuple[str, str]],
        p: TrainRerankerPayload | TrainRerankerAutoPayload,
        *,
        query_sequences: dict[str, str] | None = None,
        ref_sequences: dict[str, str] | None = None,
        query_tax_ids: dict[str, int | None] | None = None,
        ref_tax_ids: dict[str, int | None] | None = None,
    ) -> list[dict[str, Any]]:
        """Run per-aspect KNN, transfer GO terms, label, compute features."""
        # Collect neighbors per aspect
        neighbors_by_aspect: dict[str, list[list[tuple[str, float]]]] = {}
        for aspect in _ASPECTS:
            ref = ref_by_aspect[aspect]
            if not ref["accessions"]:
                neighbors_by_aspect[aspect] = [[] for _ in valid_queries]
                continue
            ref_f32 = ref["embeddings"].astype(np.float32)
            neighbors_by_aspect[aspect] = search_knn(
                query_emb,
                ref_f32,
                ref["accessions"],
                k=p.limit_per_entry,
                distance_threshold=p.distance_threshold,
                backend=p.search_backend,
                metric=p.metric,
                faiss_index_type=p.faiss_index_type,
                faiss_nlist=p.faiss_nlist,
                faiss_nprobe=p.faiss_nprobe,
            )
            del ref_f32

        # Pre-compute reranker features
        rr_distance_std: dict[str, float] = {}
        rr_vote_count: dict[str, dict[int, int]] = {}
        rr_k_position: dict[str, dict[int, int]] = {}
        go_term_freq: dict[int, int] = {}
        ref_ann_density: dict[str, int] = {}

        for q_idx, q_acc in enumerate(valid_queries):
            all_dists: list[float] = []
            rr_vote_count[q_acc] = {}
            rr_k_position[q_acc] = {}
            for aspect in _ASPECTS:
                nbs = neighbors_by_aspect[aspect]
                if q_idx < len(nbs):
                    for _, d in nbs[q_idx]:
                        all_dists.append(d)
            rr_distance_std[q_acc] = float(np.std(all_dists)) if len(all_dists) > 1 else 0.0

        for aspect in _ASPECTS:
            go_map = ref_by_aspect[aspect]["go_map"]
            # Ref annotation density
            for acc, anns in go_map.items():
                if acc not in ref_ann_density:
                    ref_ann_density[acc] = 0
                ref_ann_density[acc] += len(anns)
                for ann in anns:
                    gtid = ann["go_term_id"]
                    go_term_freq[gtid] = go_term_freq.get(gtid, 0) + 1

            # Vote count and k_position per query
            for q_idx, q_acc in enumerate(valid_queries):
                vc = rr_vote_count[q_acc]
                kp = rr_k_position[q_acc]
                nbs = neighbors_by_aspect[aspect]
                if q_idx < len(nbs):
                    for k_pos, (ref_acc, _) in enumerate(nbs[q_idx], 1):
                        for ann in go_map.get(ref_acc, []):
                            gtid = ann["go_term_id"]
                            vc[gtid] = vc.get(gtid, 0) + 1
                            if gtid not in kp:
                                kp[gtid] = k_pos

        # Pre-compute alignment and taxonomy features per unique (query, ref) pair
        pair_features: dict[tuple[str, str], dict[str, Any]] = {}
        do_alignments = p.compute_alignments and query_sequences is not None and ref_sequences is not None
        do_taxonomy = p.compute_taxonomy and query_tax_ids is not None and ref_tax_ids is not None

        if do_alignments or do_taxonomy:
            for aspect in _ASPECTS:
                nbs = neighbors_by_aspect[aspect]
                for q_idx, q_acc in enumerate(valid_queries):
                    if q_idx >= len(nbs):
                        continue
                    for ref_acc, _ in nbs[q_idx]:
                        pair_key = (q_acc, ref_acc)
                        if pair_key in pair_features:
                            continue
                        feats: dict[str, Any] = {}
                        if do_alignments:
                            q_seq = query_sequences.get(q_acc, "")
                            r_seq = ref_sequences.get(ref_acc, "")
                            if q_seq and r_seq:
                                feats.update(compute_alignment(q_seq, r_seq))
                        if do_taxonomy:
                            q_tid = query_tax_ids.get(q_acc)
                            r_tid = ref_tax_ids.get(ref_acc)
                            feats.update(compute_taxonomy(q_tid, r_tid))
                            feats["query_taxonomy_id"] = q_tid
                            feats["ref_taxonomy_id"] = r_tid
                        pair_features[pair_key] = feats

        # Build labeled predictions
        records: list[dict[str, Any]] = []
        for aspect in _ASPECTS:
            go_map = ref_by_aspect[aspect]["go_map"]
            for q_idx, q_acc in enumerate(valid_queries):
                nbs = neighbors_by_aspect[aspect]
                if q_idx >= len(nbs):
                    continue
                seen_terms: set[int] = set()
                for ref_acc, distance in nbs[q_idx]:
                    for ann in go_map.get(ref_acc, []):
                        go_term_id = ann["go_term_id"]
                        if go_term_id in seen_terms:
                            continue
                        seen_terms.add(go_term_id)

                        go_id = go_id_map.get(go_term_id)
                        if not go_id:
                            continue
                        term_aspect = aspect_map.get(go_term_id, "")
                        label = 1 if (q_acc, go_id) in gt_pairs else 0

                        pf = pair_features.get((q_acc, ref_acc), {})
                        records.append({
                            "protein_accession": q_acc,
                            "go_id": go_id,
                            "aspect": term_aspect,
                            LABEL_COLUMN: label,
                            "distance": distance,
                            "ref_protein_accession": ref_acc,
                            "qualifier": ann.get("qualifier") or "",
                            "evidence_code": ann.get("evidence_code") or "",
                            # Alignment features
                            "identity_nw": pf.get("identity_nw"),
                            "similarity_nw": pf.get("similarity_nw"),
                            "alignment_score_nw": pf.get("alignment_score_nw"),
                            "gaps_pct_nw": pf.get("gaps_pct_nw"),
                            "alignment_length_nw": pf.get("alignment_length_nw"),
                            "identity_sw": pf.get("identity_sw"),
                            "similarity_sw": pf.get("similarity_sw"),
                            "alignment_score_sw": pf.get("alignment_score_sw"),
                            "gaps_pct_sw": pf.get("gaps_pct_sw"),
                            "alignment_length_sw": pf.get("alignment_length_sw"),
                            "length_query": pf.get("length_query"),
                            "length_ref": pf.get("length_ref"),
                            # Taxonomy features
                            "taxonomic_distance": pf.get("taxonomic_distance"),
                            "taxonomic_common_ancestors": pf.get("taxonomic_common_ancestors"),
                            "taxonomic_relation": pf.get("taxonomic_relation", ""),
                            # Reranker features
                            "vote_count": rr_vote_count.get(q_acc, {}).get(go_term_id, 1),
                            "k_position": rr_k_position.get(q_acc, {}).get(go_term_id, 1),
                            "go_term_frequency": go_term_freq.get(go_term_id, 0),
                            "ref_annotation_density": ref_ann_density.get(ref_acc, 0),
                            "neighbor_distance_std": rr_distance_std.get(q_acc, 0.0),
                        })

        return records

    # ── metrics comparison ────────────────────────────────────────────────

    def _compute_comparison_metrics(
        self,
        df: pd.DataFrame,
        train_result: Any,
        eval_data: Any,
        category: str,
    ) -> dict[str, Any]:
        """Compute baseline Fmax (distance-based) and re-ranker Fmax."""
        # Baseline: score = 1 - distance (simple cosine similarity)
        baseline_scored = [
            {
                "protein_accession": row["protein_accession"],
                "go_id": row["go_id"],
                "score": max(0.0, 1.0 - float(row["distance"])) if pd.notna(row.get("distance")) else 0.0,
            }
            for _, row in df.iterrows()
        ]
        baseline_metrics = compute_cafa_metrics(baseline_scored, eval_data, category=category)

        # Re-ranker
        reranker_scores = reranker_predict(train_result.model, df)
        reranker_scored = [
            {
                "protein_accession": df.iloc[i]["protein_accession"],
                "go_id": df.iloc[i]["go_id"],
                "score": float(reranker_scores[i]),
            }
            for i in range(len(df))
        ]
        reranker_metrics = compute_cafa_metrics(reranker_scored, eval_data, category=category)

        return {
            "baseline_fmax": baseline_metrics.fmax,
            "baseline_auc_pr": baseline_metrics.auc_pr,
            "baseline_threshold": baseline_metrics.threshold_at_fmax,
            "reranker_fmax": reranker_metrics.fmax,
            "reranker_auc_pr": reranker_metrics.auc_pr,
            "reranker_threshold": reranker_metrics.threshold_at_fmax,
            "fmax_improvement": round(reranker_metrics.fmax - baseline_metrics.fmax, 4),
            "auc_pr_improvement": round(reranker_metrics.auc_pr - baseline_metrics.auc_pr, 4),
            "n_ground_truth_proteins": baseline_metrics.n_ground_truth_proteins,
        }


# ---------------------------------------------------------------------------
# Auto payload
# ---------------------------------------------------------------------------


class TrainRerankerAutoPayload(ProteaPayload, frozen=True):
    """Payload for the train_reranker_auto operation.

    Generates consecutive temporal pairs from ``train_versions``, runs KNN
    once per pair, then trains 3 per-category LightGBM models (NK, LK, PK)
    and evaluates each on the held-out test split.
    """

    name: str
    embedding_config_id: str
    ontology_snapshot_id: str

    # GOA source_version numbers for training pairs (e.g. [160,165,...,220])
    train_versions: list[int]
    # GOA source_version numbers for test evaluation (e.g. [225] or [225,229])
    test_versions: list[int]

    # Annotation source in annotation_set (default "goa")
    annotation_source: str = "goa"

    # KNN parameters
    limit_per_entry: PositiveInt = 5
    distance_threshold: float | None = None
    search_backend: str = "numpy"
    metric: str = "cosine"
    faiss_index_type: str = "Flat"
    faiss_nlist: int = 100
    faiss_nprobe: int = 10

    # LightGBM parameters
    num_boost_round: int = 1000
    early_stopping_rounds: int = 50
    val_fraction: float = 0.2
    neg_pos_ratio: float | None = None

    # Feature computation
    compute_alignments: bool = False
    compute_taxonomy: bool = False

    # IA weighting: path to IA TSV file (go_id\tia_value, no header).
    # When set, sample_weight = IA(go_term) during training so the model
    # focuses on informative (rare, specific) GO terms — aligned with
    # CAFA evaluation which uses IA weighting.
    ia_file: str | None = None

    @field_validator("embedding_config_id", "ontology_snapshot_id", "name", mode="before")
    @classmethod
    def must_be_non_empty(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("must be a non-empty string")
        return v.strip()

    @field_validator("train_versions", mode="before")
    @classmethod
    def at_least_two_train(cls, v: list[int]) -> list[int]:
        if len(v) < 2:
            raise ValueError("train_versions must have at least 2 entries to form pairs")
        return sorted(v)

    @field_validator("test_versions", mode="before")
    @classmethod
    def at_least_one_test(cls, v: list[int]) -> list[int]:
        if not v:
            raise ValueError("test_versions must have at least 1 entry")
        return sorted(v)


_CATEGORIES = ("nk", "lk", "pk")
_ASPECT_NAMES = {"P": "bpo", "F": "mfo", "C": "cco"}


# ---------------------------------------------------------------------------
# Auto operation
# ---------------------------------------------------------------------------


class TrainRerankerAutoOperation:
    """Automated multi-split temporal holdout re-ranker training.

    Trains **3 per-category models** (NK, LK, PK) in a single execution.
    Each model trains on all aspects combined, giving it ~3× more data
    than per-aspect models and better convergence.

    Pipeline:
    1. Resolve annotation_set IDs from version numbers.
    2. Load GO maps once.  Optionally load IA weights for sample weighting.
    3. For each consecutive pair in train_versions:
       a. Compute evaluation delta (all 3 categories at once).
       b. Load references + query embeddings, run KNN + GO transfer.
       c. Label predictions against each category's ground truth.
    4. For each category (NK, LK, PK):
       a. Concatenate labeled data from all splits (all aspects).
       b. Train one LightGBM model with optional IA sample weights.
       c. Evaluate on test split.
       d. Store RerankerModel as ``{name}-{category}``.
    """

    name = "train_reranker_auto"

    _single = TrainRerankerOperation()

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = TrainRerankerAutoPayload.model_validate(payload)
        t0 = time.perf_counter()

        emb_config_id = uuid.UUID(p.embedding_config_id)
        ontology_snapshot_id = uuid.UUID(p.ontology_snapshot_id)

        # ── 1. Resolve annotation set IDs ────────────────────────────────
        all_versions = sorted(set(p.train_versions + p.test_versions))
        version_to_set: dict[int, uuid.UUID] = {}
        for v in all_versions:
            aset = (
                session.query(AnnotationSet)
                .filter(
                    AnnotationSet.source == p.annotation_source,
                    AnnotationSet.source_version == str(v),
                )
                .first()
            )
            if aset is None:
                raise ValueError(
                    f"AnnotationSet not found for source='{p.annotation_source}', "
                    f"source_version='{v}'"
                )
            version_to_set[v] = aset.id

        if session.get(EmbeddingConfig, emb_config_id) is None:
            raise ValueError(f"EmbeddingConfig {emb_config_id} not found")

        # Check no name collisions for any of the 3 per-category models
        for cat in _CATEGORIES:
            model_name = f"{p.name}-{cat}"
            existing = (
                session.query(RerankerModel)
                .filter(RerankerModel.name == model_name)
                .first()
            )
            if existing is not None:
                raise ValueError(f"RerankerModel '{model_name}' already exists")

        # Load IA weights for sample weighting (optional)
        ia_weights: dict[str, float] | None = None
        if p.ia_file:
            ia_weights = {}
            with open(p.ia_file) as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    parts = line.split("\t")
                    if len(parts) >= 2:
                        ia_weights[parts[0]] = float(parts[1])
            emit(
                "train_reranker_auto.ia_loaded",
                None,
                {"ia_file": p.ia_file, "n_terms": len(ia_weights)},
                "info",
            )

        emit(
            "train_reranker_auto.start",
            None,
            {
                "name": p.name,
                "train_versions": p.train_versions,
                "test_versions": p.test_versions,
                "n_pairs": len(p.train_versions) - 1,
                "n_models": 3,
                "ia_weighted": ia_weights is not None,
            },
            "info",
        )

        # ── 2. Load GO maps ──────────────────────────────────────────────
        go_id_map, aspect_map = self._single._load_go_maps(session, ontology_snapshot_id)

        # ── 2b. Preload ALL embeddings once ─────────────────────────────
        all_embeddings, all_accessions, acc_to_idx = self._single._preload_all_embeddings(
            session, emb_config_id, emit
        )

        # ── 3. Generate training data from consecutive pairs ─────────────
        # Memory-optimised: each split writes to parquet on disk, then all
        # RAM is freed before the next split.  Training reads from disk.
        _KEEP_COLS = ["protein_accession", "go_id", "aspect"] + ALL_FEATURES + [LABEL_COLUMN]
        tmp_dir = Path(tempfile.mkdtemp(prefix="protea_reranker_"))
        per_split_stats: list[dict[str, Any]] = []
        split_files: dict[str, list[Path]] = {c: [] for c in _CATEGORIES}

        try:
            for i in range(len(p.train_versions) - 1):
                v_old = p.train_versions[i]
                v_new = p.train_versions[i + 1]
                old_set_id = version_to_set[v_old]
                new_set_id = version_to_set[v_new]

                emit(
                    "train_reranker_auto.split_start",
                    None,
                    {"split": i + 1, "v_old": v_old, "v_new": v_new},
                    "info",
                )

                # 3a. Compute delta — get all 3 categories at once
                eval_data = compute_evaluation_data(
                    session, old_set_id, new_set_id, ontology_snapshot_id
                )

                # Build gt_pairs for each category; collect union of query proteins
                cat_gt_pairs: dict[str, set[tuple[str, str]]] = {}
                all_query_accessions: set[str] = set()
                for cat in _CATEGORIES:
                    gt: dict[str, set[str]] = getattr(eval_data, cat)
                    pairs: set[tuple[str, str]] = set()
                    for protein, go_ids in gt.items():
                        for go_id in go_ids:
                            pairs.add((protein, go_id))
                    cat_gt_pairs[cat] = pairs
                    all_query_accessions.update(gt.keys())

                if not all_query_accessions:
                    emit(
                        "train_reranker_auto.split_skipped",
                        None,
                        {"split": i + 1, "reason": "no ground truth in any category"},
                        "warning",
                    )
                    per_split_stats.append({
                        "v_old": v_old, "v_new": v_new, "skipped": True,
                        "reason": "no ground truth",
                    })
                    continue

                # 3b. Build references from preloaded embeddings (only loads annotations)
                ref_by_aspect = self._single._build_reference_from_cache(
                    session, old_set_id, all_embeddings, all_accessions, acc_to_idx, emit
                )

                # 3c. Load query embeddings from preloaded cache
                query_accs = [a for a in all_query_accessions if a in acc_to_idx]
                query_indices = np.array([acc_to_idx[a] for a in query_accs], dtype=np.int32)
                query_emb = all_embeddings[query_indices].astype(np.float32) if len(query_indices) > 0 else np.empty((0, all_embeddings.shape[1]), dtype=np.float32)
                valid_queries = query_accs

                if not valid_queries:
                    emit(
                        "train_reranker_auto.split_skipped",
                        None,
                        {"split": i + 1, "reason": "no query embeddings"},
                        "warning",
                    )
                    per_split_stats.append({
                        "v_old": v_old, "v_new": v_new, "skipped": True,
                        "reason": "no query embeddings",
                    })
                    del ref_by_aspect, query_emb, valid_queries
                    gc.collect()
                    continue

                # 3d. Load sequences / taxonomy if requested
                qs: dict[str, str] | None = None
                rs: dict[str, str] | None = None
                qt: dict[str, int | None] | None = None
                rt: dict[str, int | None] | None = None
                if p.compute_alignments or p.compute_taxonomy:
                    all_ref_accs: set[str] = set()
                    for asp in _ASPECTS:
                        all_ref_accs.update(ref_by_aspect[asp]["accessions"])
                    query_set = set(valid_queries)
                    if p.compute_alignments:
                        qs = self._single._load_sequences(session, query_set)
                        rs = self._single._load_sequences(session, all_ref_accs)
                    if p.compute_taxonomy:
                        qt = self._single._load_taxonomy_ids(session, query_set)
                        rt = self._single._load_taxonomy_ids(session, all_ref_accs)

                # 3e. KNN + GO transfer (once, no labeling yet)
                session.expire_all()
                unlabeled_preds = self._single._knn_transfer_and_label(
                    session, valid_queries, query_emb, ref_by_aspect,
                    go_id_map, aspect_map,
                    set(),  # empty gt → all label=0
                    p,
                    query_sequences=qs,
                    ref_sequences=rs,
                    query_tax_ids=qt,
                    ref_tax_ids=rt,
                )

                # Free large objects immediately
                del ref_by_aspect, query_emb, valid_queries, qs, rs, qt, rt
                gc.collect()

                split_stats: dict[str, Any] = {
                    "v_old": v_old, "v_new": v_new, "skipped": False,
                    "total_unlabeled": len(unlabeled_preds),
                }

                # 3e. Build DataFrame, label per category, write to parquet.
                base_df = pd.DataFrame(unlabeled_preds, columns=_KEEP_COLS)
                del unlabeled_preds
                gc.collect()

                for cat in _CATEGORIES:
                    gt_p = cat_gt_pairs[cat]
                    labels = np.array([
                        1 if (acc, go_id) in gt_p else 0
                        for acc, go_id in zip(base_df["protein_accession"], base_df["go_id"], strict=False)
                    ], dtype=np.int8)
                    base_df[LABEL_COLUMN] = labels
                    n_pos = int(labels.sum())
                    split_stats[f"{cat}_positives"] = n_pos
                    split_stats[f"{cat}_negatives"] = len(base_df) - n_pos

                    pq_path = tmp_dir / f"train_{cat}_split{i}.parquet"
                    base_df.to_parquet(pq_path, index=False)
                    split_files[cat].append(pq_path)

                del base_df
                gc.collect()

                per_split_stats.append(split_stats)
                emit("train_reranker_auto.split_done", None, split_stats, "info")

            # Check we have data
            if not any(split_files[c] for c in _CATEGORIES):
                raise ValueError("No training data produced from any split")

            # ── 4. Test split: KNN once, label per category ──────────────
            test_old_v = p.train_versions[-1]
            test_new_v = p.test_versions[0]
            test_old_set_id = version_to_set[test_old_v]
            test_new_set_id = version_to_set[test_new_v]

            emit(
                "train_reranker_auto.test_knn",
                None,
                {"test_old": test_old_v, "test_new": test_new_v},
                "info",
            )

            test_eval_data = compute_evaluation_data(
                session, test_old_set_id, test_new_set_id, ontology_snapshot_id
            )

            # Write test data to parquet too
            test_files: dict[str, Path | None] = {c: None for c in _CATEGORIES}
            test_all_queries: set[str] = set()
            test_cat_gt: dict[str, set[tuple[str, str]]] = {}
            for cat in _CATEGORIES:
                gt: dict[str, set[str]] = getattr(test_eval_data, cat)
                pairs: set[tuple[str, str]] = set()
                for protein, go_ids in gt.items():
                    for go_id in go_ids:
                        pairs.add((protein, go_id))
                test_cat_gt[cat] = pairs
                test_all_queries.update(gt.keys())

            if test_all_queries:
                test_ref = self._single._build_reference_from_cache(
                    session, test_old_set_id, all_embeddings, all_accessions, acc_to_idx, emit
                )
                test_accs = [a for a in test_all_queries if a in acc_to_idx]
                test_indices = np.array([acc_to_idx[a] for a in test_accs], dtype=np.int32)
                test_emb = all_embeddings[test_indices].astype(np.float32) if len(test_indices) > 0 else np.empty((0, all_embeddings.shape[1]), dtype=np.float32)
                test_valid = test_accs
                if test_valid:
                    # Load sequences / taxonomy for test split
                    test_qs: dict[str, str] | None = None
                    test_rs: dict[str, str] | None = None
                    test_qt: dict[str, int | None] | None = None
                    test_rt: dict[str, int | None] | None = None
                    if p.compute_alignments or p.compute_taxonomy:
                        test_ref_accs: set[str] = set()
                        for asp in _ASPECTS:
                            test_ref_accs.update(test_ref[asp]["accessions"])
                        test_query_set = set(test_valid)
                        if p.compute_alignments:
                            test_qs = self._single._load_sequences(session, test_query_set)
                            test_rs = self._single._load_sequences(session, test_ref_accs)
                        if p.compute_taxonomy:
                            test_qt = self._single._load_taxonomy_ids(session, test_query_set)
                            test_rt = self._single._load_taxonomy_ids(session, test_ref_accs)

                    session.expire_all()
                    test_unlabeled = self._single._knn_transfer_and_label(
                        session, test_valid, test_emb, test_ref,
                        go_id_map, aspect_map, set(), p,
                        query_sequences=test_qs,
                        ref_sequences=test_rs,
                        query_tax_ids=test_qt,
                        ref_tax_ids=test_rt,
                    )
                    del test_ref, test_emb, test_valid, test_qs, test_rs, test_qt, test_rt
                    gc.collect()

                    test_base_df = pd.DataFrame(test_unlabeled, columns=_KEEP_COLS)
                    del test_unlabeled
                    gc.collect()

                    for cat in _CATEGORIES:
                        gt_p = test_cat_gt[cat]
                        labels = np.array([
                            1 if (acc, go_id) in gt_p else 0
                            for acc, go_id in zip(test_base_df["protein_accession"], test_base_df["go_id"], strict=False)
                        ], dtype=np.int8)
                        test_base_df[LABEL_COLUMN] = labels
                        pq_path = tmp_dir / f"test_{cat}.parquet"
                        test_base_df.to_parquet(pq_path, index=False)
                        test_files[cat] = pq_path

                    del test_base_df
                    gc.collect()
                else:
                    del test_ref, test_emb, test_valid
                    gc.collect()

            # ── 5. Train 3 per-category models — read from parquet ────────
            models_created: list[dict[str, Any]] = []

            for cat in _CATEGORIES:
                if not split_files[cat]:
                    continue
                model_name = f"{p.name}-{cat}"
                combined_df = pd.concat(
                    [pd.read_parquet(f) for f in split_files[cat]],
                    ignore_index=True,
                )

                if len(combined_df) == 0 or int(combined_df[LABEL_COLUMN].sum()) == 0:
                    emit(
                        "train_reranker_auto.model_skipped",
                        None,
                        {"model": model_name, "reason": "no data or no positives"},
                        "warning",
                    )
                    del combined_df
                    gc.collect()
                    continue

                # Load test data for this category
                test_df: pd.DataFrame | None = None
                if test_files.get(cat) is not None:
                    test_df = pd.read_parquet(test_files[cat])

                # Build sample weights from IA values (if available)
                sw: np.ndarray | None = None
                if ia_weights is not None:
                    sw = combined_df["go_id"].map(
                        lambda gid: ia_weights.get(gid, 1.0)
                    ).values.astype(np.float64)

                emit(
                    "train_reranker_auto.training_model",
                    None,
                    {
                        "model": model_name,
                        "samples": len(combined_df),
                        "positives": int(combined_df[LABEL_COLUMN].sum()),
                        "ia_weighted": sw is not None,
                    },
                    "info",
                )

                train_result = reranker_train(
                    combined_df,
                    num_boost_round=p.num_boost_round,
                    early_stopping_rounds=p.early_stopping_rounds,
                    val_fraction=p.val_fraction,
                    neg_pos_ratio=p.neg_pos_ratio,
                    sample_weight=sw,
                )

                # Evaluate on test split (all aspects combined)
                test_metrics: dict[str, Any] = {}
                if test_df is not None:
                    if len(test_df) > 0 and int(test_df[LABEL_COLUMN].sum()) > 0:
                        test_metrics = self._single._compute_comparison_metrics(
                            test_df, train_result, test_eval_data, cat
                        )

                full_metrics: dict[str, Any] = {
                    **train_result.metrics,
                    "category": cat,
                    "aspect": None,
                    "train_versions": p.train_versions,
                    "test_versions": p.test_versions,
                    "annotation_source": p.annotation_source,
                    "embedding_config_id": str(emb_config_id),
                    "limit_per_entry": p.limit_per_entry,
                    "search_backend": p.search_backend,
                    "n_splits": len(split_files[cat]),
                    "n_predictions": len(combined_df),
                    "per_split_stats": per_split_stats,
                    "ia_weighted": ia_weights is not None,
                }
                if test_metrics:
                    full_metrics["test_evaluation"] = {
                        "v_old": test_old_v,
                        "v_new": test_new_v,
                        **test_metrics,
                    }

                model = RerankerModel(
                    name=model_name,
                    prediction_set_id=None,
                    evaluation_set_id=None,
                    category=cat,
                    aspect=None,
                    model_data=model_to_string(train_result.model),
                    metrics=full_metrics,
                    feature_importance=train_result.feature_importance,
                )
                session.add(model)
                session.flush()

                model_summary = {
                    "reranker_model_id": str(model.id),
                    "name": model_name,
                    "category": cat,
                    "aspect": None,
                    "n_predictions": len(combined_df),
                    "positives": int(combined_df[LABEL_COLUMN].sum()),
                    **{f"test_{k}": v for k, v in test_metrics.items()},
                }
                models_created.append(model_summary)

                emit(
                    "train_reranker_auto.model_done",
                    None,
                    model_summary,
                    "info",
                )

                del combined_df, test_df, sw
                gc.collect()

        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

        elapsed = round(time.perf_counter() - t0, 1)
        result: dict[str, Any] = {
            "n_models": len(models_created),
            "models": models_created,
            "elapsed_seconds": elapsed,
        }
        emit("train_reranker_auto.done", None, result, "info")
        return OperationResult(result=result)
