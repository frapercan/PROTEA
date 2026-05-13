"""Helpers used to generate frozen re-ranker datasets in-process.

Survives as a container for the KNN, feature-engineering,
streaming-parquet, and reference-loading utilities consumed by
``ExportResearchDatasetOperation``. The module used to expose two
operations (single-pair and multi-split training) wired into the
OperationRegistry; LightGBM training itself moved to the standalone
protea-reranker-lab repo, so the operations are unregistered.
``TrainRerankerAutoOperation.execute()`` still runs the dump pipeline
(KNN + feature generation + parquet emission) for the export operation
in ``dump_only=True`` mode.

All execution is in-process; no RabbitMQ coordination.
"""

from __future__ import annotations

import gc
import logging
import shutil
import tempfile
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any, cast

if TYPE_CHECKING:
    from protea.core.parquet_export import ParquetExportContext

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import Field, field_validator
from sqlalchemy import text
from sqlalchemy.orm import Session

from protea.core._training_dump_loaders import (
    _build_skipped_outcome,
    _check_reranker_name_collisions,
    _collect_cat_gt_pairs,
    _count_embeddings_with_dim,
    _DumpRequest,
    _load_annotation_aggregations,
    _load_go_maps,
    _load_ia_weights,
    _maybe_fit_pca_state,
    _perform_dataset_dump,
    _resolve_annotation_set_ids,
    _resolve_test_eval_inputs,
    _stream_embeddings,
    _TestQueryInputs,
    _TestSequences,
    _TestSplitContext,
    _TrainSplitContext,
    _TrainSplitOutcome,
)
from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.core.disk_cache import _load_reference_pool_cached
from protea.core.domain.aspect import ASPECT_CODES as _ASPECTS
from protea.core.domain.aspect import Aspect
from protea.core.evaluation import load_evaluation_data_for_set
from protea.core.reranker import ALL_FEATURES, LABEL_COLUMN
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
from protea.infrastructure.orm.models.protein.protein import Protein
from protea.infrastructure.orm.models.sequence.sequence import Sequence

PositiveInt = Annotated[int, Field(gt=0)]

# Chunk sizes are configured via OperationTuning.annotation_chunk_size /
# stream_chunk_size and resolved at call time inside the helpers below.

_LOG = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Parameter objects for _knn_transfer_and_label
#
# The function used to take 20 keyword arguments. Two natural clusters
# (per-protein sequence/taxonomy lookups, and the streaming-parquet output
# config) are now passed as small immutable dataclasses to keep call sites
# readable.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SequenceContext:
    """Per-protein sequence and taxonomy lookups.

    All four attributes are optional; passing ``None`` disables the
    corresponding feature family (alignment / taxonomy).
    """

    query_sequences: dict[str, str] | None = None
    ref_sequences: dict[str, str] | None = None
    query_tax_ids: dict[str, int | None] | None = None
    ref_tax_ids: dict[str, int | None] | None = None


@dataclass(frozen=True)
class StreamOutput:
    """Streaming parquet output for memory-bounded dataset generation.

    When provided, ``_knn_transfer_and_label`` writes labeled rows
    directly to ``output_parquet`` in chunks of ``chunk_rows`` instead of
    accumulating the full result list in memory.
    """

    output_parquet: Path
    chunk_rows: int = 100_000


@dataclass(frozen=True)
class KnnTransferContext:
    """Bundle of KNN inputs + enrichment maps for ``_knn_transfer_and_label``.

    Groups the 12 per-call data arguments (queries, references, ontology
    maps, optional enrichment helpers) so the entry-point signature
    stays under flake8-bugbear's parameter ceiling. ``session``,
    payload ``p``, ``sequence_context``, and ``stream_output`` remain
    standalone arguments because they are configuration / IO concerns,
    not data.
    """

    valid_queries: list[str]
    query_emb: np.ndarray
    ref_by_aspect: dict[str, dict[str, Any]]
    go_id_map: dict[int, str]
    aspect_map: dict[int, str]
    gt_pairs: set[tuple[str, str]]
    query_known_gos: dict[str, set[str]] | None = None
    parent_map_str: dict[str, set[str]] | None = None
    ia_weights: dict[str, float] | None = None
    pca_state: tuple[np.ndarray, np.ndarray] | None = None
    pivot_go_ids: set[str] | frozenset[str] | None = None
    embedding_pool: np.ndarray | None = None


# ---------------------------------------------------------------------------
# Payload
# ---------------------------------------------------------------------------


# NOTE: a single-pair ``TrainRerankerPayload`` used to live here for an
# Operation that trained one LightGBM model per (old, new) snapshot pair.
# That Operation was retired when LightGBM training moved to
# protea-reranker-lab; the payload was kept around for tests until T0.6
# pruned it (no production code referenced it).


# ---------------------------------------------------------------------------
# Dataset-export pipeline helpers
#
# These were originally instance methods of the (now-removed)
# TrainRerankerOperation class. They had zero ``self`` references — the
# class wrapper was only there to share helpers with TrainRerankerAutoOperation.
# Module-level functions are clearer and let unit tests call them directly
# without instantiating an Operation.
# ---------------------------------------------------------------------------


def _load_parent_map(session: Session, snapshot_id: uuid.UUID) -> dict[str, set[str]]:
    """Return ``{child_go_id: {parent_go_id, ...}}`` for is_a + part_of edges.

    Used to drive True-Path-Rule max-propagation of predicted scores
    before computing Fmax / AUC-PR, so the internal training-time
    metric matches what cafaeval reports externally.
    """
    rows = session.execute(
        text(
            "SELECT c.go_id AS child, p.go_id AS parent "
            "FROM go_term_relationship r "
            "JOIN go_term c ON c.id = r.child_go_term_id "
            "JOIN go_term p ON p.id = r.parent_go_term_id "
            "WHERE r.ontology_snapshot_id = :snap_id "
            "AND r.relation_type IN ('is_a', 'part_of')"
        ),
        {"snap_id": snapshot_id},
    ).fetchall()
    parent_map: dict[str, set[str]] = {}
    for child, parent in rows:
        parent_map.setdefault(str(child), set()).add(str(parent))
    return parent_map


# ── bulk embedding preload (used by dump_helper) ─────────────

# Nil UUID: the dump preload spans every annotation set for a config.
_DUMP_ANN_SET_SENTINEL = uuid.UUID(int=0)


def _preload_all_embeddings(
    session: Session,
    emb_config_id: uuid.UUID,
    emit: EmitFn,
) -> tuple[np.ndarray, list[str], dict[str, int]]:
    """Load ALL embeddings once (disk-cached) and return (embs, accs, idx)."""
    from protea.config.tuning import get_tuning

    conn = session.connection()
    total, dim = _count_embeddings_with_dim(conn, emb_config_id)
    emit("dump_helper.preloading_embeddings", None, {"total": total, "dim": dim}, "info")

    def _db_loader() -> tuple[list[str], np.ndarray]:
        stream_chunk = get_tuning().operation.stream_chunk_size
        embs, accs = _stream_embeddings(conn, emb_config_id, total, dim, stream_chunk)
        return accs, embs

    accessions, embeddings = _load_reference_pool_cached(
        emb_config_id,
        _DUMP_ANN_SET_SENTINEL,
        _db_loader,
        expected_count=total,
        emit=lambda ev, fields: emit(f"dump_helper.{ev}", None, fields, "info"),
    )
    acc_to_idx = {acc: i for i, acc in enumerate(accessions)}
    emit(
        "dump_helper.embeddings_preloaded",
        None,
        {
            "total": len(accessions),
            "dim": dim,
            "memory_mb": round(embeddings.nbytes / 1024 / 1024, 1),
        },
        "info",
    )
    return embeddings, accessions, acc_to_idx


def _build_reference_from_cache(
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
    aspect_accs, aspect_go_map = _load_annotation_aggregations(
        conn, annotation_set_id, acc_to_idx
    )

    result: dict[str, dict[str, Any]] = {}
    for asp in _ASPECTS:
        indices = np.array(
            [acc_to_idx[a] for a in aspect_accs[asp]],
            dtype=np.int32,
        )
        asp_accessions = [all_accessions[i] for i in indices]
        # Store indices into the shared preload pool instead of a fancy-indexed
        # copy: with ~500k refs × 1024 × float16, the copy was ~1 GB per aspect
        # and stayed alive until the split ended. The consumer
        # (_knn_transfer_and_label) materialises a transient float32 view on
        # demand, one aspect at a time.
        result[asp] = {
            "accessions": asp_accessions,
            "indices": indices,
            "go_map": aspect_go_map[asp],
        }
        emit(
            "dump_helper.aspect_loaded",
            None,
            {"aspect": asp, "references": len(indices)},
            "info",
        )

    return result


# ── reference embeddings per aspect ───────────────────────────────────


def _load_sequences(
    session: Session,
    accessions: set[str],
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


def _load_taxonomy_ids(
    session: Session,
    accessions: set[str],
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


# ── KNN + transfer + label ────────────────────────────────────────────


def _knn_transfer_and_label(
    session: Session,
    p: TrainRerankerAutoPayload,
    ctx: KnnTransferContext,
    *,
    sequence_context: SequenceContext | None = None,
    stream_output: StreamOutput | None = None,
) -> list[dict[str, Any]] | dict[str, Any]:
    """Run per-aspect KNN, transfer GO terms, label, compute features.

    Thin wrapper that delegates to ``_KnnTransferRunner`` (T2B.5 partial
    #8 Method Object refactor; runner lives in
    ``protea.core._knn_transfer_runner`` so this module stays under the
    §3 file-LOC ceiling). Behaviour is unchanged; the runner holds the
    per-call state as attributes so the phase methods do not have to
    thread 20+ locals through their signatures.

    ``ctx.query_known_gos`` is ``{protein_accession: {go_id}}`` of
    annotations the query already carries before the prediction cutoff
    (from ``EvaluationData.known``). Used to compute query-side Anc2Vec
    coherence features (the PK-killer signal: how close is each candidate
    GO to the query's existing annotation profile).

    Streaming mode: when ``stream_output`` is given, records are written
    to disk in ``stream_output.chunk_rows`` chunks as they are generated
    (re-ordered to iterate per ``(q_acc, aspect)`` group so the
    ancestor-expansion stays local). In this mode the function returns
    ``{"parquet_path": str, "n_rows": int}`` instead of the full list.
    ``ctx.pivot_go_ids`` (orthogonal to streaming) filters records by
    go_id; useful in either mode.
    """
    # Local import keeps the runner module decoupled from helpers' import
    # graph and avoids the cycle (the runner needs the dataclass types
    # exposed by this module).
    from protea.core._knn_transfer_runner import run_knn_transfer_and_label

    return run_knn_transfer_and_label(
        session,
        p,
        ctx,
        sequence_context=sequence_context,
        stream_output=stream_output,
    )



# ---------------------------------------------------------------------------
# Auto payload
# ---------------------------------------------------------------------------


# ProteaPayload is a pydantic BaseModel, not a dataclass;
# mypy's dataclass-frozen-from-non-frozen check is a false positive.
class TrainRerankerAutoPayload(ProteaPayload, frozen=True):  # type: ignore[misc]
    """Payload for the dump_helper operation.

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

    # KNN parameters. Default to FAISS IVFFlat: numpy brute-force on 500k+
    # refs materialises a full (n_queries x n_refs) distance matrix that
    # peaks at ~10 GB per aspect. IVFFlat keeps peak memory ~2.5 GB and
    # is 5-10x faster.
    limit_per_entry: PositiveInt = 5
    distance_threshold: float | None = None
    search_backend: str = "faiss"
    metric: str = "cosine"
    faiss_index_type: str = "IVFFlat"
    faiss_nlist: int = 256
    faiss_nprobe: int = 32

    # LightGBM parameters
    num_boost_round: int = 1000
    early_stopping_rounds: int = 50
    val_fraction: float = 0.2
    neg_pos_ratio: float | None = None

    # Reranker objective: "binary" (default, classic logloss+AUC) or
    # "lambdarank" (listwise ranking loss with groups keyed by query
    # protein). LambdaRank optimizes ranking order directly and tends to
    # improve Fmax on retrieval-style tasks.
    reranker_objective: str = "binary"

    # Feature computation
    compute_alignments: bool = False
    compute_taxonomy: bool = False

    # IA weighting: path to IA TSV file (go_id\tia_value, no header).
    # When set, sample_weight = IA(go_term) during training so the model
    # focuses on informative (rare, specific) GO terms — aligned with
    # CAFA evaluation which uses IA weighting.
    ia_file: str | None = None

    # Ancestor expansion: when True, synthesize candidate records for every
    # ancestor of each leaf GO term voted by a neighbor (True Path Rule at
    # vote time). Weight of the inherited vote = IA(ancestor)/IA(leaf) when
    # an IA table is available; 1.0 otherwise. Expands the candidate set
    # and helps the reranker learn on abstract terms that never get direct
    # KNN votes but do appear in ground truth.
    expand_votes_to_ancestors: bool = False

    # Sequence-embedding PCA: when True, fit PCA(16) once on the reference
    # embedding pool, project each query, and emit 16 extra features
    # (emb_pca_query_0..15) per candidate row. Gives LightGBM a location
    # signal in PLM space beyond the scalar query<->ref distance.
    use_embedding_pca: bool = False

    # Training scope:
    #   "per_category" (default) — 3 models, one per NK/LK/PK (all aspects).
    #   "per_cell"               — up to 9 models ({cat}-{aspect}) plus the
    #                              3 per-category fallbacks. Any cell whose
    #                              positive count falls below
    #                              ``per_cell_min_positives`` is skipped and
    #                              the per-category model is used instead
    #                              (CCO-LK protection).
    training_scope: str = "per_category"
    per_cell_min_positives: PositiveInt = 200

    # Dataset dump: when ``dump_to`` is set, consolidate the generated
    # per-split / test parquet shards into ``{dump_to}/train.parquet`` +
    # ``{dump_to}/eval.parquet`` + ``manifest.json``. When ``dump_only`` is
    # True, skip the LightGBM training stage and return after the dump —
    # used by the protea-reranker-lab repo to iterate on frozen datasets.
    dump_to: str | None = None
    dump_only: bool = False

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

    @field_validator("training_scope", mode="before")
    @classmethod
    def scope_is_valid(cls, v: str) -> str:
        allowed = {"per_category", "per_cell"}
        if v not in allowed:
            raise ValueError(f"training_scope must be one of {sorted(allowed)}, got {v!r}")
        return v


_CATEGORIES = ("nk", "lk", "pk")
# Lower-cased CAFA codes — used only for model-name suffixes ("model-NK-bpo").
# Built from the canonical Aspect enum so the three encodings stay in sync.
_ASPECT_NAMES: dict[str, str] = {a.code: a.cafa.lower() for a in Aspect}


def _prepare_test_query_inputs(
    session: Session,
    ctx: _TestSplitContext,
    emit: EmitFn,
) -> _TestQueryInputs:
    """Resolve the test-pair reference set + query embeddings."""
    test_ref = _build_reference_from_cache(
        session,
        ctx.test_old_set_id,
        ctx.embedding_pool,
        ctx.all_accessions,
        ctx.acc_to_idx,
        emit,
    )
    test_accs = [a for a in ctx.test_all_queries if a in ctx.acc_to_idx]
    test_indices = np.array([ctx.acc_to_idx[a] for a in test_accs], dtype=np.int32)
    test_emb = (
        ctx.embedding_pool[test_indices].astype(np.float32)
        if len(test_indices) > 0
        else np.empty((0, ctx.embedding_pool.shape[1]), dtype=np.float32)
    )
    return _TestQueryInputs(ref_by_aspect=test_ref, valid=test_accs, emb=test_emb)


def _load_test_sequences_and_taxonomy(
    session: Session,
    payload: TrainRerankerAutoPayload,
    valid_queries: list[str],
    test_ref: dict[str, dict[str, Any]],
) -> _TestSequences:
    """Optionally fetch sequence + taxonomy lookups for the test split."""
    if not (payload.compute_alignments or payload.compute_taxonomy):
        return _TestSequences(None, None, None, None)
    test_ref_accs: set[str] = set()
    for asp in _ASPECTS:
        test_ref_accs.update(test_ref[asp]["accessions"])
    test_query_set = set(valid_queries)
    qs = _load_sequences(session, test_query_set) if payload.compute_alignments else None
    rs = _load_sequences(session, test_ref_accs) if payload.compute_alignments else None
    qt = _load_taxonomy_ids(session, test_query_set) if payload.compute_taxonomy else None
    rt = _load_taxonomy_ids(session, test_ref_accs) if payload.compute_taxonomy else None
    return _TestSequences(qs, rs, qt, rt)


def _stream_test_predictions(
    session: Session,
    ctx: _TestSplitContext,
    q_inputs: _TestQueryInputs,
    sequences: _TestSequences,
    output_path: Path,
) -> dict[str, Any]:
    """Run KNN+transfer for the test split, streaming rows to ``output_path``."""
    p = ctx.payload
    info = _knn_transfer_and_label(
        session,
        p,
        KnnTransferContext(
            valid_queries=q_inputs.valid,
            query_emb=q_inputs.emb,
            ref_by_aspect=q_inputs.ref_by_aspect,
            go_id_map=ctx.go_id_map,
            aspect_map=ctx.aspect_map,
            gt_pairs=set(),
            query_known_gos=ctx.test_eval_data.known,
            parent_map_str=ctx.parent_map if p.expand_votes_to_ancestors else None,
            ia_weights=ctx.ia_weights,
            pca_state=ctx.pca_state,
            pivot_go_ids=ctx.pivot_go_ids,
            embedding_pool=ctx.embedding_pool,
        ),
        sequence_context=SequenceContext(
            query_sequences=sequences.query_sequences,
            ref_sequences=sequences.ref_sequences,
            query_tax_ids=sequences.query_tax_ids,
            ref_tax_ids=sequences.ref_tax_ids,
        ),
        stream_output=StreamOutput(output_parquet=output_path),
    )
    return cast("dict[str, Any]", info)


def _compute_test_cat_membership(
    eval_data: Any,
    go_id_map: dict[Any, str],
    aspect_map: dict[Any, str],
) -> dict[str, set[tuple[str, str]]]:
    """Derive per-cat ``(protein, aspect)`` membership from eval data."""
    aspect_by_go_id: dict[str, str] = {
        go_id: aspect_map[term_id]
        for term_id, go_id in go_id_map.items()
        if term_id in aspect_map
    }
    membership: dict[str, set[tuple[str, str]]] = {}
    for cat in _CATEGORIES:
        gt = getattr(eval_data, cat)
        members: set[tuple[str, str]] = set()
        for protein, go_ids in gt.items():
            for go_id in go_ids:
                asp = aspect_by_go_id.get(go_id, "")
                if asp:
                    members.add((protein, asp))
        membership[cat] = members
    return membership


def _write_labeled_test_batches(
    pf: pq.ParquetFile,
    project_cols: list[str],
    membership: dict[str, set[tuple[str, str]]],
    test_cat_gt: dict[str, set[tuple[str, str]]],
    cat_paths: dict[str, Path],
) -> set[str]:
    """Stream pyarrow batches into per-cat labeled shards. Returns cats written."""
    cat_writers: dict[str, pq.ParquetWriter] = {}
    try:
        for batch in pf.iter_batches(batch_size=200_000, columns=project_cols):
            if LABEL_COLUMN in batch.schema.names:
                batch = batch.drop_columns([LABEL_COLUMN])
            accs = batch.column("protein_accession").to_pylist()
            asps = batch.column("aspect").to_pylist()
            for cat in _CATEGORIES:
                members = membership[cat]
                mask_list = [
                    (a, asp) in members for a, asp in zip(accs, asps, strict=False)
                ]
                if not any(mask_list):
                    continue
                mask_arr = pa.array(mask_list, type=pa.bool_())
                cat_batch = batch.filter(mask_arr)
                cat_accs = cat_batch.column("protein_accession").to_pylist()
                cat_gids = cat_batch.column("go_id").to_pylist()
                gt_p = test_cat_gt[cat]
                labels = pa.array(
                    [
                        1 if (a, g) in gt_p else 0
                        for a, g in zip(cat_accs, cat_gids, strict=False)
                    ],
                    type=pa.int8(),
                )
                cat_batch = cat_batch.append_column(LABEL_COLUMN, labels)
                table = pa.Table.from_batches([cat_batch])
                if cat not in cat_writers:
                    cat_writers[cat] = pq.ParquetWriter(
                        str(cat_paths[cat]), table.schema
                    )
                cat_writers[cat].write_table(table)
    finally:
        for w in cat_writers.values():
            w.close()
    return set(cat_writers.keys())


def _label_test_split_per_category(
    unlabeled_path: Path,
    ctx: _TestSplitContext,
    test_files: dict[str, Path | None],
) -> None:
    """Fan a streamed unlabeled test parquet out into per-category shards.

    Mutates ``test_files[cat]`` in place — the orchestrator owns the
    dict so the no-data branch can leave it as the all-``None`` initial
    map. The intermediate parquet is unlinked once the writers close.
    """
    pf = pq.ParquetFile(str(unlabeled_path))
    project_cols = [c for c in ctx.keep_cols if c in pf.schema_arrow.names]
    membership = _compute_test_cat_membership(
        ctx.test_eval_data, ctx.go_id_map, ctx.aspect_map
    )
    cat_paths: dict[str, Path] = {
        cat: ctx.tmp_dir / f"test_{cat}.parquet" for cat in _CATEGORIES
    }
    written = _write_labeled_test_batches(
        pf, project_cols, membership, ctx.test_cat_gt, cat_paths
    )
    for cat in written:
        test_files[cat] = cat_paths[cat]
    unlabeled_path.unlink(missing_ok=True)


def _run_test_split(
    session: Session,
    ctx: _TestSplitContext,
    emit: EmitFn,
) -> dict[str, Path | None]:
    """Run KNN once for the test pair and label per category.

    Streams predictions to an intermediate parquet so the per-cat
    labeled shards can be written without materialising ~10M records
    in memory. Returns ``{cat: Path | None}`` — ``None`` for cats with
    no rows in the test pair.
    """
    test_files: dict[str, Path | None] = {c: None for c in _CATEGORIES}
    if not ctx.test_all_queries:
        return test_files
    q_inputs = _prepare_test_query_inputs(session, ctx, emit)
    if not q_inputs.valid:
        gc.collect()
        return test_files
    sequences = _load_test_sequences_and_taxonomy(
        session, ctx.payload, q_inputs.valid, q_inputs.ref_by_aspect
    )
    session.expire_all()
    test_unlabeled_path = ctx.tmp_dir / "test_unlabeled.parquet"
    info = _stream_test_predictions(session, ctx, q_inputs, sequences, test_unlabeled_path)
    gc.collect()
    n_rows = int(info.get("n_rows", 0))
    if n_rows > 0 and test_unlabeled_path.exists():
        _label_test_split_per_category(test_unlabeled_path, ctx, test_files)
    gc.collect()
    return test_files


# ---------------------------------------------------------------------------
# Per-train-split helpers (T2B.5 partial #6)
#
# Mirror the test-split helpers above for the per-pair training loop.
# Each helper covers one phase of the per-iteration body so the
# orchestrator stays under the §3 60-LOC method ceiling. Several
# helpers reuse the test-side bundles (``_TestQueryInputs``,
# ``_TestSequences``) and pure functions (``_compute_test_cat_membership``,
# ``_load_test_sequences_and_taxonomy``) since the data shape is
# identical between sides; a follow-up renames them generically.
# ---------------------------------------------------------------------------


def _resolve_train_split_eval(
    session: Session,
    ctx: _TrainSplitContext,
    v_old: int,
    v_new: int,
) -> Any:
    """Look up the EvaluationSet for a train pair and load its delta.

    Raises ``RuntimeError`` if the eval set is missing because the dump
    pipeline assumes the deltas were materialized beforehand.
    """
    old_set_id = ctx.version_to_set[v_old]
    new_set_id = ctx.version_to_set[v_new]
    eset = (
        session.query(EvaluationSet)
        .filter_by(
            old_annotation_set_id=old_set_id,
            new_annotation_set_id=new_set_id,
        )
        .one_or_none()
    )
    if eset is None:
        raise RuntimeError(
            f"EvaluationSet missing for train pair ({v_old}->{v_new}). "
            "Materialize it via scripts/materialize_lab_intervals.py "
            "or POST /annotations/evaluation-sets/generate before retrying."
        )
    eval_data, _ = load_evaluation_data_for_set(session, eset)
    return eval_data


def _prepare_split_query_inputs(
    session: Session,
    ctx: _TrainSplitContext,
    old_set_id: uuid.UUID,
    all_query_accessions: set[str],
    emit: EmitFn,
) -> _TestQueryInputs:
    """Build references and the query-embedding slice for one split."""
    ref_by_aspect = _build_reference_from_cache(
        session,
        old_set_id,
        ctx.embedding_pool,
        ctx.all_accessions,
        ctx.acc_to_idx,
        emit,
    )
    query_accs = [a for a in all_query_accessions if a in ctx.acc_to_idx]
    query_indices = np.array(
        [ctx.acc_to_idx[a] for a in query_accs], dtype=np.int32
    )
    query_emb = (
        ctx.embedding_pool[query_indices].astype(np.float32)
        if len(query_indices) > 0
        else np.empty((0, ctx.embedding_pool.shape[1]), dtype=np.float32)
    )
    return _TestQueryInputs(
        ref_by_aspect=ref_by_aspect, valid=query_accs, emb=query_emb
    )


def _knn_and_filter_to_pivot(
    session: Session,
    ctx: _TrainSplitContext,
    q_inputs: _TestQueryInputs,
    eval_data: Any,
    sequences: _TestSequences,
) -> list[dict[str, Any]]:
    """Run KNN+transfer for one train split and filter to the pivot universe."""
    p = ctx.payload
    raw_preds = _knn_transfer_and_label(
        session,
        p,
        KnnTransferContext(
            valid_queries=q_inputs.valid,
            query_emb=q_inputs.emb,
            ref_by_aspect=q_inputs.ref_by_aspect,
            go_id_map=ctx.go_id_map,
            aspect_map=ctx.aspect_map,
            gt_pairs=set(),
            query_known_gos=eval_data.known,
            parent_map_str=ctx.parent_map if p.expand_votes_to_ancestors else None,
            ia_weights=ctx.ia_weights,
            pca_state=ctx.pca_state,
            embedding_pool=ctx.embedding_pool,
        ),
        sequence_context=SequenceContext(
            query_sequences=sequences.query_sequences,
            ref_sequences=sequences.ref_sequences,
            query_tax_ids=sequences.query_tax_ids,
            ref_tax_ids=sequences.ref_tax_ids,
        ),
    )
    return cast(
        "list[dict[str, Any]]",
        [r for r in raw_preds if r["go_id"] in ctx.pivot_go_ids],  # type: ignore[index]
    )


def _label_and_write_train_split_shards(
    unlabeled_preds: list[dict[str, Any]],
    ctx: _TrainSplitContext,
    cat_gt_pairs: dict[str, set[tuple[str, str]]],
    eval_data: Any,
    split_index: int,
    split_stats: dict[str, Any],
) -> dict[str, Path]:
    """Label rows per category, write parquet shards. Mutates ``split_stats``."""
    base_df = pd.DataFrame(unlabeled_preds, columns=ctx.keep_cols)
    membership = _compute_test_cat_membership(
        eval_data, ctx.go_id_map, ctx.aspect_map
    )
    cat_paths: dict[str, Path] = {}
    for cat in _CATEGORIES:
        members = membership[cat]
        cat_mask = np.fromiter(
            (
                (acc, asp) in members
                for acc, asp in zip(
                    base_df["protein_accession"],
                    base_df["aspect"],
                    strict=False,
                )
            ),
            count=len(base_df),
            dtype=bool,
        )
        cat_df = base_df.loc[cat_mask].copy()
        if cat_df.empty:
            split_stats[f"{cat}_positives"] = 0
            split_stats[f"{cat}_negatives"] = 0
            continue
        gt_p = cat_gt_pairs[cat]
        labels = np.fromiter(
            (
                1 if (acc, go_id) in gt_p else 0
                for acc, go_id in zip(
                    cat_df["protein_accession"],
                    cat_df["go_id"],
                    strict=False,
                )
            ),
            count=len(cat_df),
            dtype=np.int8,
        )
        cat_df[LABEL_COLUMN] = labels
        n_pos = int(labels.sum())
        split_stats[f"{cat}_positives"] = n_pos
        split_stats[f"{cat}_negatives"] = len(cat_df) - n_pos
        pq_path = ctx.tmp_dir / f"train_{cat}_split{split_index}.parquet"
        cat_df.to_parquet(pq_path, index=False)
        cat_paths[cat] = pq_path
    return cat_paths


def _emit_split_skipped(emit: EmitFn, split_num: int, reason: str) -> None:
    """Emit the audit-trail event for a skipped training split."""
    emit(
        "dump_helper.split_skipped",
        None,
        {"split": split_num, "reason": reason},
        "warning",
    )


def _run_train_split(
    session: Session,
    ctx: _TrainSplitContext,
    split_index: int,
    emit: EmitFn,
) -> _TrainSplitOutcome:
    """Run one training-split iteration end-to-end."""
    p = ctx.payload
    v_old = p.train_versions[split_index]
    v_new = p.train_versions[split_index + 1]
    emit(
        "dump_helper.split_start",
        None,
        {"split": split_index + 1, "v_old": v_old, "v_new": v_new},
        "info",
    )
    eval_data = _resolve_train_split_eval(session, ctx, v_old, v_new)
    cat_gt_pairs, all_query_accessions = _collect_cat_gt_pairs(eval_data)
    if not all_query_accessions:
        _emit_split_skipped(emit, split_index + 1, "no ground truth in any category")
        return _build_skipped_outcome(v_old, v_new, "no ground truth")
    q_inputs = _prepare_split_query_inputs(
        session, ctx, ctx.version_to_set[v_old], all_query_accessions, emit
    )
    if not q_inputs.valid:
        _emit_split_skipped(emit, split_index + 1, "no query embeddings")
        gc.collect()
        return _build_skipped_outcome(v_old, v_new, "no query embeddings")
    sequences = _load_test_sequences_and_taxonomy(
        session, p, q_inputs.valid, q_inputs.ref_by_aspect
    )
    session.expire_all()
    unlabeled_preds = _knn_and_filter_to_pivot(
        session, ctx, q_inputs, eval_data, sequences
    )
    gc.collect()
    split_stats: dict[str, Any] = {
        "v_old": v_old,
        "v_new": v_new,
        "skipped": False,
        "total_unlabeled": len(unlabeled_preds),
    }
    cat_paths = _label_and_write_train_split_shards(
        unlabeled_preds, ctx, cat_gt_pairs, eval_data, split_index, split_stats
    )
    session.expunge_all()
    gc.collect()
    emit("dump_helper.split_done", None, split_stats, "info")
    return _TrainSplitOutcome(split_files=cat_paths, stats=split_stats, skipped=False)


# ---------------------------------------------------------------------------
# Auto operation
# ---------------------------------------------------------------------------


class _DumpRunner:
    """Method Object for ``TrainRerankerAutoOperation.execute``.

    Holds per-run state as attributes so the per-phase methods do
    not need to thread 13+ variables through their signatures.
    """

    def __init__(
        self,
        session: Session,
        payload: dict[str, Any],
        emit: EmitFn,
        dump_fn: Any,
    ) -> None:
        self.session = session
        self.p = TrainRerankerAutoPayload.model_validate(payload)
        self.emit = emit
        self._dump_fn = dump_fn
        self.t0 = time.perf_counter()

    def run(self) -> OperationResult:
        self._resolve_setup()
        self._load_inputs()
        self._init_accumulators()
        self.tmp_dir = Path(tempfile.mkdtemp(prefix="protea_reranker_"))
        try:
            self._run_train_splits()
            if not any(self.split_files[c] for c in _CATEGORIES):
                raise ValueError("No training data produced from any split")
            self._run_test_split()
            del self.all_embeddings, self.all_accessions, self.acc_to_idx
            gc.collect()
            return self._dump()
        finally:
            shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def _resolve_setup(self) -> None:
        """Phase 1: validate IDs, resolve annotation sets, emit start."""
        self.emb_config_id = uuid.UUID(self.p.embedding_config_id)
        self.ontology_snapshot_id = uuid.UUID(self.p.ontology_snapshot_id)
        all_versions = sorted(set(self.p.train_versions + self.p.test_versions))
        self.version_to_set, self.version_to_native = _resolve_annotation_set_ids(
            self.session, self.p.annotation_source, all_versions
        )
        if self.session.get(EmbeddingConfig, self.emb_config_id) is None:
            raise ValueError(f"EmbeddingConfig {self.emb_config_id} not found")
        candidate_names: list[str] = [f"{self.p.name}-{cat}" for cat in _CATEGORIES]
        if self.p.training_scope == "per_cell":
            candidate_names.extend(
                f"{self.p.name}-{cat}-{_ASPECT_NAMES[asp]}"
                for cat in _CATEGORIES
                for asp in _ASPECTS
            )
        if not self.p.dump_only:
            _check_reranker_name_collisions(self.session, candidate_names)
        self.ia_weights = _load_ia_weights(self.p.ia_file)
        if self.ia_weights is not None:
            self.emit(
                "dump_helper.ia_loaded",
                None,
                {"ia_file": self.p.ia_file, "n_terms": len(self.ia_weights)},
                "info",
            )
        self.emit(
            "dump_helper.start",
            None,
            {
                "name": self.p.name,
                "train_versions": self.p.train_versions,
                "test_versions": self.p.test_versions,
                "n_pairs": len(self.p.train_versions) - 1,
                "training_scope": self.p.training_scope,
                "max_models": len(candidate_names),
                "per_cell_min_positives": int(self.p.per_cell_min_positives)
                if self.p.training_scope == "per_cell"
                else None,
                "ia_weighted": self.ia_weights is not None,
            },
            "info",
        )

    def _load_inputs(self) -> None:
        """Phase 2: GO maps, parent map, embedding preload, optional PCA fit."""
        self.go_id_map, self.aspect_map, self.pivot_go_ids = _load_go_maps(
            self.session,
            self.ontology_snapshot_id,
            set(self.version_to_native.values()),
        )
        self.parent_map = _load_parent_map(self.session, self.ontology_snapshot_id)
        self.all_embeddings, self.all_accessions, self.acc_to_idx = _preload_all_embeddings(
            self.session, self.emb_config_id, self.emit
        )
        self.pca_state = _maybe_fit_pca_state(
            self.emb_config_id, self.all_embeddings, self.p.use_embedding_pca, self.emit
        )

    def _init_accumulators(self) -> None:
        """Initialise per-split accumulator state."""
        self.keep_cols: list[str] = (
            ["protein_accession", "go_id"] + ALL_FEATURES + [LABEL_COLUMN]
        )
        self.per_split_stats: list[dict[str, Any]] = []
        self.split_files: dict[str, list[Path]] = {c: [] for c in _CATEGORIES}
        self.valid_split_versions: list[tuple[int, int]] = []

    def _train_split_context(self) -> _TrainSplitContext:
        return _TrainSplitContext(
            payload=self.p,
            version_to_set=self.version_to_set,
            embedding_pool=self.all_embeddings,
            all_accessions=self.all_accessions,
            acc_to_idx=self.acc_to_idx,
            go_id_map=self.go_id_map,
            aspect_map=self.aspect_map,
            parent_map=self.parent_map,
            ia_weights=self.ia_weights,
            pca_state=self.pca_state,
            pivot_go_ids=self.pivot_go_ids,
            keep_cols=self.keep_cols,
            tmp_dir=self.tmp_dir,
        )

    def _run_train_splits(self) -> None:
        train_ctx = self._train_split_context()
        for i in range(len(self.p.train_versions) - 1):
            outcome = _run_train_split(self.session, train_ctx, i, self.emit)
            for cat, path in outcome.split_files.items():
                self.split_files[cat].append(path)
            if not outcome.skipped:
                self.valid_split_versions.append(
                    (self.p.train_versions[i], self.p.train_versions[i + 1])
                )
            self.per_split_stats.append(outcome.stats)

    def _run_test_split(self) -> None:
        _eset, test_eval_data, self.test_old_v, self.test_new_v = _resolve_test_eval_inputs(
            self.session, self.p.train_versions, self.p.test_versions, self.version_to_set
        )
        test_old_set_id = self.version_to_set[self.test_old_v]
        self.emit(
            "dump_helper.test_knn",
            None,
            {"test_old": self.test_old_v, "test_new": self.test_new_v},
            "info",
        )
        test_cat_gt, test_all_queries = _collect_cat_gt_pairs(test_eval_data)
        self.test_files = _run_test_split(
            self.session,
            _TestSplitContext(
                payload=self.p,
                test_eval_data=test_eval_data,
                test_cat_gt=test_cat_gt,
                test_all_queries=test_all_queries,
                test_old_set_id=test_old_set_id,
                embedding_pool=self.all_embeddings,
                all_accessions=self.all_accessions,
                acc_to_idx=self.acc_to_idx,
                go_id_map=self.go_id_map,
                aspect_map=self.aspect_map,
                parent_map=self.parent_map,
                ia_weights=self.ia_weights,
                pca_state=self.pca_state,
                pivot_go_ids=self.pivot_go_ids,
                keep_cols=self.keep_cols,
                tmp_dir=self.tmp_dir,
            ),
            self.emit,
        )

    def _dump(self) -> OperationResult:
        return _perform_dataset_dump(
            _DumpRequest(
                payload=self.p,
                split_files=self.split_files,
                valid_split_versions=self.valid_split_versions,
                test_files=self.test_files,
                test_old_v=self.test_old_v,
                test_new_v=self.test_new_v,
                emb_config_id=self.emb_config_id,
                ontology_snapshot_id=self.ontology_snapshot_id,
            ),
            self._dump_fn,
            self.t0,
            self.emit,
        )


class TrainRerankerAutoOperation:
    """Automated multi-split temporal holdout re-ranker training.

    Trains **3 per-category models** (NK, LK, PK) in a single execution.
    Each model trains on all aspects combined, giving it ~3× more data
    than per-aspect models and better convergence.

    Pipeline:

    1. Resolve annotation set IDs from version numbers.
    2. Load GO maps once; optionally load IA weights for sample weighting.
    3. For each consecutive pair in ``train_versions``, compute the
       evaluation delta (all 3 categories at once), load references and
       query embeddings, run KNN + GO transfer, and label predictions
       against each category's ground truth.
    4. For each category (NK, LK, PK), concatenate the labeled data from
       all splits, train one LightGBM model with optional IA sample
       weights, evaluate on the test split, and store a ``RerankerModel``
       as ``{name}-{category}``.
    """

    # Unregistered since LightGBM training moved to protea-reranker-lab.
    # Kept as in-process helper invoked from ExportResearchDatasetOperation.
    name = "research_dataset_dump_helper"
    description = (
        "Run KNN + feature generation across multiple temporal holdout "
        "pairs and emit frozen parquets. Originally also trained "
        "LightGBM models; that path now lives in protea-reranker-lab."
    )

    def summarize_payload(self, payload: dict[str, Any], *, session: Session | None = None) -> str:
        p = payload or {}
        bits: list[str] = []
        if p.get("name"):
            bits.append(str(p["name"]))

        cfg_id_raw = p.get("embedding_config_id")
        if cfg_id_raw and session is not None:
            try:
                cfg = session.get(EmbeddingConfig, uuid.UUID(str(cfg_id_raw)))
            except Exception:
                cfg = None
            if cfg is not None:
                model_label = cfg.display_name or cfg.model_name or str(cfg.id)[:8]
                bits.append(model_label)

        train = p.get("train_versions") or []
        test = p.get("test_versions") or []
        if train:
            bits.append(f"train={train[0]}→{train[-1]} (n={len(train)})")
        if test:
            bits.append(f"test={','.join(str(v) for v in test)}")
        if p.get("num_boost_round"):
            bits.append(f"rounds={p['num_boost_round']}")
        return " · ".join(bits)

    @staticmethod
    def _dump_frozen_dataset(ctx: ParquetExportContext) -> dict[str, Any]:
        """Thin wrapper that delegates to ``parquet_export`` — kept so
        ``dump_helper`` can still dump a frozen dataset to a local
        path via ``dump_to=...``. New code should prefer the
        ``export_research_dataset`` operation which publishes via the
        configured ``ArtifactStore``.

        The caller is responsible for filling ``producer_version`` and
        ``producer_git_sha`` on the context. Pass ``store=None`` to
        skip artifact-store upload.
        """
        from protea.core.parquet_export import export_reranker_parquets

        result = export_reranker_parquets(ctx)
        # Preserve the historical return contract — callers rely on
        # ``dump_dir`` instead of ``stage_dir``.
        result["dump_dir"] = result.pop("stage_dir", str(ctx.stage_dir))
        return result

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        return _DumpRunner(session, payload, emit, self._dump_frozen_dataset).run()
