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
import logging
import shutil
import tempfile
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import Field, field_validator
from sqlalchemy import text
from sqlalchemy.orm import Session

from protea.core.anc2vec_embeddings import Anc2VecIndex
from protea.core.anc2vec_embeddings import get_index as get_anc2vec_index
from protea.core.annotation_intern import intern_string
from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.core.domain.aspect import ASPECT_CODES as _ASPECTS
from protea.core.domain.aspect import Aspect
from protea.core.evaluation import load_evaluation_data_for_set
from protea.core.feature_engineering import compute_alignment, compute_taxonomy
from protea.core.knn_search import search_knn
from protea.core.reranker import (
    ALL_FEATURES,
    EMBEDDING_PCA_DIM,
    LABEL_COLUMN,
    fit_embedding_pca,
)
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
from protea.infrastructure.orm.models.embedding.reranker_model import RerankerModel
from protea.infrastructure.orm.models.protein.protein import Protein
from protea.infrastructure.orm.models.sequence.sequence import Sequence

PositiveInt = Annotated[int, Field(gt=0)]

_ANNOTATION_CHUNK_SIZE = 10_000
_STREAM_CHUNK_SIZE = 2_000

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

    # KNN parameters — default to FAISS IVFFlat. Numpy brute-force on 500k+ refs
    # materialises a full (n_queries × n_refs) distance matrix that peaks at
    # ~10 GB per aspect. IVFFlat keeps peak memory ~2.5 GB and is 5-10× faster.
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

    # Feature computation
    compute_alignments: bool = False
    compute_taxonomy: bool = False

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
    # signal in PLM space beyond the scalar query↔ref distance.
    use_embedding_pca: bool = False

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



# ---------------------------------------------------------------------------
# Dataset-export pipeline helpers
#
# These were originally instance methods of the (now-removed)
# TrainRerankerOperation class. They had zero ``self`` references — the
# class wrapper was only there to share helpers with TrainRerankerAutoOperation.
# Module-level functions are clearer and let unit tests call them directly
# without instantiating an Operation.
# ---------------------------------------------------------------------------


def _load_parent_map(
    session: Session, snapshot_id: uuid.UUID
) -> dict[str, set[str]]:
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

# ── bulk embedding preload (used by train_reranker_auto) ─────────────


def _preload_all_embeddings(
    session: Session,
    emb_config_id: uuid.UUID,
    emit: EmitFn,
) -> tuple[np.ndarray, list[str], dict[str, int]]:
    """Load ALL embeddings once into memory.

    Returns (embeddings_f16, accessions, acc_to_idx).
    This avoids reloading 527K vectors from PostgreSQL on every split.
    """
    conn = session.connection()

    count_row = conn.execute(
        text(
            "SELECT COUNT(*), "
            "       (SELECT vector_dims(se2.embedding) "
            "          FROM sequence_embedding se2 "
            "         WHERE se2.embedding_config_id = :ecid LIMIT 1) "
            "  FROM protein p "
            "  JOIN sequence_embedding se "
            "    ON se.sequence_id = p.sequence_id "
            "   AND se.embedding_config_id = :ecid"
        ),
        {"ecid": emb_config_id},
    ).one()
    total, dim = int(count_row[0]), int(count_row[1]) if count_row[1] else 960

    emit(
        "train_reranker_auto.preloading_embeddings",
        None,
        {"total": total, "dim": dim},
        "info",
    )

    embeddings = np.empty((total, dim), dtype=np.float16)
    accessions: list[str] = []
    result_proxy = conn.execute(
        text(
            "SELECT p.accession, se.embedding "
            "  FROM protein p "
            "  JOIN sequence_embedding se "
            "    ON se.sequence_id = p.sequence_id "
            "   AND se.embedding_config_id = :ecid"
        ),
        {"ecid": emb_config_id},
    ).yield_per(_STREAM_CHUNK_SIZE)

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

    ann_rows = conn.execute(
        text(
            "SELECT pga.protein_accession, gt.aspect, pga.go_term_id, "
            "       pga.qualifier, pga.evidence_code "
            "  FROM protein_go_annotation pga "
            "  JOIN go_term gt ON gt.id = pga.go_term_id "
            " WHERE pga.annotation_set_id = :asid "
            "   AND gt.aspect IN ('P', 'F', 'C') "
            "   AND (pga.qualifier IS NULL OR pga.qualifier NOT LIKE '%%NOT%%')"
        ),
        {"asid": annotation_set_id},
    ).yield_per(50_000)

    aspect_accs: dict[str, set[str]] = {a: set() for a in _ASPECTS}
    aspect_go_map: dict[str, dict[str, list[dict[str, Any]]]] = {a: {} for a in _ASPECTS}
    for acc, asp, go_term_id, qualifier, evidence_code in ann_rows:
        if asp in aspect_accs and acc in acc_to_idx:
            aspect_accs[asp].add(acc)
            aspect_go_map[asp].setdefault(acc, []).append(
                {
                    "go_term_id": go_term_id,
                    # Flyweight — see ``protea.core.annotation_intern``.
                    "qualifier": intern_string(qualifier),
                    "evidence_code": intern_string(evidence_code),
                }
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
            "train_reranker.aspect_loaded",
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
    session: Session,
    accessions: set[str],
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
    session: Session,
    valid_queries: list[str],
    query_emb: np.ndarray,
    ref_by_aspect: dict[str, dict[str, Any]],
    go_id_map: dict[int, str],
    aspect_map: dict[int, str],
    gt_pairs: set[tuple[str, str]],
    p: TrainRerankerPayload | TrainRerankerAutoPayload,
    *,
    sequence_context: SequenceContext | None = None,
    query_known_gos: dict[str, set[str]] | None = None,
    parent_map_str: dict[str, set[str]] | None = None,
    ia_weights: dict[str, float] | None = None,
    pca_state: tuple[np.ndarray, np.ndarray] | None = None,
    pivot_go_ids: set[str] | frozenset[str] | None = None,
    stream_output: StreamOutput | None = None,
    embedding_pool: np.ndarray | None = None,
) -> list[dict[str, Any]] | dict[str, Any]:
    """Run per-aspect KNN, transfer GO terms, label, compute features.

    ``query_known_gos`` is ``{protein_accession: {go_id}}`` of annotations
    the query already carries before the prediction cutoff (from
    ``EvaluationData.known``). Used to compute query-side Anc2Vec coherence
    features — the PK-killer signal: how close is each candidate GO to the
    query's existing annotation profile?

    Streaming mode: when ``stream_output`` is given, records are
    written to disk in ``stream_output.chunk_rows`` chunks as they
    are generated (re-ordered to iterate per ``(q_acc, aspect)``
    group so the ancestor-expansion stays local). In this mode the
    function returns ``{"parquet_path": str, "n_rows": int}`` instead
    of the full list. ``pivot_go_ids`` (orthogonal to streaming)
    filters records by go_id; useful in either mode.
    """
    # Unpack the parameter objects so the body keeps using the original
    # local names — body untouched, only the call surface shrinks.
    ctx = sequence_context or SequenceContext()
    query_sequences = ctx.query_sequences
    ref_sequences = ctx.ref_sequences
    query_tax_ids = ctx.query_tax_ids
    ref_tax_ids = ctx.ref_tax_ids

    if stream_output is not None:
        output_parquet: Path | None = stream_output.output_parquet
        chunk_rows: int = stream_output.chunk_rows
    else:
        output_parquet = None
        chunk_rows = 100_000

    # Collect neighbors per aspect
    neighbors_by_aspect: dict[str, list[list[tuple[str, float]]]] = {}
    for aspect in _ASPECTS:
        ref = ref_by_aspect[aspect]
        if not ref["accessions"]:
            neighbors_by_aspect[aspect] = [[] for _ in valid_queries]
            continue
        # Two source shapes are supported:
        #   - ``ref["indices"]`` + ``embedding_pool`` (preload-aware path);
        #     no per-aspect float16 copy is held in the dict.
        #   - ``ref["embeddings"]`` (legacy path used by the single-version
        #     train_reranker that loads embeddings per aspect from SQL).
        indices = ref.get("indices")
        if indices is not None and embedding_pool is not None:
            ref_f32 = embedding_pool[indices].astype(np.float32)
        else:
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
        # Embeddings/indices are no longer needed past this point — only
        # ``go_map`` and ``accessions`` are read downstream. Releasing
        # ~940 MB per aspect (f16, ~500k × dim) keeps RSS flat before
        # the record-building phase.
        ref["embeddings"] = None
        ref["indices"] = None
    gc.collect()

    # Pre-compute reranker features
    rr_distance_std: dict[str, float] = {}
    rr_vote_count: dict[str, dict[int, int]] = {}
    rr_k_position: dict[str, dict[int, int]] = {}
    # Consensus features: collect the distances of neighbors that voted
    # for each candidate term, per query. We compute min/mean on the fly
    # and never store the full list to keep memory bounded.
    rr_vote_min_d: dict[str, dict[int, float]] = {}
    rr_vote_sum_d: dict[str, dict[int, float]] = {}
    go_term_freq: dict[int, int] = {}
    ref_ann_density: dict[str, int] = {}
    k_limit = int(p.limit_per_entry) or 1

    for q_idx, q_acc in enumerate(valid_queries):
        all_dists: list[float] = []
        rr_vote_count[q_acc] = {}
        rr_k_position[q_acc] = {}
        rr_vote_min_d[q_acc] = {}
        rr_vote_sum_d[q_acc] = {}
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

        # Vote count, k_position and voting-distance stats per query
        for q_idx, q_acc in enumerate(valid_queries):
            vc = rr_vote_count[q_acc]
            kp = rr_k_position[q_acc]
            vmin = rr_vote_min_d[q_acc]
            vsum = rr_vote_sum_d[q_acc]
            nbs = neighbors_by_aspect[aspect]
            if q_idx < len(nbs):
                for k_pos, (ref_acc, nb_d) in enumerate(nbs[q_idx], 1):
                    for ann in go_map.get(ref_acc, []):
                        gtid = ann["go_term_id"]
                        vc[gtid] = vc.get(gtid, 0) + 1
                        if gtid not in kp:
                            kp[gtid] = k_pos
                        if gtid not in vmin or nb_d < vmin[gtid]:
                            vmin[gtid] = float(nb_d)
                        vsum[gtid] = vsum.get(gtid, 0.0) + float(nb_d)

    # Pre-compute alignment and taxonomy features per unique (query, ref) pair.
    # Nested by q_acc so per-query state can be popped atomically once the
    # record-building loop is done with each query — keeps RSS bounded in
    # the test split (30k queries × 5 nbrs × 3 aspects ≈ 450k entries).
    pair_features: dict[str, dict[str, dict[str, Any]]] = {}
    do_alignments = (
        p.compute_alignments and query_sequences is not None and ref_sequences is not None
    )
    do_taxonomy = p.compute_taxonomy and query_tax_ids is not None and ref_tax_ids is not None

    if do_alignments or do_taxonomy:
        # Heartbeat: this loop can run for hours on large splits with
        # alignments enabled. Without periodic logging a stall is
        # indistinguishable from slow progress. Logs every 30s with
        # pairs-computed / elapsed / current aspect so operators can
        # spot stalls or plateaus in the worker log.
        _hb_t0 = time.perf_counter()
        _hb_last = _hb_t0
        _hb_interval = 30.0
        _hb_n = 0
        for aspect in _ASPECTS:
            nbs = neighbors_by_aspect[aspect]
            for q_idx, q_acc in enumerate(valid_queries):
                if q_idx >= len(nbs):
                    continue
                q_pairs = pair_features.setdefault(q_acc, {})
                for ref_acc, _ in nbs[q_idx]:
                    if ref_acc in q_pairs:
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
                    q_pairs[ref_acc] = feats
                    _hb_n += 1
                    now = time.perf_counter()
                    if now - _hb_last >= _hb_interval:
                        _LOG.info(
                            "pair_features heartbeat: pairs=%d aspect=%s q_idx=%d/%d elapsed=%.1fs rate=%.0f/s",
                            _hb_n, aspect, q_idx, len(valid_queries),
                            now - _hb_t0, _hb_n / max(1e-9, now - _hb_t0),
                        )
                        _hb_last = now

    # Taxonomic-consensus features: mirror the (neighbor_vote_fraction,
    # neighbor_min_distance, neighbor_mean_distance) design but aggregate
    # taxonomic signal across the subset of neighbors that voted for each
    # candidate term. Requires ``compute_taxonomy=True`` — otherwise
    # pair_features has no taxonomy keys and all three features stay NaN.
    #
    #   tax_voters_same_frac              — fraction of voters in same organism
    #   tax_voters_close_frac             — fraction in "close relatives" bucket
    #   tax_voters_mean_common_ancestors  — mean lineage overlap across voters
    _CLOSE_RELATIONS = frozenset({
        "same", "ancestor", "descendant", "child", "parent", "close",
    })
    tax_same_cnt: dict[str, dict[int, int]] = {}
    tax_close_cnt: dict[str, dict[int, int]] = {}
    tax_ca_sum: dict[str, dict[int, float]] = {}
    tax_ca_n: dict[str, dict[int, int]] = {}
    if do_taxonomy:
        for aspect in _ASPECTS:
            go_map = ref_by_aspect[aspect]["go_map"]
            nbs_all = neighbors_by_aspect[aspect]
            for q_idx, q_acc in enumerate(valid_queries):
                if q_idx >= len(nbs_all):
                    continue
                same_d = tax_same_cnt.setdefault(q_acc, {})
                close_d = tax_close_cnt.setdefault(q_acc, {})
                sum_d = tax_ca_sum.setdefault(q_acc, {})
                n_d = tax_ca_n.setdefault(q_acc, {})
                q_pairs = pair_features.get(q_acc, {})
                for ref_acc, _ in nbs_all[q_idx]:
                    pf = q_pairs.get(ref_acc, {})
                    rel = pf.get("taxonomic_relation") or ""
                    ca = pf.get("taxonomic_common_ancestors")
                    is_same = rel == "same"
                    is_close = rel in _CLOSE_RELATIONS
                    for ann in go_map.get(ref_acc, []):
                        gtid = ann["go_term_id"]
                        if is_same:
                            same_d[gtid] = same_d.get(gtid, 0) + 1
                        if is_close:
                            close_d[gtid] = close_d.get(gtid, 0) + 1
                        if isinstance(ca, int | float) and ca is not None:
                            sum_d[gtid] = sum_d.get(gtid, 0.0) + float(ca)
                            n_d[gtid] = n_d.get(gtid, 0) + 1

    # Anc2Vec semantic-coherence features: for each (query, aspect), project
    # all GO terms annotated to the query's KNN neighbors into a unit-normed
    # 200-dim space, then score each candidate against that neighbor set.
    #
    #   anc2vec_neighbor_cos    — cos(cand, mean(neighbor_gos)) centroid fit
    #   anc2vec_neighbor_maxcos — max cos(cand, g) over neighbor GOs
    #   anc2vec_has_emb         — 1 if candidate has a non-zero Anc2Vec vector
    anc_idx: Anc2VecIndex = get_anc2vec_index()
    all_go_id_strs: set[str] = set()
    for aspect in _ASPECTS:
        for anns in ref_by_aspect[aspect]["go_map"].values():
            for ann in anns:
                gid_str = go_id_map.get(ann["go_term_id"])
                if gid_str:
                    all_go_id_strs.add(gid_str)
    # Query-known GOs join the projection pool so we can embed them too.
    if query_known_gos:
        for _gos in query_known_gos.values():
            all_go_id_strs.update(_gos)
    all_go_id_list = sorted(all_go_id_strs)
    idx_of_go: dict[str, int] = {g: i for i, g in enumerate(all_go_id_list)}
    all_emb = anc_idx.batch(all_go_id_list)
    raw_norms = np.linalg.norm(all_emb, axis=1)
    has_emb_mask = raw_norms > 0.0
    safe_norms = np.where(has_emb_mask, raw_norms, 1.0)[:, None]
    all_norm = (all_emb / safe_norms).astype(np.float32)
    all_norm[~has_emb_mask] = 0.0

    neighbor_info: dict[tuple[str, str], tuple[np.ndarray | None, np.ndarray | None]] = {}
    for aspect in _ASPECTS:
        go_map = ref_by_aspect[aspect]["go_map"]
        nbs_all = neighbors_by_aspect[aspect]
        for q_idx, q_acc in enumerate(valid_queries):
            if q_idx >= len(nbs_all):
                continue
            rows: list[int] = []
            seen: set[str] = set()
            for ref_acc, _ in nbs_all[q_idx]:
                for ann in go_map.get(ref_acc, []):
                    gid_str = go_id_map.get(ann["go_term_id"])
                    if not gid_str or gid_str in seen:
                        continue
                    seen.add(gid_str)
                    i = idx_of_go.get(gid_str)
                    if i is not None and has_emb_mask[i]:
                        rows.append(i)
            if not rows:
                neighbor_info[(q_acc, aspect)] = (None, None)
                continue
            nmat = all_norm[rows]
            centroid = nmat.mean(axis=0)
            cn = float(np.linalg.norm(centroid))
            centroid_unit = (centroid / cn).astype(np.float32) if cn > 0.0 else None
            neighbor_info[(q_acc, aspect)] = (centroid_unit, nmat)

    # Query-side Anc2Vec: centroid + embedding matrix of the query's own
    # known GO set (all aspects pooled). For NK queries this stays empty →
    # features fall back to NaN. For LK/PK queries this captures the
    # "annotation profile" the candidate should stay close to — the
    # canonical PK-discriminator signal.
    query_known_info: dict[str, tuple[np.ndarray | None, np.ndarray | None, int]] = {}
    if query_known_gos:
        for q_acc in valid_queries:
            known = query_known_gos.get(q_acc, set())
            if not known:
                query_known_info[q_acc] = (None, None, 0)
                continue
            rows = [
                idx_of_go[g]
                for g in known
                if g in idx_of_go and has_emb_mask[idx_of_go[g]]
            ]
            if not rows:
                query_known_info[q_acc] = (None, None, len(known))
                continue
            kmat = all_norm[rows]
            centroid = kmat.mean(axis=0)
            cn = float(np.linalg.norm(centroid))
            centroid_unit = (centroid / cn).astype(np.float32) if cn > 0.0 else None
            query_known_info[q_acc] = (centroid_unit, kmat, len(known))

    # Sequence-embedding PCA: per-query projection onto the precomputed
    # principal components of the reference embedding pool. Emits 16
    # features (emb_pca_query_0..15) at record-creation time. NaN when
    # pca_state is None → LightGBM routes as missing.
    pca_query_proj: np.ndarray | None = None
    if pca_state is not None and query_emb.size:
        pca_mean, pca_components = pca_state
        pca_query_proj = (
            (query_emb.astype(np.float32) - pca_mean) @ pca_components.T
        ).astype(np.float32)

    # Build labeled predictions
    #
    # This block is structured per ``(q_acc, aspect)`` group so that
    # ancestor expansion is local and intermediate state never holds
    # more than one group worth of records. In list mode (the legacy
    # default) records accumulate in memory. In streaming mode
    # (``output_parquet`` given) each group flushes through a
    # pyarrow ParquetWriter in ``chunk_rows`` batches — the caller
    # never sees the ~10M-row list so the test split avoids OOM.
    expand = getattr(p, "expand_votes_to_ancestors", False) and bool(parent_map_str)
    k_limit_f = float(k_limit)
    ancestor_closure: dict[str, set[str]] = {}

    def _ancestors(gid: str) -> set[str]:
        cached = ancestor_closure.get(gid)
        if cached is not None:
            return cached
        seen: set[str] = set()
        stack = [gid]
        while stack:
            node = stack.pop()
            for parent in (parent_map_str or {}).get(node, ()):
                if parent not in seen:
                    seen.add(parent)
                    stack.append(parent)
        ancestor_closure[gid] = seen
        return seen

    def _ia_weight(anc_gid: str, leaf_gid: str) -> float:
        if not ia_weights:
            return 1.0
        anc_w = float(ia_weights.get(anc_gid, 0.0))
        leaf_w = float(ia_weights.get(leaf_gid, 0.0))
        if leaf_w <= 0.0:
            return 1.0
        return anc_w / leaf_w

    streaming = output_parquet is not None
    records: list[dict[str, Any]] = []
    buffer: list[dict[str, Any]] = []
    writer: pq.ParquetWriter | None = None
    n_rows = 0
    _nan_pca = [float("nan")] * EMBEDDING_PCA_DIM

    def _flush() -> None:
        nonlocal writer, n_rows
        if not buffer:
            return
        table = pa.Table.from_pylist(buffer)
        if writer is None:
            writer = pq.ParquetWriter(str(output_parquet), table.schema)
        writer.write_table(table)
        n_rows += len(buffer)
        buffer.clear()

    def _emit(rec: dict[str, Any]) -> None:
        if pivot_go_ids is not None and rec["go_id"] not in pivot_go_ids:
            return
        if streaming:
            buffer.append(rec)
            if len(buffer) >= chunk_rows:
                _flush()
        else:
            records.append(rec)

    for q_idx, q_acc in enumerate(valid_queries):
        q_pca_row = (
            pca_query_proj[q_idx].tolist()
            if pca_query_proj is not None
            else _nan_pca
        )
        q_known_cent, q_known_mat, q_known_n = query_known_info.get(
            q_acc, (None, None, 0)
        )
        q_pairs_features = pair_features.get(q_acc, {})
        for aspect in _ASPECTS:
            go_map = ref_by_aspect[aspect]["go_map"]
            nbs = neighbors_by_aspect[aspect]
            if q_idx >= len(nbs):
                continue
            centroid_unit, nmat = neighbor_info.get(
                (q_acc, aspect), (None, None)
            )

            leaf_by_gid: dict[str, dict[str, Any]] = {}
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

                    pf = q_pairs_features.get(ref_acc, {})

                    cand_i = idx_of_go.get(go_id, -1)
                    if cand_i >= 0 and has_emb_mask[cand_i]:
                        cand_vec = all_norm[cand_i]
                        anc_cos = (
                            float(cand_vec @ centroid_unit)
                            if centroid_unit is not None
                            else float("nan")
                        )
                        anc_maxcos = (
                            float((nmat @ cand_vec).max())
                            if nmat is not None
                            else float("nan")
                        )
                        anc_has = 1.0
                        anc_q_cos = (
                            float(cand_vec @ q_known_cent)
                            if q_known_cent is not None
                            else float("nan")
                        )
                        anc_q_maxcos = (
                            float((q_known_mat @ cand_vec).max())
                            if q_known_mat is not None
                            else float("nan")
                        )
                    else:
                        anc_cos = float("nan")
                        anc_maxcos = float("nan")
                        anc_has = 0.0
                        anc_q_cos = float("nan")
                        anc_q_maxcos = float("nan")

                    rec = {
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
                        # Consensus features (this candidate term only)
                        "neighbor_vote_fraction": (
                            rr_vote_count.get(q_acc, {}).get(go_term_id, 1) / k_limit
                        ),
                        "neighbor_min_distance": rr_vote_min_d.get(q_acc, {}).get(
                            go_term_id, float(distance)
                        ),
                        "neighbor_mean_distance": (
                            rr_vote_sum_d.get(q_acc, {}).get(go_term_id, float(distance))
                            / max(1, rr_vote_count.get(q_acc, {}).get(go_term_id, 1))
                        ),
                        # Anc2Vec semantic-coherence features
                        "anc2vec_neighbor_cos": anc_cos,
                        "anc2vec_neighbor_maxcos": anc_maxcos,
                        "anc2vec_has_emb": anc_has,
                        # Query-side Anc2Vec (PK-killer): cand vs query's
                        # pre-cutoff annotation profile. NaN for NK queries.
                        "anc2vec_query_known_cos": anc_q_cos,
                        "anc2vec_query_known_maxcos": anc_q_maxcos,
                        "anc2vec_query_known_count": float(q_known_n),
                        # Taxonomic consensus across voting neighbors
                        "tax_voters_same_frac": (
                            tax_same_cnt.get(q_acc, {}).get(go_term_id, 0)
                            / max(1, rr_vote_count.get(q_acc, {}).get(go_term_id, 1))
                            if do_taxonomy
                            else float("nan")
                        ),
                        "tax_voters_close_frac": (
                            tax_close_cnt.get(q_acc, {}).get(go_term_id, 0)
                            / max(1, rr_vote_count.get(q_acc, {}).get(go_term_id, 1))
                            if do_taxonomy
                            else float("nan")
                        ),
                        "tax_voters_mean_common_ancestors": (
                            (
                                tax_ca_sum.get(q_acc, {}).get(go_term_id, 0.0)
                                / max(1, tax_ca_n.get(q_acc, {}).get(go_term_id, 1))
                            )
                            if (
                                do_taxonomy
                                and tax_ca_n.get(q_acc, {}).get(go_term_id, 0) > 0
                            )
                            else float("nan")
                        ),
                        **{
                            f"emb_pca_query_{i}": q_pca_row[i]
                            for i in range(EMBEDDING_PCA_DIM)
                        },
                    }
                    leaf_by_gid[go_id] = rec

            # Ancestor expansion for this (q_acc, aspect) group only.
            # Synthesize records for every ancestor of each predicted
            # leaf so the candidate set covers the full upward closure.
            # Neighbor-vote-fraction is additively weighted by
            # IA(ancestor)/IA(leaf) when an IA table is provided
            # (else weight = 1). The closest leaf donates its per-pair
            # features (distance, alignment, taxonomy) to the
            # synthesized ancestor record.
            synth: dict[str, dict[str, Any]] = {}
            if expand:
                for leaf_gid, leaf_rec in list(leaf_by_gid.items()):
                    leaf_d = float(leaf_rec.get("distance", 1.0))
                    for anc in _ancestors(leaf_gid):
                        w = _ia_weight(anc, leaf_gid)
                        if anc in leaf_by_gid:
                            leaf_anc = leaf_by_gid[anc]
                            leaf_anc["neighbor_vote_fraction"] = min(
                                1.0,
                                float(leaf_anc.get("neighbor_vote_fraction", 0.0))
                                + w / k_limit_f,
                            )
                            lmd = float(leaf_rec.get("neighbor_min_distance", leaf_d))
                            cur_md = float(leaf_anc.get("neighbor_min_distance", leaf_d))
                            if lmd < cur_md:
                                leaf_anc["neighbor_min_distance"] = lmd
                            continue
                        entry = synth.get(anc)
                        if entry is None or leaf_d < float(entry["distance"]):
                            base = dict(leaf_rec)
                            base["go_id"] = anc
                            base[LABEL_COLUMN] = 1 if (q_acc, anc) in gt_pairs else 0
                            prior_frac = (
                                float(entry["neighbor_vote_fraction"])
                                if entry is not None
                                else 0.0
                            )
                            base["neighbor_vote_fraction"] = min(
                                1.0, prior_frac + w / k_limit_f
                            )
                            synth[anc] = base
                        else:
                            entry["neighbor_vote_fraction"] = min(
                                1.0,
                                float(entry["neighbor_vote_fraction"]) + w / k_limit_f,
                            )

            for rec in leaf_by_gid.values():
                _emit(rec)
            for rec in synth.values():
                _emit(rec)

            # Free per-(q,aspect) state. Keeps neighbor_info / nbs[q_idx]
            # bounded to "queries not yet processed" instead of growing
            # for the full run.
            neighbor_info.pop((q_acc, aspect), None)
            if q_idx < len(nbs):
                nbs[q_idx] = []

        # Free per-query state. Without this the test split's intermediate
        # dicts (pair_features, rr_*, tax_*, query_known_info, neighbor_info)
        # accumulate ~5 GB across 30k queries before the function returns,
        # dominating RSS during the 2-3h record-building loop and tripping
        # systemd-oomd.
        pair_features.pop(q_acc, None)
        rr_vote_count.pop(q_acc, None)
        rr_k_position.pop(q_acc, None)
        rr_vote_min_d.pop(q_acc, None)
        rr_vote_sum_d.pop(q_acc, None)
        rr_distance_std.pop(q_acc, None)
        tax_same_cnt.pop(q_acc, None)
        tax_close_cnt.pop(q_acc, None)
        tax_ca_sum.pop(q_acc, None)
        tax_ca_n.pop(q_acc, None)
        query_known_info.pop(q_acc, None)
        if (q_idx + 1) % 1000 == 0:
            gc.collect()

    if streaming:
        _flush()
        if writer is not None:
            writer.close()
        return {"parquet_path": str(output_parquet), "n_rows": n_rows}
    return records



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

    # KNN parameters — see TrainRerankerPayload for rationale.
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

    # Ancestor expansion (see TrainRerankerPayload.expand_votes_to_ancestors).
    expand_votes_to_ancestors: bool = False

    # Sequence-embedding PCA (see TrainRerankerPayload.use_embedding_pca).
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


# ---------------------------------------------------------------------------
# Auto operation
# ---------------------------------------------------------------------------


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

    name = "train_reranker_auto"
    description = (
        "Train one LightGBM re-ranker per category (NK/LK/PK) across multiple "
        "consecutive temporal holdout pairs and evaluate on a held-out split."
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
    def _dump_frozen_dataset(
        *,
        dump_dir: Path,
        split_files: dict[str, list[Path]],
        valid_split_versions: list[tuple[int, int]],
        test_files: dict[str, Path | None],
        test_old_v: int,
        test_new_v: int,
        name: str,
        k: int,
        embedding_config_id: str,
        ontology_snapshot_id: str,
        annotation_source: str,
    ) -> dict[str, Any]:
        """Thin wrapper that delegates to ``parquet_export`` — kept so
        ``train_reranker_auto`` can still dump a frozen dataset to a local
        path via ``dump_to=...``. New code should prefer the
        ``export_research_dataset`` operation which publishes via the
        configured ``ArtifactStore``.
        """
        from protea import __version__ as _protea_version
        from protea.core.parquet_export import (
            export_reranker_parquets,
            resolve_protea_git_sha,
        )

        result = export_reranker_parquets(
            stage_dir=dump_dir,
            split_files=split_files,
            valid_split_versions=valid_split_versions,
            test_files=test_files,
            test_old_v=test_old_v,
            test_new_v=test_new_v,
            name=name,
            k=k,
            embedding_config_id=embedding_config_id,
            ontology_snapshot_id=ontology_snapshot_id,
            annotation_source=annotation_source,
            store=None,
            producer_version=_protea_version,
            producer_git_sha=resolve_protea_git_sha(),
        )
        # Preserve the historical return contract — callers rely on
        # ``dump_dir`` instead of ``stage_dir``.
        result["dump_dir"] = result.pop("stage_dir", str(dump_dir))
        return result

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
        version_to_native: dict[int, uuid.UUID] = {}
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
            version_to_native[v] = aset.ontology_snapshot_id

        if session.get(EmbeddingConfig, emb_config_id) is None:
            raise ValueError(f"EmbeddingConfig {emb_config_id} not found")

        # Candidate model names (also used in the start event's max_models count).
        candidate_names: list[str] = [f"{p.name}-{cat}" for cat in _CATEGORIES]
        if p.training_scope == "per_cell":
            candidate_names.extend(
                f"{p.name}-{cat}-{_ASPECT_NAMES[asp]}"
                for cat in _CATEGORIES
                for asp in _ASPECTS
            )
        # Name-collision check — skipped when dump_only=True since no
        # RerankerModel rows are written.
        if not p.dump_only:
            for model_name in candidate_names:
                existing = session.query(RerankerModel).filter(RerankerModel.name == model_name).first()
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

        max_models = len(candidate_names)
        emit(
            "train_reranker_auto.start",
            None,
            {
                "name": p.name,
                "train_versions": p.train_versions,
                "test_versions": p.test_versions,
                "n_pairs": len(p.train_versions) - 1,
                "training_scope": p.training_scope,
                "max_models": max_models,
                "per_cell_min_positives": int(p.per_cell_min_positives)
                if p.training_scope == "per_cell"
                else None,
                "ia_weighted": ia_weights is not None,
            },
            "info",
        )

        # ── 2. Load GO maps ──────────────────────────────────────────────
        # Each training/test GOA release has its own contemporary OBO snapshot,
        # so reference annotations use native go_term.id UUIDs — different from
        # the pivot snapshot's ids. Build a union id→(go_id, aspect) map across
        # the pivot + every native snapshot encountered, so native go_term_ids
        # from any set resolve correctly. Predictions are later filtered to
        # terms that also exist in the pivot term universe, matching the
        # reconciled ground-truth space.
        map_snapshots = {ontology_snapshot_id} | set(version_to_native.values())
        union_rows = session.execute(
            text(
                "SELECT id, go_id, aspect FROM go_term "
                "WHERE ontology_snapshot_id = ANY(:snap_ids)"
            ),
            {"snap_ids": [str(s) for s in map_snapshots]},
        ).fetchall()
        go_id_map: dict[Any, str] = {row_id: go_id for row_id, go_id, _ in union_rows}
        aspect_map: dict[Any, str] = {
            row_id: aspect for row_id, _, aspect in union_rows if aspect
        }
        pivot_rows = session.execute(
            text(
                "SELECT go_id FROM go_term "
                "WHERE ontology_snapshot_id = :snap_id AND aspect IS NOT NULL"
            ),
            {"snap_id": ontology_snapshot_id},
        ).fetchall()
        pivot_go_ids: set[str] = {row[0] for row in pivot_rows}

        # Parent map on pivot — used for TPR max-propagation in test metrics.
        parent_map = _load_parent_map(session, ontology_snapshot_id)

        # Optional sequence-embedding PCA — fit once on the preloaded pool.
        pca_state: tuple[np.ndarray, np.ndarray] | None = None

        # ── 2b. Preload ALL embeddings once ─────────────────────────────
        all_embeddings, all_accessions, acc_to_idx = _preload_all_embeddings(
            session, emb_config_id, emit
        )

        if p.use_embedding_pca and all_embeddings.size:
            pca_state = fit_embedding_pca(all_embeddings, EMBEDDING_PCA_DIM)
            emit(
                "train_reranker_auto.pca_fit",
                None,
                {
                    "n_refs": int(all_embeddings.shape[0]),
                    "dim_in": int(all_embeddings.shape[1]),
                    "dim_out": EMBEDDING_PCA_DIM,
                },
                "info",
            )

        # ── 3. Generate training data from consecutive pairs ─────────────
        # Memory-optimised: each split writes to parquet on disk, then all
        # RAM is freed before the next split.  Training reads from disk.
        # "aspect" is now part of ALL_FEATURES (categorical) — keep protein_accession/go_id
        # for grouping/metrics but avoid duplicating the column name.
        _KEEP_COLS = ["protein_accession", "go_id"] + ALL_FEATURES + [LABEL_COLUMN]
        tmp_dir = Path(tempfile.mkdtemp(prefix="protea_reranker_"))
        per_split_stats: list[dict[str, Any]] = []
        split_files: dict[str, list[Path]] = {c: [] for c in _CATEGORIES}
        # Parallel to split_files[cat]: records (v_old, v_new) for each
        # successfully generated split shard, so --dump-to can stamp a
        # ``snapshot_pair`` column per row.
        valid_split_versions: list[tuple[int, int]] = []

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

                # 3a. Load delta from the persisted EvaluationSet artifact.
                # Per the project's "no on-the-fly reuse" rule, the delta
                # must be materialized beforehand via
                # /annotations/evaluation-sets/generate (or the
                # scripts/materialize_lab_intervals.py helper).
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
                    per_split_stats.append(
                        {
                            "v_old": v_old,
                            "v_new": v_new,
                            "skipped": True,
                            "reason": "no ground truth",
                        }
                    )
                    continue

                # 3b. Build references from preloaded embeddings (only loads annotations)
                ref_by_aspect = _build_reference_from_cache(
                    session, old_set_id, all_embeddings, all_accessions, acc_to_idx, emit
                )

                # 3c. Load query embeddings from preloaded cache
                query_accs = [a for a in all_query_accessions if a in acc_to_idx]
                query_indices = np.array([acc_to_idx[a] for a in query_accs], dtype=np.int32)
                query_emb = (
                    all_embeddings[query_indices].astype(np.float32)
                    if len(query_indices) > 0
                    else np.empty((0, all_embeddings.shape[1]), dtype=np.float32)
                )
                valid_queries = query_accs

                if not valid_queries:
                    emit(
                        "train_reranker_auto.split_skipped",
                        None,
                        {"split": i + 1, "reason": "no query embeddings"},
                        "warning",
                    )
                    per_split_stats.append(
                        {
                            "v_old": v_old,
                            "v_new": v_new,
                            "skipped": True,
                            "reason": "no query embeddings",
                        }
                    )
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
                        qs = _load_sequences(session, query_set)
                        rs = _load_sequences(session, all_ref_accs)
                    if p.compute_taxonomy:
                        qt = _load_taxonomy_ids(session, query_set)
                        rt = _load_taxonomy_ids(session, all_ref_accs)

                # 3e. KNN + GO transfer (once, no labeling yet)
                session.expire_all()
                unlabeled_preds = _knn_transfer_and_label(
                    session,
                    valid_queries,
                    query_emb,
                    ref_by_aspect,
                    go_id_map,
                    aspect_map,
                    set(),  # empty gt → all label=0
                    p,
                    sequence_context=SequenceContext(
                        query_sequences=qs,
                        ref_sequences=rs,
                        query_tax_ids=qt,
                        ref_tax_ids=rt,
                    ),
                    query_known_gos=eval_data.known,
                    parent_map_str=parent_map if p.expand_votes_to_ancestors else None,
                    ia_weights=ia_weights,
                    pca_state=pca_state,
                    embedding_pool=all_embeddings,
                )

                # Restrict predictions to terms present in the pivot universe —
                # ground truth was reconciled into pivot space above.
                unlabeled_preds = [
                    r for r in unlabeled_preds if r["go_id"] in pivot_go_ids
                ]

                # Free large objects immediately
                del ref_by_aspect, query_emb, valid_queries, qs, rs, qt, rt
                gc.collect()

                split_stats: dict[str, Any] = {
                    "v_old": v_old,
                    "v_new": v_new,
                    "skipped": False,
                    "total_unlabeled": len(unlabeled_preds),
                }

                # 3e. Build DataFrame, label per category, write to parquet.
                base_df = pd.DataFrame(unlabeled_preds, columns=_KEEP_COLS)
                del unlabeled_preds
                gc.collect()

                for cat in _CATEGORIES:
                    gt_p = cat_gt_pairs[cat]
                    labels = np.array(
                        [
                            1 if (acc, go_id) in gt_p else 0
                            for acc, go_id in zip(
                                base_df["protein_accession"], base_df["go_id"], strict=False
                            )
                        ],
                        dtype=np.int8,
                    )
                    base_df[LABEL_COLUMN] = labels
                    n_pos = int(labels.sum())
                    split_stats[f"{cat}_positives"] = n_pos
                    split_stats[f"{cat}_negatives"] = len(base_df) - n_pos

                    pq_path = tmp_dir / f"train_{cat}_split{i}.parquet"
                    base_df.to_parquet(pq_path, index=False)
                    split_files[cat].append(pq_path)

                del base_df
                gc.collect()

                # Evict the SQLAlchemy identity map — ``session.expire_all``
                # (called earlier) only marks rows stale but keeps them in
                # the identity map, so across 12+ splits the cached
                # ``Sequence.protein_sequence`` strings leak ~30 GB. Full
                # expunge + gc forces the ORM to release them before the
                # next split's ``_load_sequences`` repopulates the cache.
                session.expunge_all()
                gc.collect()

                valid_split_versions.append((v_old, v_new))
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

            test_eset = (
                session.query(EvaluationSet)
                .filter_by(
                    old_annotation_set_id=test_old_set_id,
                    new_annotation_set_id=test_new_set_id,
                )
                .one_or_none()
            )
            if test_eset is None:
                raise RuntimeError(
                    f"EvaluationSet missing for test pair ({test_old_v}->{test_new_v}). "
                    "Materialize it via scripts/materialize_lab_intervals.py "
                    "or POST /annotations/evaluation-sets/generate before retrying."
                )
            test_eval_data, _ = load_evaluation_data_for_set(session, test_eset)

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
                test_ref = _build_reference_from_cache(
                    session, test_old_set_id, all_embeddings, all_accessions, acc_to_idx, emit
                )
                test_accs = [a for a in test_all_queries if a in acc_to_idx]
                test_indices = np.array([acc_to_idx[a] for a in test_accs], dtype=np.int32)
                test_emb = (
                    all_embeddings[test_indices].astype(np.float32)
                    if len(test_indices) > 0
                    else np.empty((0, all_embeddings.shape[1]), dtype=np.float32)
                )
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
                            test_qs = _load_sequences(session, test_query_set)
                            test_rs = _load_sequences(session, test_ref_accs)
                        if p.compute_taxonomy:
                            test_qt = _load_taxonomy_ids(session, test_query_set)
                            test_rt = _load_taxonomy_ids(session, test_ref_accs)

                    session.expire_all()
                    # Stream the test split directly to parquet instead of
                    # materialising ~10M records in a Python list. The
                    # record-building loop inside ``_knn_transfer_and_label``
                    # already filters by ``pivot_go_ids``, so the on-disk
                    # intermediate is the pivot-reconciled set. Per-cat
                    # labeling reads the intermediate back in pyarrow
                    # batches and fans out to one writer per category —
                    # peak RSS stays bounded by ``batch_size`` rows.
                    test_unlabeled_path = tmp_dir / "test_unlabeled.parquet"
                    test_stream_info = _knn_transfer_and_label(
                        session,
                        test_valid,
                        test_emb,
                        test_ref,
                        go_id_map,
                        aspect_map,
                        set(),
                        p,
                        sequence_context=SequenceContext(
                            query_sequences=test_qs,
                            ref_sequences=test_rs,
                            query_tax_ids=test_qt,
                            ref_tax_ids=test_rt,
                        ),
                        query_known_gos=test_eval_data.known,
                        parent_map_str=parent_map if p.expand_votes_to_ancestors else None,
                        ia_weights=ia_weights,
                        pca_state=pca_state,
                        pivot_go_ids=pivot_go_ids,
                        stream_output=StreamOutput(output_parquet=test_unlabeled_path),
                        embedding_pool=all_embeddings,
                    )
                    del test_ref, test_emb, test_valid, test_qs, test_rs, test_qt, test_rt
                    gc.collect()

                    n_rows = int(test_stream_info.get("n_rows", 0))
                    if n_rows > 0 and test_unlabeled_path.exists():
                        pf = pq.ParquetFile(str(test_unlabeled_path))
                        project_cols = [
                            c for c in _KEEP_COLS if c in pf.schema_arrow.names
                        ]
                        cat_writers: dict[str, pq.ParquetWriter] = {}
                        cat_paths: dict[str, Path] = {
                            cat: tmp_dir / f"test_{cat}.parquet" for cat in _CATEGORIES
                        }
                        try:
                            for batch in pf.iter_batches(
                                batch_size=200_000, columns=project_cols
                            ):
                                # Drop any pre-existing LABEL_COLUMN (it was
                                # written as zero during streaming) so we can
                                # append a fresh per-cat label column without
                                # triggering a schema mismatch.
                                if LABEL_COLUMN in batch.schema.names:
                                    batch = batch.drop_columns([LABEL_COLUMN])
                                accs = batch.column("protein_accession").to_pylist()
                                gids = batch.column("go_id").to_pylist()
                                for cat in _CATEGORIES:
                                    gt_p = test_cat_gt[cat]
                                    labels = pa.array(
                                        [
                                            1 if (a, g) in gt_p else 0
                                            for a, g in zip(accs, gids, strict=False)
                                        ],
                                        type=pa.int8(),
                                    )
                                    cat_batch = batch.append_column(LABEL_COLUMN, labels)
                                    table = pa.Table.from_batches([cat_batch])
                                    if cat not in cat_writers:
                                        cat_writers[cat] = pq.ParquetWriter(
                                            str(cat_paths[cat]), table.schema
                                        )
                                    cat_writers[cat].write_table(table)
                        finally:
                            for w in cat_writers.values():
                                w.close()
                        for cat in _CATEGORIES:
                            if cat in cat_writers:
                                test_files[cat] = cat_paths[cat]
                        test_unlabeled_path.unlink(missing_ok=True)
                    gc.collect()
                else:
                    del test_ref, test_emb, test_valid
                    gc.collect()

            # Release the preloaded embedding pool — from here on the
            # pipeline only reads from the per-split parquets on disk.
            # Keeps ~1.2 GB out of RSS before dump / training.
            del all_embeddings, all_accessions, acc_to_idx
            gc.collect()

            # ── 4b. Dataset dump ──────────────────────────────────────────
            # Training moved to protea-reranker-lab; this operation now
            # only materializes the frozen parquets + manifest that the
            # lab consumes via ``scripts/pull_dataset.py``. Callers must
            # pass ``dump_to`` — ExportResearchDatasetOperation always does.
            if not p.dump_to:
                raise ValueError(
                    "train_reranker_auto requires dump_to — LightGBM "
                    "training has been moved to protea-reranker-lab. Use "
                    "ExportResearchDatasetOperation / POST /datasets."
                )
            dump_stats = self._dump_frozen_dataset(
                dump_dir=Path(p.dump_to),
                split_files=split_files,
                valid_split_versions=valid_split_versions,
                test_files=test_files,
                test_old_v=test_old_v,
                test_new_v=test_new_v,
                name=p.name,
                k=int(p.limit_per_entry),
                embedding_config_id=str(emb_config_id),
                ontology_snapshot_id=str(ontology_snapshot_id),
                annotation_source=p.annotation_source,
            )
            emit("train_reranker_auto.dump_done", None, dump_stats, "info")
            elapsed = round(time.perf_counter() - t0, 1)
            result: dict[str, Any] = {
                "dumped": True,
                "dump_path": str(p.dump_to),
                "dump_stats": dump_stats,
                "elapsed_seconds": elapsed,
            }
            emit("train_reranker_auto.done", None, result, "info")
            return OperationResult(result=result)

        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)
