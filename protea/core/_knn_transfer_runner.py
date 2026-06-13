"""Method Object for ``training_dump_helpers._knn_transfer_and_label``.

Hosts the ``_KnnTransferRunner`` class (T2B.5 partial #8 refactor) so the
parent ``training_dump_helpers`` module stays under the §3 file-LOC ceiling.
The runner orchestrates KNN search + feature pre-computation + per-query
record building, delegating the per-(query, candidate) record dictionary
construction to ``_LeafRecordBuilder``.

Public API: ``run_knn_transfer_and_label`` (called from the thin wrapper in
``training_dump_helpers``). The class itself is private to keep the
collaborator pair tightly coupled.
"""

from __future__ import annotations

import gc
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy.orm import Session

from protea.core._pair_feature_compute import (
    build_pair_feature_dict,
    precompute_alignment_features,
)
from protea.core.domain.aspect import ASPECT_CODES as _ASPECTS
from protea.core.knn_search import search_knn
from protea.core.reranker import EMBEDDING_PCA_DIM

if TYPE_CHECKING:
    from protea.core.training_dump_helpers import (
        KnnTransferContext,
        SequenceContext,
        StreamOutput,
        TrainRerankerAutoPayload,
    )

_LOG = logging.getLogger(__name__)

# Relations counted as "close" by the taxonomic-consensus voter aggregator.
# Top-level so ``_KnnTransferRunner`` can reference it without rebuilding the
# frozenset on every call.
_TAX_CLOSE_RELATIONS = frozenset(
    {
        "same",
        "ancestor",
        "descendant",
        "child",
        "parent",
        "close",
    }
)


@dataclass(frozen=True)
class _LeafContext:
    """Per-(q_acc, aspect) inputs for the leaf-record builder.

    Bundles the per-query / per-aspect state needed to materialise a
    ``leaf_by_gid`` dict so the builder method signature stays under the
    §3 6-arg ceiling.
    """

    q_idx: int
    q_acc: str
    aspect: str
    q_pca_row: list[float]
    q_known_cent: np.ndarray | None
    q_known_mat: np.ndarray | None
    q_known_n: int
    q_pairs_features: dict[str, dict[str, Any]]


@dataclass(frozen=True)
class _LeafInputs:
    """Per-(query, candidate-GO) inputs for ``_make_leaf_record``.

    Groups the candidate-specific raw inputs (GO id, ref accession, KNN
    distance, neighbour Anc2Vec context, query-side Anc2Vec context) so
    the dict-builder takes a single Parameter Object instead of 14
    positional / keyword arguments.
    """

    q_acc: str
    go_id: str
    go_term_id: int
    ref_acc: str
    distance: float
    ann: dict[str, Any]
    pf: dict[str, Any]
    centroid_unit: np.ndarray | None
    nmat: np.ndarray | None
    q_known_cent: np.ndarray | None
    q_known_mat: np.ndarray | None
    q_known_n: int
    q_pca_row: list[float]


def run_knn_transfer_and_label(
    session: Session,
    p: TrainRerankerAutoPayload,
    ctx: KnnTransferContext,
    *,
    sequence_context: SequenceContext | None = None,
    stream_output: StreamOutput | None = None,
) -> list[dict[str, Any]] | dict[str, Any]:
    """Public entry point: instantiate the runner and drive ``run()``."""
    runner = _KnnTransferRunner(
        session=session,
        p=p,
        ctx=ctx,
        sequence_context=sequence_context,
        stream_output=stream_output,
    )
    return runner.run()


class _KnnTransferRunner:
    """Method Object for ``_knn_transfer_and_label``.

    Holds the per-call state (KNN results, intermediate feature dicts,
    streaming buffers) as attributes so the per-phase methods stay
    short. ``run`` is the top-level orchestrator: KNN, then feature
    pre-computation phases, then the per-query record-building loop.
    """

    def __init__(
        self,
        *,
        session: Session,
        p: TrainRerankerAutoPayload,
        ctx: KnnTransferContext,
        sequence_context: SequenceContext | None,
        stream_output: StreamOutput | None,
    ) -> None:
        # Local imports dodge the import cycle (collaborators reference
        # the runner type, runner instantiates the collaborators).
        from protea.core._anc2vec_phases import _Anc2VecPhases
        from protea.core._leaf_record_builder import _LeafRecordBuilder

        self.session = session
        self.p = p
        self._unpack_ctx(ctx)
        self._unpack_sequence_context(sequence_context)
        self._unpack_stream_output(stream_output)
        self._init_phase_state()
        self._builder = _LeafRecordBuilder(self)
        self._anc2vec = _Anc2VecPhases(self)

    def _unpack_ctx(self, ctx: KnnTransferContext) -> None:
        """Unpack ``KnnTransferContext`` fields into attributes."""
        self.valid_queries = ctx.valid_queries
        self.query_emb = ctx.query_emb
        self.ref_by_aspect = ctx.ref_by_aspect
        self.go_id_map = ctx.go_id_map
        self.aspect_map = ctx.aspect_map
        self.gt_pairs = ctx.gt_pairs
        self.query_known_gos = ctx.query_known_gos
        self.parent_map_str = ctx.parent_map_str
        self.ia_weights = ctx.ia_weights
        self.pca_state = ctx.pca_state
        self.pivot_go_ids = ctx.pivot_go_ids
        self.embedding_pool = ctx.embedding_pool

    def _unpack_sequence_context(self, sequence_context: SequenceContext | None) -> None:
        """Unpack the optional ``SequenceContext`` and resolve toggles."""
        # Local import avoids the circular dependency at runtime: the
        # sibling module imports from training_dump_helpers, which imports
        # this module via ``run_knn_transfer_and_label``. The TYPE_CHECKING
        # block above keeps the type alias usable at static-check time.
        from protea.core.training_dump_helpers import SequenceContext as _SeqCtx

        seq_ctx = sequence_context or _SeqCtx()
        self.query_sequences = seq_ctx.query_sequences
        self.ref_sequences = seq_ctx.ref_sequences
        self.query_tax_ids = seq_ctx.query_tax_ids
        self.ref_tax_ids = seq_ctx.ref_tax_ids
        self.do_alignments = (
            self.p.compute_alignments
            and self.query_sequences is not None
            and self.ref_sequences is not None
        )
        self.do_taxonomy = (
            self.p.compute_taxonomy
            and self.query_tax_ids is not None
            and self.ref_tax_ids is not None
        )

    def _unpack_stream_output(self, stream_output: StreamOutput | None) -> None:
        """Resolve streaming mode + buffer state."""
        if stream_output is not None:
            self.output_parquet: Path | None = stream_output.output_parquet
            self.chunk_rows: int = stream_output.chunk_rows
        else:
            self.output_parquet = None
            self.chunk_rows = 100_000
        self.streaming = self.output_parquet is not None
        self.records: list[dict[str, Any]] = []
        self.buffer: list[dict[str, Any]] = []
        self.writer: pq.ParquetWriter | None = None
        self.n_rows = 0

    def _init_phase_state(self) -> None:
        """Initialise per-phase accumulator dicts.

        Pre-allocating the empty containers keeps the type hints close
        to the attribute declarations and lets the phase methods read /
        mutate without an extra ``hasattr`` dance.
        """
        self.k_limit = int(self.p.limit_per_entry) or 1
        self.k_limit_f = float(self.k_limit)
        self._nan_pca = [float("nan")] * EMBEDDING_PCA_DIM
        self.neighbors_by_aspect: dict[str, list[list[tuple[str, float]]]] = {}
        self.rr_distance_std: dict[str, float] = {}
        self.rr_vote_count: dict[str, dict[int, int]] = {}
        self.rr_k_position: dict[str, dict[int, int]] = {}
        self.rr_vote_min_d: dict[str, dict[int, float]] = {}
        self.rr_vote_sum_d: dict[str, dict[int, float]] = {}
        self.go_term_freq: dict[int, int] = {}
        self.ref_ann_density: dict[str, int] = {}
        self.pair_features: dict[str, dict[str, dict[str, Any]]] = {}
        self.tax_same_cnt: dict[str, dict[int, int]] = {}
        self.tax_close_cnt: dict[str, dict[int, int]] = {}
        self.tax_ca_sum: dict[str, dict[int, float]] = {}
        self.tax_ca_n: dict[str, dict[int, int]] = {}
        self.idx_of_go: dict[str, int] = {}
        self.has_emb_mask: np.ndarray = np.zeros(0, dtype=bool)
        self.all_norm: np.ndarray = np.zeros((0, 0), dtype=np.float32)
        self.neighbor_info: dict[tuple[str, str], tuple[np.ndarray | None, np.ndarray | None]] = {}
        self.query_known_info: dict[str, tuple[np.ndarray | None, np.ndarray | None, int]] = {}
        self.pca_query_proj: np.ndarray | None = None
        self.expand = getattr(self.p, "expand_votes_to_ancestors", False) and bool(
            self.parent_map_str
        )
        self.ancestor_closure: dict[str, set[str]] = {}
        # Lineage producer inputs: convert the set-valued parent map to
        # the producer's list-typed shape once at init and capture the
        # known-GO map (or an empty fallback) for per-query lookup.
        self._lineage_parents: dict[str, list[str]] = (
            {gid: list(ps) for gid, ps in self.parent_map_str.items()}
            if self.parent_map_str
            else {}
        )
        self._lineage_known: dict[str, set[str]] = self.query_known_gos or {}

    def run(self) -> list[dict[str, Any]] | dict[str, Any]:
        """Drive the full KNN-transfer-label pipeline."""
        self._run_knn()
        self._compute_reranker_features()
        self._compute_pair_features()
        self._compute_tax_consensus()
        self._anc2vec.build_index()
        self._anc2vec.compute_neighbor_centroids()
        self._anc2vec.compute_query_centroids()
        self._compute_pca_proj()
        return self._build_records()

    # ── phase 1: per-aspect KNN ────────────────────────────────────────

    def _run_knn(self) -> None:
        """Per-aspect KNN search; populates ``neighbors_by_aspect``."""
        for aspect in _ASPECTS:
            ref = self.ref_by_aspect[aspect]
            if not ref["accessions"]:
                self.neighbors_by_aspect[aspect] = [[] for _ in self.valid_queries]
                continue
            # Two source shapes are supported:
            #   - ``ref["indices"]`` + ``embedding_pool`` (preload-aware path);
            #     no per-aspect float16 copy is held in the dict.
            #   - ``ref["embeddings"]`` (legacy path used by the single-version
            #     dump_helper that loads embeddings per aspect from SQL).
            indices = ref.get("indices")
            if indices is not None and self.embedding_pool is not None:
                ref_f32 = self.embedding_pool[indices].astype(np.float32)
            else:
                ref_f32 = ref["embeddings"].astype(np.float32)
            self.neighbors_by_aspect[aspect] = search_knn(
                self.query_emb,
                ref_f32,
                ref["accessions"],
                k=self.p.limit_per_entry,
                distance_threshold=self.p.distance_threshold,
                backend=self.p.search_backend,
                metric=self.p.metric,
                faiss_index_type=self.p.faiss_index_type,
                faiss_nlist=self.p.faiss_nlist,
                faiss_nprobe=self.p.faiss_nprobe,
            )
            del ref_f32
            # Embeddings/indices are no longer needed past this point. Releasing
            # ~940 MB per aspect (f16, ~500k × dim) keeps RSS flat before the
            # record-building phase.
            ref["embeddings"] = None
            ref["indices"] = None
        gc.collect()

    # ── phase 2: vote / k_position / density / distance-std stats ─────

    def _compute_reranker_features(self) -> None:
        """Vote count, k_position, distance std, term frequency, density."""
        for q_idx, q_acc in enumerate(self.valid_queries):
            all_dists: list[float] = []
            self.rr_vote_count[q_acc] = {}
            self.rr_k_position[q_acc] = {}
            self.rr_vote_min_d[q_acc] = {}
            self.rr_vote_sum_d[q_acc] = {}
            for aspect in _ASPECTS:
                nbs = self.neighbors_by_aspect[aspect]
                if q_idx < len(nbs):
                    for _, d in nbs[q_idx]:
                        all_dists.append(d)
            self.rr_distance_std[q_acc] = float(np.std(all_dists)) if len(all_dists) > 1 else 0.0
        for aspect in _ASPECTS:
            self._tally_aspect_votes(aspect)

    def _tally_aspect_votes(self, aspect: str) -> None:
        """Per-aspect vote counts, k_position, term freq, density."""
        go_map = self.ref_by_aspect[aspect]["go_map"]
        for acc, anns in go_map.items():
            if acc not in self.ref_ann_density:
                self.ref_ann_density[acc] = 0
            self.ref_ann_density[acc] += len(anns)
            for ann in anns:
                gtid = ann["go_term_id"]
                self.go_term_freq[gtid] = self.go_term_freq.get(gtid, 0) + 1
        for q_idx, q_acc in enumerate(self.valid_queries):
            vc = self.rr_vote_count[q_acc]
            kp = self.rr_k_position[q_acc]
            vmin = self.rr_vote_min_d[q_acc]
            vsum = self.rr_vote_sum_d[q_acc]
            nbs = self.neighbors_by_aspect[aspect]
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

    # ── phase 3: per-(query, ref) alignment + taxonomy features ───────

    def _compute_pair_features(self) -> None:
        """Per-pair alignment and taxonomy features.

        Nested by q_acc so per-query state can be popped atomically once
        the record-building loop is done with each query — keeps RSS
        bounded. Alignments (the hotspot) are pre-computed for all unique
        ``(q_acc, ref_acc)`` pairs in one parallel + on-disk-cached pass
        (``precompute_alignment_features``), then merged with the cheap
        per-pair taxonomy lookup. Value-preserving: the merged dict equals
        the original per-pair ``compute_alignment`` + ``compute_taxonomy``
        output, only the alignment half is concurrent + memoised across
        jobs / smaller-K datasets.
        """
        if not (self.do_alignments or self.do_taxonomy):
            return
        align_by_pair = self._precompute_alignments()
        # Heartbeat: this loop can run for hours on large splits; periodic
        # logging distinguishes a stall from slow progress.
        hb_t0 = time.perf_counter()
        hb_last = hb_t0
        hb_n = 0
        for aspect in _ASPECTS:
            nbs = self.neighbors_by_aspect[aspect]
            for q_idx, q_acc in enumerate(self.valid_queries):
                if q_idx >= len(nbs):
                    continue
                q_pairs = self.pair_features.setdefault(q_acc, {})
                for ref_acc, _ in nbs[q_idx]:
                    if ref_acc in q_pairs:
                        continue
                    q_pairs[ref_acc] = build_pair_feature_dict(
                        q_acc,
                        ref_acc,
                        align_by_pair,
                        do_alignments=self.do_alignments,
                        do_taxonomy=self.do_taxonomy,
                        tax_ids=(self.query_tax_ids, self.ref_tax_ids),
                    )
                    hb_n += 1
                    now = time.perf_counter()
                    if now - hb_last >= 30.0:
                        _LOG.info(
                            "pair_features heartbeat: pairs=%d aspect=%s "
                            "q_idx=%d/%d elapsed=%.1fs rate=%.0f/s",
                            hb_n,
                            aspect,
                            q_idx,
                            len(self.valid_queries),
                            now - hb_t0,
                            hb_n / max(1e-9, now - hb_t0),
                        )
                        hb_last = now

    def _precompute_alignments(self) -> dict[tuple[str, str], dict[str, Any]]:
        """Batch-align unique pairs (parallel + on-disk cached); {} if off."""
        if not self.do_alignments:
            return {}
        assert self.query_sequences is not None
        assert self.ref_sequences is not None
        return precompute_alignment_features(
            aspects=_ASPECTS,
            neighbors_by_aspect=self.neighbors_by_aspect,
            valid_queries=self.valid_queries,
            query_sequences=self.query_sequences,
            ref_sequences=self.ref_sequences,
        )

    # ── phase 4: taxonomic-consensus voter aggregation ────────────────

    def _compute_tax_consensus(self) -> None:
        """Aggregate taxonomic signal across voters per candidate term.

        Mirrors the (neighbor_vote_fraction, neighbor_min_distance,
        neighbor_mean_distance) design but aggregates taxonomic signal
        across the subset of neighbors that voted for each candidate
        term. Requires ``compute_taxonomy=True``; otherwise the three
        features stay NaN.
        """
        if not self.do_taxonomy:
            return
        for aspect in _ASPECTS:
            self._tally_aspect_tax_consensus(aspect)

    def _tally_aspect_tax_consensus(self, aspect: str) -> None:
        """Per-aspect taxonomic-consensus voter accumulation."""
        go_map = self.ref_by_aspect[aspect]["go_map"]
        nbs_all = self.neighbors_by_aspect[aspect]
        for q_idx, q_acc in enumerate(self.valid_queries):
            if q_idx >= len(nbs_all):
                continue
            same_d = self.tax_same_cnt.setdefault(q_acc, {})
            close_d = self.tax_close_cnt.setdefault(q_acc, {})
            sum_d = self.tax_ca_sum.setdefault(q_acc, {})
            n_d = self.tax_ca_n.setdefault(q_acc, {})
            q_pairs = self.pair_features.get(q_acc, {})
            for ref_acc, _ in nbs_all[q_idx]:
                pf = q_pairs.get(ref_acc, {})
                rel = pf.get("taxonomic_relation") or ""
                ca = pf.get("taxonomic_common_ancestors")
                is_same = rel == "same"
                is_close = rel in _TAX_CLOSE_RELATIONS
                for ann in go_map.get(ref_acc, []):
                    gtid = ann["go_term_id"]
                    if is_same:
                        same_d[gtid] = same_d.get(gtid, 0) + 1
                    if is_close:
                        close_d[gtid] = close_d.get(gtid, 0) + 1
                    if isinstance(ca, int | float) and ca is not None:
                        sum_d[gtid] = sum_d.get(gtid, 0.0) + float(ca)
                        n_d[gtid] = n_d.get(gtid, 0) + 1

    # ── phases 5/6/7: Anc2Vec (delegated to ``_Anc2VecPhases``) ───────

    # ── phase 8: PCA projection of query embeddings ───────────────────

    def _compute_pca_proj(self) -> None:
        """Per-query projection onto the precomputed PCA components.

        Emits 16 features (emb_pca_query_0..15) at record-creation time.
        NaN when ``pca_state`` is None (LightGBM routes as missing).
        """
        if self.pca_state is None or not self.query_emb.size:
            self.pca_query_proj = None
            return
        pca_mean, pca_components = self.pca_state
        self.pca_query_proj = (
            (self.query_emb.astype(np.float32) - pca_mean) @ pca_components.T
        ).astype(np.float32)

    # ── phase 9: per-(q_acc, aspect) record builder + emit ────────────

    def _build_records(self) -> list[dict[str, Any]] | dict[str, Any]:
        """Per-``(q_acc, aspect)`` record-building loop.

        Iterating per group keeps ancestor expansion local and bounds
        intermediate state to one group's worth of records. In list mode
        (the legacy default) records accumulate in memory. In streaming
        mode (``output_parquet`` given) each group flushes through a
        pyarrow ParquetWriter in ``chunk_rows`` batches.
        """
        for q_idx, q_acc in enumerate(self.valid_queries):
            self._build_records_for_query(q_idx, q_acc)
            self._cleanup_query_state(q_idx, q_acc)
        if self.streaming:
            self._flush()
            if self.writer is not None:
                self.writer.close()
            return {"parquet_path": str(self.output_parquet), "n_rows": self.n_rows}
        return self.records

    def _build_records_for_query(self, q_idx: int, q_acc: str) -> None:
        """Build leaves + ancestor expansion for one query, all aspects."""
        q_pca_row = (
            self.pca_query_proj[q_idx].tolist()
            if self.pca_query_proj is not None
            else self._nan_pca
        )
        q_known_cent, q_known_mat, q_known_n = self.query_known_info.get(q_acc, (None, None, 0))
        q_pairs_features = self.pair_features.get(q_acc, {})
        builder = self._builder
        for aspect in _ASPECTS:
            nbs = self.neighbors_by_aspect[aspect]
            if q_idx >= len(nbs):
                continue
            leaf_ctx = _LeafContext(
                q_idx=q_idx,
                q_acc=q_acc,
                aspect=aspect,
                q_pca_row=q_pca_row,
                q_known_cent=q_known_cent,
                q_known_mat=q_known_mat,
                q_known_n=q_known_n,
                q_pairs_features=q_pairs_features,
            )
            leaf_by_gid = builder.build_leaves_for_aspect(leaf_ctx)
            synth = builder.expand_ancestors(q_acc, leaf_by_gid)
            # S3b: union the protein's InterPro-only terms (absent from the
            # KNN set) into leaf_by_gid, post ancestor expansion so KNN
            # votes are untouched. Lineage / InterPro post-passes + emit
            # then cover them with no further wiring.
            builder.add_interpro_only(leaf_ctx, leaf_by_gid, synth)
            self._apply_lineage_features(q_acc, leaf_by_gid, synth)
            self._apply_interpro_features(leaf_by_gid, synth)
            for rec in leaf_by_gid.values():
                self._emit(rec)
            for rec in synth.values():
                self._emit(rec)
            # Free per-(q, aspect) state. Keeps neighbor_info / nbs[q_idx]
            # bounded to "queries not yet processed".
            self.neighbor_info.pop((q_acc, aspect), None)
            if q_idx < len(nbs):
                nbs[q_idx] = []

    def _apply_lineage_features(
        self,
        q_acc: str,
        leaf_by_gid: dict[str, dict[str, Any]],
        synth: dict[str, dict[str, Any]],
    ) -> None:
        """Invoke ``compute_lineage_features`` on this group's records.

        Mutates each record in place with the 4 ``lineage_*`` columns.
        Lazy import keeps the GO-DAG dependency out of the runner's
        import graph for unrelated unit tests.
        """
        from protea_method.lineage import compute_lineage_features

        combined: list[dict[str, Any]] = [
            *leaf_by_gid.values(),
            *synth.values(),
        ]
        if not combined:
            return
        compute_lineage_features(
            combined,
            parents=self._lineage_parents,
            known_by_protein={q_acc: self._lineage_known.get(q_acc, set())},
        )

    def _apply_interpro_features(
        self,
        leaf_by_gid: dict[str, dict[str, Any]],
        synth: dict[str, dict[str, Any]],
    ) -> None:
        """Fill the 11 InterPro columns on this group's records.

        Left-joins the env-configured InterPro GO-prediction table on
        ``(protein, go_id)`` and overwrites the zero-fill defaults for
        matched leaves, synthesised ancestors and the InterPro-only union
        rows already merged into ``leaf_by_gid`` (S3b). When the table is
        empty (env var unset) every record keeps the builder defaults, so
        all 11 columns stay present unconditionally.
        """
        table = self._builder.get_interpro_table()
        if not table:
            return
        from protea.core._interpro_features import apply_interpro_features

        apply_interpro_features([*leaf_by_gid.values(), *synth.values()], table)

    # ── phase 10: streaming buffer + per-query state cleanup ──────────

    def _flush(self) -> None:
        if not self.buffer:
            return
        table = pa.Table.from_pylist(self.buffer)
        if self.writer is None:
            self.writer = pq.ParquetWriter(str(self.output_parquet), table.schema)
        self.writer.write_table(table)
        self.n_rows += len(self.buffer)
        self.buffer.clear()

    def _emit(self, rec: dict[str, Any]) -> None:
        if self.pivot_go_ids is not None and rec["go_id"] not in self.pivot_go_ids:
            return
        if self.streaming:
            self.buffer.append(rec)
            if len(self.buffer) >= self.chunk_rows:
                self._flush()
        else:
            self.records.append(rec)

    def _cleanup_query_state(self, q_idx: int, q_acc: str) -> None:
        """Free per-query intermediate state.

        Without this the test split's intermediate dicts (pair_features,
        rr_*, tax_*, query_known_info, neighbor_info) accumulate ~5 GB
        across 30k queries before the function returns, dominating RSS
        during the 2-3h record-building loop and tripping systemd-oomd.
        """
        self.pair_features.pop(q_acc, None)
        self.rr_vote_count.pop(q_acc, None)
        self.rr_k_position.pop(q_acc, None)
        self.rr_vote_min_d.pop(q_acc, None)
        self.rr_vote_sum_d.pop(q_acc, None)
        self.rr_distance_std.pop(q_acc, None)
        self.tax_same_cnt.pop(q_acc, None)
        self.tax_close_cnt.pop(q_acc, None)
        self.tax_ca_sum.pop(q_acc, None)
        self.tax_ca_n.pop(q_acc, None)
        self.query_known_info.pop(q_acc, None)
        if (q_idx + 1) % 1000 == 0:
            gc.collect()
