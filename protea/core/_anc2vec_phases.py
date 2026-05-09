"""Anc2Vec feature-pre-computation phases for the KNN runner.

Extracted from ``_knn_transfer_runner._KnnTransferRunner`` (T2B.5
partial #8) to keep the runner class under the §3 500-LOC ceiling.
The three phases here build the shared Anc2Vec projection pool and
the per-(q, aspect) and per-query centroids that the leaf-record
builder consumes downstream.

Stateless apart from the runner back-reference; all reads / writes
go through ``self.runner`` attributes so the runner remains the
single source of truth for cross-phase state.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import numpy as np

from protea.core.anc2vec_embeddings import Anc2VecIndex
from protea.core.anc2vec_embeddings import get_index as get_anc2vec_index
from protea.core.domain.aspect import ASPECT_CODES as _ASPECTS

if TYPE_CHECKING:
    from protea.core._knn_transfer_runner import _KnnTransferRunner


class _Anc2VecPhases:
    """Anc2Vec phases collaborator.

    Owns the three Anc2Vec preparation phases:

    * ``build_index`` (phase 5): unit-normed embedding matrix shared
      across queries.
    * ``compute_neighbor_centroids`` (phase 6): per-(q, aspect) centroid
      and full matrix of neighbour-term vectors.
    * ``compute_query_centroids`` (phase 7): query-known centroid +
      matrix (PK-killer signal).
    """

    def __init__(self, runner: _KnnTransferRunner) -> None:
        self.runner = runner

    def build_index(self) -> None:
        """Build the unit-normed Anc2Vec matrix shared across queries.

        Embeds every GO term annotated to a neighbour plus every term
        the queries already know, then row-normalises. Downstream phases
        index into ``runner.all_norm`` via ``runner.idx_of_go`` and gate
        by ``runner.has_emb_mask``.
        """
        runner = self.runner
        anc_idx: Anc2VecIndex = get_anc2vec_index()
        all_go_id_strs: set[str] = set()
        for aspect in _ASPECTS:
            for anns in runner.ref_by_aspect[aspect]["go_map"].values():
                for ann in anns:
                    gid_str = runner.go_id_map.get(ann["go_term_id"])
                    if gid_str:
                        all_go_id_strs.add(gid_str)
        if runner.query_known_gos:
            for _gos in runner.query_known_gos.values():
                all_go_id_strs.update(_gos)
        all_go_id_list = sorted(all_go_id_strs)
        runner.idx_of_go = {g: i for i, g in enumerate(all_go_id_list)}
        all_emb = anc_idx.batch(all_go_id_list)
        raw_norms = np.linalg.norm(all_emb, axis=1)
        runner.has_emb_mask = raw_norms > 0.0
        safe_norms = np.where(runner.has_emb_mask, raw_norms, 1.0)[:, None]
        all_norm = (all_emb / safe_norms).astype(np.float32)
        all_norm[~runner.has_emb_mask] = 0.0
        runner.all_norm = all_norm

    def compute_neighbor_centroids(self) -> None:
        """Per-(q_acc, aspect) centroid + matrix of neighbour-term vectors."""
        runner = self.runner
        for aspect in _ASPECTS:
            go_map = runner.ref_by_aspect[aspect]["go_map"]
            nbs_all = runner.neighbors_by_aspect[aspect]
            for q_idx, q_acc in enumerate(runner.valid_queries):
                if q_idx >= len(nbs_all):
                    continue
                self._fill_one_cell(q_idx, q_acc, aspect, go_map, nbs_all)

    def _fill_one_cell(
        self,
        q_idx: int,
        q_acc: str,
        aspect: str,
        go_map: dict[str, list[dict[str, Any]]],
        nbs_all: list[list[tuple[str, float]]],
    ) -> None:
        """Compute centroid + matrix for one ``(q_acc, aspect)`` cell."""
        runner = self.runner
        rows: list[int] = []
        seen: set[str] = set()
        for ref_acc, _ in nbs_all[q_idx]:
            for ann in go_map.get(ref_acc, []):
                gid_str = runner.go_id_map.get(ann["go_term_id"])
                if not gid_str or gid_str in seen:
                    continue
                seen.add(gid_str)
                i = runner.idx_of_go.get(gid_str)
                if i is not None and runner.has_emb_mask[i]:
                    rows.append(i)
        if not rows:
            runner.neighbor_info[(q_acc, aspect)] = (None, None)
            return
        nmat = runner.all_norm[rows]
        centroid = nmat.mean(axis=0)
        cn = float(np.linalg.norm(centroid))
        centroid_unit = (centroid / cn).astype(np.float32) if cn > 0.0 else None
        runner.neighbor_info[(q_acc, aspect)] = (centroid_unit, nmat)

    def compute_query_centroids(self) -> None:
        """Centroid + embedding matrix of the query's pre-cutoff GO set.

        For NK queries this stays empty (features fall back to NaN). For
        LK/PK queries this captures the annotation profile the candidate
        should stay close to — the canonical PK-discriminator signal.
        """
        runner = self.runner
        if not runner.query_known_gos:
            return
        for q_acc in runner.valid_queries:
            known = runner.query_known_gos.get(q_acc, set())
            if not known:
                runner.query_known_info[q_acc] = (None, None, 0)
                continue
            rows = [
                runner.idx_of_go[g]
                for g in known
                if g in runner.idx_of_go and runner.has_emb_mask[runner.idx_of_go[g]]
            ]
            if not rows:
                runner.query_known_info[q_acc] = (None, None, len(known))
                continue
            kmat = runner.all_norm[rows]
            centroid = kmat.mean(axis=0)
            cn = float(np.linalg.norm(centroid))
            centroid_unit = (centroid / cn).astype(np.float32) if cn > 0.0 else None
            runner.query_known_info[q_acc] = (centroid_unit, kmat, len(known))
