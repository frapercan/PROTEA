"""Per-(query, candidate) record builder collaborator for the KNN runner.

Extracted from ``_knn_transfer_runner._KnnTransferRunner`` (T2B.5 partial
#8) to keep both the runner class and the runner module under the §3
LOC ceilings. The builder is a stateless collaborator that reads the
runner's pre-computed feature dictionaries and materialises the
per-(query, candidate-GO) record dict + ancestor expansion.

Design:

* ``_LeafRecordBuilder`` holds a back-reference to its
  ``_KnnTransferRunner`` so it can read attributes (``rr_*``,
  ``tax_*``, ``idx_of_go``, ``all_norm``, ``has_emb_mask``, ...).
* Method signatures use the small ``_LeafContext`` / ``_LeafInputs``
  parameter objects defined in the runner module.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import numpy as np

from protea.core.reranker import EMBEDDING_PCA_DIM, LABEL_COLUMN

if TYPE_CHECKING:
    from protea.core._knn_transfer_runner import (
        _KnnTransferRunner,
        _LeafContext,
        _LeafInputs,
    )


class _LeafRecordBuilder:
    """Builds the per-(query, candidate-GO) record dicts + ancestor expansion.

    Stateless apart from the runner back-reference. Splitting it out of
    ``_KnnTransferRunner`` keeps that class focused on phase
    orchestration and KNN/feature pre-computation; this collaborator
    owns the dict-materialisation hot path.
    """

    def __init__(self, runner: _KnnTransferRunner) -> None:
        self.runner = runner

    # ── leaf record materialisation ───────────────────────────────────

    def build_leaves_for_aspect(
        self, ctx: _LeafContext
    ) -> dict[str, dict[str, Any]]:
        """Build the leaf record dict for one ``(q_acc, aspect)`` pair."""
        from protea.core._knn_transfer_runner import _LeafInputs

        runner = self.runner
        go_map = runner.ref_by_aspect[ctx.aspect]["go_map"]
        nbs = runner.neighbors_by_aspect[ctx.aspect]
        centroid_unit, nmat = runner.neighbor_info.get(  # type: ignore[assignment]
            (ctx.q_acc, ctx.aspect), (None, None)
        )
        leaf_by_gid: dict[str, dict[str, Any]] = {}
        seen_terms: set[int] = set()
        for ref_acc, distance in nbs[ctx.q_idx]:
            for ann in go_map.get(ref_acc, []):
                go_term_id = ann["go_term_id"]
                if go_term_id in seen_terms:
                    continue
                seen_terms.add(go_term_id)
                go_id = runner.go_id_map.get(go_term_id)
                if not go_id:
                    continue
                pf = ctx.q_pairs_features.get(ref_acc, {})
                inputs = _LeafInputs(
                    q_acc=ctx.q_acc,
                    go_id=go_id,
                    go_term_id=go_term_id,
                    ref_acc=ref_acc,
                    distance=distance,
                    ann=ann,
                    pf=pf,
                    centroid_unit=centroid_unit,
                    nmat=nmat,
                    q_known_cent=ctx.q_known_cent,
                    q_known_mat=ctx.q_known_mat,
                    q_known_n=ctx.q_known_n,
                    q_pca_row=ctx.q_pca_row,
                )
                leaf_by_gid[go_id] = self.make_leaf_record(inputs)
        return leaf_by_gid

    def make_leaf_record(self, inputs: _LeafInputs) -> dict[str, Any]:
        """Materialise the per-(query, candidate-GO) record dictionary."""
        runner = self.runner
        anc_cos, anc_maxcos, anc_has, anc_q_cos, anc_q_maxcos = self._anc2vec_features(
            inputs.go_id,
            inputs.centroid_unit,
            inputs.nmat,
            inputs.q_known_cent,
            inputs.q_known_mat,
        )
        term_aspect = runner.aspect_map.get(inputs.go_term_id, "")
        label = 1 if (inputs.q_acc, inputs.go_id) in runner.gt_pairs else 0
        vote_count = runner.rr_vote_count.get(inputs.q_acc, {}).get(inputs.go_term_id, 1)
        rec: dict[str, Any] = {
            "protein_accession": inputs.q_acc,
            "go_id": inputs.go_id,
            "aspect": term_aspect,
            LABEL_COLUMN: label,
            "distance": inputs.distance,
            "ref_protein_accession": inputs.ref_acc,
            "qualifier": inputs.ann.get("qualifier") or "",
            "evidence_code": inputs.ann.get("evidence_code") or "",
        }
        rec.update(self._alignment_fields(inputs.pf))
        rec.update(self._taxonomy_fields(inputs.pf))
        rec.update(
            self._reranker_fields(
                inputs.q_acc,
                inputs.go_term_id,
                inputs.ref_acc,
                inputs.distance,
                vote_count,
            )
        )
        rec.update(
            self._anc2vec_fields(
                anc_cos, anc_maxcos, anc_has, anc_q_cos, anc_q_maxcos, inputs.q_known_n
            )
        )
        rec.update(self._tax_consensus_fields(inputs.q_acc, inputs.go_term_id, vote_count))
        rec.update(
            {f"emb_pca_query_{i}": inputs.q_pca_row[i] for i in range(EMBEDDING_PCA_DIM)}
        )
        return rec

    @staticmethod
    def _alignment_fields(pf: dict[str, Any]) -> dict[str, Any]:
        """Extract the 12 NW + SW + length alignment fields from pair_features."""
        return {
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
        }

    @staticmethod
    def _taxonomy_fields(pf: dict[str, Any]) -> dict[str, Any]:
        return {
            "taxonomic_distance": pf.get("taxonomic_distance"),
            "taxonomic_common_ancestors": pf.get("taxonomic_common_ancestors"),
            "taxonomic_relation": pf.get("taxonomic_relation", ""),
        }

    def _reranker_fields(
        self,
        q_acc: str,
        go_term_id: int,
        ref_acc: str,
        distance: float,
        vote_count: int,
    ) -> dict[str, Any]:
        runner = self.runner
        return {
            "vote_count": vote_count,
            "k_position": runner.rr_k_position.get(q_acc, {}).get(go_term_id, 1),
            "go_term_frequency": runner.go_term_freq.get(go_term_id, 0),
            "ref_annotation_density": runner.ref_ann_density.get(ref_acc, 0),
            "neighbor_distance_std": runner.rr_distance_std.get(q_acc, 0.0),
            "neighbor_vote_fraction": vote_count / runner.k_limit,
            "neighbor_min_distance": runner.rr_vote_min_d.get(q_acc, {}).get(
                go_term_id, float(distance)
            ),
            "neighbor_mean_distance": (
                runner.rr_vote_sum_d.get(q_acc, {}).get(go_term_id, float(distance))
                / max(1, vote_count)
            ),
        }

    @staticmethod
    def _anc2vec_fields(
        anc_cos: float,
        anc_maxcos: float,
        anc_has: float,
        anc_q_cos: float,
        anc_q_maxcos: float,
        q_known_n: int,
    ) -> dict[str, Any]:
        return {
            "anc2vec_neighbor_cos": anc_cos,
            "anc2vec_neighbor_maxcos": anc_maxcos,
            "anc2vec_has_emb": anc_has,
            "anc2vec_query_known_cos": anc_q_cos,
            "anc2vec_query_known_maxcos": anc_q_maxcos,
            "anc2vec_query_known_count": float(q_known_n),
        }

    def _tax_consensus_fields(
        self, q_acc: str, go_term_id: int, vote_count: int
    ) -> dict[str, Any]:
        return {
            "tax_voters_same_frac": self._tax_same_frac(q_acc, go_term_id, vote_count),
            "tax_voters_close_frac": self._tax_close_frac(q_acc, go_term_id, vote_count),
            "tax_voters_mean_common_ancestors": self._tax_ca_mean(q_acc, go_term_id),
        }

    def _anc2vec_features(
        self,
        go_id: str,
        centroid_unit: np.ndarray | None,
        nmat: np.ndarray | None,
        q_known_cent: np.ndarray | None,
        q_known_mat: np.ndarray | None,
    ) -> tuple[float, float, float, float, float]:
        """Return ``(neighbor_cos, neighbor_maxcos, has_emb, q_cos, q_maxcos)``."""
        runner = self.runner
        cand_i = runner.idx_of_go.get(go_id, -1)
        if cand_i < 0 or not runner.has_emb_mask[cand_i]:
            return float("nan"), float("nan"), 0.0, float("nan"), float("nan")
        cand_vec = runner.all_norm[cand_i]
        anc_cos = (
            float(cand_vec @ centroid_unit)
            if centroid_unit is not None
            else float("nan")
        )
        anc_maxcos = (
            float((nmat @ cand_vec).max()) if nmat is not None else float("nan")
        )
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
        return anc_cos, anc_maxcos, 1.0, anc_q_cos, anc_q_maxcos

    def _tax_same_frac(self, q_acc: str, go_term_id: int, vote_count: int) -> float:
        runner = self.runner
        if not runner.do_taxonomy:
            return float("nan")
        return runner.tax_same_cnt.get(q_acc, {}).get(go_term_id, 0) / max(1, vote_count)

    def _tax_close_frac(self, q_acc: str, go_term_id: int, vote_count: int) -> float:
        runner = self.runner
        if not runner.do_taxonomy:
            return float("nan")
        return runner.tax_close_cnt.get(q_acc, {}).get(go_term_id, 0) / max(1, vote_count)

    def _tax_ca_mean(self, q_acc: str, go_term_id: int) -> float:
        runner = self.runner
        if not runner.do_taxonomy:
            return float("nan")
        n = runner.tax_ca_n.get(q_acc, {}).get(go_term_id, 0)
        if n <= 0:
            return float("nan")
        return runner.tax_ca_sum.get(q_acc, {}).get(go_term_id, 0.0) / max(1, n)

    # ── ancestor expansion (per (q_acc, aspect) group) ────────────────

    def _ancestors(self, gid: str) -> set[str]:
        runner = self.runner
        cached = runner.ancestor_closure.get(gid)
        if cached is not None:
            return cached
        seen: set[str] = set()
        stack = [gid]
        while stack:
            node = stack.pop()
            for parent in (runner.parent_map_str or {}).get(node, ()):
                if parent not in seen:
                    seen.add(parent)
                    stack.append(parent)
        runner.ancestor_closure[gid] = seen
        return seen

    def _ia_weight(self, anc_gid: str, leaf_gid: str) -> float:
        runner = self.runner
        if not runner.ia_weights:
            return 1.0
        anc_w = float(runner.ia_weights.get(anc_gid, 0.0))
        leaf_w = float(runner.ia_weights.get(leaf_gid, 0.0))
        if leaf_w <= 0.0:
            return 1.0
        return anc_w / leaf_w

    def expand_ancestors(
        self,
        q_acc: str,
        leaf_by_gid: dict[str, dict[str, Any]],
    ) -> dict[str, dict[str, Any]]:
        """Synthesise records for every ancestor of each predicted leaf.

        Neighbor-vote-fraction is additively weighted by IA(ancestor)/
        IA(leaf) when an IA table is provided (else weight = 1). The
        closest leaf donates its per-pair features (distance, alignment,
        taxonomy) to the synthesised ancestor record.
        """
        synth: dict[str, dict[str, Any]] = {}
        if not self.runner.expand:
            return synth
        for leaf_gid, leaf_rec in list(leaf_by_gid.items()):
            self._expand_one_leaf(q_acc, leaf_gid, leaf_rec, leaf_by_gid, synth)
        return synth

    def _expand_one_leaf(
        self,
        q_acc: str,
        leaf_gid: str,
        leaf_rec: dict[str, Any],
        leaf_by_gid: dict[str, dict[str, Any]],
        synth: dict[str, dict[str, Any]],
    ) -> None:
        """Propagate one leaf's votes / distance into ``synth`` + leaf_by_gid."""
        runner = self.runner
        leaf_d = float(leaf_rec.get("distance", 1.0))
        for anc in self._ancestors(leaf_gid):
            w = self._ia_weight(anc, leaf_gid)
            if anc in leaf_by_gid:
                leaf_anc = leaf_by_gid[anc]
                leaf_anc["neighbor_vote_fraction"] = min(
                    1.0,
                    float(leaf_anc.get("neighbor_vote_fraction", 0.0))
                    + w / runner.k_limit_f,
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
                base[LABEL_COLUMN] = 1 if (q_acc, anc) in runner.gt_pairs else 0
                prior_frac = (
                    float(entry["neighbor_vote_fraction"]) if entry is not None else 0.0
                )
                base["neighbor_vote_fraction"] = min(1.0, prior_frac + w / runner.k_limit_f)
                synth[anc] = base
            else:
                entry["neighbor_vote_fraction"] = min(
                    1.0,
                    float(entry["neighbor_vote_fraction"]) + w / runner.k_limit_f,
                )
