"""Unit tests for the S3b InterPro-only union into export candidates.

Covers the per-protein index, the ``KNN UNION InterPro`` dedup/aspect
selection, and the InterPro-only record shape: ``knn_present`` False,
KNN-derived features NaN, query-side features kept, GT label propagated,
and all 11 ``interpro_*`` columns populated after the post-pass.
"""

from __future__ import annotations

import math
from types import SimpleNamespace

from protea.core._interpro_features import (
    _InterProMatch,
    apply_interpro_features,
    index_by_protein,
    select_interpro_only_go_ids,
)
from protea.core._knn_transfer_runner import _LeafContext
from protea.core._leaf_record_builder import _LeafRecordBuilder
from protea.core.reranker import EMBEDDING_PCA_DIM, LABEL_COLUMN

# KNN-derived columns that must be NaN (missing) on an InterPro-only row.
_KNN_NAN_FEATURES = (
    "distance",
    "k_position",
    "go_term_frequency",
    "ref_annotation_density",
    "neighbor_distance_std",
    "neighbor_min_distance",
    "neighbor_mean_distance",
    "tax_voters_same_frac",
    "tax_voters_close_frac",
    "tax_voters_mean_common_ancestors",
    "anc2vec_neighbor_cos",
    "anc2vec_neighbor_maxcos",
)


def _fake_builder(
    gt_pairs: set[tuple[str, str]],
    *,
    go_id_map: dict[int, str] | None = None,
    aspect_map: dict[int, str] | None = None,
) -> _LeafRecordBuilder:
    # idx_of_go empty -> _anc2vec_features short-circuits to NaN with no
    # embedding lookup, so no all_norm / has_emb_mask needed.
    runner = SimpleNamespace(
        gt_pairs=gt_pairs,
        idx_of_go={},
        go_id_map=go_id_map or {},
        aspect_map=aspect_map or {},
    )
    return _LeafRecordBuilder(runner)  # type: ignore[arg-type]


def _ctx(q_acc: str, aspect: str) -> _LeafContext:
    return _LeafContext(
        q_idx=0,
        q_acc=q_acc,
        aspect=aspect,
        q_pca_row=[0.1 * i for i in range(EMBEDDING_PCA_DIM)],
        q_known_cent=None,
        q_known_mat=None,
        q_known_n=0,
        q_pairs_features={},
    )


def test_index_by_protein_groups_go_ids() -> None:
    table = {
        ("P1", "GO:1"): _InterProMatch(0.9, 1, frozenset()),
        ("P1", "GO:2"): _InterProMatch(0.5, 1, frozenset()),
        ("P2", "GO:3"): _InterProMatch(0.7, 1, frozenset()),
    }
    assert index_by_protein(table) == {"P1": {"GO:1", "GO:2"}, "P2": {"GO:3"}}


def test_select_interpro_only_dedups_and_filters_aspect() -> None:
    # InterPro predicts B, C (aspect P) and D (aspect F). KNN already has A
    # and B. So the P-aspect InterPro-only set is just C: B is the overlap
    # ("both"), D is the wrong aspect, A is KNN-only.
    go_aspect = {"GO:A": "P", "GO:B": "P", "GO:C": "P", "GO:D": "F"}
    excluded = {"GO:A", "GO:B"}  # KNN leaves + synth ancestors
    out = select_interpro_only_go_ids(["GO:B", "GO:C", "GO:D"], go_aspect, "P", excluded)
    assert out == ["GO:C"]


def test_select_skips_terms_absent_from_aspect_map() -> None:
    # A GO id with no aspect (outside the ontology snapshot) is dropped.
    out = select_interpro_only_go_ids(["GO:X"], {}, "P", set())
    assert out == []


def test_interpro_only_record_flags_label_and_nan() -> None:
    builder = _fake_builder(gt_pairs={("P1", "GO:C")})
    rec = builder.make_interpro_only_record(_ctx("P1", "P"), "GO:C")

    # Union semantics: InterPro-only candidate, not from KNN.
    assert rec["knn_present"] is False
    assert rec["go_id"] == "GO:C"
    assert rec["aspect"] == "P"
    # GT label propagated from the same gt_pairs as KNN candidates.
    assert rec[LABEL_COLUMN] == 1

    # KNN-derived features are NaN (LightGBM reads them as missing).
    for col in _KNN_NAN_FEATURES:
        assert math.isnan(rec[col]), col
    # Alignment columns carry no KNN evidence (null/NaN, no fabricated 0).
    assert rec["identity_nw"] is None

    # Genuine zero-counts: no KNN neighbour voted for this term.
    assert rec["vote_count"] == 0
    assert rec["neighbor_vote_fraction"] == 0.0

    # Query-side features stay real (not KNN-derived).
    assert rec["emb_pca_query_0"] == 0.0
    assert rec[f"emb_pca_query_{EMBEDDING_PCA_DIM - 1}"] == 0.1 * (EMBEDDING_PCA_DIM - 1)

    # The 11 interpro columns are present as defaults pre-post-pass.
    defaults = _LeafRecordBuilder._interpro_default_fields()
    for col in defaults:
        assert col in rec


def test_add_interpro_only_three_unique_candidates_end_to_end() -> None:
    # KNN candidate set {GO:A, GO:B}; InterPro predicts {GO:B, GO:C}.
    # Union -> 3 unique candidates; GO:B is the dedup overlap (KNN row),
    # GO:C is the InterPro-only recall gain.
    builder = _fake_builder(
        gt_pairs={("P1", "GO:C")},
        go_id_map={1: "GO:A", 2: "GO:B", 3: "GO:C"},
        aspect_map={1: "P", 2: "P", 3: "P"},
    )
    # Pre-seed the table cache so the loader does not touch the env.
    table = {
        ("P1", "GO:B"): _InterProMatch(0.4, 1, frozenset()),
        ("P1", "GO:C"): _InterProMatch(0.8, 2, frozenset({"pfam"})),
    }
    builder._interpro_table = table

    # KNN leaves already carry GO:A and GO:B (the "both" overlap); synth
    # (ancestors) is empty in this fixture.
    leaf_by_gid = {
        "GO:A": {"protein_accession": "P1", "go_id": "GO:A", "knn_present": True},
        "GO:B": {"protein_accession": "P1", "go_id": "GO:B", "knn_present": True},
    }
    synth: dict[str, dict] = {}

    builder.add_interpro_only(_ctx("P1", "P"), leaf_by_gid, synth)

    # Union is exactly 3 unique candidates; GO:B was deduped (not doubled).
    assert set(leaf_by_gid) == {"GO:A", "GO:B", "GO:C"}
    assert leaf_by_gid["GO:B"]["knn_present"] is True  # stayed the KNN row

    # Populate interpro_* the way the runner post-pass does.
    apply_interpro_features(list(leaf_by_gid.values()), table)

    rec_c = leaf_by_gid["GO:C"]
    assert rec_c["interpro_present"] is True
    assert rec_c["interpro_hit"] is True
    assert rec_c["interpro_score"] == 0.8
    assert rec_c["interpro_db_pfam"] is True
    assert rec_c["knn_present"] is False
    assert rec_c[LABEL_COLUMN] == 1
    assert math.isnan(rec_c["distance"])
