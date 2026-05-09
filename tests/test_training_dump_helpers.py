"""Unit tests for protea.core.training_dump_helpers.

Covers the module-level helpers (``_load_sequences``,
``_load_taxonomy_ids``) that ``TrainRerankerAutoOperation`` uses to
drive the dataset-export pipeline. Heavy DB / model training is no
longer tested here: LightGBM training lives in protea-reranker-lab.
"""

from __future__ import annotations

import uuid
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from protea.core._training_dump_loaders import (
    _TestQueryInputs,
    _TestSequences,
    _TestSplitContext,
)
from protea.core.training_dump_helpers import (
    _compute_test_cat_membership,
    _label_test_split_per_category,
    _load_sequences,
    _load_taxonomy_ids,
    _load_test_sequences_and_taxonomy,
    _prepare_test_query_inputs,
    _run_test_split,
    _write_labeled_test_batches,
)

# ---------------------------------------------------------------------------
# _load_sequences (used by TrainRerankerAutoOperation in dump_only mode)
# ---------------------------------------------------------------------------


class TestLoadSequences:
    def test_returns_dict(self):
        session = MagicMock()
        session.query.return_value.join.return_value.filter.return_value.all.return_value = [
            ("P1", "MKVLWAGS"),
            ("P2", "ACDEF"),
        ]

        result = _load_sequences(session, {"P1", "P2"})
        assert result == {"P1": "MKVLWAGS", "P2": "ACDEF"}

    def test_empty_accessions(self):
        session = MagicMock()
        result = _load_sequences(session, set())
        assert result == {}


# ---------------------------------------------------------------------------
# _load_taxonomy_ids (used by TrainRerankerAutoOperation in dump_only mode)
# ---------------------------------------------------------------------------


class TestLoadTaxonomyIds:
    def test_returns_dict(self):
        session = MagicMock()
        session.query.return_value.filter.return_value.all.return_value = [
            ("P1", 9606),
            ("P2", 10090),
        ]

        result = _load_taxonomy_ids(session, {"P1", "P2"})
        assert result == {"P1": 9606, "P2": 10090}

    def test_none_taxonomy_id(self):
        session = MagicMock()
        session.query.return_value.filter.return_value.all.return_value = [
            ("P1", None),
        ]

        result = _load_taxonomy_ids(session, {"P1"})
        assert result == {"P1": None}


# Import np to keep the test module's structure consistent with other test
# files even though the explicit numpy assertions used by the previous
# TrainReranker test surface have been removed.
_ = np


# ---------------------------------------------------------------------------
# _compute_test_cat_membership (T2B.5 partial #5)
# ---------------------------------------------------------------------------


def _make_eval_data(nk=None, lk=None, pk=None) -> MagicMock:
    eval_data = MagicMock()
    eval_data.nk = nk or {}
    eval_data.lk = lk or {}
    eval_data.pk = pk or {}
    return eval_data


class TestComputeTestCatMembership:
    def test_resolves_protein_aspect_pairs_from_eval_data(self) -> None:
        # go_id_map: term_id -> go_id; aspect_map: term_id -> aspect.
        # GO:0001 is BP, GO:0002 is CC; GO:0003 has no aspect (skipped).
        go_id_map = {1: "GO:0001", 2: "GO:0002", 3: "GO:0003"}
        aspect_map = {1: "P", 2: "C"}
        eval_data = _make_eval_data(
            nk={"P1": {"GO:0001"}},
            lk={"P2": {"GO:0002", "GO:0003"}},
            pk={"P3": {"GO:0001", "GO:0002"}},
        )
        membership = _compute_test_cat_membership(eval_data, go_id_map, aspect_map)
        assert membership["nk"] == {("P1", "P")}
        # GO:0003 is skipped because it has no aspect mapping.
        assert membership["lk"] == {("P2", "C")}
        assert membership["pk"] == {("P3", "P"), ("P3", "C")}

    def test_empty_categories_produce_empty_sets(self) -> None:
        membership = _compute_test_cat_membership(_make_eval_data(), {}, {})
        assert membership == {"nk": set(), "lk": set(), "pk": set()}


# ---------------------------------------------------------------------------
# _load_test_sequences_and_taxonomy (T2B.5 partial #5)
# ---------------------------------------------------------------------------


class TestLoadTestSequencesAndTaxonomy:
    def test_returns_all_none_when_no_flags_set(self) -> None:
        payload = MagicMock(compute_alignments=False, compute_taxonomy=False)
        out = _load_test_sequences_and_taxonomy(MagicMock(), payload, ["P1"], {})
        assert out == _TestSequences(None, None, None, None)

    def test_loads_only_alignments_when_taxonomy_disabled(self) -> None:
        payload = MagicMock(compute_alignments=True, compute_taxonomy=False)
        test_ref = {
            "P": {"accessions": ["R1", "R2"]},
            "F": {"accessions": ["R3"]},
            "C": {"accessions": []},
        }
        with patch(
            "protea.core.training_dump_helpers._load_sequences",
            side_effect=[{"P1": "M"}, {"R1": "AA", "R2": "BB", "R3": "CC"}],
        ) as mock_seq, patch(
            "protea.core.training_dump_helpers._load_taxonomy_ids"
        ) as mock_tax:
            out = _load_test_sequences_and_taxonomy(
                MagicMock(), payload, ["P1"], test_ref
            )
        assert out.query_sequences == {"P1": "M"}
        assert out.ref_sequences == {"R1": "AA", "R2": "BB", "R3": "CC"}
        assert out.query_tax_ids is None
        assert out.ref_tax_ids is None
        assert mock_seq.call_count == 2
        mock_tax.assert_not_called()

    def test_loads_taxonomy_when_only_taxonomy_enabled(self) -> None:
        payload = MagicMock(compute_alignments=False, compute_taxonomy=True)
        test_ref = {a: {"accessions": []} for a in ("P", "F", "C")}
        with patch(
            "protea.core.training_dump_helpers._load_sequences"
        ) as mock_seq, patch(
            "protea.core.training_dump_helpers._load_taxonomy_ids",
            side_effect=[{"P1": 9606}, {}],
        ) as mock_tax:
            out = _load_test_sequences_and_taxonomy(
                MagicMock(), payload, ["P1"], test_ref
            )
        assert out.query_tax_ids == {"P1": 9606}
        assert out.ref_tax_ids == {}
        assert out.query_sequences is None
        assert out.ref_sequences is None
        mock_seq.assert_not_called()
        assert mock_tax.call_count == 2


# ---------------------------------------------------------------------------
# _prepare_test_query_inputs (T2B.5 partial #5)
# ---------------------------------------------------------------------------


def _make_test_split_context(
    *,
    test_all_queries: set[str],
    acc_to_idx: dict[str, int],
    embedding_pool: np.ndarray,
    test_eval_data: MagicMock | None = None,
    test_cat_gt: dict | None = None,
    tmp_dir: Path | None = None,
    keep_cols: list[str] | None = None,
) -> _TestSplitContext:
    return _TestSplitContext(
        payload=MagicMock(
            compute_alignments=False,
            compute_taxonomy=False,
            expand_votes_to_ancestors=False,
        ),
        test_eval_data=test_eval_data or _make_eval_data(),
        test_cat_gt=test_cat_gt or {"nk": set(), "lk": set(), "pk": set()},
        test_all_queries=test_all_queries,
        test_old_set_id=uuid.uuid4(),
        embedding_pool=embedding_pool,
        all_accessions=list(acc_to_idx.keys()),
        acc_to_idx=acc_to_idx,
        go_id_map={},
        aspect_map={},
        parent_map=None,
        ia_weights=None,
        pca_state=None,
        pivot_go_ids=set(),
        keep_cols=keep_cols or ["protein_accession", "go_id", "aspect"],
        tmp_dir=tmp_dir or Path("/tmp"),
    )


class TestPrepareTestQueryInputs:
    def test_filters_unknown_accessions_and_slices_pool(self) -> None:
        # P3 is in test_all_queries but not in acc_to_idx — must be dropped.
        acc_to_idx = {"P1": 0, "P2": 1}
        pool = np.arange(8, dtype=np.float16).reshape(2, 4)
        ctx = _make_test_split_context(
            test_all_queries={"P1", "P2", "P3"},
            acc_to_idx=acc_to_idx,
            embedding_pool=pool,
        )
        ref_stub = {a: {"accessions": []} for a in ("P", "F", "C")}
        with patch(
            "protea.core.training_dump_helpers._build_reference_from_cache",
            return_value=ref_stub,
        ):
            out = _prepare_test_query_inputs(MagicMock(), ctx, MagicMock())
        assert isinstance(out, _TestQueryInputs)
        assert set(out.valid) == {"P1", "P2"}
        assert out.emb.shape == (2, 4)
        assert out.emb.dtype == np.float32
        assert out.ref_by_aspect is ref_stub

    def test_returns_empty_emb_when_no_overlap(self) -> None:
        acc_to_idx: dict[str, int] = {}
        pool = np.empty((0, 4), dtype=np.float16)
        ctx = _make_test_split_context(
            test_all_queries={"P1"},
            acc_to_idx=acc_to_idx,
            embedding_pool=pool,
        )
        ref_stub = {a: {"accessions": []} for a in ("P", "F", "C")}
        with patch(
            "protea.core.training_dump_helpers._build_reference_from_cache",
            return_value=ref_stub,
        ):
            out = _prepare_test_query_inputs(MagicMock(), ctx, MagicMock())
        assert out.valid == []
        assert out.emb.shape == (0, 4)


# ---------------------------------------------------------------------------
# _run_test_split (T2B.5 partial #5)
# ---------------------------------------------------------------------------


class TestRunTestSplit:
    def test_returns_none_dict_when_no_queries(self) -> None:
        ctx = _make_test_split_context(
            test_all_queries=set(),
            acc_to_idx={},
            embedding_pool=np.empty((0, 4), dtype=np.float16),
        )
        out = _run_test_split(MagicMock(), ctx, MagicMock())
        assert out == {"nk": None, "lk": None, "pk": None}

    def test_short_circuits_when_no_valid_queries(self, tmp_path: Path) -> None:
        # test_all_queries non-empty, but acc_to_idx is empty so q_inputs.valid == [].
        ctx = _make_test_split_context(
            test_all_queries={"P_unknown"},
            acc_to_idx={},
            embedding_pool=np.empty((0, 4), dtype=np.float16),
            tmp_dir=tmp_path,
        )
        ref_stub = {a: {"accessions": []} for a in ("P", "F", "C")}
        with patch(
            "protea.core.training_dump_helpers._build_reference_from_cache",
            return_value=ref_stub,
        ), patch(
            "protea.core.training_dump_helpers._knn_transfer_and_label"
        ) as mock_knn:
            out = _run_test_split(MagicMock(), ctx, MagicMock())
        assert out == {"nk": None, "lk": None, "pk": None}
        mock_knn.assert_not_called()

    def test_skips_label_when_stream_returns_zero_rows(self, tmp_path: Path) -> None:
        ctx = _make_test_split_context(
            test_all_queries={"P1"},
            acc_to_idx={"P1": 0},
            embedding_pool=np.zeros((1, 4), dtype=np.float16),
            tmp_dir=tmp_path,
        )
        ref_stub = {a: {"accessions": []} for a in ("P", "F", "C")}
        with patch(
            "protea.core.training_dump_helpers._build_reference_from_cache",
            return_value=ref_stub,
        ), patch(
            "protea.core.training_dump_helpers._knn_transfer_and_label",
            return_value={"n_rows": 0},
        ), patch(
            "protea.core.training_dump_helpers._label_test_split_per_category"
        ) as mock_label:
            out = _run_test_split(MagicMock(), ctx, MagicMock())
        assert out == {"nk": None, "lk": None, "pk": None}
        mock_label.assert_not_called()


# ---------------------------------------------------------------------------
# _label_test_split_per_category + _write_labeled_test_batches
# (T2B.5 partial #5)
# ---------------------------------------------------------------------------


def _write_unlabeled_parquet(path: Path, rows: list[tuple[str, str, str]]) -> None:
    """Persist a tiny ``(protein_accession, go_id, aspect)`` parquet."""
    table = pa.table(
        {
            "protein_accession": [r[0] for r in rows],
            "go_id": [r[1] for r in rows],
            "aspect": [r[2] for r in rows],
        }
    )
    pq.write_table(table, str(path))


class TestLabelTestSplitPerCategory:
    def test_writes_one_shard_per_category_with_labels(self, tmp_path: Path) -> None:
        unlabeled = tmp_path / "test_unlabeled.parquet"
        _write_unlabeled_parquet(
            unlabeled,
            [
                ("P1", "GO:0001", "P"),  # NK + positive
                ("P1", "GO:0099", "P"),  # NK + negative (not in gt)
                ("P2", "GO:0002", "C"),  # LK + positive
                ("P9", "GO:0001", "P"),  # belongs to no cat
            ],
        )
        ctx = _make_test_split_context(
            test_all_queries={"P1", "P2"},
            acc_to_idx={"P1": 0, "P2": 1},
            embedding_pool=np.zeros((2, 4), dtype=np.float16),
            test_eval_data=_make_eval_data(
                nk={"P1": {"GO:0001"}},
                lk={"P2": {"GO:0002"}},
                pk={},
            ),
            test_cat_gt={
                "nk": {("P1", "GO:0001")},
                "lk": {("P2", "GO:0002")},
                "pk": set(),
            },
            tmp_dir=tmp_path,
            keep_cols=["protein_accession", "go_id", "aspect"],
        )
        # go_id_map / aspect_map: tie GO:0001 -> P, GO:0002 -> C so the
        # membership computation slots P1 into NK, P2 into LK.
        ctx = ctx._replace(
            go_id_map={1: "GO:0001", 2: "GO:0002", 99: "GO:0099"},
            aspect_map={1: "P", 2: "C", 99: "P"},
        )
        test_files: dict[str, Path | None] = {"nk": None, "lk": None, "pk": None}

        _label_test_split_per_category(unlabeled, ctx, test_files)

        # Intermediate parquet was unlinked.
        assert not unlabeled.exists()
        # NK and LK shards exist; PK had no rows so it stays None.
        assert test_files["nk"] is not None and test_files["nk"].exists()
        assert test_files["lk"] is not None and test_files["lk"].exists()
        assert test_files["pk"] is None

        nk_table = pq.read_table(str(test_files["nk"]))
        # Two NK rows: GO:0001 (positive=1) and GO:0099 (negative=0).
        labels = nk_table.column("label").to_pylist()
        assert sorted(labels) == [0, 1]


class TestWriteLabeledTestBatches:
    def test_returns_set_of_cats_with_data(self, tmp_path: Path) -> None:
        unlabeled = tmp_path / "u.parquet"
        _write_unlabeled_parquet(
            unlabeled,
            [("P1", "GO:0001", "P")],
        )
        membership = {
            "nk": {("P1", "P")},
            "lk": set(),
            "pk": set(),
        }
        cat_gt = {"nk": {("P1", "GO:0001")}, "lk": set(), "pk": set()}
        cat_paths = {cat: tmp_path / f"{cat}.parquet" for cat in ("nk", "lk", "pk")}
        pf = pq.ParquetFile(str(unlabeled))
        written = _write_labeled_test_batches(
            pf,
            ["protein_accession", "go_id", "aspect"],
            membership,
            cat_gt,
            cat_paths,
        )
        assert written == {"nk"}
        assert cat_paths["nk"].exists()
        assert not cat_paths["lk"].exists()


# Keep pytest happy when only some optional helpers are exercised.
_ = pytest
