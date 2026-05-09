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
    _build_skipped_outcome,
    _TestQueryInputs,
    _TestSequences,
    _TestSplitContext,
    _TrainSplitContext,
    _TrainSplitOutcome,
)
from protea.core.training_dump_helpers import (
    _compute_test_cat_membership,
    _emit_split_skipped,
    _knn_and_filter_to_pivot,
    _label_and_write_train_split_shards,
    _label_test_split_per_category,
    _load_sequences,
    _load_taxonomy_ids,
    _load_test_sequences_and_taxonomy,
    _prepare_split_query_inputs,
    _prepare_test_query_inputs,
    _resolve_train_split_eval,
    _run_test_split,
    _run_train_split,
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


# ---------------------------------------------------------------------------
# Train-side helpers (T2B.5 partial #6)
# ---------------------------------------------------------------------------


def _make_train_split_context(
    *,
    train_versions: list[int] | None = None,
    version_to_set: dict[int, uuid.UUID] | None = None,
    acc_to_idx: dict[str, int] | None = None,
    embedding_pool: np.ndarray | None = None,
    go_id_map: dict | None = None,
    aspect_map: dict | None = None,
    pivot_go_ids: set[str] | None = None,
    tmp_dir: Path | None = None,
    keep_cols: list[str] | None = None,
) -> _TrainSplitContext:
    payload = MagicMock(
        train_versions=train_versions or [170, 200, 230],
        compute_alignments=False,
        compute_taxonomy=False,
        expand_votes_to_ancestors=False,
    )
    return _TrainSplitContext(
        payload=payload,
        version_to_set=version_to_set or {170: uuid.uuid4(), 200: uuid.uuid4(), 230: uuid.uuid4()},
        embedding_pool=embedding_pool if embedding_pool is not None else np.zeros((0, 4), dtype=np.float16),
        all_accessions=list((acc_to_idx or {}).keys()),
        acc_to_idx=acc_to_idx or {},
        go_id_map=go_id_map or {},
        aspect_map=aspect_map or {},
        parent_map=None,
        ia_weights=None,
        pca_state=None,
        pivot_go_ids=pivot_go_ids or set(),
        keep_cols=keep_cols or ["protein_accession", "go_id", "aspect"],
        tmp_dir=tmp_dir or Path("/tmp"),
    )


class TestBuildSkippedOutcome:
    def test_returns_skipped_outcome_with_reason(self) -> None:
        outcome = _build_skipped_outcome(170, 200, "no ground truth")
        assert isinstance(outcome, _TrainSplitOutcome)
        assert outcome.skipped is True
        assert outcome.split_files == {}
        assert outcome.stats == {
            "v_old": 170,
            "v_new": 200,
            "skipped": True,
            "reason": "no ground truth",
        }


class TestEmitSplitSkipped:
    def test_emits_warning_with_split_and_reason(self) -> None:
        emit = MagicMock()
        _emit_split_skipped(emit, 3, "no query embeddings")
        emit.assert_called_once_with(
            "dump_helper.split_skipped",
            None,
            {"split": 3, "reason": "no query embeddings"},
            "warning",
        )


class TestResolveTrainSplitEval:
    def test_returns_eval_data_for_existing_pair(self) -> None:
        old_id = uuid.uuid4()
        new_id = uuid.uuid4()
        ctx = _make_train_split_context(version_to_set={170: old_id, 200: new_id})
        eset = MagicMock()
        eval_data = MagicMock()
        session = MagicMock()
        session.query.return_value.filter_by.return_value.one_or_none.return_value = eset
        with patch(
            "protea.core.training_dump_helpers.load_evaluation_data_for_set",
            return_value=(eval_data, "pivot"),
        ):
            out = _resolve_train_split_eval(session, ctx, 170, 200)
        assert out is eval_data

    def test_missing_eset_raises_runtime_error(self) -> None:
        ctx = _make_train_split_context(
            version_to_set={170: uuid.uuid4(), 200: uuid.uuid4()}
        )
        session = MagicMock()
        session.query.return_value.filter_by.return_value.one_or_none.return_value = None
        with pytest.raises(RuntimeError, match="EvaluationSet missing for train pair"):
            _resolve_train_split_eval(session, ctx, 170, 200)


class TestPrepareSplitQueryInputs:
    def test_filters_unknown_accessions_and_slices_pool(self) -> None:
        acc_to_idx = {"P1": 0, "P2": 1}
        pool = np.arange(8, dtype=np.float16).reshape(2, 4)
        ctx = _make_train_split_context(acc_to_idx=acc_to_idx, embedding_pool=pool)
        ref_stub: dict[str, dict[str, list]] = {a: {"accessions": []} for a in ("P", "F", "C")}
        with patch(
            "protea.core.training_dump_helpers._build_reference_from_cache",
            return_value=ref_stub,
        ):
            out = _prepare_split_query_inputs(
                MagicMock(), ctx, uuid.uuid4(), {"P1", "P2", "P_unknown"}, MagicMock()
            )
        assert isinstance(out, _TestQueryInputs)
        assert set(out.valid) == {"P1", "P2"}
        assert out.emb.shape == (2, 4)
        assert out.emb.dtype == np.float32
        assert out.ref_by_aspect is ref_stub

    def test_returns_empty_emb_when_no_overlap(self) -> None:
        ctx = _make_train_split_context(
            acc_to_idx={}, embedding_pool=np.empty((0, 4), dtype=np.float16)
        )
        ref_stub: dict[str, dict[str, list]] = {a: {"accessions": []} for a in ("P", "F", "C")}
        with patch(
            "protea.core.training_dump_helpers._build_reference_from_cache",
            return_value=ref_stub,
        ):
            out = _prepare_split_query_inputs(
                MagicMock(), ctx, uuid.uuid4(), {"P1"}, MagicMock()
            )
        assert out.valid == []
        assert out.emb.shape == (0, 4)


class TestKnnAndFilterToPivot:
    def test_filters_predictions_to_pivot_universe(self) -> None:
        ctx = _make_train_split_context(pivot_go_ids={"GO:0001", "GO:0002"})
        q_inputs = _TestQueryInputs(
            ref_by_aspect={a: {"accessions": []} for a in ("P", "F", "C")},
            valid=["P1"],
            emb=np.zeros((1, 4), dtype=np.float32),
        )
        sequences = _TestSequences(None, None, None, None)
        eval_data = MagicMock(known={})
        raw_preds = [
            {"protein_accession": "P1", "go_id": "GO:0001", "aspect": "P"},
            {"protein_accession": "P1", "go_id": "GO:9999", "aspect": "P"},  # outside pivot
            {"protein_accession": "P1", "go_id": "GO:0002", "aspect": "C"},
        ]
        with patch(
            "protea.core.training_dump_helpers._knn_transfer_and_label",
            return_value=raw_preds,
        ):
            out = _knn_and_filter_to_pivot(MagicMock(), ctx, q_inputs, eval_data, sequences)
        assert {r["go_id"] for r in out} == {"GO:0001", "GO:0002"}


class TestLabelAndWriteTrainSplitShards:
    def test_writes_one_shard_per_cat_with_data(self, tmp_path: Path) -> None:
        ctx = _make_train_split_context(
            go_id_map={1: "GO:0001", 2: "GO:0002"},
            aspect_map={1: "P", 2: "C"},
            tmp_dir=tmp_path,
            keep_cols=["protein_accession", "go_id", "aspect"],
        )
        eval_data = _make_eval_data(
            nk={"P1": {"GO:0001"}},
            lk={"P2": {"GO:0002"}},
            pk={},
        )
        cat_gt_pairs = {
            "nk": {("P1", "GO:0001")},
            "lk": {("P2", "GO:0002")},
            "pk": set(),
        }
        unlabeled = [
            {"protein_accession": "P1", "go_id": "GO:0001", "aspect": "P"},
            {"protein_accession": "P2", "go_id": "GO:0002", "aspect": "C"},
            {"protein_accession": "P9", "go_id": "GO:0001", "aspect": "P"},  # not in any cat
        ]
        split_stats: dict = {}
        cat_paths = _label_and_write_train_split_shards(
            unlabeled, ctx, cat_gt_pairs, eval_data, split_index=0, split_stats=split_stats
        )
        assert "nk" in cat_paths and cat_paths["nk"].exists()
        assert "lk" in cat_paths and cat_paths["lk"].exists()
        assert "pk" not in cat_paths
        assert split_stats["nk_positives"] == 1
        assert split_stats["lk_positives"] == 1
        assert split_stats["pk_positives"] == 0


class TestRunTrainSplit:
    def _patch_targets(self):
        return {
            "_resolve_train_split_eval": "protea.core.training_dump_helpers._resolve_train_split_eval",
            "_prepare_split_query_inputs": "protea.core.training_dump_helpers._prepare_split_query_inputs",
            "_load_test_sequences_and_taxonomy": "protea.core.training_dump_helpers._load_test_sequences_and_taxonomy",
            "_knn_and_filter_to_pivot": "protea.core.training_dump_helpers._knn_and_filter_to_pivot",
            "_label_and_write_train_split_shards": "protea.core.training_dump_helpers._label_and_write_train_split_shards",
        }

    def test_skipped_when_no_ground_truth(self) -> None:
        ctx = _make_train_split_context(train_versions=[170, 200])
        emit = MagicMock()
        eval_data = _make_eval_data()  # all empty
        with patch(self._patch_targets()["_resolve_train_split_eval"], return_value=eval_data):
            outcome = _run_train_split(MagicMock(), ctx, 0, emit)
        assert outcome.skipped is True
        assert outcome.stats["reason"] == "no ground truth"
        assert outcome.split_files == {}
        # split_skipped warning is emitted.
        skip_calls = [c for c in emit.call_args_list if c.args[0] == "dump_helper.split_skipped"]
        assert len(skip_calls) == 1

    def test_skipped_when_no_valid_query_embeddings(self) -> None:
        ctx = _make_train_split_context(train_versions=[170, 200], acc_to_idx={})
        emit = MagicMock()
        eval_data = _make_eval_data(nk={"P_unknown": {"GO:0001"}})
        empty_q = _TestQueryInputs(
            ref_by_aspect={a: {"accessions": []} for a in ("P", "F", "C")},
            valid=[],
            emb=np.empty((0, 4), dtype=np.float32),
        )
        with patch(
            self._patch_targets()["_resolve_train_split_eval"], return_value=eval_data
        ), patch(
            self._patch_targets()["_prepare_split_query_inputs"], return_value=empty_q
        ):
            outcome = _run_train_split(MagicMock(), ctx, 0, emit)
        assert outcome.skipped is True
        assert outcome.stats["reason"] == "no query embeddings"

    def test_full_run_returns_split_outcome(self, tmp_path: Path) -> None:
        ctx = _make_train_split_context(
            train_versions=[170, 200],
            acc_to_idx={"P1": 0},
            embedding_pool=np.zeros((1, 4), dtype=np.float16),
            tmp_dir=tmp_path,
        )
        emit = MagicMock()
        eval_data = _make_eval_data(nk={"P1": {"GO:0001"}})
        q = _TestQueryInputs(
            ref_by_aspect={a: {"accessions": []} for a in ("P", "F", "C")},
            valid=["P1"],
            emb=np.zeros((1, 4), dtype=np.float32),
        )
        sequences = _TestSequences(None, None, None, None)
        cat_paths = {"nk": tmp_path / "train_nk_split0.parquet"}
        cat_paths["nk"].write_text("dummy")
        targets = self._patch_targets()
        with patch(targets["_resolve_train_split_eval"], return_value=eval_data), patch(
            targets["_prepare_split_query_inputs"], return_value=q
        ), patch(
            targets["_load_test_sequences_and_taxonomy"], return_value=sequences
        ), patch(
            targets["_knn_and_filter_to_pivot"], return_value=[{"go_id": "GO:0001"}]
        ), patch(
            targets["_label_and_write_train_split_shards"], return_value=cat_paths
        ):
            outcome = _run_train_split(MagicMock(), ctx, 0, emit)
        assert outcome.skipped is False
        assert outcome.split_files == cat_paths
        assert outcome.stats["skipped"] is False
        assert outcome.stats["total_unlabeled"] == 1


# Keep pytest happy when only some optional helpers are exercised.
_pytest_ref = pytest
