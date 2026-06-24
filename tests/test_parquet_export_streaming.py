"""Streaming-assembly equivalence test for ``parquet_export``.

The final dump assembly was rewritten to stream shards through a
``pyarrow`` ``ParquetWriter`` (one ~200k-row batch resident) instead of
reading every shard into pandas and ``pd.concat``-ing the whole
~76M-row training set (~108 GB committed at the last step of a 6.7h
run). This is the twin of the per-split streaming fix in #654.

The golden gate in :mod:`tests.test_parquet_export_golden` already pins
the byte-exact single-shard output. This test covers the part the
golden gate does NOT: a MULTI-shard, MULTI-category, MULTI-snapshot-pair
assembly. It asserts the streamed ``train.parquet`` / ``eval.parquet``
carry the SAME rows, columns, dtypes, and ORDER that the legacy
``pd.read_parquet`` + ``pd.concat`` path would have produced (the
downstream LightGBM lab + ``schema_sha`` depend on exact equality).
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from protea_contracts import ALL_FEATURES

from protea.core.parquet_export import (
    ParquetExportContext,
    _reorder,
    export_reranker_parquets,
)

_ASPECT_NAMES = {"P": "bpo", "F": "mfo", "C": "cco"}
_RESERVED = [
    "protein_accession",
    "go_term_id",
    "label",
    "category",
    "snapshot_pair",
]


def _row(acc: str, go: str, aspect: str, fill: float) -> dict[str, object]:
    """One synthetic shard row carrying every canonical feature column."""
    row: dict[str, object] = {
        "protein_accession": acc,
        "go_id": go,
        "label": 0,
        "aspect": aspect,
    }
    for col in ALL_FEATURES:
        if col in row:
            continue
        if col in {"qualifier", "evidence_code", "taxonomic_relation"}:
            row[col] = "x"
        else:
            row[col] = fill
    return row


def _write_shard(path: Path, rows: list[dict[str, object]]) -> Path:
    pd.DataFrame(rows).to_parquet(path, index=False, compression="snappy")
    return path


def _legacy_concat(
    shard_specs: list[tuple[Path, str, str]],
) -> pd.DataFrame:
    """Reproduce the pre-streaming pandas assembly for a split."""
    frames: list[pd.DataFrame] = []
    for shard_path, cat, snap_pair in shard_specs:
        sdf = pd.read_parquet(shard_path)
        sdf["category"] = cat
        sdf["snapshot_pair"] = snap_pair
        if "aspect" in sdf.columns:
            sdf["aspect"] = sdf["aspect"].map(_ASPECT_NAMES).fillna(sdf["aspect"])
        sdf = sdf.rename(columns={"go_id": "go_term_id"})
        frames.append(sdf)
    return _reorder(pd.concat(frames, ignore_index=True), _RESERVED)


def _build_multishard_ctx(stage_dir: Path) -> ParquetExportContext:
    """Two snapshot pairs x two cats (train), two cats (eval)."""
    train_nk_0 = _write_shard(
        stage_dir / "_train_nk_0.parquet",
        [_row("P1", "GO:0000001", "P", 0.1), _row("P2", "GO:0000002", "F", 0.2)],
    )
    train_nk_1 = _write_shard(
        stage_dir / "_train_nk_1.parquet",
        [_row("P3", "GO:0000003", "C", 0.3)],
    )
    train_lk_0 = _write_shard(
        stage_dir / "_train_lk_0.parquet",
        [_row("P4", "GO:0000004", "P", 0.4)],
    )
    train_lk_1 = _write_shard(
        stage_dir / "_train_lk_1.parquet",
        [_row("P5", "GO:0000005", "F", 0.5), _row("P6", "GO:0000006", "C", 0.6)],
    )
    eval_nk = _write_shard(
        stage_dir / "_eval_nk.parquet",
        [_row("Q1", "GO:0000007", "P", 0.7)],
    )
    eval_pk = _write_shard(
        stage_dir / "_eval_pk.parquet",
        [_row("Q2", "GO:0000008", "F", 0.8), _row("Q3", "GO:0000009", "C", 0.9)],
    )
    return ParquetExportContext(
        stage_dir=stage_dir,
        split_files={"nk": [train_nk_0, train_nk_1], "lk": [train_lk_0, train_lk_1]},
        valid_split_versions=[(220, 221), (221, 222)],
        test_files={"nk": eval_nk, "lk": None, "pk": eval_pk},
        test_old_v=222,
        test_new_v=223,
        name="stream-equiv",
        k=5,
        embedding_config_id="00000000-0000-0000-0000-000000000001",
        ontology_snapshot_id="00000000-0000-0000-0000-000000000002",
        annotation_source="goa",
        store=None,
        producer_version="stream",
        producer_git_sha="test",
        validate_with_contracts=False,
    )


class TestStreamingAssemblyEquivalence:
    def test_streamed_train_equals_legacy_concat(self, tmp_path: Path) -> None:
        ctx = _build_multishard_ctx(tmp_path)
        result = export_reranker_parquets(ctx)

        produced = pd.read_parquet(tmp_path / "train.parquet")
        expected = _legacy_concat(
            [
                (tmp_path / "_train_nk_0.parquet", "nk", "v220-v221"),
                (tmp_path / "_train_nk_1.parquet", "nk", "v221-v222"),
                (tmp_path / "_train_lk_0.parquet", "lk", "v220-v221"),
                (tmp_path / "_train_lk_1.parquet", "lk", "v221-v222"),
            ]
        )
        pd.testing.assert_frame_equal(produced, expected)
        assert result["n_train_rows"] == len(expected) == 6
        assert result["train_snapshot_pairs"] == ["v220-v221", "v221-v222"]

    def test_streamed_eval_equals_legacy_concat(self, tmp_path: Path) -> None:
        ctx = _build_multishard_ctx(tmp_path)
        result = export_reranker_parquets(ctx)

        produced = pd.read_parquet(tmp_path / "eval.parquet")
        expected = _legacy_concat(
            [
                (tmp_path / "_eval_nk.parquet", "nk", "v222-v223"),
                (tmp_path / "_eval_pk.parquet", "pk", "v222-v223"),
            ]
        )
        pd.testing.assert_frame_equal(produced, expected)
        assert result["n_eval_rows"] == len(expected) == 3

    def test_empty_split_writes_no_file(self, tmp_path: Path) -> None:
        ctx = ParquetExportContext(
            stage_dir=tmp_path,
            split_files={},
            valid_split_versions=[],
            test_files={"nk": None, "lk": None, "pk": None},
            test_old_v=222,
            test_new_v=223,
            name="empty",
            k=5,
            embedding_config_id="00000000-0000-0000-0000-000000000001",
            ontology_snapshot_id="00000000-0000-0000-0000-000000000002",
            annotation_source="goa",
            store=None,
            validate_with_contracts=False,
        )
        result = export_reranker_parquets(ctx)
        assert result["n_train_rows"] == 0
        assert result["n_eval_rows"] == 0
        assert not (tmp_path / "train.parquet").exists()
        assert not (tmp_path / "eval.parquet").exists()
