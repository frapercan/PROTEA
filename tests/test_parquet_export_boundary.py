"""T1.8 boundary validation tests for parquet_export.

Pins the boundary invariant: shards written to disk must contain
exactly the canonical ``ALL_FEATURES`` columns (plus reserved
columns). Missing or unknown feature columns must raise instead of
silently shipping a partial dump that future LightGBM training would
choke on.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from protea_contracts import ALL_FEATURES


def _full_feature_row() -> dict[str, object]:
    """Build a single shard row with reserved cols + every ALL_FEATURES col.

    Per-shard reserved columns differ from the merged-dump reserved set:
    shards carry ``go_id`` and ``aspect``; ``category``, ``snapshot_pair``
    and the rename ``go_id->go_term_id`` are added by ``export_reranker_parquets``.
    """
    row: dict[str, object] = {
        "protein_accession": "P12345",
        "go_id": "GO:0000001",
        "label": 0,
        "aspect": "P",
    }
    for col in ALL_FEATURES:
        if col in row:
            continue
        if col in {"qualifier", "evidence_code", "taxonomic_relation"}:
            row[col] = "x"
        else:
            row[col] = 0.0
    return row


def _write_shard(path: Path, rows: list[dict[str, object]]) -> Path:
    df = pd.DataFrame(rows) if rows else pd.DataFrame()
    df.to_parquet(path, index=False, compression="snappy")
    return path


def _call_export(
    stage_dir: Path,
    train_rows: list[dict[str, object]],
    eval_rows: list[dict[str, object]],
) -> dict[str, object]:
    from protea.core.parquet_export import export_reranker_parquets

    train_shard = _write_shard(stage_dir / "_train_nk.parquet", train_rows)
    eval_shard = _write_shard(stage_dir / "_eval_nk.parquet", eval_rows)
    return export_reranker_parquets(
        stage_dir=stage_dir,
        split_files={"nk": [train_shard]},
        valid_split_versions=[(220, 221)],
        test_files={"nk": eval_shard},
        test_old_v=221,
        test_new_v=222,
        name="t18-test",
        k=5,
        embedding_config_id="00000000-0000-0000-0000-000000000001",
        ontology_snapshot_id="00000000-0000-0000-0000-000000000002",
        annotation_source="goa",
        store=None,
        producer_version="t18",
        producer_git_sha=None,
        validate_with_contracts=False,
    )


class TestExportBoundaryInvariant:
    def test_full_columns_passes(self, tmp_path: Path) -> None:
        result = _call_export(tmp_path, [_full_feature_row()], [_full_feature_row()])
        assert (tmp_path / "manifest.json").exists()
        assert "schema_sha" in result

    def test_missing_feature_column_in_train_raises(self, tmp_path: Path) -> None:
        row = _full_feature_row()
        del row["distance"]  # drop one canonical feature
        with pytest.raises(ValueError, match="canonical column invariant"):
            _call_export(tmp_path, [row], [_full_feature_row()])

    def test_missing_feature_column_in_eval_raises(self, tmp_path: Path) -> None:
        row = _full_feature_row()
        del row["k_position"]
        with pytest.raises(ValueError, match="canonical column invariant"):
            _call_export(tmp_path, [_full_feature_row()], [row])

    def test_typo_feature_name_raises(self, tmp_path: Path) -> None:
        row = _full_feature_row()
        del row["distance"]
        row["distnace"] = 0.0  # typo: not in ALL_FEATURES
        with pytest.raises(ValueError, match="canonical column invariant"):
            _call_export(tmp_path, [row], [_full_feature_row()])

    def test_empty_eval_does_not_trigger(self, tmp_path: Path) -> None:
        # Empty eval shard skipped by the writer; the invariant only
        # gates non-empty data.
        result = _call_export(tmp_path, [_full_feature_row()], [])
        assert (tmp_path / "manifest.json").exists()
        assert int(result["n_eval_rows"]) == 0
