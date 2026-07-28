"""Tests for the resumable + streaming export rework (F-EXP-RESET).

Covers the two behaviours the existing suites did not:

* **Streaming write keeps the canonical schema.** The minijobs
  assembler streams per-pair shards through a ``pyarrow`` ``ParquetWriter``
  instead of ``pd.concat``-ing the whole split (the ~54 GB write-OOM that
  forced ``PROTEA_EXPORT_MINIJOBS=0``). The streamed ``train.parquet``
  must carry exactly the canonical export schema plus the trailing
  ``snapshot_pair`` column, in order. Schema drift broke this before
  (#649/#650/#653), so this is a regression gate.

* **Resumability skips completed cuts.** The serial dump runner records a
  per-cut done-marker as each snapshot-pair cut finishes and, on a
  re-run, restores completed cuts from their markers instead of
  recomputing from cut 1. A marker whose shard file is missing (a cut
  that died mid-flush) is recomputed, not trusted.
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.parquet as pq

from protea.core._export_schema import (
    CANONICAL_COLUMN_ORDER,
    CANONICAL_EXPORT_SCHEMA,
    export_table_from_records,
)
from protea.core.operations.export_minijobs._export_write import (
    _ShardEntry,
    _stream_assemble,
)


class _StubStore:
    """In-memory ArtifactStore stub keyed by raw store key."""

    def __init__(self) -> None:
        self._data: dict[str, bytes] = {}

    def put(self, key: str, src: Path | bytes) -> str:
        payload = Path(src).read_bytes() if isinstance(src, (str, Path)) else src
        self._data[key] = payload
        return f"s3://test-bucket/{key}"

    def get(self, key: str) -> bytes:
        return self._data[key]


def _canonical_record() -> dict[str, Any]:
    """One record covering every canonical column with a typed default."""
    rec: dict[str, Any] = {}
    for field in CANONICAL_EXPORT_SCHEMA:
        if field.type == pa.string():
            rec[field.name] = "x"
        elif field.type == pa.bool_():
            rec[field.name] = False
        elif field.type == pa.int64():
            rec[field.name] = 0
        else:
            rec[field.name] = 0.0
    return rec


def _canonical_shard_bytes(n_rows: int) -> bytes:
    """A per-pair feature shard written under the canonical schema."""
    table = export_table_from_records([_canonical_record() for _ in range(n_rows)])
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        path = Path(tmp.name)
    try:
        pq.write_table(table, str(path))
        return path.read_bytes()
    finally:
        path.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Part 1: streaming write preserves the canonical schema
# ---------------------------------------------------------------------------


class TestStreamingCanonicalSchema:
    def test_streamed_train_has_canonical_schema_plus_snapshot_pair(
        self, tmp_path: Path
    ) -> None:
        store = _StubStore()
        store._data["temp/c/features/train-220.parquet"] = _canonical_shard_bytes(2)
        store._data["temp/c/features/train-221.parquet"] = _canonical_shard_bytes(3)
        shards = [
            _ShardEntry(
                pair_id="train-220",
                temp_uri="s3://test-bucket/temp/c/features/train-220.parquet",
                is_eval=False,
                n_rows=2,
            ),
            _ShardEntry(
                pair_id="train-221",
                temp_uri="s3://test-bucket/temp/c/features/train-221.parquet",
                is_eval=False,
                n_rows=3,
            ),
        ]

        asm = _stream_assemble(store, shards, tmp_path)

        assert asm.n_train_rows == 5
        assert asm.train_path is not None
        written_schema = pq.read_schema(str(asm.train_path))
        assert list(written_schema.names) == list(CANONICAL_COLUMN_ORDER) + ["snapshot_pair"]
        # Canonical column dtypes survive the stream unchanged.
        for field in CANONICAL_EXPORT_SCHEMA:
            assert written_schema.field(field.name).type == field.type
        assert written_schema.field("snapshot_pair").type == pa.string()


# ---------------------------------------------------------------------------
# Part 2: resumability skips completed cuts
# ---------------------------------------------------------------------------


def _make_runner(resume: Any) -> Any:
    """Build a bare ``_DumpRunner`` with just the attrs resume logic touches."""
    from protea.core.training_dump._runner import _DumpRunner

    runner = _DumpRunner.__new__(_DumpRunner)
    runner.session = MagicMock()
    runner.emit = lambda *a, **k: None
    runner._resume = resume
    return runner


def _outcome(shard_path: Path | None) -> Any:
    from protea.core._training_dump_loaders import _TrainSplitOutcome

    split_files = {"nk": shard_path} if shard_path is not None else {}
    return _TrainSplitOutcome(
        split_files=split_files,
        stats={"v_old": 220, "v_new": 221, "skipped": False},
        skipped=False,
    )


class TestResumeSkipsCompletedCuts:
    def test_resume_skips_complete_and_runs_only_missing(
        self, tmp_path: Path, monkeypatch: Any
    ) -> None:
        from protea.core.training_dump import _runner as runner_mod
        from protea.core.training_dump._resume import (
            open_resume_session,
            train_outcome_to_record,
        )

        resume = open_resume_session("ds", "fp", base=tmp_path)

        # Cut 0 already finished: real shard on disk + a done-marker.
        shard0 = resume.dir / "train_nk_split0.parquet"
        shard0.write_bytes(_canonical_shard_bytes(1))
        resume.write_record("train", 0, train_outcome_to_record(_outcome(shard0)))

        calls: list[int] = []

        def _fake_run_train_split(session: Any, ctx: Any, i: int, emit: Any) -> Any:
            calls.append(i)
            shard = resume.dir / f"train_nk_split{i}.parquet"
            shard.write_bytes(_canonical_shard_bytes(1))
            return _outcome(shard)

        monkeypatch.setattr(runner_mod, "_run_train_split", _fake_run_train_split)
        runner = _make_runner(resume)

        # Cut 0 restored from marker: heavy fn NOT called.
        out0 = runner._train_split_or_resume(MagicMock(), 0)
        assert out0.split_files["nk"] == shard0
        assert calls == []

        # Cut 1 missing: heavy fn called once, marker now written.
        out1 = runner._train_split_or_resume(MagicMock(), 1)
        assert calls == [1]
        assert out1.split_files["nk"].exists()
        assert resume.is_complete("train", 1)

    def test_marker_with_missing_shard_is_recomputed(self, tmp_path: Path) -> None:
        from protea.core.training_dump._resume import (
            open_resume_session,
            train_outcome_to_record,
        )

        resume = open_resume_session("ds", "fp", base=tmp_path)
        ghost = resume.dir / "train_nk_split0.parquet"  # never written
        resume.write_record("train", 0, train_outcome_to_record(_outcome(ghost)))

        # Marker exists but the shard does not -> not complete (recompute).
        assert resume.is_complete("train", 0) is False

    def test_skipped_cut_marker_is_complete_without_shards(self, tmp_path: Path) -> None:
        from protea.core.training_dump._resume import (
            open_resume_session,
            train_outcome_to_record,
        )

        resume = open_resume_session("ds", "fp", base=tmp_path)
        resume.write_record("train", 0, train_outcome_to_record(_outcome(None)))
        # A skipped cut writes no shards, so the marker alone is complete.
        assert resume.is_complete("train", 0) is True

    def test_cleanup_removes_staging_dir(self, tmp_path: Path) -> None:
        from protea.core.training_dump._resume import open_resume_session

        resume = open_resume_session("ds", "fp", base=tmp_path)
        assert resume.dir.exists()
        resume.cleanup()
        assert not resume.dir.exists()

    def test_fingerprint_changes_with_config(self) -> None:
        from protea.core.training_dump._resume import config_fingerprint

        base = {"name": "ds", "train_versions": [1, 2], "k": 5}
        changed = {"name": "ds", "train_versions": [1, 2, 3], "k": 5}
        assert config_fingerprint(base) != config_fingerprint(changed)
        assert config_fingerprint(base) == config_fingerprint(dict(base))
