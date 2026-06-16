"""Unit tests for the stable-feature-cache orchestration (export decouple).

Targets ``protea.core.operations._export_stable_cache_runner.run_with_stable_cache``.
Exercises the cache MISS -> WRITE and HIT -> SKIP-COMPUTE branches against an
in-memory fake artifact store, with the heavy dump and the classifier post-pass
mocked. Pure-python; no DB, no live stack.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd

from protea.core.contracts.operation import OperationResult
from protea.core.operations import _export_stable_cache_runner as runner
from protea.core.training_dump._payload import TrainRerankerAutoPayload


class _FakeStore:
    """Minimal in-memory ArtifactStore for the cache I/O contract."""

    def __init__(self) -> None:
        self.blobs: dict[str, bytes] = {}

    def exists(self, key: str) -> bool:
        return key in self.blobs

    def get(self, key: str) -> bytes:
        return self.blobs[key]

    def put(self, key: str, src: Path | bytes) -> str:
        self.blobs[key] = src if isinstance(src, (bytes, bytearray)) else Path(src).read_bytes()
        return f"file://{key}"


def _payload(**overrides: Any) -> TrainRerankerAutoPayload:
    base: dict[str, Any] = {
        "name": "decouple-runner-test",
        "embedding_config_id": "11111111-1111-1111-1111-111111111111",
        "ontology_snapshot_id": "22222222-2222-2222-2222-222222222222",
        "train_versions": [160, 165],
        "test_versions": [170],
        "limit_per_entry": 5,
        "compute_classifier": True,
    }
    base.update(overrides)
    return TrainRerankerAutoPayload.model_validate(base)


def _stage_stable_parquets(stage_dir: Path) -> None:
    """Write a minimal stable train/eval parquet + manifest into the stage dir."""
    df = pd.DataFrame(
        {
            "protein_accession": ["Q1"],
            "go_term_id": ["GO:0000099"],
            "classifier_score": [0.0],
            "classifier_present": [0.0],
        }
    )
    df.to_parquet(stage_dir / "train.parquet", index=False)
    df.to_parquet(stage_dir / "eval.parquet", index=False)
    (stage_dir / "manifest.json").write_text("{}")


def _noop_emit(*_a: Any, **_k: Any) -> None:
    return None


def test_cache_miss_runs_dump_then_writes_stable_table(tmp_path: Path) -> None:
    store = _FakeStore()
    p = _payload()
    dump_calls: list[bool] = []

    def _dump_fn(_session, _payload, stage, _emit, *, classifier_on: bool) -> OperationResult:
        dump_calls.append(classifier_on)
        _stage_stable_parquets(stage)
        return OperationResult(result={"dumped": True})

    with (
        patch("protea.infrastructure.session.session_scope") as mock_scope,
        patch(
            "protea.core.training_dump._classifier_postpass.apply_classifier_to_frame"
        ) as mock_clf,
    ):
        mock_scope.return_value.__enter__.return_value = MagicMock()
        runner.run_with_stable_cache(p, MagicMock(), store, tmp_path, _noop_emit, _dump_fn)

    # Heavy pass ran exactly once with the classifier OFF (stable-only).
    assert dump_calls == [False]
    # Stable table (incl. manifest) was written to the cache.
    keys = list(store.blobs.keys())
    assert any(k.endswith("train.parquet") for k in keys)
    assert any(k.endswith("eval.parquet") for k in keys)
    assert any(k.endswith("cache_manifest.json") for k in keys)
    # Classifier was re-stamped on the staged parquets (compute_classifier=True).
    assert mock_clf.called


def test_cache_hit_skips_dump(tmp_path: Path) -> None:
    store = _FakeStore()
    p = _payload()
    # Pre-seed the cache as if a prior export wrote the stable table.
    from protea.core.training_dump._stable_feature_cache import (
        compute_stable_cache_key,
        stable_cache_prefix,
    )

    prefix = stable_cache_prefix(compute_stable_cache_key(p))
    seed_dir = tmp_path / "_seed"
    seed_dir.mkdir()
    _stage_stable_parquets(seed_dir)
    for name in ("train.parquet", "eval.parquet", "manifest.json"):
        store.blobs[prefix + name] = (seed_dir / name).read_bytes()

    dump = MagicMock()
    with (
        patch("protea.infrastructure.session.session_scope") as mock_scope,
        patch(
            "protea.core.training_dump._classifier_postpass.apply_classifier_to_frame"
        ) as mock_clf,
    ):
        mock_scope.return_value.__enter__.return_value = MagicMock()
        runner.run_with_stable_cache(p, MagicMock(), store, tmp_path, _noop_emit, dump)

    # The heavy dump was NOT invoked: the stable table came from the cache.
    dump.assert_not_called()
    # The staged parquets were hydrated from the cache.
    assert (tmp_path / "train.parquet").exists()
    assert (tmp_path / "eval.parquet").exists()
    # Classifier re-stamp still runs for THIS export's classifier.
    assert mock_clf.called
