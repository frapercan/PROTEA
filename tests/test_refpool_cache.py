"""Unit tests for the shared reference-pool disk cache.

Covers ``_load_reference_pool_cached`` (the cache-aware loader exposed by
:mod:`protea.core.disk_cache`) plus the count-validation wired into
``_load_from_disk_cache``. These pin three behaviours:

* miss path runs the DB loader and writes the npy pair,
* hit path returns the cached arrays without invoking the DB loader,
* stale path (row-count mismatch) treats the cache as invalid and
  rebuilds it from the DB loader.

The DB loader is a plain ``Callable`` so the tests do not need to stand
up a real ``Session``; the cache layer is the only contract being pinned.
"""
from __future__ import annotations

import uuid
from pathlib import Path

import numpy as np
import pytest

from protea.core import disk_cache
from protea.core.disk_cache import (
    _disk_cache_paths,
    _load_from_disk_cache,
    _load_reference_pool_cached,
    _save_to_disk_cache,
)


@pytest.fixture
def cache_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Redirect the module-level cache dir into ``tmp_path``.

    The cache dir is read at import time into ``disk_cache._DISK_CACHE_DIR``
    and the path helpers close over that module global, so the override
    has to mutate the module attribute (not just the env var).
    """
    monkeypatch.setattr(disk_cache, "_DISK_CACHE_DIR", tmp_path)
    return tmp_path


def _sample_pool(n: int = 4, dim: int = 3) -> tuple[list[str], np.ndarray]:
    accessions = [f"P{i}" for i in range(n)]
    embeddings = np.arange(n * dim, dtype=np.float16).reshape(n, dim)
    return accessions, embeddings


class TestLoadFromDiskCacheCountValidation:
    def test_returns_cache_when_count_matches(self, cache_dir: Path) -> None:
        cfg_id, ann_id = uuid.uuid4(), uuid.uuid4()
        accessions, embeddings = _sample_pool()
        _save_to_disk_cache(cfg_id, ann_id, accessions, embeddings)

        out = _load_from_disk_cache(cfg_id, ann_id, expected_count=len(accessions))

        assert out is not None
        assert out["accessions"] == accessions
        np.testing.assert_array_equal(out["embeddings"], embeddings)

    def test_returns_none_on_count_mismatch(self, cache_dir: Path) -> None:
        cfg_id, ann_id = uuid.uuid4(), uuid.uuid4()
        accessions, embeddings = _sample_pool(n=4)
        _save_to_disk_cache(cfg_id, ann_id, accessions, embeddings)

        # Pretend the DB now reports 7 rows (caller passes the fresh count).
        out = _load_from_disk_cache(cfg_id, ann_id, expected_count=7)

        assert out is None

    def test_returns_none_when_files_missing(self, cache_dir: Path) -> None:
        out = _load_from_disk_cache(uuid.uuid4(), uuid.uuid4(), expected_count=3)
        assert out is None


class TestLoadReferencePoolCached:
    def test_miss_invokes_db_loader_and_writes_npz(self, cache_dir: Path) -> None:
        cfg_id, ann_id = uuid.uuid4(), uuid.uuid4()
        accessions, embeddings = _sample_pool()
        calls: list[str] = []

        def db_loader() -> tuple[list[str], np.ndarray]:
            calls.append("db")
            return accessions, embeddings

        events: list[tuple[str, dict]] = []

        out_acc, out_emb = _load_reference_pool_cached(
            cfg_id,
            ann_id,
            db_loader,
            expected_count=len(accessions),
            emit=lambda ev, fields: events.append((ev, fields)),
        )

        assert calls == ["db"]
        assert out_acc == accessions
        np.testing.assert_array_equal(out_emb, embeddings)

        emb_path, acc_path = _disk_cache_paths(cfg_id, ann_id)
        assert emb_path.exists() and acc_path.exists()

        event_names = [ev for ev, _ in events]
        assert "refpool.cache_write" in event_names
        write_fields = dict(events)["refpool.cache_write"]
        assert write_fields["rows"] == len(accessions)
        assert write_fields["bytes"] > 0

    def test_hit_skips_db_loader(self, cache_dir: Path) -> None:
        cfg_id, ann_id = uuid.uuid4(), uuid.uuid4()
        accessions, embeddings = _sample_pool()
        _save_to_disk_cache(cfg_id, ann_id, accessions, embeddings)

        calls: list[str] = []

        def db_loader() -> tuple[list[str], np.ndarray]:
            calls.append("db")
            return [], np.empty((0,), dtype=np.float16)

        events: list[tuple[str, dict]] = []

        out_acc, out_emb = _load_reference_pool_cached(
            cfg_id,
            ann_id,
            db_loader,
            expected_count=len(accessions),
            emit=lambda ev, fields: events.append((ev, fields)),
        )

        assert calls == []  # DB loader never invoked on hit.
        assert out_acc == accessions
        np.testing.assert_array_equal(out_emb, embeddings)
        assert events and events[0][0] == "refpool.cache_hit"
        assert events[0][1]["rows"] == len(accessions)

    def test_stale_count_rebuilds_from_db(self, cache_dir: Path) -> None:
        cfg_id, ann_id = uuid.uuid4(), uuid.uuid4()
        old_accs, old_emb = _sample_pool(n=2)
        _save_to_disk_cache(cfg_id, ann_id, old_accs, old_emb)

        new_accs, new_emb = _sample_pool(n=5)
        calls: list[str] = []

        def db_loader() -> tuple[list[str], np.ndarray]:
            calls.append("db")
            return new_accs, new_emb

        events: list[tuple[str, dict]] = []

        out_acc, out_emb = _load_reference_pool_cached(
            cfg_id,
            ann_id,
            db_loader,
            expected_count=len(new_accs),
            emit=lambda ev, fields: events.append((ev, fields)),
        )

        assert calls == ["db"]
        assert out_acc == new_accs
        np.testing.assert_array_equal(out_emb, new_emb)

        event_names = [ev for ev, _ in events]
        assert "refpool.cache_stale" in event_names
        assert "refpool.cache_write" in event_names

        # Reload after the rebuild: same key, same fresh count -> hit.
        out2_acc, out2_emb = _load_reference_pool_cached(
            cfg_id,
            ann_id,
            db_loader,
            expected_count=len(new_accs),
            emit=None,
        )
        assert out2_acc == new_accs
        np.testing.assert_array_equal(out2_emb, new_emb)
        # No new DB call on the second pass.
        assert calls == ["db"]

    def test_emit_is_optional(self, cache_dir: Path) -> None:
        cfg_id, ann_id = uuid.uuid4(), uuid.uuid4()
        accessions, embeddings = _sample_pool()

        out_acc, out_emb = _load_reference_pool_cached(
            cfg_id,
            ann_id,
            lambda: (accessions, embeddings),
            expected_count=len(accessions),
        )

        assert out_acc == accessions
        np.testing.assert_array_equal(out_emb, embeddings)
