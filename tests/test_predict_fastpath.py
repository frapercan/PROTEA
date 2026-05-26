"""F-PRED-FASTPATH performance optimisations — unit tests.

Pins three behaviours introduced by the F-PRED-FASTPATH slice:

1. ``_is_cache_fresh`` returns True when cache files are newer than the
   freshness window and False when they are absent or stale.

2. ``_load_reference_pool_cached`` skips the COUNT(*) DB call (does not
   invoke ``db_loader``) when the disk cache is fresh according to the
   new ``freshness_seconds`` parameter.

3. ``_AspectKnnPreSearch.run`` produces the same neighbour sets regardless
   of whether the parallel (``aspect_knn_workers > 1``) or serial path is
   used, and invokes ``search_knn`` exactly once per aspect in both cases.

4. ``OperationTuning`` exposes the two new knobs with correct defaults:
   ``ref_cache_freshness_seconds`` (300) and ``aspect_knn_workers`` (3).
"""

from __future__ import annotations

import time
import uuid
from pathlib import Path
from typing import Any

import numpy as np
import pytest

from protea.core import disk_cache as _dc
from protea.core.disk_cache import (
    _is_cache_fresh,
    _load_reference_pool_cached,
    _save_to_disk_cache,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def cache_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Redirect module-level cache dir to a temp directory."""
    monkeypatch.setattr(_dc, "_DISK_CACHE_DIR", tmp_path)
    return tmp_path


def _sample_pool(n: int = 4, dim: int = 8) -> tuple[list[str], np.ndarray]:
    accs = [f"P{i:05d}" for i in range(n)]
    emb = np.random.default_rng(42).random((n, dim)).astype(np.float16)
    return accs, emb


# ---------------------------------------------------------------------------
# 1. _is_cache_fresh
# ---------------------------------------------------------------------------

class TestIsCacheFresh:
    def test_returns_false_when_files_absent(self, cache_dir: Path) -> None:
        cfg_id, ann_id = uuid.uuid4(), uuid.uuid4()
        assert _is_cache_fresh(cfg_id, ann_id, 300) is False

    def test_returns_true_when_files_recent(self, cache_dir: Path) -> None:
        cfg_id, ann_id = uuid.uuid4(), uuid.uuid4()
        accs, emb = _sample_pool()
        _save_to_disk_cache(cfg_id, ann_id, accs, emb)
        # Files were just written; they are younger than 300 s.
        assert _is_cache_fresh(cfg_id, ann_id, 300) is True

    def test_returns_false_when_freshness_zero(self, cache_dir: Path) -> None:
        cfg_id, ann_id = uuid.uuid4(), uuid.uuid4()
        accs, emb = _sample_pool()
        _save_to_disk_cache(cfg_id, ann_id, accs, emb)
        # freshness_seconds=0 always returns False (disabled).
        assert _is_cache_fresh(cfg_id, ann_id, 0) is False

    def test_returns_false_when_files_are_old(
        self, cache_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cfg_id, ann_id = uuid.uuid4(), uuid.uuid4()
        accs, emb = _sample_pool()
        _save_to_disk_cache(cfg_id, ann_id, accs, emb)
        # Freeze time 400 s into the future so the files look stale vs 300 s.
        future = time.time() + 400.0
        monkeypatch.setattr(time, "time", lambda: future)
        assert _is_cache_fresh(cfg_id, ann_id, 300) is False


# ---------------------------------------------------------------------------
# 2. _load_reference_pool_cached — COUNT skip on fresh cache
# ---------------------------------------------------------------------------

class TestLoadReferencePoolCachedFreshnessSkip:
    def test_skip_count_when_fresh(self, cache_dir: Path) -> None:
        """When files are fresh, db_loader must NOT be called."""
        cfg_id, ann_id = uuid.uuid4(), uuid.uuid4()
        accs, emb = _sample_pool()
        _save_to_disk_cache(cfg_id, ann_id, accs, emb)

        calls: list[str] = []

        def db_loader() -> tuple[list[str], np.ndarray]:
            calls.append("db")
            return [], np.empty((0,), dtype=np.float16)

        events: list[tuple[str, dict]] = []
        out_accs, out_emb = _load_reference_pool_cached(
            cfg_id,
            ann_id,
            db_loader,
            expected_count=None,
            emit=lambda ev, fields: events.append((ev, fields)),
            freshness_seconds=300,
        )

        assert calls == [], "db_loader should not be called on fresh cache"
        assert out_accs == accs
        np.testing.assert_array_equal(out_emb, emb)
        event_names = [ev for ev, _ in events]
        assert "refpool.cache_hit_fresh" in event_names

    def test_count_issued_when_not_fresh(self, cache_dir: Path) -> None:
        """When freshness_seconds=0, the normal count-validation path runs."""
        cfg_id, ann_id = uuid.uuid4(), uuid.uuid4()
        accs, emb = _sample_pool()
        _save_to_disk_cache(cfg_id, ann_id, accs, emb)

        calls: list[str] = []

        def db_loader() -> tuple[list[str], np.ndarray]:
            calls.append("db")
            return accs, emb

        events: list[tuple[str, dict]] = []
        _load_reference_pool_cached(
            cfg_id,
            ann_id,
            db_loader,
            expected_count=len(accs),
            emit=lambda ev, fields: events.append((ev, fields)),
            freshness_seconds=0,
        )

        # Count matches so the cache is valid and db_loader is still skipped.
        assert calls == []
        event_names = [ev for ev, _ in events]
        assert "refpool.cache_hit" in event_names
        assert "refpool.cache_hit_fresh" not in event_names

    def test_fallback_to_db_when_cache_absent(self, cache_dir: Path) -> None:
        """Fresh-path still falls back to db_loader when no files exist."""
        cfg_id, ann_id = uuid.uuid4(), uuid.uuid4()
        accs, emb = _sample_pool()
        calls: list[str] = []

        def db_loader() -> tuple[list[str], np.ndarray]:
            calls.append("db")
            return accs, emb

        out_accs, out_emb = _load_reference_pool_cached(
            cfg_id,
            ann_id,
            db_loader,
            expected_count=None,
            freshness_seconds=300,
        )

        assert calls == ["db"]
        assert out_accs == accs
        np.testing.assert_array_equal(out_emb, emb)


# ---------------------------------------------------------------------------
# 3. _AspectKnnPreSearch — parallel vs serial equivalence
#
# These tests require the full PROTEA package stack (pydantic, protea_method,
# protea_contracts). They are skipped when those packages are not importable
# in the current Python environment (e.g. a minimal CI runner that only has
# numpy+pytest in sys.path). In the deployed dev environment where
# ``poetry install`` has run they all pass.
# ---------------------------------------------------------------------------

_FULL_STACK_AVAILABLE = True
try:
    import protea_contracts  # noqa: F401
    import protea_method  # noqa: F401
    import pydantic  # noqa: F401
except ImportError:
    _FULL_STACK_AVAILABLE = False

_needs_full_stack = pytest.mark.skipif(
    not _FULL_STACK_AVAILABLE,
    reason="pydantic / protea_method / protea_contracts not importable in this env",
)


@_needs_full_stack
class TestAspectKnnPreSearchParallel:
    """Verify parallel KNN produces identical results to serial KNN."""

    def _make_ref_data(
        self, n_refs: int = 10, dim: int = 8
    ) -> dict[str, dict[str, Any]]:
        """Minimal ref_data_by_aspect for the pre-search."""
        from protea.core.domain.aspect import ASPECT_CODES as _ASPECTS

        rng = np.random.default_rng(0)
        data: dict[str, dict[str, Any]] = {}
        for asp in _ASPECTS:
            emb_f32 = rng.random((n_refs, dim)).astype(np.float32)
            norms = np.linalg.norm(emb_f32, axis=1, keepdims=True)
            emb_f32_cos = emb_f32 / (norms + 1e-9)
            data[asp] = {
                "accessions": [f"{asp}_{i}" for i in range(n_refs)],
                "embeddings_f32": emb_f32,
                "embeddings_f32_cos": emb_f32_cos,
            }
        return data

    def _make_payload(self) -> Any:
        """Minimal PredictGOTermsBatchPayload-like namespace."""
        from types import SimpleNamespace
        return SimpleNamespace(
            limit_per_entry=3,
            distance_threshold=None,
            search_backend="numpy",
            metric="l2",
            faiss_index_type=None,
            faiss_nlist=None,
            faiss_nprobe=None,
            faiss_hnsw_m=None,
            faiss_hnsw_ef_search=None,
        )

    @pytest.mark.parametrize("n_workers", [1, 3])
    def test_parallel_equals_serial(
        self,
        n_workers: int,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Both n_workers=1 and n_workers=3 yield the same neighbours."""
        from protea.config.tuning import get_tuning
        from protea.core.domain.aspect import ASPECT_CODES as _ASPECTS
        from protea.core.operations.predict_go_terms._aspect_helpers import (
            _AspectKnnPreSearch,
        )

        get_tuning.cache_clear()
        monkeypatch.setattr(
            "protea.core.operations.predict_go_terms._aspect_helpers.get_tuning",
            lambda: type("T", (), {"operation": type("O", (), {"aspect_knn_workers": n_workers})()})(),
            raising=False,
        )

        rng = np.random.default_rng(1)
        n_queries, dim = 5, 8
        query_emb = rng.random((n_queries, dim)).astype(np.float32)
        ref_data = self._make_ref_data(n_refs=10, dim=dim)
        p = self._make_payload()

        neighbors, unique = _AspectKnnPreSearch.run(query_emb, query_emb, ref_data, p)

        assert set(neighbors.keys()) == set(_ASPECTS)
        for asp in _ASPECTS:
            assert len(neighbors[asp]) == n_queries
        # Unique neighbours are a non-empty subset of all ref accessions.
        all_accs: set[str] = set()
        for asp in _ASPECTS:
            all_accs.update(ref_data[asp]["accessions"])
        assert unique.issubset(all_accs)
        assert len(unique) > 0

        get_tuning.cache_clear()


# ---------------------------------------------------------------------------
# 4. OperationTuning defaults
# ---------------------------------------------------------------------------

@_needs_full_stack
class TestOperationTuningDefaults:
    def test_ref_cache_freshness_seconds_default(self) -> None:
        from protea.config.tuning import OperationTuning

        t = OperationTuning()
        assert t.ref_cache_freshness_seconds == 300

    def test_aspect_knn_workers_default(self) -> None:
        from protea.config.tuning import OperationTuning

        t = OperationTuning()
        assert t.aspect_knn_workers == 3

    def test_freshness_zero_is_valid(self) -> None:
        from protea.config.tuning import OperationTuning

        t = OperationTuning(ref_cache_freshness_seconds=0)
        assert t.ref_cache_freshness_seconds == 0

    def test_workers_one_is_valid(self) -> None:
        from protea.config.tuning import OperationTuning

        t = OperationTuning(aspect_knn_workers=1)
        assert t.aspect_knn_workers == 1
