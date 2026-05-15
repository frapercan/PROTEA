"""Unit tests for the PROTEA-side reranker loader helpers.

Covers ``_uri_to_key`` (URI / store-key translation) and the
``load_reranker`` cache-aware path; the pure ``predict`` /
``prepare_dataset`` / ``model_from_string`` helpers re-exported from
``protea_method`` are already exercised in ``tests/test_reranker.py``.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np

from protea.core import reranker
from protea.infrastructure.storage import LocalFsArtifactStore


def _train_tiny_booster(tmp_path: Path) -> Path:
    """Materialise a real LightGBM booster small enough to round-trip."""
    import lightgbm as lgb

    rng = np.random.RandomState(0)
    x = rng.randn(40, 4).astype(np.float32)
    y = (x[:, 0] > 0).astype(np.int32)
    dataset = lgb.Dataset(x, label=y)
    booster = lgb.train(
        {"objective": "binary", "verbose": -1, "num_leaves": 4, "min_data_in_leaf": 1},
        dataset,
        num_boost_round=2,
    )
    out_path = tmp_path / "tiny_booster.txt"
    booster.save_model(str(out_path))
    return out_path


class TestUriToKey:
    def test_s3_uri_strips_bucket_returns_key(self) -> None:
        store = MagicMock()
        key = reranker._uri_to_key("s3://my-bucket/path/to/model.txt", store)
        assert key == "path/to/model.txt"

    def test_s3_uri_without_key_returns_empty_string(self) -> None:
        store = MagicMock()
        # ``s3://bucket`` (no slash) means the key is empty.
        key = reranker._uri_to_key("s3://bucket", store)
        assert key == ""

    def test_file_uri_under_localfs_root_yields_relative_key(self, tmp_path: Path) -> None:
        store = LocalFsArtifactStore(root=tmp_path)
        target = tmp_path / "sub" / "model.txt"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"x")
        uri = f"file://{target}"
        key = reranker._uri_to_key(uri, store)
        assert key == str(Path("sub") / "model.txt")

    def test_file_uri_outside_localfs_root_returns_absolute_path(self, tmp_path: Path) -> None:
        # When the absolute path can't be made relative to the store
        # root, the helper falls back to the absolute string rather
        # than raising.
        store_root = tmp_path / "store"
        outside = tmp_path / "elsewhere" / "model.txt"
        store_root.mkdir(parents=True, exist_ok=True)
        outside.parent.mkdir(parents=True, exist_ok=True)
        outside.write_bytes(b"x")
        store = LocalFsArtifactStore(root=store_root)
        key = reranker._uri_to_key(f"file://{outside}", store)
        assert key == str(outside)

    def test_unknown_scheme_returned_verbatim(self) -> None:
        store = MagicMock()
        key = reranker._uri_to_key("custom://some/place", store)
        assert key == "custom://some/place"

    def test_file_uri_with_nonlocal_store_returned_verbatim(self) -> None:
        # MinIO-style store: the helper must NOT try to interpret a
        # file:// URI relative to a non-local store root.
        store = MagicMock(spec=[])
        uri = "file:///tmp/whatever.txt"
        key = reranker._uri_to_key(uri, store)
        assert key == uri


class TestDefaultCacheDir:
    def test_points_under_storage_reranker_cache(self) -> None:
        cache_dir = reranker._default_cache_dir()
        # Cache dir lives at <repo-root>/storage/reranker_cache.
        assert cache_dir.parts[-2:] == ("storage", "reranker_cache")


class TestLoadReranker:
    def setup_method(self) -> None:
        # Each test gets a clean in-process cache.
        reranker._BOOSTER_CACHE.clear()

    def test_materialises_blob_when_cache_miss(self, tmp_path: Path) -> None:
        booster_path = _train_tiny_booster(tmp_path)
        blob = booster_path.read_bytes()

        store_root = tmp_path / "store"
        store_root.mkdir()
        cache_dir = tmp_path / "cache"

        store = MagicMock()
        store.get.return_value = blob

        booster = reranker.load_reranker(
            "s3://bucket/some/model.txt",
            feature_schema_sha="abc123",
            store=store,
            cache_dir=cache_dir,
        )
        # ``store.get`` was called with the URI-derived key.
        store.get.assert_called_once_with("some/model.txt")
        # On-disk file exists in the cache dir.
        cached_files = list(cache_dir.iterdir())
        assert len(cached_files) == 1
        # Returned object is a usable booster.
        assert booster.num_trees() > 0

    def test_in_process_cache_skips_store_get_on_second_call(self, tmp_path: Path) -> None:
        booster_path = _train_tiny_booster(tmp_path)
        blob = booster_path.read_bytes()

        cache_dir = tmp_path / "cache"
        store = MagicMock()
        store.get.return_value = blob
        uri = "s3://bucket/m.txt"

        first = reranker.load_reranker(
            uri, feature_schema_sha="sha1", store=store, cache_dir=cache_dir
        )
        second = reranker.load_reranker(
            uri, feature_schema_sha="sha1", store=store, cache_dir=cache_dir
        )
        assert first is second
        # Store fetched exactly once across both calls.
        assert store.get.call_count == 1

    def test_disk_cache_skips_store_get_when_file_already_present(self, tmp_path: Path) -> None:
        booster_path = _train_tiny_booster(tmp_path)
        blob = booster_path.read_bytes()
        cache_dir = tmp_path / "cache"

        # First call seeds the on-disk cache.
        store = MagicMock()
        store.get.return_value = blob
        uri = "s3://bucket/disk.txt"
        reranker.load_reranker(uri, feature_schema_sha="disk-sha", store=store, cache_dir=cache_dir)

        # Second call from a different in-process state ("new" process)
        # must NOT re-fetch from the store.
        reranker._BOOSTER_CACHE.clear()
        new_store = MagicMock()
        booster = reranker.load_reranker(
            uri, feature_schema_sha="disk-sha", store=new_store, cache_dir=cache_dir
        )
        new_store.get.assert_not_called()
        assert booster.num_trees() > 0

    def test_uses_default_cache_dir_when_none(self, tmp_path: Path) -> None:
        # The default dir must be honoured when the caller omits the
        # kwarg; we patch the default-dir helper to avoid touching the
        # real ``storage/reranker_cache`` tree.
        booster_path = _train_tiny_booster(tmp_path)
        blob = booster_path.read_bytes()

        default_cache = tmp_path / "default_cache"
        store = MagicMock()
        store.get.return_value = blob

        with patch.object(reranker, "_default_cache_dir", return_value=default_cache):
            reranker.load_reranker(
                "s3://b/x.txt",
                feature_schema_sha="auto",
                store=store,
            )
        # File landed under the patched default dir.
        assert any(default_cache.iterdir())
