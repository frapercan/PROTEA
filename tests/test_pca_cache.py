"""Unit tests for the ``protea.core.pca_cache`` shim.

The heavy ``load_or_fit_pca_state`` body lives in ``protea_method``;
this module is a thin re-export with PROTEA-specific path resolution
and on-disk save/load helpers. The tests below cover the file I/O
round-trip and the resilience to a missing or corrupt cache file.
"""

from __future__ import annotations

import uuid
from pathlib import Path
from unittest.mock import patch

import numpy as np

from protea.core import pca_cache


class TestPcaStatePath:
    def test_path_uses_module_default_dir(self) -> None:
        config_id = uuid.uuid4()
        path = pca_cache._pca_state_path(config_id)
        assert path.name == f"{config_id}.npz"
        assert path.parent == pca_cache._PCA_ARTIFACTS_DIR


class TestSaveAndLoadPcaState:
    def test_round_trip_returns_arrays(self, tmp_path: Path) -> None:
        config_id = uuid.uuid4()
        mean = np.array([1.0, 2.0, 3.0], dtype=np.float32)
        components = np.eye(3, dtype=np.float32)

        # Redirect the artifact dir to tmp_path for hermetic IO.
        with patch.object(pca_cache, "_PCA_ARTIFACTS_DIR", tmp_path):
            pca_cache._save_pca_state(config_id, mean, components)
            loaded = pca_cache._load_pca_state(config_id)

        assert loaded is not None
        loaded_mean, loaded_components = loaded
        np.testing.assert_array_equal(loaded_mean, mean)
        np.testing.assert_array_equal(loaded_components, components)
        # dtype must be float32 regardless of caller-supplied dtype.
        assert loaded_mean.dtype == np.float32
        assert loaded_components.dtype == np.float32

    def test_load_returns_none_when_file_missing(self, tmp_path: Path) -> None:
        config_id = uuid.uuid4()
        with patch.object(pca_cache, "_PCA_ARTIFACTS_DIR", tmp_path):
            assert pca_cache._load_pca_state(config_id) is None

    def test_load_returns_none_when_file_corrupt(self, tmp_path: Path) -> None:
        config_id = uuid.uuid4()
        # Write garbage where a .npz should be.
        with patch.object(pca_cache, "_PCA_ARTIFACTS_DIR", tmp_path):
            path = pca_cache._pca_state_path(config_id)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"not a valid npz file")
            assert pca_cache._load_pca_state(config_id) is None

    def test_save_creates_missing_parent_dir(self, tmp_path: Path) -> None:
        config_id = uuid.uuid4()
        nested = tmp_path / "deep" / "nested"
        mean = np.zeros(2, dtype=np.float32)
        components = np.zeros((2, 2), dtype=np.float32)
        with patch.object(pca_cache, "_PCA_ARTIFACTS_DIR", nested):
            pca_cache._save_pca_state(config_id, mean, components)
            assert pca_cache._pca_state_path(config_id).exists()

    def test_loaded_arrays_are_c_contiguous(self, tmp_path: Path) -> None:
        # The loader applies ``ascontiguousarray`` so downstream code
        # can rely on a fortran-free layout (FAISS / numpy ops).
        config_id = uuid.uuid4()
        mean = np.asfortranarray(np.arange(3, dtype=np.float32))
        components = np.asfortranarray(np.arange(9, dtype=np.float32).reshape(3, 3))
        with patch.object(pca_cache, "_PCA_ARTIFACTS_DIR", tmp_path):
            pca_cache._save_pca_state(config_id, mean, components)
            loaded = pca_cache._load_pca_state(config_id)
        assert loaded is not None
        loaded_mean, loaded_components = loaded
        assert loaded_mean.flags["C_CONTIGUOUS"]
        assert loaded_components.flags["C_CONTIGUOUS"]


class TestLoadOrFitPcaStateDelegation:
    def test_forwards_artifacts_dir_to_library(self, tmp_path: Path) -> None:
        config_id = uuid.uuid4()
        embeddings = np.random.RandomState(0).randn(10, 8).astype(np.float32)
        expected = (
            np.zeros(8, dtype=np.float32),
            np.eye(8, dtype=np.float32),
        )

        with (
            patch.object(pca_cache, "_PCA_ARTIFACTS_DIR", tmp_path),
            patch.object(pca_cache, "_lib_load_or_fit", return_value=expected) as mocked,
        ):
            result = pca_cache._load_or_fit_pca_state(config_id, embeddings)

        assert result is expected
        # The PROTEA shim must always pass artifacts_dir=<PROTEA dir>.
        call_kwargs = mocked.call_args.kwargs
        assert call_kwargs["artifacts_dir"] == tmp_path
