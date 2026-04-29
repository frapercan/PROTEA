"""Unit tests for the artifact-storage abstraction.

These tests exercise the local filesystem backend only; the MinIO backend
is integration-tested out-of-band (requires a running MinIO container,
covered by the compose profile).
"""

from __future__ import annotations

import logging
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

import pytest

from protea.infrastructure.settings import Settings
from protea.infrastructure.storage import ArtifactStore, get_artifact_store
from protea.infrastructure.storage.local import LocalFsArtifactStore


@pytest.fixture()
def base_settings(tmp_path: Path) -> Settings:
    return Settings(
        db_url="",
        amqp_url="",
        artifacts_dir=tmp_path / "legacy_eval",
        admin_token="",
        storage_backend="local",
        storage_root=tmp_path / "artifacts",
    )


class TestLocalFsArtifactStore:
    def test_put_bytes_roundtrips(self, tmp_path: Path):
        store = LocalFsArtifactStore(root=tmp_path)
        uri = store.put("rerankers/demo/model.txt", b"hello world")
        assert uri.startswith("file://")
        assert store.get("rerankers/demo/model.txt") == b"hello world"
        assert store.exists("rerankers/demo/model.txt")

    def test_put_file_roundtrips(self, tmp_path: Path):
        src = tmp_path / "src.bin"
        src.write_bytes(b"binary payload")
        store = LocalFsArtifactStore(root=tmp_path / "dest")
        uri = store.put("sub/dir/blob.bin", src)
        assert uri.endswith("/sub/dir/blob.bin")
        assert store.get("sub/dir/blob.bin") == b"binary payload"

    def test_absent_key(self, tmp_path: Path):
        store = LocalFsArtifactStore(root=tmp_path)
        assert not store.exists("nope")

    def test_creates_intermediate_dirs(self, tmp_path: Path):
        store = LocalFsArtifactStore(root=tmp_path / "a" / "b")
        store.put("c/d/e/f.txt", b"x")
        assert (tmp_path / "a" / "b" / "c" / "d" / "e" / "f.txt").read_bytes() == b"x"


class TestFactory:
    def test_returns_local_by_default(self, base_settings: Settings):
        store = get_artifact_store(base_settings)
        assert isinstance(store, LocalFsArtifactStore)
        assert isinstance(store, ArtifactStore)

    def test_minio_with_missing_config_falls_back(
        self, base_settings: Settings, caplog: pytest.LogCaptureFixture
    ):
        s = replace(base_settings, storage_backend="minio")
        with caplog.at_level(logging.WARNING):
            store = get_artifact_store(s)
        assert isinstance(store, LocalFsArtifactStore)
        assert any("minio" in r.message.lower() for r in caplog.records)

    def test_minio_unreachable_raises(self, base_settings: Settings):
        """Unreachable MinIO must fail loudly.

        The earlier silent fallback to local produced ``Dataset`` rows
        with ``backend=local`` and ``file://…`` URIs that the lab could
        not resolve from a different host — a subtle data-corruption
        bug. Readiness / export should fail fast instead.
        """
        s = replace(
            base_settings,
            storage_backend="minio",
            minio_endpoint="localhost:1",
            minio_access_key="x",
            minio_secret_key="y",
        )
        from protea.infrastructure.storage import minio_store
        from protea.infrastructure.storage.factory import ArtifactStoreUnavailable

        with patch.object(
            minio_store.MinioArtifactStore, "_ensure_bucket",
            side_effect=ConnectionError("boom"),
        ):
            with pytest.raises(ArtifactStoreUnavailable, match="MinIO"):
                get_artifact_store(s)
