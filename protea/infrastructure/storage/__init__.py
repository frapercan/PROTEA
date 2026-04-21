"""Artifact storage abstraction.

An :class:`ArtifactStore` is the single write/read surface for large blobs
produced by PROTEA (exported datasets, reranker models, etc.) — separate
from the filesystem-based ``artifacts_dir`` used by the evaluation pipeline.

Two backends are provided:

* :class:`~protea.infrastructure.storage.local.LocalFsArtifactStore` — a
  plain directory on disk. Default, always available.
* :class:`~protea.infrastructure.storage.minio_store.MinioArtifactStore` —
  S3-compatible object storage (self-hosted). Requires the ``minio``
  extra (``pip install 'protea[storage]'``).

Choose a backend through :func:`get_artifact_store` which reads
``PROTEA_STORAGE_BACKEND`` / ``settings.storage_backend``.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable
from pathlib import Path


@runtime_checkable
class ArtifactStore(Protocol):
    """Minimal blob store interface.

    Keys are forward-slash separated strings (``foo/bar/baz.txt``). URIs
    returned by :meth:`put` / :meth:`url` are backend-specific
    (``file:///…`` for local, ``s3://bucket/key`` for MinIO) and should be
    persisted as-is in the database so consumers can resolve them without
    knowing the backend.
    """

    def put(self, key: str, src: Path | bytes) -> str:
        """Store ``src`` under ``key`` and return its URI."""
        ...

    def get(self, key: str) -> bytes:
        """Fetch raw bytes stored at ``key``."""
        ...

    def url(self, key: str) -> str:
        """Return the URI for ``key`` without performing I/O."""
        ...

    def exists(self, key: str) -> bool:
        """Whether ``key`` is present in the store."""
        ...


from .factory import get_artifact_store  # noqa: E402
from .local import LocalFsArtifactStore  # noqa: E402

__all__ = ["ArtifactStore", "LocalFsArtifactStore", "get_artifact_store"]
