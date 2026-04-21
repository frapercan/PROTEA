"""Backend selection for :class:`ArtifactStore`.

Reads ``settings.storage_backend`` (with the ``PROTEA_STORAGE_BACKEND``
env var override resolved by :func:`load_settings`) and returns a
concrete store. If MinIO is configured but unreachable at construction
time, emits a warning and falls back to the local filesystem — production
should never silently degrade, but dev stacks should not crash because
an optional service is down.
"""

from __future__ import annotations

import logging
from pathlib import Path

from protea.infrastructure.settings import Settings

logger = logging.getLogger(__name__)


def get_artifact_store(settings: Settings):
    backend = (settings.storage_backend or "local").lower()

    if backend == "minio":
        if not (
            settings.minio_endpoint
            and settings.minio_access_key
            and settings.minio_secret_key
        ):
            logger.warning(
                "storage_backend=minio but MinIO config is incomplete "
                "(endpoint/access_key/secret_key); falling back to local"
            )
            return _make_local(settings)
        try:
            from .minio_store import MinioArtifactStore

            return MinioArtifactStore(
                endpoint=settings.minio_endpoint,
                bucket=settings.minio_bucket,
                access_key=settings.minio_access_key,
                secret_key=settings.minio_secret_key,
                secure=settings.minio_secure,
            )
        except Exception as exc:
            logger.warning(
                "MinIO unreachable (%s); falling back to local filesystem",
                exc,
            )
            return _make_local(settings)

    return _make_local(settings)


def _make_local(settings: Settings):
    from .local import LocalFsArtifactStore

    root = Path(settings.storage_root or settings.artifacts_dir)
    return LocalFsArtifactStore(root=root)
