"""Backend selection for :class:`ArtifactStore`.

Reads ``settings.storage_backend`` (with the ``PROTEA_STORAGE_BACKEND``
env var override resolved by :func:`load_settings`) and returns a
concrete store. Missing MinIO configuration (no endpoint/credentials)
falls back to the local filesystem with a warning so dev stacks boot
cleanly. An explicit ``backend: minio`` with an unreachable endpoint
raises :class:`ArtifactStoreUnavailable` — silent degradation here
would produce ``Dataset`` rows with ``storage_backend=local`` +
``file://…`` URIs that the lab cannot resolve from another host, which
is a harder bug to spot than a startup failure.
"""

from __future__ import annotations

import logging
from pathlib import Path

from protea.infrastructure.settings import Settings

logger = logging.getLogger(__name__)


class ArtifactStoreUnavailable(RuntimeError):
    """Raised when the configured MinIO backend cannot be reached."""


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
            raise ArtifactStoreUnavailable(
                f"MinIO backend configured but unreachable at "
                f"{settings.minio_endpoint!r}: {exc}. Start the storage "
                f"profile (docker compose --profile storage up) or switch "
                f"to backend: local in system.yaml."
            ) from exc

    return _make_local(settings)


def _make_local(settings: Settings):
    from .local import LocalFsArtifactStore

    root = Path(settings.storage_root or settings.artifacts_dir)
    return LocalFsArtifactStore(root=root)
