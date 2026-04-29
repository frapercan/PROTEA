"""MinIO-backed artifact store.

The ``minio`` client is imported lazily so the package can be installed
without it (``protea[storage]`` opts in). A clear ImportError is raised
on instantiation if the dependency is missing.
"""

from __future__ import annotations

import io
from pathlib import Path


class MinioArtifactStore:
    """S3-compatible object store (MinIO).

    URIs are ``s3://<bucket>/<key>``. The endpoint, credentials, and bucket
    come from settings; the caller should not need to touch them directly.
    """

    def __init__(
        self,
        *,
        endpoint: str,
        bucket: str,
        access_key: str,
        secret_key: str,
        secure: bool = False,
    ) -> None:
        try:
            from minio import Minio
        except ImportError as exc:
            raise ImportError(
                "MinioArtifactStore requires the 'minio' package. "
                "Install with: pip install 'protea[storage]'"
            ) from exc

        self._client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )
        self.bucket = bucket
        self._ensure_bucket()

    def _ensure_bucket(self) -> None:
        if not self._client.bucket_exists(self.bucket):
            self._client.make_bucket(self.bucket)

    def put(self, key: str, src: Path | bytes) -> str:
        if isinstance(src, (bytes, bytearray)):
            data = bytes(src)
            self._client.put_object(
                self.bucket, key, io.BytesIO(data), length=len(data)
            )
        else:
            self._client.fput_object(self.bucket, key, str(Path(src)))
        return self.url(key)

    def get(self, key: str) -> bytes:
        response = self._client.get_object(self.bucket, key)
        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()

    def url(self, key: str) -> str:
        return f"s3://{self.bucket}/{key}"

    def exists(self, key: str) -> bool:
        from minio.error import S3Error

        try:
            self._client.stat_object(self.bucket, key)
            return True
        except S3Error:
            return False

    def delete(self, key: str) -> bool:
        from minio.error import S3Error

        try:
            self._client.stat_object(self.bucket, key)
        except S3Error:
            return False
        self._client.remove_object(self.bucket, key)
        return True
