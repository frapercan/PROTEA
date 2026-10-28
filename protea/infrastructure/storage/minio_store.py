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

    def _key(self, address: str) -> str:
        """A key, from a key or from a URI THIS store would have produced.

        The protocol promises that a URI from :meth:`put` "should be persisted
        as-is in the database so consumers can resolve them without knowing the
        backend", and until 2026-10-28 that was false: handed back exactly what
        ``put`` returned, ``get`` passed ``s3://bucket/key`` to MinIO as the
        object name and MinIO answered ``XMinioInvalidObjectName`` on a path
        with the scheme embedded twice, ``/bucket/s3://bucket/key``.

        So the obvious call -- store it, read the address out of the row, fetch
        it -- was the one that could not work. Measured the hard way: it cost a
        dispatched ``apply_learned_encoder`` job, which failed in 0,0 min.

        A URI naming ANOTHER BUCKET, or another backend's scheme, is refused
        rather than read as a key. Both are configuration errors, and a key
        that happened to begin ``file://`` would otherwise become a silent miss
        on an object nobody can find.
        """
        if "://" not in address:
            return address
        scheme, _, rest = address.partition("://")
        if scheme != "s3":
            raise ValueError(
                f"{address!r} is a {scheme!r} address and this store speaks s3. "
                "A URI from another backend is a configuration error, not a key: "
                "reading it as one would look for an object whose name carries a "
                "scheme, which nothing ever wrote."
            )
        bucket, _, key = rest.partition("/")
        if bucket != self.bucket:
            raise ValueError(
                f"{address!r} names bucket {bucket!r} and this store is bound to "
                f"{self.bucket!r}. Fetching it from the wrong bucket would answer "
                "with a different object or with nothing, and both are worse than "
                "refusing."
            )
        if not key:
            raise ValueError(f"{address!r} names a bucket and no object")
        return key

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
        key = self._key(key)
        response = self._client.get_object(self.bucket, key)
        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()

    def url(self, key: str) -> str:
        return f"s3://{self.bucket}/{key}"

    def exists(self, key: str) -> bool:
        key = self._key(key)
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
