"""Filesystem-backed artifact store."""

from __future__ import annotations

import shutil
from pathlib import Path


class LocalFsArtifactStore:
    """Stores blobs under ``root`` as regular files.

    URIs are ``file:///absolute/path/…`` so a consumer that reads a URI out
    of the DB can ``open(urlparse(uri).path, "rb")`` without any client.
    """

    def __init__(self, root: Path) -> None:
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def _full_path(self, key: str) -> Path:
        safe = key.lstrip("/")
        return self.root / safe

    def put(self, key: str, src: Path | bytes) -> str:
        dest = self._full_path(key)
        dest.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(src, (bytes, bytearray)):
            dest.write_bytes(bytes(src))
        else:
            shutil.copyfile(Path(src), dest)
        return self.url(key)

    def get(self, key: str) -> bytes:
        return self._full_path(key).read_bytes()

    def url(self, key: str) -> str:
        return f"file://{self._full_path(key).as_posix()}"

    def exists(self, key: str) -> bool:
        return self._full_path(key).exists()

    def delete(self, key: str) -> bool:
        path = self._full_path(key)
        if not path.exists():
            return False
        path.unlink()
        return True
