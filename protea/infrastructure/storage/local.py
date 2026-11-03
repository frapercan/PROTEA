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

    def _key(self, address: str) -> str:
        """A key, from a key or from a URI THIS store would have produced.

        The same repair as :meth:`MinioArtifactStore._key`, and the protocol
        makes the same promise for both: a URI persisted in a row can be
        resolved by a consumer that does not know the backend.

        A ``file://`` path outside this store's root is refused rather than
        reduced to a key, because the only honest readings of it are "another
        deployment's root" and "an absolute path that is not in the store", and
        serving either from this root would answer with the wrong object.
        """
        if "://" not in address:
            return address
        scheme, _, rest = address.partition("://")
        if scheme != "file":
            raise ValueError(
                f"{address!r} is a {scheme!r} address and this store speaks file. "
                "A URI from another backend is a configuration error, not a key."
            )
        path = Path("/" + rest.lstrip("/"))
        try:
            return path.relative_to(self.root).as_posix()
        except ValueError:
            raise ValueError(
                f"{address!r} is outside this store's root {str(self.root)!r}. It "
                "names another deployment's root or a path that was never in the "
                "store, and resolving it against this root would answer with a "
                "different object."
            ) from None

    def _full_path(self, key: str) -> Path:
        safe = self._key(key).lstrip("/")
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
