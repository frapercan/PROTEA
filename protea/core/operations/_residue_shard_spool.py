"""The node's spool of residue-code shards, between the card and the artifact store.

When the node sets ``operation.residue_shard_spool_dir``, ``encode_residue_vocabulary_batch``
writes each shard here instead of putting it in the artifact store itself, and
``upload_residue_shard``, a second worker of the same node on :data:`UPLOAD_QUEUE`, puts it in
the store while the card encodes the next batch. A file is named after the shard's store key,
which carries the shard's own hash, so a batch encoded twice writes the same bytes to the
same file.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path, PurePosixPath

#: The queue of the node's upload worker. An operation queue: its messages have no job row.
UPLOAD_QUEUE = "protea.embeddings.upload"
#: Every shard key starts here (``encode_residue_vocabulary.shard_key``).
SHARD_PREFIX = "residue-codes/"


def spool_root() -> Path | None:
    """The node's spool folder, or None when the card puts every shard in the store itself."""
    from protea.config.tuning import get_tuning

    root = get_tuning().operation.residue_shard_spool_dir
    return Path(root) if root else None


def spooled_path(root: Path, key: str) -> Path:
    """Where the shard stored under ``key`` waits in the spool.

    The key arrives in a message, so one that could name a file outside the spool is refused.
    """
    parts = PurePosixPath(key).parts
    if not key.startswith(SHARD_PREFIX) or ".." in parts or len(parts) < 2:
        raise ValueError(f"{key!r} is not a residue-code shard key")
    return root.joinpath(*parts)


def refuse_without_room(root: Path, min_free_gib: float) -> None:
    """Refuse to spool while the spool's file system has less than ``min_free_gib`` free.

    The spool only grows when nothing uploads it, so a full disk means the upload worker
    stopped. The batch fails before it spends the card, and its sequences stay pending for
    the next run of the pass.
    """
    root.mkdir(parents=True, exist_ok=True)
    free = shutil.disk_usage(root).free
    if free < min_free_gib * 2**30:
        raise RuntimeError(
            f"the spool {root} has {free / 2**30:.1f} GiB free and must keep {min_free_gib} "
            f"GiB: is the node's worker on {UPLOAD_QUEUE} running?"
        )


def write_spooled(path: Path, blob: bytes) -> None:
    """Write ``blob`` at ``path`` whole or not at all, and on the disk before returning.

    The batch acknowledges its message once this returns, so the file must outlive a power
    cut: it is written beside its final name, flushed, renamed over that name, and the
    folder is flushed too.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    part = path.with_name(f"{path.name}.{os.getpid()}.part")
    with open(part, "wb") as handle:
        handle.write(blob)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(part, path)
    folder = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(folder)
    finally:
        os.close(folder)


def spool_backlog(root: Path) -> tuple[int, int]:
    """How many shards wait in the spool, and their bytes."""
    files = [p for p in root.rglob("*.bin") if p.is_file()]
    return len(files), sum(p.stat().st_size for p in files)
