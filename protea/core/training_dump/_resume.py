"""Resumable staging for the serial export dump.

The monolithic ``export_research_dataset`` path runs each temporal
snapshot-pair cut SERIALLY and stages its per-category parquet shards
under one staging directory. Historically that directory was an
ephemeral ``tempfile.mkdtemp`` wiped in a ``finally`` block, so a
cancelled / killed / rebooted worker that re-claimed the ``RUNNING`` job
restarted from cut 1 and lost every completed cut (each cut is a ~45 min
KNN + feature pass). Two ~10h runs were lost this way.

This module makes the staging directory STABLE (keyed by the dataset
name plus a fingerprint of the cut-affecting config) and records a
per-cut JSON done-marker as each cut finishes. On (re-)start the runner
reconstructs the already-completed cuts from their markers and skips
straight to the first unfinished cut. The directory is removed only
after the consolidated dataset has been assembled successfully; a
failure (or a kill) leaves it in place so the next run resumes.

A marker is treated as complete only when it exists AND every shard
path it references is still present on disk, so a half-written cut (the
process died mid-flush) is recomputed rather than trusted.

The bookkeeping here is pure filesystem + JSON; it holds no DB session
and no heavy state, which keeps it unit-testable without a database.
"""

from __future__ import annotations

import hashlib
import json
import logging
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from protea.core._training_dump_loaders import _TrainSplitOutcome

_LOG = logging.getLogger(__name__)

#: Env var overriding the base directory under which per-dataset staging
#: directories live. Defaults to ``<repo_root>/storage/export_resume`` so
#: the shards survive a reboot (unlike ``/tmp``).
_RESUME_DIR_ENV = "PROTEA_EXPORT_RESUME_DIR"

#: Config keys whose change must invalidate a resumed staging directory:
#: a different embedding / ontology / version set / k / feature flag set
#: would produce different shards, so reusing old ones would corrupt the
#: dataset. ``name`` is included so two datasets never share a directory.
_FINGERPRINT_KEYS: tuple[str, ...] = (
    "name",
    "embedding_config_id",
    "ontology_snapshot_id",
    "annotation_source",
    "train_versions",
    "test_versions",
    "limit_per_entry",
    "search_backend",
    "compute_alignments",
    "compute_taxonomy",
    "expand_votes_to_ancestors",
    "use_embedding_pca",
    "compute_self_prior",
    "compute_association",
    "compute_classifier",
)


def _resume_root(base: Path | None = None) -> Path:
    """Resolve the base directory holding per-dataset staging dirs."""
    import os

    if base is not None:
        return base
    env = os.environ.get(_RESUME_DIR_ENV, "").strip()
    if env:
        return Path(env)
    # protea/core/training_dump/_resume.py -> parents[3] == repo root.
    repo_root = Path(__file__).resolve().parents[3]
    return repo_root / "storage" / "export_resume"


def config_fingerprint(payload: Any) -> str:
    """Stable 12-hex fingerprint of the cut-affecting config.

    Accepts either a mapping or any object exposing the
    :data:`_FINGERPRINT_KEYS` as attributes (e.g. a pydantic payload).
    A change in any fingerprinted key yields a different directory so a
    resumed run never mixes shards from an incompatible config.
    """
    def _get(key: str) -> Any:
        if isinstance(payload, dict):
            return payload.get(key)
        return getattr(payload, key, None)

    material = {key: _get(key) for key in _FINGERPRINT_KEYS}
    blob = json.dumps(material, sort_keys=True, default=str).encode()
    return hashlib.sha256(blob).hexdigest()[:12]


def _safe_name(name: str) -> str:
    """Filesystem-safe slug for a dataset name (path component)."""
    return "".join(c if c.isalnum() or c in "-_." else "_" for c in name) or "export"


def _marker_name(kind: str, index: int) -> str:
    """Filename for one cut's done-marker.

    ``kind`` is ``"train"`` or ``"test"``; ``index`` is the train-pair
    index (always ``0`` for the single test cut).
    """
    return f"{kind}_split{index}.done.json"


@dataclass(frozen=True)
class ResumeSession:
    """Filesystem bookkeeping for one resumable export run.

    Owns the stable staging directory and the per-cut done-markers. All
    methods are best-effort: a corrupt or unreadable marker is treated
    as "not complete" so the cut is recomputed rather than trusted.
    """

    dir: Path

    def is_complete(self, kind: str, index: int) -> bool:
        """True when cut ``(kind, index)`` finished and its shards persist."""
        record = self._read_raw(kind, index)
        if record is None:
            return False
        for shard in self._referenced_shards(record):
            if not Path(shard).exists():
                _LOG.warning(
                    "export resume: marker %s/%s references missing shard %s; "
                    "recomputing this cut.",
                    kind,
                    index,
                    shard,
                )
                return False
        return True

    def read_record(self, kind: str, index: int) -> dict[str, Any]:
        """Read the JSON record for a completed cut. Raises if absent."""
        record = self._read_raw(kind, index)
        if record is None:
            raise FileNotFoundError(f"no resume marker for {kind}/{index}")
        return record

    def write_record(self, kind: str, index: int, record: dict[str, Any]) -> None:
        """Atomically write the done-marker for a finished cut."""
        self.dir.mkdir(parents=True, exist_ok=True)
        target = self.dir / _marker_name(kind, index)
        tmp = target.with_suffix(target.suffix + ".tmp")
        tmp.write_text(json.dumps(record, sort_keys=True, indent=2))
        tmp.replace(target)

    def cleanup(self) -> None:
        """Remove the whole staging directory (called only on success)."""
        shutil.rmtree(self.dir, ignore_errors=True)

    def _read_raw(self, kind: str, index: int) -> dict[str, Any] | None:
        target = self.dir / _marker_name(kind, index)
        if not target.exists():
            return None
        try:
            data = json.loads(target.read_text())
        except (ValueError, OSError):
            return None
        return data if isinstance(data, dict) else None

    @staticmethod
    def _referenced_shards(record: dict[str, Any]) -> list[str]:
        shards: list[str] = []
        for value in (record.get("split_files") or {}).values():
            if value:
                shards.append(str(value))
        for value in (record.get("test_files") or {}).values():
            if value:
                shards.append(str(value))
        return shards


def open_resume_session(name: str, fingerprint: str, base: Path | None = None) -> ResumeSession:
    """Open (creating if needed) the staging directory for one export run."""
    staging = _resume_root(base) / f"{_safe_name(name)}-{fingerprint}"
    staging.mkdir(parents=True, exist_ok=True)
    return ResumeSession(dir=staging)


def train_outcome_to_record(outcome: _TrainSplitOutcome) -> dict[str, Any]:
    """Serialise a completed train-split outcome into a marker record."""
    return {
        "kind": "train",
        "skipped": bool(outcome.skipped),
        "split_files": {cat: str(path) for cat, path in outcome.split_files.items()},
        "stats": outcome.stats,
    }


def record_to_train_outcome(record: dict[str, Any]) -> _TrainSplitOutcome:
    """Rebuild a train-split outcome from a marker record."""
    from protea.core._training_dump_loaders import _TrainSplitOutcome

    return _TrainSplitOutcome(
        split_files={cat: Path(path) for cat, path in (record.get("split_files") or {}).items()},
        stats=dict(record.get("stats") or {}),
        skipped=bool(record.get("skipped", False)),
    )


def test_files_to_record(test_files: dict[str, Path | None]) -> dict[str, Any]:
    """Serialise the test-split shard map into a marker record."""
    return {
        "kind": "test",
        "test_files": {
            cat: (str(path) if path is not None else None) for cat, path in test_files.items()
        },
    }


def record_to_test_files(record: dict[str, Any]) -> dict[str, Path | None]:
    """Rebuild the test-split shard map from a marker record."""
    out: dict[str, Path | None] = {}
    for cat, path in (record.get("test_files") or {}).items():
        out[cat] = Path(path) if path else None
    return out


__all__ = [
    "ResumeSession",
    "config_fingerprint",
    "open_resume_session",
    "record_to_test_files",
    "record_to_train_outcome",
    "test_files_to_record",
    "train_outcome_to_record",
]
