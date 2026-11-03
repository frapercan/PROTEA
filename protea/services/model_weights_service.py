"""The registry of model weights: list, register by reference, upload in parts (ADR-D54).

A row of ``model_weights`` says which file an artifact store key holds (sha256 and
size), what kind of weight it is, what it was fitted on and when it arrived. Two ways
in, both checked against the bytes:

* **By reference.** The file is already in the store, such as vocabulary v1 at
  ``residue-vocabulary/vocabulary-v1-06fed75ca76d17be.npz``. The caller gives the key and
  the sha256 its producer recorded; the object is read in chunks and the row is refused
  if the digest differs.
* **Uploaded in parts.** The web proxy caps a request at 100 MB and vocabulary v1 alone is
  260 MB, so a client creates an upload, sends numbered parts of a fixed size and
  completes it. The parts and the joined file live on the API host's disk (never whole in
  memory), the joined file is hashed while it is written, put in the store under a
  content-addressed key, ``model-weights/<kind>/<name>-<sha256[:16]><ext>``, and the
  upload folder is removed whatever happens.

The detail of a weight also says what uses it: the configurations made from it and the
jobs whose payload names its key, computed when read so they cannot drift.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import shutil
import time
import uuid
from collections.abc import Iterable
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from protea.config.tuning import get_tuning
from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
from protea.infrastructure.orm.models.embedding.model_weights import WEIGHT_KINDS, ModelWeights
from protea.infrastructure.orm.models.job import Job

#: A name becomes part of a store key, so it is a plain file-name stem.
NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,119}$")
SHA256 = re.compile(r"^[0-9a-f]{64}$")

#: Extension to format, longest suffix first.
FORMATS = (
    (".tar.gz", "tar.gz"),
    (".safetensors", "safetensors"),
    (".npz", "npz"),
    (".npy", "npy"),
    (".pth", "pt"),
    (".pt", "pt"),
    (".onnx", "onnx"),
    (".json", "json"),
    (".tgz", "tar.gz"),
    (".tar", "tar"),
)

#: Payload fields through which an operation names a weights file.
PAYLOAD_KEYS = ("vocabulary_artifact_uri", "encoder_artifact_uri")

#: An upload left unfinished is removed after a day.
STALE_UPLOAD_SECONDS = 24 * 3600

_CHUNK = 8 << 20


class WeightsError(ValueError):
    """The request is wrong: an unknown kind, a bad name or hash, a missing part."""


class WeightsNotFound(LookupError):
    """The upload or the store object does not exist."""


class WeightsConflict(Exception):
    """The key is already registered; ``existing_id`` is its row."""

    def __init__(self, message: str, existing_id: uuid.UUID) -> None:
        super().__init__(message)
        self.existing_id = existing_id


@dataclass(frozen=True)
class WeightsInfo:
    """What a caller says about a weight; the file itself says its hash, size and format."""

    kind: str
    name: str
    description: str | None = None
    meta: dict[str, Any] = field(default_factory=dict)
    trained_on_annotation_set_id: uuid.UUID | None = None
    created_by: str | None = None

    def checked(self) -> WeightsInfo:
        validate_kind(self.kind)
        validate_name(self.name)
        return self

    def row_fields(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "name": self.name,
            "description": self.description,
            "meta": self.meta or {},
            "trained_on_annotation_set_id": self.trained_on_annotation_set_id,
            "created_by": self.created_by,
        }


# ── Small pure pieces ─────────────────────────────────────────────────────────


def validate_kind(kind: str) -> str:
    if kind not in WEIGHT_KINDS:
        raise WeightsError(f"unknown kind {kind!r}; one of {', '.join(WEIGHT_KINDS)}")
    return kind


def validate_name(name: str) -> str:
    if not NAME.match(name or ""):
        raise WeightsError(
            f"name {name!r} must be a file-name stem: letters, digits, dot, dash or "
            "underscore, starting with a letter or digit, at most 120 characters"
        )
    return name


def validate_sha256(digest: str) -> str:
    digest = (digest or "").strip().lower()
    if not SHA256.match(digest):
        raise WeightsError("sha256 must be 64 lowercase hexadecimal characters")
    return digest


def format_of(filename: str) -> tuple[str, str]:
    """(format, extension) of a file name; ``bin`` and no extension when unknown."""
    lower = filename.lower()
    for ext, fmt in FORMATS:
        if lower.endswith(ext):
            return fmt, ext
    return "bin", ""


def content_key(kind: str, name: str, sha256: str, ext: str) -> str:
    """The content-addressed key an uploaded file gets; the resolver verifies its digest."""
    return f"model-weights/{kind}/{name}-{sha256[:16]}{ext}"


def hash_chunks(chunks: Iterable[bytes]) -> tuple[str, int]:
    """sha256 and size of a stream of bytes."""
    h = hashlib.sha256()
    size = 0
    for chunk in chunks:
        h.update(chunk)
        size += len(chunk)
    return h.hexdigest(), size


def _store_chunks(store: Any, key: str) -> Iterable[bytes]:
    """The object in chunks when the store can stream it, else whole."""
    stream = getattr(store, "iter_chunks", None)
    if stream is not None:
        return stream(key, _CHUNK)
    return [store.get(key)]


def weights_dict(row: ModelWeights) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "kind": row.kind,
        "name": row.name,
        "description": row.description,
        "format": row.format,
        "uri": row.uri,
        "sha256": row.sha256,
        "size_bytes": row.size_bytes,
        "meta": row.meta or {},
        "trained_on_annotation_set_id": (
            str(row.trained_on_annotation_set_id) if row.trained_on_annotation_set_id else None
        ),
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "created_by": row.created_by,
    }


# ── Reading ───────────────────────────────────────────────────────────────────


def _derived_configs(session: Session, row: ModelWeights) -> list[EmbeddingConfig]:
    """Configurations made from a weight. A residue vocabulary's carry its digest in the name."""
    if row.kind != "residue_vocabulary":
        return []
    return list(
        session.scalars(
            select(EmbeddingConfig)
            .where(
                EmbeddingConfig.model_backend == "residue-vocabulary",
                EmbeddingConfig.model_name.like(f"residue-vocabulary:{row.sha256[:16]}:%"),
            )
            .order_by(EmbeddingConfig.created_at.desc())
        )
    )


def _jobs_naming(session: Session, uri: str, limit: int = 20) -> list[Job]:
    clauses = [Job.payload[k].astext == uri for k in PAYLOAD_KEYS]
    return list(
        session.scalars(
            select(Job).where(or_(*clauses)).order_by(Job.created_at.desc()).limit(limit)
        )
    )


def list_weights(session: Session, kind: str | None = None) -> list[dict[str, Any]]:
    """Every registered weight, newest first, with how many configurations use it."""
    query = select(ModelWeights).order_by(ModelWeights.created_at.desc())
    if kind:
        query = query.where(ModelWeights.kind == validate_kind(kind))
    out = []
    for row in session.scalars(query):
        item = weights_dict(row)
        item["configurations"] = len(_derived_configs(session, row))
        out.append(item)
    return out


def weights_detail(session: Session, weights_id: uuid.UUID) -> dict[str, Any] | None:
    """One weight with what uses it: configurations made from it and jobs naming its key."""
    row = session.get(ModelWeights, weights_id)
    if row is None:
        return None
    item = weights_dict(row)
    item["used_by"] = {
        "configurations": [
            {"id": str(c.id), "name": c.display_name or c.model_name, "kind": c.kind}
            for c in _derived_configs(session, row)
        ],
        "jobs": [
            {
                "id": str(j.id),
                "operation": j.operation,
                "status": getattr(j.status, "value", j.status),
                "created_at": j.created_at.isoformat() if j.created_at else None,
            }
            for j in _jobs_naming(session, row.uri)
        ],
    }
    return item


# ── Writing ───────────────────────────────────────────────────────────────────


def _refuse_registered(session: Session, uri: str) -> None:
    existing = session.scalars(select(ModelWeights).where(ModelWeights.uri == uri)).first()
    if existing is not None:
        raise WeightsConflict(f"{uri} is already registered", existing.id)


def _insert(session: Session, **fields: Any) -> ModelWeights:
    row = ModelWeights(**fields)
    session.add(row)
    session.flush()
    return row


def register_by_reference(
    session: Session, store: Any, info: WeightsInfo, uri: str, sha256: str
) -> ModelWeights:
    """Register a file already in the store, after hashing it there."""
    info.checked()
    digest = validate_sha256(sha256)
    if not uri or uri.startswith("/"):
        raise WeightsError("uri must be an artifact store key, not a path")
    _refuse_registered(session, uri)
    if not store.exists(uri):
        raise WeightsNotFound(f"{uri} is not in the artifact store")
    found, size = hash_chunks(_store_chunks(store, uri))
    if found != digest:
        raise WeightsError(f"{uri} hashes to {found}, not to the sha256 given ({digest})")
    fmt, _ = format_of(uri)
    return _insert(session, format=fmt, uri=uri, sha256=found, size_bytes=size, **info.row_fields())


# ── Uploads in parts ──────────────────────────────────────────────────────────


#: Where uploads go when ``api.weights_upload_dir`` is unset: the API user's cache
#: folder, on disk. Not the system temporary folder, which on the API host is a tmpfs:
#: an upload there takes RAM from the machine that runs Postgres, RabbitMQ and MinIO.
DEFAULT_UPLOAD_ROOT = Path.home() / ".cache" / "protea" / "weights-uploads"

#: Room kept free beyond the file itself while an upload is open.
UPLOAD_HEADROOM_BYTES = 1 << 30


def upload_root() -> Path:
    configured = get_tuning().api.weights_upload_dir
    root = Path(configured) if configured else DEFAULT_UPLOAD_ROOT
    root.mkdir(parents=True, exist_ok=True)
    return root


def _refuse_without_room(root: Path, size_bytes: int) -> None:
    """Refuse an upload the disk cannot hold, before any byte is sent.

    The parts and the joined file never coexist whole (``_join`` drops each part once
    copied), so an upload needs the file's size plus one part; this asks for the file
    plus a gigabyte rather than fill the disk midway.
    """
    free = shutil.disk_usage(root).free
    if free < size_bytes + UPLOAD_HEADROOM_BYTES:
        raise WeightsError(
            f"{root} has {free} bytes free; an upload of {size_bytes} bytes needs "
            f"{size_bytes + UPLOAD_HEADROOM_BYTES}. Point api.weights_upload_dir at a larger disk"
        )


def _upload_dir(upload_id: str) -> Path:
    try:
        uuid.UUID(upload_id)
    except ValueError as exc:
        raise WeightsError(f"{upload_id!r} is not an upload id") from exc
    path = upload_root() / upload_id
    if not (path / "manifest.json").exists():
        raise WeightsNotFound(f"no upload {upload_id}")
    return path


def _is_upload(path: Path) -> bool:
    """A folder this module made: named by a UUID and holding a manifest."""
    try:
        uuid.UUID(path.name)
    except ValueError:
        return False
    return path.is_dir() and (path / "manifest.json").exists()


def _purge_stale(now: float) -> None:
    """Remove unfinished uploads older than a day and nothing else under the root, since
    ``api.weights_upload_dir`` may point at a folder that holds other things."""
    for path in upload_root().iterdir():
        if _is_upload(path) and now - path.stat().st_mtime > STALE_UPLOAD_SECONDS:
            shutil.rmtree(path, ignore_errors=True)


def create_upload(
    info: WeightsInfo, filename: str, size_bytes: int, sha256: str | None = None
) -> dict[str, Any]:
    """Open an upload: a folder with its manifest. Returns the id, the part size and the parts."""
    info.checked()
    limits = get_tuning().api
    if size_bytes <= 0 or size_bytes > limits.max_weights_bytes:
        raise WeightsError(f"size must be between 1 byte and {limits.max_weights_bytes} bytes")
    expected = validate_sha256(sha256) if sha256 else None
    _purge_stale(time.time())
    _refuse_without_room(upload_root(), size_bytes)
    upload_id = str(uuid.uuid4())
    folder = upload_root() / upload_id
    folder.mkdir()
    part_bytes = limits.weights_part_bytes
    info_fields = asdict(info)
    if info.trained_on_annotation_set_id:
        info_fields["trained_on_annotation_set_id"] = str(info.trained_on_annotation_set_id)
    manifest = {
        "info": info_fields,
        "filename": filename,
        "size_bytes": size_bytes,
        "sha256": expected,
        "part_bytes": part_bytes,
        "parts": math.ceil(size_bytes / part_bytes),
    }
    (folder / "manifest.json").write_text(json.dumps(manifest))
    return {"upload_id": upload_id, "part_bytes": part_bytes, "parts": manifest["parts"]}


def _manifest(folder: Path) -> dict[str, Any]:
    manifest: dict[str, Any] = json.loads((folder / "manifest.json").read_text())
    return manifest


def _expected_part_size(manifest: dict[str, Any], n: int) -> int:
    last = manifest["parts"] - 1
    if n < last:
        return int(manifest["part_bytes"])
    return int(manifest["size_bytes"] - manifest["part_bytes"] * last)


def put_part(upload_id: str, n: int, data: bytes) -> dict[str, Any]:
    """Store part ``n``; its size must be the part size (the last one, the remainder)."""
    folder = _upload_dir(upload_id)
    manifest = _manifest(folder)
    if not 0 <= n < manifest["parts"]:
        raise WeightsError(f"part {n} is outside 0..{manifest['parts'] - 1}")
    want = _expected_part_size(manifest, n)
    if len(data) != want:
        raise WeightsError(f"part {n} has {len(data)} bytes; it must have {want}")
    tmp = folder / f"part-{n:06d}.tmp"
    tmp.write_bytes(data)
    tmp.replace(folder / f"part-{n:06d}")
    received = sorted(p.name for p in folder.glob("part-[0-9]*") if not p.name.endswith(".tmp"))
    return {
        "upload_id": upload_id,
        "part": n,
        "received": len(received),
        "parts": manifest["parts"],
    }


def _join(folder: Path, parts: int) -> tuple[Path, str, int]:
    """Concatenate the parts into one file, hashing while writing.

    Each part is deleted once copied, so the folder holds the file's size plus at most
    one part, never twice the file.
    """
    joined = folder / "joined"
    h = hashlib.sha256()
    size = 0
    with joined.open("wb") as out:
        for n in range(parts):
            path = folder / f"part-{n:06d}"
            with path.open("rb") as part:
                while chunk := part.read(_CHUNK):
                    h.update(chunk)
                    size += len(chunk)
                    out.write(chunk)
            path.unlink()
    return joined, h.hexdigest(), size


def complete_upload(session: Session, store: Any, upload_id: str) -> ModelWeights:
    """Join, hash, check, put in the store under the content-addressed key, register.

    The upload folder is removed whether this succeeds or fails, so a failed upload is
    started again rather than left half on disk.
    """
    folder = _upload_dir(upload_id)
    try:
        manifest = _manifest(folder)
        missing = [n for n in range(manifest["parts"]) if not (folder / f"part-{n:06d}").exists()]
        if missing:
            raise WeightsError(
                f"parts missing: {missing[:10]}{' ...' if len(missing) > 10 else ''}"
            )
        joined, digest, size = _join(folder, manifest["parts"])
        if size != manifest["size_bytes"]:
            raise WeightsError(f"joined {size} bytes, announced {manifest['size_bytes']}")
        if manifest["sha256"] and digest != manifest["sha256"]:
            raise WeightsError(f"the file hashes to {digest}, not to the sha256 announced")
        fields = dict(manifest["info"])
        trained = fields.get("trained_on_annotation_set_id")
        fields["trained_on_annotation_set_id"] = uuid.UUID(trained) if trained else None
        info = WeightsInfo(**fields)
        fmt, ext = format_of(manifest["filename"])
        key = content_key(info.kind, info.name, digest, ext)
        _refuse_registered(session, key)
        if not store.exists(key):
            store.put(key, joined)
        return _insert(
            session, format=fmt, uri=key, sha256=digest, size_bytes=size, **info.row_fields()
        )
    finally:
        shutil.rmtree(folder, ignore_errors=True)


def abort_upload(upload_id: str) -> None:
    shutil.rmtree(_upload_dir(upload_id), ignore_errors=True)
