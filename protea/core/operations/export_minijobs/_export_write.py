"""Final assembly stage for the export minijob pipeline.

Implements F-EXPORT-MINIJOB.4 (write half).

Each ``ExportFeaturesBatchOperation`` invocation publishes one write
message per snapshot pair onto ``protea.training.write``. The
:class:`ExportWriteOperation` consumer:

1. Records the per-pair URI as a ``pair_write_received`` ``JobEvent``
   on the coordinator's parent job so the assembly stage can recover
   every shard URI without a second cross-stage payload.
2. Atomically increments the parent's ``progress_current``. The
   delivery that wins the race to bring ``progress_current ==
   progress_total`` becomes the *assembler*: it STREAMS every per-pair
   feature parquet into ``train.parquet`` + ``eval.parquet`` (one batch
   resident via a ``pyarrow`` ``ParquetWriter``, not a whole-split
   ``pd.concat``), builds + uploads ``manifest.json``, inserts the
   ``Dataset`` row, emits ``dataset.created`` + ``pair_write_done``,
   and transitions the parent job to ``SUCCEEDED``.
3. Best-effort cleans up the temp shards after a successful upload.

The assembled layout mirrors the monolithic
:class:`~protea.core.operations.export_research_dataset.ExportResearchDatasetOperation`:
``datasets/<output_name>/{train,eval}.parquet`` + ``manifest.json``.
"""

from __future__ import annotations

import hashlib
import json
import logging
import tempfile
import uuid
from pathlib import Path
from typing import Any, NamedTuple

import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import select
from sqlalchemy import update as sa_update
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.core.operations.export_minijobs._export_features_batch import (
    _EMPTY_SHARD_URI,
    _uri_to_key,
)
from protea.core.parquet_export import resolve_protea_git_sha
from protea.core.schema_sha_v2 import maybe_v2
from protea.core.utils import utcnow
from protea.infrastructure.orm.models.embedding.dataset import Dataset
from protea.infrastructure.orm.models.job import Job, JobEvent, JobStatus
from protea.infrastructure.settings import load_settings
from protea.infrastructure.storage import get_artifact_store

_LOG = logging.getLogger(__name__)

_PAIR_RECEIVED_EVENT = "pair_write_received"

#: Rows materialised at once while streaming a per-pair shard into the
#: consolidated split parquet. Only one batch is resident, so the
#: assembler RSS stays bounded instead of the old whole-split
#: ``pd.concat`` that spiked to ~54 GB and forced ``PROTEA_EXPORT_MINIJOBS=0``.
_STREAM_BATCH_ROWS = 200_000


class ExportWritePayload(ProteaPayload, frozen=True):
    """Payload for one per-pair write minijob.

    Matches the dispatch shape produced by
    :func:`protea.core.operations.export_minijobs._export_features_batch._build_write_msg`.
    """

    coordinator_job_id: str
    pair_id: str
    temp_features_uri: str
    n_rows: int = 0
    output_name: str = ""
    embedding_config_id: str = ""
    ontology_snapshot_id: str = ""
    annotation_set_id: str = ""
    annotation_source: str = "goa"
    k: int = 5
    is_eval: bool = False


class _ShardEntry(NamedTuple):
    """One per-pair shard recovered from the JobEvent ledger."""

    pair_id: str
    temp_uri: str
    is_eval: bool
    n_rows: int


class _AssemblyOutcome(NamedTuple):
    """Bundle returned by the assembly helper."""

    train_uri: str | None
    eval_uri: str | None
    manifest_uri: str
    manifest_sha: str
    schema_sha: str
    n_train_rows: int
    n_eval_rows: int
    train_snapshot_pairs: list[str]
    eval_snapshot_pair: str | None
    key_prefix: str
    storage_backend: str


class ExportWriteOperation:
    """Assemble per-pair feature shards into the final dataset.

    Race semantics: a single atomic ``UPDATE … SET progress_current =
    progress_current + 1 RETURNING progress_current, progress_total``
    decides which delivery is the *assembler*; only that delivery runs
    the upload + ``Dataset`` insert + parent ``SUCCEEDED`` transition.
    Earlier deliveries simply record their URI in a ``JobEvent`` and
    return.
    """

    name = "export_write"
    description = (
        "OperationConsumer: assemble per-pair feature shards, upload the "
        "frozen dataset artefacts, and register the Dataset row "
        "(F-EXPORT-MINIJOB.4)."
    )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        p = payload or {}
        parts: list[str] = []
        if p.get("output_name"):
            parts.append(str(p["output_name"]))
        if p.get("pair_id"):
            parts.append(f"pair={p['pair_id']}")
        return " · ".join(parts)

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = ExportWritePayload.model_validate(payload)
        parent_id = uuid.UUID(p.coordinator_job_id)

        _record_pair_received(session, parent_id, p)
        row = _increment_parent_progress(session, parent_id)
        if row is None or row.progress_current < row.progress_total:
            return _interim_result(p)

        outcome = _assemble_and_publish(session, parent_id, p, emit)
        _finalize_parent(session, parent_id, emit)
        return OperationResult(
            result={
                "output_name": p.output_name,
                "coordinator_job_id": p.coordinator_job_id,
                "key_prefix": outcome.key_prefix,
                "train_uri": outcome.train_uri,
                "eval_uri": outcome.eval_uri,
                "manifest_uri": outcome.manifest_uri,
                "manifest_sha": outcome.manifest_sha,
                "schema_sha": outcome.schema_sha,
                "n_train_rows": outcome.n_train_rows,
                "n_eval_rows": outcome.n_eval_rows,
                "storage_backend": outcome.storage_backend,
            }
        )


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


def _interim_result(p: ExportWritePayload) -> OperationResult:
    """Result for a non-terminal write delivery (not the assembler)."""
    return OperationResult(
        result={
            "output_name": p.output_name,
            "pair_id": p.pair_id,
            "coordinator_job_id": p.coordinator_job_id,
            "assembled": False,
        }
    )


def _record_pair_received(
    session: Session, parent_id: uuid.UUID, p: ExportWritePayload
) -> None:
    """Persist the per-pair URI on the parent's event ledger."""
    session.add(
        JobEvent(
            job_id=parent_id,
            event=_PAIR_RECEIVED_EVENT,
            fields={
                "pair_id": p.pair_id,
                "temp_uri": p.temp_features_uri,
                "n_rows": int(p.n_rows or 0),
                "is_eval": bool(p.is_eval),
            },
            level="info",
        )
    )
    session.flush()


def _increment_parent_progress(session: Session, parent_id: uuid.UUID) -> Any:
    """Atomically bump ``progress_current``; return the new ``(current, total)`` row."""
    return session.execute(
        sa_update(Job)
        .where(Job.id == parent_id, Job.status == JobStatus.RUNNING)
        .values(progress_current=Job.progress_current + 1)
        .returning(Job.progress_current, Job.progress_total)
    ).fetchone()


def _load_all_pair_shards(session: Session, parent_id: uuid.UUID) -> list[_ShardEntry]:
    """Read every ``pair_write_received`` row for the parent into shard entries.

    Deduplicates by ``pair_id`` (keep the latest URI seen) so a
    re-delivery does not double-count.
    """
    rows = session.execute(
        select(JobEvent.fields).where(
            JobEvent.job_id == parent_id,
            JobEvent.event == _PAIR_RECEIVED_EVENT,
        )
    ).all()
    by_pair: dict[str, _ShardEntry] = {}
    for (fields,) in rows:
        pid = str(fields.get("pair_id", ""))
        if not pid:
            continue
        by_pair[pid] = _ShardEntry(
            pair_id=pid,
            temp_uri=str(fields.get("temp_uri", "")),
            is_eval=bool(fields.get("is_eval", False)),
            n_rows=int(fields.get("n_rows", 0)),
        )
    return list(by_pair.values())


class _StreamShardWriter:
    """Streaming writer for one consolidated split parquet.

    Opens a single :class:`pyarrow.parquet.ParquetWriter` lazily on the
    first non-empty batch (so an all-empty split leaves no file, matching
    the legacy ``if not df.empty`` guard) and appends ``snapshot_pair``
    to each batch. Only one ~200k-row batch is ever resident, replacing
    the whole-split ``pd.concat`` that spiked to ~54 GB.
    """

    def __init__(self, out_path: Path) -> None:
        self.out_path = out_path
        self.n_rows = 0
        self.schema: pa.Schema | None = None
        self._writer: pq.ParquetWriter | None = None

    @property
    def wrote(self) -> bool:
        return self.schema is not None

    def write_table(self, table: pa.Table) -> None:
        if table.num_rows == 0:
            return
        if self._writer is None:
            self.schema = table.schema
            self._writer = pq.ParquetWriter(
                str(self.out_path), table.schema, compression="snappy"
            )
        self._writer.write_table(table)
        self.n_rows += table.num_rows

    def close(self) -> None:
        if self._writer is not None:
            self._writer.close()
            self._writer = None


class _StreamAssembly(NamedTuple):
    """Result of streaming per-pair shards into the consolidated splits."""

    train_path: Path | None
    eval_path: Path | None
    train_pairs: list[str]
    eval_pair: str | None
    n_train_rows: int
    n_eval_rows: int
    schema_sha: str


def _stream_shard_into(
    store: Any, entry: _ShardEntry, writer: _StreamShardWriter
) -> None:
    """Stream one per-pair shard into ``writer`` batch by batch.

    Downloads the shard, stamps ``snapshot_pair`` on every batch, and
    writes through the consolidated-split ``ParquetWriter``. Empty
    sentinel shards (``_EMPTY_SHARD_URI`` or a zero-column parquet) are
    skipped without materialising anything.
    """
    if not entry.temp_uri or entry.temp_uri == _EMPTY_SHARD_URI:
        return
    raw = store.get(_uri_to_key(entry.temp_uri))
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        path = Path(tmp.name)
        path.write_bytes(raw)
    try:
        pf = pq.ParquetFile(str(path))
        if pf.metadata is None or pf.metadata.num_columns == 0:
            return  # empty-shard sentinel: no columns to consolidate.
        for batch in pf.iter_batches(batch_size=_STREAM_BATCH_ROWS):
            if batch.num_rows == 0:
                continue
            table = pa.Table.from_batches([batch])
            snap = pa.array([entry.pair_id] * table.num_rows, type=pa.string())
            writer.write_table(table.append_column("snapshot_pair", snap))
    finally:
        path.unlink(missing_ok=True)


def _stream_assemble(
    store: Any, shards: list[_ShardEntry], stage_dir: Path
) -> _StreamAssembly:
    """Stream per-pair shards into ``train.parquet`` / ``eval.parquet``.

    Replaces the whole-split ``pd.concat`` (the ~54 GB write-OOM that
    forced ``PROTEA_EXPORT_MINIJOBS=0``) with a per-batch
    ``ParquetWriter`` stream. ``snapshot_pair`` is stamped on every row
    exactly as the legacy concat did, and the column set / order is the
    per-pair shard's canonical schema plus the trailing ``snapshot_pair``,
    so the published schema is unchanged.
    """
    train_writer = _StreamShardWriter(stage_dir / "train.parquet")
    eval_writer = _StreamShardWriter(stage_dir / "eval.parquet")
    train_pairs: list[str] = []
    eval_pair: str | None = None
    try:
        # Sort for deterministic ordering: train-* first by pair_id, then eval-*.
        for entry in sorted(shards, key=lambda e: (e.is_eval, e.pair_id)):
            if entry.is_eval:
                if eval_pair is None:
                    eval_pair = entry.pair_id
                _stream_shard_into(store, entry, eval_writer)
            else:
                if entry.pair_id not in train_pairs:
                    train_pairs.append(entry.pair_id)
                _stream_shard_into(store, entry, train_writer)
    finally:
        train_writer.close()
        eval_writer.close()
    return _StreamAssembly(
        train_path=train_writer.out_path if train_writer.wrote else None,
        eval_path=eval_writer.out_path if eval_writer.wrote else None,
        train_pairs=train_pairs,
        eval_pair=eval_pair,
        n_train_rows=train_writer.n_rows,
        n_eval_rows=eval_writer.n_rows,
        schema_sha=_schema_sha_from_writers(train_writer, eval_writer),
    )


def _schema_sha_from_writers(
    train_writer: _StreamShardWriter, eval_writer: _StreamShardWriter
) -> str:
    """SHA-256 (12 hex) over the sorted column names of the assembled split.

    Mirrors :func:`protea.core.parquet_export._compute_schema_sha` shape
    so downstream readers see the same fingerprint format regardless of
    producer path. Prefers the train schema, falling back to eval.
    """
    schema = train_writer.schema if train_writer.wrote else eval_writer.schema
    cols = sorted(str(c) for c in schema.names) if schema is not None else []
    return hashlib.sha256(json.dumps(cols, sort_keys=True).encode()).hexdigest()[:12]


def _build_manifest(
    p: ExportWritePayload,
    train_pairs: list[str],
    eval_pair: str | None,
    n_train: int,
    n_eval: int,
    schema_sha: str,
) -> dict[str, Any]:
    """Assemble the ``manifest.json`` dict (ManifestV1 v2 shape)."""
    from protea import __version__ as _protea_version

    return {
        "schema_version": "v2",
        "name": p.output_name,
        "k": int(p.k),
        "embedding_config_id": p.embedding_config_id,
        "ontology_snapshot_id": p.ontology_snapshot_id,
        "annotation_source": p.annotation_source,
        "train_snapshot_pairs": train_pairs,
        "eval_snapshot_pair": eval_pair,
        "schema_sha": schema_sha,
        "n_train_rows": int(n_train),
        "n_eval_rows": int(n_eval),
        "format": "parquet",
        "producer_version": _protea_version,
        "producer_git_sha": resolve_protea_git_sha(),
    }


def _upload_artefacts(
    store: Any,
    key_prefix: str,
    asm: _StreamAssembly,
    manifest: dict[str, Any],
    stage: Path,
) -> tuple[str | None, str | None, str, str]:
    """Upload the streamed ``train``/``eval`` parquets + ``manifest.json``.

    The split parquets were already streamed to disk under ``stage`` by
    :func:`_stream_assemble`; this only uploads them (no whole-frame
    ``to_parquet``). Returns ``(train_uri, eval_uri, manifest_uri,
    manifest_sha)``.
    """
    train_uri = (
        store.put(key_prefix + "/train.parquet", asm.train_path)
        if asm.train_path is not None
        else None
    )
    eval_uri = (
        store.put(key_prefix + "/eval.parquet", asm.eval_path)
        if asm.eval_path is not None
        else None
    )
    manifest_bytes = json.dumps(manifest, indent=2, sort_keys=True).encode()
    manifest_sha = hashlib.sha256(manifest_bytes).hexdigest()
    manifest_path = stage / "manifest.json"
    manifest_path.write_bytes(manifest_bytes)
    manifest_uri = store.put(key_prefix + "/manifest.json", manifest_path)
    return train_uri, eval_uri, manifest_uri, manifest_sha


def _insert_dataset_row(
    session: Session,
    p: ExportWritePayload,
    outcome: _AssemblyOutcome,
    parent_id: uuid.UUID,
) -> uuid.UUID:
    """Insert the ``Dataset`` row and return its id."""
    dataset = Dataset(
        name=p.output_name,
        operation="export_research_dataset",
        job_id=parent_id,
        storage_backend=outcome.storage_backend,
        key_prefix=outcome.key_prefix + "/",
        train_uri=outcome.train_uri,
        eval_uri=outcome.eval_uri,
        manifest_uri=outcome.manifest_uri,
        schema_sha=outcome.schema_sha,
        schema_sha_v2=maybe_v2(outcome.schema_sha or None),
        manifest_sha=outcome.manifest_sha,
        n_train_rows=int(outcome.n_train_rows),
        n_eval_rows=int(outcome.n_eval_rows),
        k=int(p.k),
        annotation_source=p.annotation_source,
        embedding_config_id=uuid.UUID(p.embedding_config_id) if p.embedding_config_id else None,
        ontology_snapshot_id=uuid.UUID(p.ontology_snapshot_id) if p.ontology_snapshot_id else None,
        train_snapshot_pairs=list(outcome.train_snapshot_pairs),
        eval_snapshot_pair=outcome.eval_snapshot_pair,
        producer_version=None,
        producer_git_sha=resolve_protea_git_sha(),
        meta={},
    )
    session.add(dataset)
    session.flush()
    return dataset.id


def _assemble_and_publish(
    session: Session,
    parent_id: uuid.UUID,
    p: ExportWritePayload,
    emit: EmitFn,
) -> _AssemblyOutcome:
    """Run the terminal-delivery work: concat, upload, insert, emit."""
    settings = load_settings(Path(__file__).resolve().parents[4])
    store = get_artifact_store(settings)
    shards = _load_all_pair_shards(session, parent_id)
    key_prefix = f"datasets/{p.output_name}"
    with tempfile.TemporaryDirectory(prefix="protea_export_write_") as tmp:
        stage = Path(tmp)
        asm = _stream_assemble(store, shards, stage)
        manifest = _build_manifest(
            p,
            asm.train_pairs,
            asm.eval_pair,
            asm.n_train_rows,
            asm.n_eval_rows,
            asm.schema_sha,
        )
        train_uri, eval_uri, manifest_uri, manifest_sha = _upload_artefacts(
            store, key_prefix, asm, manifest, stage
        )
    outcome = _AssemblyOutcome(
        train_uri=train_uri,
        eval_uri=eval_uri,
        manifest_uri=manifest_uri,
        manifest_sha=manifest_sha,
        schema_sha=asm.schema_sha,
        n_train_rows=asm.n_train_rows,
        n_eval_rows=asm.n_eval_rows,
        train_snapshot_pairs=asm.train_pairs,
        eval_snapshot_pair=asm.eval_pair,
        key_prefix=key_prefix,
        storage_backend=settings.storage_backend,
    )
    dataset_id = _insert_dataset_row(session, p, outcome, parent_id)
    _emit_terminal_events(emit, p, outcome, dataset_id)
    _cleanup_temp_shards(store, shards)
    return outcome


def _emit_terminal_events(
    emit: EmitFn,
    p: ExportWritePayload,
    outcome: _AssemblyOutcome,
    dataset_id: uuid.UUID,
) -> None:
    """Emit ``dataset.created`` + ``pair_write_done`` for the terminal delivery."""
    emit(
        "dataset.created",
        None,
        {
            "dataset_id": str(dataset_id),
            "name": p.output_name,
            "train_uri": outcome.train_uri,
            "eval_uri": outcome.eval_uri,
            "manifest_uri": outcome.manifest_uri,
            "manifest_sha": outcome.manifest_sha,
            "schema_sha": outcome.schema_sha,
            "n_train_rows": outcome.n_train_rows,
            "n_eval_rows": outcome.n_eval_rows,
        },
        "info",
    )
    emit(
        "pair_write_done",
        None,
        {
            "coordinator_job_id": p.coordinator_job_id,
            "output_name": p.output_name,
            "dataset_id": str(dataset_id),
        },
        "info",
    )


def _finalize_parent(session: Session, parent_id: uuid.UUID, emit: EmitFn) -> None:
    """Transition the coordinator job ``RUNNING → SUCCEEDED``."""
    closed = session.execute(
        sa_update(Job)
        .where(Job.id == parent_id, Job.status == JobStatus.RUNNING)
        .values(status=JobStatus.SUCCEEDED, finished_at=utcnow())
        .returning(Job.id)
    ).fetchone()
    if not closed:
        return
    session.add(
        JobEvent(
            job_id=parent_id,
            event="job.succeeded",
            fields={"via": "export_write_assembled"},
            level="info",
        )
    )
    emit(
        "export_write.parent_succeeded",
        None,
        {"parent_job_id": str(parent_id)},
        "info",
    )


def _cleanup_temp_shards(store: Any, shards: list[_ShardEntry]) -> None:
    """Best-effort delete of the per-pair temp parquets."""
    for entry in shards:
        if not entry.temp_uri or entry.temp_uri == _EMPTY_SHARD_URI:
            continue
        try:
            store.delete(_uri_to_key(entry.temp_uri))
        except Exception as exc:
            _LOG.warning(
                "export_write: temp shard cleanup failed (non-fatal). uri=%s error=%s",
                entry.temp_uri,
                exc,
            )
