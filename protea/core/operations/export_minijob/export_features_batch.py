"""export_features_batch: CPU feature generation for one snapshot pair.

This is an ``OperationConsumer`` operation (no DB Job row per message).
It runs on ``protea.training.features`` workers.

Execution flow:
1. Read the temp KNN artefact written by ``export_knn_batch``.
2. Run the CPU feature path using the existing dump pipeline.
3. Write the per-pair feature shard as parquet to:
   ``temp/coordinator/<coordinator_job_id>/features/<pair_id>.parquet``.
4. Emit ``pair_features_done`` event.
5. Increment the parent coordinator job's progress counter via
   ``update_parent_progress``; the last shard marks the coordinator
   SUCCEEDED.
"""

from __future__ import annotations

import logging
import tempfile
import time
import uuid
from io import BytesIO
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, OperationResult
from protea.core.contracts.parent_progress import update_parent_progress
from protea.core.operations.export_minijob._payloads import ExportFeaturesBatchPayload

_LOG = logging.getLogger(__name__)


class ExportFeaturesBatchOperation:
    """OperationConsumer: run CPU features for one snapshot pair.

    No DB Job row is created per message. Progress is reported back to
    the coordinator job via ``update_parent_progress``.
    """

    name = "export_features_batch"
    description = (
        "CPU minijob: load KNN temp artefact, run feature engineering "
        "for one snapshot pair, write per-pair feature parquet shard."
    )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        p = payload or {}
        bits = []
        if p.get("pair_id"):
            bits.append(str(p["pair_id"]))
        if p.get("output_name"):
            bits.append(str(p["output_name"]))
        return " · ".join(bits)

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = ExportFeaturesBatchPayload.model_validate(payload)
        coordinator_job_id = uuid.UUID(p.coordinator_job_id)
        t0 = time.perf_counter()
        emit(
            "export_features_batch.start",
            None,
            {"pair_id": p.pair_id, "pair_type": p.pair_type},
            "info",
        )
        knn_data = _read_knn_artefact(p.knn_temp_uri, emit)
        records = _run_feature_pipeline(session, p, knn_data, emit)
        shard_key = _features_shard_key(p.coordinator_job_id, p.pair_id)
        shard_uri = _write_feature_shard(shard_key, records, emit)
        elapsed = time.perf_counter() - t0
        emit(
            "export_features_batch.pair_features_done",
            None,
            {
                "pair_id": p.pair_id,
                "shard_uri": shard_uri,
                "n_rows": len(records),
                "elapsed_seconds": elapsed,
            },
            "info",
        )
        update_parent_progress(
            session,
            coordinator_job_id,
            emit,
            event_name="export_features_batch.coordinator_succeeded",
        )
        return OperationResult(
            result={
                "pair_id": p.pair_id,
                "pair_type": p.pair_type,
                "shard_uri": shard_uri,
                "n_rows": len(records),
                "elapsed_seconds": elapsed,
            }
        )


def _features_shard_key(coordinator_job_id: str, pair_id: str) -> str:
    """Return the artifact-store key for a feature shard."""
    return f"temp/coordinator/{coordinator_job_id}/features/{pair_id}.parquet"


def _read_knn_artefact(knn_temp_uri: str, emit: EmitFn) -> dict[str, Any]:
    """Fetch the KNN npz artefact from the artifact store."""
    from protea.infrastructure.settings import load_settings
    from protea.infrastructure.storage import get_artifact_store

    project_root = Path(__file__).resolve().parents[5]
    settings = load_settings(project_root)
    store = get_artifact_store(settings)
    key = _uri_to_key(knn_temp_uri, store)
    emit("export_features_batch.read_knn", None, {"uri": knn_temp_uri}, "info")
    raw = store.get(key)
    npz = np.load(BytesIO(raw))
    return {
        "indices": npz["indices"],
        "distances": npz["distances"],
        "query_accessions": npz["query_accessions"].tolist(),
        "ref_accessions": npz["ref_accessions"].tolist(),
    }


def _uri_to_key(uri: str, store: Any) -> str:
    """Extract the store key from a URI string.

    LocalFsArtifactStore uses ``file:///abs/path`` URIs.
    MinioArtifactStore uses ``s3://bucket/key``.
    """
    if uri.startswith("file://"):
        from protea.infrastructure.storage.local import LocalFsArtifactStore

        if isinstance(store, LocalFsArtifactStore):
            abs_path = uri[len("file://"):]
            store_root = str(store._root)
            if abs_path.startswith(store_root):
                return abs_path[len(store_root):].lstrip("/")
    if uri.startswith("s3://"):
        parts = uri[len("s3://"):].split("/", 1)
        return parts[1] if len(parts) > 1 else uri
    return uri


def _run_feature_pipeline(
    session: Session,
    p: ExportFeaturesBatchPayload,
    knn_data: dict[str, Any],
    emit: EmitFn,
) -> list[dict[str, Any]]:
    """Run the CPU feature path for one snapshot pair via the dump pipeline.

    Re-uses ``TrainRerankerAutoOperation`` in ``dump_only=True`` mode
    scoped to ``(ref_version, query_version)`` so feature engineering is
    identical to the monolithic path.
    """
    from protea.core.training_dump_helpers import TrainRerankerAutoOperation

    auto = TrainRerankerAutoOperation()
    pair_payload = _build_pair_payload(p)
    emit(
        "export_features_batch.feature_pipeline_start",
        None,
        {"pair_id": p.pair_id, "pair_type": p.pair_type},
        "info",
    )
    with tempfile.TemporaryDirectory(prefix="protea_features_") as tmp_dir:
        pair_payload["dump_to"] = tmp_dir

        def _relay(event: str, scope: Any, evt_payload: dict[str, Any], level: str) -> None:
            emit(f"export_features_batch.{event}", scope, evt_payload, level)

        auto.execute(session, pair_payload, emit=_relay)
        records = _load_shard_records(tmp_dir, emit)

    emit(
        "export_features_batch.feature_pipeline_done",
        None,
        {"pair_id": p.pair_id, "n_rows": len(records)},
        "info",
    )
    return records


def _build_pair_payload(p: ExportFeaturesBatchPayload) -> dict[str, Any]:
    """Build the payload dict for the single-pair dump call."""
    return {
        "name": p.output_name,
        "embedding_config_id": p.embedding_config_id,
        "ontology_snapshot_id": p.ontology_snapshot_id,
        "train_versions": [p.ref_version, p.query_version],
        "test_versions": [p.query_version],
        "annotation_source": p.annotation_source,
        "limit_per_entry": p.k,
        "search_backend": p.search_backend,
        "compute_alignments": p.compute_alignments,
        "compute_taxonomy": p.compute_taxonomy,
        "expand_votes_to_ancestors": p.expand_votes_to_ancestors,
        "use_embedding_pca": p.use_embedding_pca,
        "training_scope": "per_cell",
        "dump_only": True,
    }


def _load_shard_records(tmp_dir: str, emit: EmitFn) -> list[dict[str, Any]]:
    """Read all parquet files produced by the dump and return as records."""
    shard_dir = Path(tmp_dir)
    parquet_files = sorted(shard_dir.glob("*.parquet"))
    if not parquet_files:
        emit("export_features_batch.no_shards", None, {"tmp_dir": tmp_dir}, "warning")
        return []
    tables = [pq.read_table(str(f)) for f in parquet_files]
    combined = pa.concat_tables(tables)
    return combined.to_pylist()


def _write_feature_shard(
    shard_key: str,
    records: list[dict[str, Any]],
    emit: EmitFn,
) -> str:
    """Write per-pair feature records to a parquet shard in the artifact store."""
    from protea.infrastructure.settings import load_settings
    from protea.infrastructure.storage import get_artifact_store

    project_root = Path(__file__).resolve().parents[5]
    settings = load_settings(project_root)
    store = get_artifact_store(settings)
    table = pa.Table.from_pylist(records) if records else pa.table({})
    buf = BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)
    uri = store.put(shard_key, buf.getvalue())
    emit("export_features_batch.shard_written", None, {"key": shard_key, "uri": uri}, "info")
    return uri
