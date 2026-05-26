"""Features batch operation: compute CPU features for one snapshot pair.

Implements F-EXPORT-MINIJOB.3 (features half).

Receives a per-pair features payload that includes the temp KNN URI from
``ExportKnnBatchOperation``, loads the pre-computed KNN results from the
artifact store, then runs the full feature pipeline (anc2vec, lineage,
reranker features, pair features, PCA) using the existing helpers from
``_KnnTransferRunner``.  Writes the per-pair feature parquet shard to:
  ``temp/coordinator/<coordinator_job_id>/features/<pair_id>.parquet``
then publishes a follow-on message to ``protea.training.write``.
"""

from __future__ import annotations

import logging
import tempfile
import uuid
from pathlib import Path
from typing import Any, NamedTuple

import numpy as np
from sqlalchemy.orm import Session

from protea.core._training_dump_loaders import (
    _load_go_maps,
    _maybe_fit_pca_state,
)
from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.core.domain.aspect import ASPECT_CODES as _ASPECTS
from protea.core.training_dump._contexts import KnnTransferContext, SequenceContext
from protea.core.training_dump._data_loaders import (
    _build_reference_from_cache,
    _load_parent_map,
    _load_sequences,
    _load_taxonomy_ids,
    _preload_all_embeddings,
)
from protea.infrastructure.settings import load_settings
from protea.infrastructure.storage import get_artifact_store

_LOG = logging.getLogger(__name__)

_WRITE_QUEUE = "protea.training.write"


class ExportFeaturesBatchPayload(ProteaPayload, frozen=True):
    """Payload for a single per-pair feature-computation minijob."""

    coordinator_job_id: str
    pair_id: str
    temp_knn_uri: str | None
    embedding_config_id: str
    annotation_set_id: str
    ontology_snapshot_id: str
    output_name: str = ""
    annotation_source: str = "goa"
    k: int = 5
    is_eval: bool = False
    compute_alignments: bool = False
    compute_taxonomy: bool = False
    expand_votes_to_ancestors: bool = False
    use_embedding_pca: bool = False


class _LoadedDbData(NamedTuple):
    """Bundle of DB-loaded data required by the feature pipeline.

    Groups the embedding pool, per-aspect reference set, GO maps, parent
    map, and PCA state so the methods that consume them stay well under
    the §3 6-arg ceiling.
    """

    all_embeddings: np.ndarray
    acc_to_idx: dict[str, int]
    ref_by_aspect: dict[str, dict[str, Any]]
    go_id_map: dict[Any, str]
    aspect_map: dict[Any, str]
    parent_map: dict[str, set[str]] | None
    pca_state: tuple[np.ndarray, np.ndarray] | None
    pivot_go_ids: set[str]


class ExportFeaturesBatchOperation:
    """Compute CPU features for one snapshot pair and write a temp parquet shard.

    Pipeline:
    1. Load the pre-computed KNN npz from the artifact store.
    2. Reload embeddings + per-aspect reference data for this annotation set.
    3. Load GO maps + parent map + optional PCA state.
    4. Run feature phases (anc2vec, lineage, reranker features, alignment,
       taxonomy, PCA projection) via ``_PrecomputedKnnRunner``.
    5. Write the labeled feature records to a temp parquet at
       ``temp/coordinator/<coordinator_job_id>/features/<pair_id>.parquet``.
    6. Emit ``pair_features_done`` with the temp URI.
    7. Publish follow-on message to ``protea.training.write``.
    """

    name = "export_features_batch"
    description = (
        "OperationConsumer: compute CPU features for one snapshot pair "
        "and write a temp parquet shard (F-EXPORT-MINIJOB.3)."
    )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        p = payload or {}
        parts: list[str] = []
        if p.get("pair_id"):
            parts.append(f"pair={p['pair_id']}")
        return " · ".join(parts)

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = ExportFeaturesBatchPayload.model_validate(payload)
        emit(
            "export_features_batch.start",
            None,
            {"coordinator_job_id": p.coordinator_job_id, "pair_id": p.pair_id},
            "info",
        )

        settings = _load_project_settings()
        store = get_artifact_store(settings)
        temp_key = f"temp/coordinator/{p.coordinator_job_id}/features/{p.pair_id}.parquet"

        if p.temp_knn_uri is None:
            _LOG.warning(
                "export_features_batch: no temp_knn_uri for pair %s; empty shard", p.pair_id
            )
            return self._success(p, _write_empty_parquet(store, temp_key), 0, emit)

        neighbors_by_aspect, query_accs_from_npz = _load_knn_npz(store, p.temp_knn_uri)
        if not query_accs_from_npz:
            return self._success(p, _write_empty_parquet(store, temp_key), 0, emit)

        db = _load_db_data(session, p, emit)
        query_accs = [a for a in query_accs_from_npz if a in db.acc_to_idx]
        if not query_accs:
            return self._success(p, _write_empty_parquet(store, temp_key), 0, emit)

        ctx = _build_knn_transfer_ctx(db, query_accs)
        seq_ctx = _build_sequence_context(session, p, query_accs, db.ref_by_aspect)
        n_rows, temp_uri = _run_and_upload(session, ctx, p, seq_ctx, neighbors_by_aspect, (store, temp_key))
        return self._success(p, temp_uri, n_rows, emit)

    def _success(
        self,
        p: ExportFeaturesBatchPayload,
        temp_uri: str,
        n_rows: int,
        emit: EmitFn,
    ) -> OperationResult:
        """Emit done events and build the success OperationResult."""
        emit(
            "export_features_batch.done",
            None,
            {
                "coordinator_job_id": p.coordinator_job_id,
                "pair_id": p.pair_id,
                "n_rows": n_rows,
                "temp_uri": temp_uri,
            },
            "info",
        )
        emit(
            "pair_features_done",
            None,
            {
                "coordinator_job_id": p.coordinator_job_id,
                "pair_id": p.pair_id,
                "temp_uri": temp_uri,
                "n_rows": n_rows,
            },
            "info",
        )
        return OperationResult(
            result={
                "pair_id": p.pair_id,
                "temp_uri": temp_uri,
                "n_rows": n_rows,
                "coordinator_job_id": p.coordinator_job_id,
            },
            publish_operations=[(_WRITE_QUEUE, _build_write_msg(p, temp_uri, n_rows))],
        )


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


def _load_db_data(
    session: Session,
    p: ExportFeaturesBatchPayload,
    emit: EmitFn,
) -> _LoadedDbData:
    """Load all DB data needed for the feature pipeline."""
    emb_config_id = uuid.UUID(p.embedding_config_id)
    annotation_set_id = uuid.UUID(p.annotation_set_id)
    ontology_snapshot_id = uuid.UUID(p.ontology_snapshot_id)

    all_embeddings, all_accessions, acc_to_idx = _preload_all_embeddings(session, emb_config_id, emit)
    ref_by_aspect = _build_reference_from_cache(
        session, annotation_set_id, all_embeddings, all_accessions, acc_to_idx, emit
    )
    go_id_map, aspect_map, pivot_go_ids = _load_go_maps(
        session, ontology_snapshot_id, {annotation_set_id}
    )
    parent_map = _load_parent_map(session, ontology_snapshot_id)
    pca_state = _maybe_fit_pca_state(emb_config_id, all_embeddings, p.use_embedding_pca, emit)
    return _LoadedDbData(
        all_embeddings=all_embeddings,
        acc_to_idx=acc_to_idx,
        ref_by_aspect=ref_by_aspect,
        go_id_map=go_id_map,
        aspect_map=aspect_map,
        parent_map=parent_map,
        pca_state=pca_state,
        pivot_go_ids=pivot_go_ids,
    )


def _build_knn_transfer_ctx(db: _LoadedDbData, query_accs: list[str]) -> KnnTransferContext:
    """Build the KnnTransferContext from pre-loaded DB data."""
    query_emb = db.all_embeddings[
        np.array([db.acc_to_idx[a] for a in query_accs], dtype=np.int32)
    ].astype(np.float32)
    return KnnTransferContext(
        valid_queries=query_accs,
        query_emb=query_emb,
        ref_by_aspect=db.ref_by_aspect,
        go_id_map=db.go_id_map,
        aspect_map=db.aspect_map,
        gt_pairs=set(),
        query_known_gos=None,
        parent_map_str=db.parent_map,
        ia_weights=None,
        pca_state=db.pca_state,
        pivot_go_ids=db.pivot_go_ids,
        embedding_pool=db.all_embeddings,
    )


def _build_sequence_context(
    session: Session,
    p: ExportFeaturesBatchPayload,
    query_accs: list[str],
    ref_by_aspect: dict[str, dict[str, Any]],
) -> SequenceContext | None:
    """Load optional sequence/taxonomy data when requested by the payload."""
    if not (p.compute_alignments or p.compute_taxonomy):
        return None
    ref_accs: set[str] = set()
    for asp_data in ref_by_aspect.values():
        ref_accs.update(asp_data["accessions"])
    query_set = set(query_accs)
    qs = _load_sequences(session, query_set) if p.compute_alignments else None
    rs = _load_sequences(session, ref_accs) if p.compute_alignments else None
    qt = _load_taxonomy_ids(session, query_set) if p.compute_taxonomy else None
    rt = _load_taxonomy_ids(session, ref_accs) if p.compute_taxonomy else None
    return SequenceContext(query_sequences=qs, ref_sequences=rs, query_tax_ids=qt, ref_tax_ids=rt)


def _run_and_upload(
    session: Session,
    ctx: KnnTransferContext,
    p: ExportFeaturesBatchPayload,
    seq_ctx: SequenceContext | None,
    neighbors_by_aspect: dict[str, list[list[tuple[str, float]]]],
    store_and_key: tuple[Any, str],
) -> tuple[int, str]:
    """Run feature pipeline + upload parquet. Returns (n_rows, uri)."""
    from protea.core._precomputed_knn_runner import PrecomputedKnnConfig, run_with_precomputed_knn

    store, temp_key = store_and_key
    cfg = PrecomputedKnnConfig(
        limit_per_entry=p.k,
        compute_alignments=p.compute_alignments,
        compute_taxonomy=p.compute_taxonomy,
        expand_votes_to_ancestors=p.expand_votes_to_ancestors,
        search_backend="faiss",
        metric="cosine",
        faiss_index_type="IVFFlat",
        faiss_nlist=256,
        faiss_nprobe=32,
        distance_threshold=None,
        use_embedding_pca=p.use_embedding_pca,
    )
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        output_path = Path(tmp.name)
    try:
        info = run_with_precomputed_knn(
            session=session, ctx=ctx, cfg=cfg,
            neighbors_by_aspect=neighbors_by_aspect,
            sequence_context=seq_ctx, output_parquet=output_path,
        )
        n_rows = int(info.get("n_rows", 0))
        temp_uri = store.put(temp_key, output_path)
    finally:
        output_path.unlink(missing_ok=True)
    return n_rows, temp_uri


def _load_knn_npz(
    store: Any,
    uri: str,
) -> tuple[dict[str, list[list[tuple[str, float]]]], list[str]]:
    """Load KNN results from the artifact store npz.

    Returns (neighbors_by_aspect, query_accs) where ``neighbors_by_aspect``
    is keyed by aspect code and each value is a per-query list of
    ``[(ref_acc, distance), ...]``.
    """
    raw_bytes = store.get(_uri_to_key(uri))
    with tempfile.NamedTemporaryFile(suffix=".npz", delete=False) as tmp:
        tmp_path = Path(tmp.name)
        tmp_path.write_bytes(raw_bytes)
    try:
        data = np.load(str(tmp_path), allow_pickle=True)
        query_accs: list[str] = data["query_accs"].tolist()
        neighbors: dict[str, list[list[tuple[str, float]]]] = {}
        for asp in _ASPECTS:
            nb_accs_key = f"nb_accs_{asp}"
            nb_dists_key = f"nb_dists_{asp}"
            if nb_accs_key not in data or nb_dists_key not in data:
                neighbors[asp] = [[] for _ in query_accs]
                continue
            nb_accs = data[nb_accs_key]
            nb_dists = data[nb_dists_key]
            asp_nbs: list[list[tuple[str, float]]] = []
            for i in range(len(query_accs)):
                row: list[tuple[str, float]] = [
                    (str(nb_accs[i, j]), float(nb_dists[i, j]))
                    for j in range(nb_accs.shape[1])
                    if i < nb_accs.shape[0] and str(nb_accs[i, j]) and not np.isnan(float(nb_dists[i, j]))
                ]
                asp_nbs.append(row)
            neighbors[asp] = asp_nbs
    finally:
        tmp_path.unlink(missing_ok=True)
    return neighbors, query_accs


def _uri_to_key(uri: str) -> str:
    """Strip any scheme prefix to get the raw store key."""
    for prefix in ("file://", "s3://", "local://"):
        if uri.startswith(prefix):
            stripped = uri[len(prefix):]
            if prefix in ("file://", "local://"):
                path = Path(stripped)
                for i, part in enumerate(path.parts):
                    if part in ("datasets", "temp"):
                        return "/".join(path.parts[i:])
                return stripped
            return stripped
    if uri.startswith("/"):
        path = Path(uri)
        for i, part in enumerate(path.parts):
            if part in ("datasets", "temp"):
                return "/".join(path.parts[i:])
    return uri


def _write_empty_parquet(store: Any, key: str) -> str:
    """Write an empty parquet sentinel for a pair with no feature rows."""
    import pandas as pd

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = Path(tmp.name)
    try:
        pd.DataFrame().to_parquet(str(tmp_path), index=False)
        uri = store.put(key, tmp_path)
    finally:
        tmp_path.unlink(missing_ok=True)
    return uri


def _build_write_msg(
    p: ExportFeaturesBatchPayload,
    temp_uri: str,
    n_rows: int,
) -> dict[str, Any]:
    """Build the follow-on write-batch dispatch payload."""
    return {
        "operation": "export_write",
        "payload": {
            "coordinator_job_id": p.coordinator_job_id,
            "pair_id": p.pair_id,
            "temp_features_uri": temp_uri,
            "n_rows": n_rows,
            "output_name": p.output_name,
            "embedding_config_id": p.embedding_config_id,
            "ontology_snapshot_id": p.ontology_snapshot_id,
            "annotation_set_id": p.annotation_set_id,
            "annotation_source": p.annotation_source,
            "k": p.k,
            "is_eval": p.is_eval,
        },
    }


def _load_project_settings() -> Any:
    """Resolve project root and load settings."""
    return load_settings(Path(__file__).resolve().parents[4])
