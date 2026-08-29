"""KNN batch operation: run KNN for one snapshot pair and write a temp shard.

Implements F-EXPORT-MINIJOB.3.

Receives a per-pair KNN payload dispatched by ``ExportCoordinatorOperation``,
runs KNN search for (annotation_set_id, embedding_config_id), writes the
top-k indices/distances/accessions to a temp npz under:
  ``temp/coordinator/<coordinator_job_id>/knn/<pair_id>.npz``
then publishes a follow-on message to ``protea.training.features`` so the
CPU feature-computation minijob can pick it up.
"""

from __future__ import annotations

import logging
import tempfile
import uuid
from pathlib import Path
from typing import Any

import numpy as np
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.core.domain.aspect import ASPECT_CODES as _ASPECTS
from protea.core.knn_search import search_knn
from protea.core.training_dump._data_loaders import (
    _build_reference_from_cache,
    _preload_all_embeddings,
)
from protea.core.utils import contract_payload
from protea.infrastructure.settings import load_settings
from protea.infrastructure.storage import get_artifact_store

_LOG = logging.getLogger(__name__)

_FEATURES_QUEUE = "protea.training.features"


class ExportKnnBatchPayload(ProteaPayload, frozen=True):
    """Payload for a single per-pair KNN minijob dispatched by the coordinator."""

    coordinator_job_id: str
    pair_id: str
    train_snapshot_id: int
    test_snapshot_id: int
    embedding_config_id: str
    annotation_set_id: str
    ontology_snapshot_id: str
    k: int = 5
    search_backend: str = "faiss"
    is_eval: bool = False
    output_name: str = ""
    annotation_source: str = "goa"
    compute_alignments: bool = False
    compute_taxonomy: bool = False
    expand_votes_to_ancestors: bool = False
    use_embedding_pca: bool = False


class ExportKnnBatchOperation:
    """Run KNN for one snapshot pair and write a temp npz shard.

    Pipeline:
    1. Load all embeddings for ``embedding_config_id`` (disk-cache warm path).
    2. Build per-aspect reference set for ``annotation_set_id``.
    3. Run KNN search (backend selected by ``search_backend``).
    4. Write ``(valid_queries, indices, distances, ref_accessions_per_aspect)``
       to a temp npz at
       ``temp/coordinator/<coordinator_job_id>/knn/<pair_id>.npz``.
    5. Emit ``pair_knn_done`` with the temp URI.
    6. Publish follow-on message to ``protea.training.features``.
    """

    name = "export_knn_batch"
    description = (
        "OperationConsumer: run KNN for one snapshot pair and write a temp "
        "npz shard (F-EXPORT-MINIJOB.3)."
    )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        p = payload or {}
        parts: list[str] = []
        if p.get("pair_id"):
            parts.append(f"pair={p['pair_id']}")
        if p.get("k"):
            parts.append(f"k={p['k']}")
        return " · ".join(parts)

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = ExportKnnBatchPayload.model_validate(contract_payload(payload))
        emb_config_id = uuid.UUID(p.embedding_config_id)
        annotation_set_id = uuid.UUID(p.annotation_set_id)

        emit(
            "export_knn_batch.start",
            None,
            {"coordinator_job_id": p.coordinator_job_id, "pair_id": p.pair_id, "k": p.k},
            "info",
        )

        settings = _load_project_settings()
        store = get_artifact_store(settings)
        temp_key = f"temp/coordinator/{p.coordinator_job_id}/knn/{p.pair_id}.npz"

        all_embeddings, all_accessions, acc_to_idx = _preload_all_embeddings(
            session, emb_config_id, emit
        )
        ref_by_aspect = _build_reference_from_cache(
            session, annotation_set_id, all_embeddings, all_accessions, acc_to_idx, emit
        )
        query_accs = _collect_query_accs(ref_by_aspect, acc_to_idx)

        if not query_accs:
            return self._handle_empty(p, store, temp_key, emit)

        neighbors_by_aspect = _run_knn_per_aspect(
            all_embeddings, acc_to_idx, query_accs, ref_by_aspect, p
        )
        temp_uri = _write_knn_npz(store, temp_key, query_accs, neighbors_by_aspect)

        emit(
            "export_knn_batch.done",
            None,
            {
                "coordinator_job_id": p.coordinator_job_id,
                "pair_id": p.pair_id,
                "n_queries": len(query_accs),
                "temp_uri": temp_uri,
            },
            "info",
        )
        emit(
            "pair_knn_done",
            None,
            {"coordinator_job_id": p.coordinator_job_id, "pair_id": p.pair_id, "temp_uri": temp_uri},
            "info",
        )
        return OperationResult(
            result={
                "pair_id": p.pair_id,
                "temp_uri": temp_uri,
                "n_queries": len(query_accs),
                "coordinator_job_id": p.coordinator_job_id,
            },
            publish_operations=[(_FEATURES_QUEUE, _build_features_msg(p, temp_uri))],
        )

    def _handle_empty(
        self,
        p: ExportKnnBatchPayload,
        store: Any,
        temp_key: str,
        emit: EmitFn,
    ) -> OperationResult:
        """Return early when there are no query accessions for this annotation set."""
        emit(
            "export_knn_batch.no_queries",
            None,
            {"coordinator_job_id": p.coordinator_job_id, "pair_id": p.pair_id},
            "warning",
        )
        temp_uri = _write_empty_npz(store, temp_key)
        emit(
            "pair_knn_done",
            None,
            {"coordinator_job_id": p.coordinator_job_id, "pair_id": p.pair_id, "temp_uri": temp_uri, "n_queries": 0},
            "info",
        )
        return OperationResult(
            result={
                "pair_id": p.pair_id,
                "temp_uri": temp_uri,
                "n_queries": 0,
                "coordinator_job_id": p.coordinator_job_id,
            },
            publish_operations=[(_FEATURES_QUEUE, _build_features_msg(p, temp_uri))],
        )


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


def _collect_query_accs(
    ref_by_aspect: dict[str, dict[str, Any]],
    acc_to_idx: dict[str, int],
) -> list[str]:
    """Union all per-aspect reference accessions that have an embedding."""
    all_ref_accs: set[str] = set()
    for asp_data in ref_by_aspect.values():
        all_ref_accs.update(asp_data["accessions"])
    return [a for a in all_ref_accs if a in acc_to_idx]


def _run_knn_per_aspect(
    all_embeddings: np.ndarray,
    acc_to_idx: dict[str, int],
    query_accs: list[str],
    ref_by_aspect: dict[str, dict[str, Any]],
    p: ExportKnnBatchPayload,
) -> dict[str, list[list[tuple[str, float]]]]:
    """Run per-aspect KNN search and return neighbors keyed by aspect."""
    query_indices = np.array([acc_to_idx[a] for a in query_accs], dtype=np.int32)
    query_emb = all_embeddings[query_indices].astype(np.float32)

    neighbors_by_aspect: dict[str, list[list[tuple[str, float]]]] = {}
    for asp in _ASPECTS:
        ref = ref_by_aspect[asp]
        if not ref["accessions"]:
            neighbors_by_aspect[asp] = [[] for _ in query_accs]
            continue
        indices = ref.get("indices")
        ref_f32 = (
            all_embeddings[indices].astype(np.float32)
            if indices is not None
            else ref["embeddings"].astype(np.float32)
        )
        neighbors_by_aspect[asp] = search_knn(
            query_emb,
            ref_f32,
            ref["accessions"],
            k=p.k,
            backend=p.search_backend,
        )
        del ref_f32
    return neighbors_by_aspect


def _write_knn_npz(
    store: Any,
    key: str,
    query_accs: list[str],
    neighbors_by_aspect: dict[str, list[list[tuple[str, float]]]],
) -> str:
    """Serialise KNN results to a temp npz and upload to the artifact store."""
    arrays: dict[str, Any] = {"query_accs": np.array(query_accs, dtype=object)}
    n_q = len(query_accs)
    for asp in _ASPECTS:
        nbs = neighbors_by_aspect.get(asp, [])
        if not nbs:
            arrays[f"nb_accs_{asp}"] = np.empty((n_q, 0), dtype=object)
            arrays[f"nb_dists_{asp}"] = np.empty((n_q, 0), dtype=np.float32)
            continue
        max_k = max((len(row) for row in nbs), default=0)
        nb_accs = np.full((n_q, max_k), "", dtype=object)
        nb_dists = np.full((n_q, max_k), np.nan, dtype=np.float32)
        for i, row in enumerate(nbs):
            for j, (acc, dist) in enumerate(row):
                nb_accs[i, j] = acc
                nb_dists[i, j] = float(dist)
        arrays[f"nb_accs_{asp}"] = nb_accs
        arrays[f"nb_dists_{asp}"] = nb_dists

    with tempfile.NamedTemporaryFile(suffix=".npz", delete=False) as tmp:
        tmp_path = Path(tmp.name)
    try:
        np.savez_compressed(str(tmp_path), **arrays)
        uri = store.put(key, tmp_path)
    finally:
        tmp_path.unlink(missing_ok=True)
    return uri


def _write_empty_npz(store: Any, key: str) -> str:
    """Write an empty npz sentinel for a pair with no queries."""
    arrays: dict[str, Any] = {"query_accs": np.array([], dtype=object)}
    with tempfile.NamedTemporaryFile(suffix=".npz", delete=False) as tmp:
        tmp_path = Path(tmp.name)
    try:
        np.savez_compressed(str(tmp_path), **arrays)
        uri = store.put(key, tmp_path)
    finally:
        tmp_path.unlink(missing_ok=True)
    return uri


def _build_features_msg(
    p: ExportKnnBatchPayload,
    temp_uri: str,
) -> dict[str, Any]:
    """Build the follow-on features-batch dispatch payload.

    The top-level ``job_id`` is what ``OperationConsumer._decode_message``
    reads to populate ``decoded.parent_job_id``; without it the consumer
    emits every progress/done event to a None target and the coordinator
    never sees the features stage finish (silent emit loss, leads to a
    reaper kill at 6h instead of timely completion).
    """
    return {
        "operation": "export_features_batch",
        "job_id": p.coordinator_job_id,
        "payload": {
            "coordinator_job_id": p.coordinator_job_id,
            "pair_id": p.pair_id,
            "temp_knn_uri": temp_uri,
            "embedding_config_id": p.embedding_config_id,
            "annotation_set_id": p.annotation_set_id,
            "ontology_snapshot_id": p.ontology_snapshot_id,
            "output_name": p.output_name,
            "annotation_source": p.annotation_source,
            "k": p.k,
            "is_eval": p.is_eval,
            "compute_alignments": p.compute_alignments,
            "compute_taxonomy": p.compute_taxonomy,
            "expand_votes_to_ancestors": p.expand_votes_to_ancestors,
            "use_embedding_pca": p.use_embedding_pca,
        },
    }


def _load_project_settings() -> Any:
    """Resolve project root and load settings."""
    return load_settings(Path(__file__).resolve().parents[4])
