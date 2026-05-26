"""Pydantic payload models for the export-minijob pipeline.

All four operations (export_coordinator, export_knn_batch,
export_features_batch) share these types so the coordinator can build
and publish the downstream payloads without importing the downstream
operation modules (which would create a circular dependency).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Any

from protea_contracts import ProteaPayload
from pydantic import Field, field_validator

PositiveInt = Annotated[int, Field(gt=0)]


class ExportCoordinatorPayload(ProteaPayload, frozen=True):
    """Coordinator payload: cell spec that drives the minijob fan-out.

    Fields mirror :class:`ExportResearchDatasetPayload` but are extended
    with the search-backend knob so each minijob can run KNN with the
    right backend without re-reading the coordinator payload.
    """

    embedding_config_id: str
    ontology_snapshot_id: str
    annotation_set_id: str
    train_versions: list[int]
    test_versions: list[int]
    output_name: str
    k: PositiveInt = 5
    search_backend: str = "faiss"
    annotation_source: str = "goa"
    compute_alignments: bool = False
    compute_taxonomy: bool = False
    expand_votes_to_ancestors: bool = False
    use_embedding_pca: bool = False

    @field_validator("output_name", "embedding_config_id", "ontology_snapshot_id", "annotation_set_id", mode="before")
    @classmethod
    def must_be_non_empty(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("must be a non-empty string")
        return v.strip()

    @field_validator("train_versions", mode="before")
    @classmethod
    def at_least_two_train(cls, v: list[int]) -> list[int]:
        if len(v) < 2:
            raise ValueError("train_versions must have at least 2 entries")
        return sorted(v)

    @field_validator("test_versions", mode="before")
    @classmethod
    def at_least_one_test(cls, v: list[int]) -> list[int]:
        if not v:
            raise ValueError("test_versions must have at least 1 entry")
        return sorted(v)


class ExportKnnBatchPayload(ProteaPayload, frozen=True):
    """Payload for a single KNN minijob published by the coordinator.

    Each minijob covers one snapshot pair (train or eval). The worker:
      1. Loads the reference embedding pool from the disk cache.
      2. Runs KNN for the query snapshot using ``search_backend``.
      3. Writes ``(pair_id, top_k_indices, top_k_distances)`` to the
         artifact store under the coordinator's temp prefix.
      4. Publishes an ``export_features_batch`` message to
         ``protea.training.features``.

    ``pair_type`` is ``"train"`` or ``"eval"``; used for logging only.
    """

    coordinator_job_id: str
    pair_id: str
    pair_type: str
    query_version: int
    ref_version: int
    embedding_config_id: str
    annotation_set_id: str
    ontology_snapshot_id: str
    output_name: str
    k: PositiveInt = 5
    search_backend: str = "faiss"
    annotation_source: str = "goa"
    compute_alignments: bool = False
    compute_taxonomy: bool = False
    expand_votes_to_ancestors: bool = False
    use_embedding_pca: bool = False
    total_expected_pairs: int = 1


class ExportFeaturesBatchPayload(ProteaPayload, frozen=True):
    """Payload for the CPU feature-generation minijob.

    Published by ``export_knn_batch`` once KNN results are written.
    The worker reads the temp KNN artefact, runs the CPU feature path,
    and writes a per-pair feature shard to the artifact store.
    """

    coordinator_job_id: str
    pair_id: str
    pair_type: str
    query_version: int
    ref_version: int
    embedding_config_id: str
    annotation_set_id: str
    ontology_snapshot_id: str
    output_name: str
    knn_temp_uri: str
    k: PositiveInt = 5
    search_backend: str = "faiss"
    annotation_source: str = "goa"
    compute_alignments: bool = False
    compute_taxonomy: bool = False
    expand_votes_to_ancestors: bool = False
    use_embedding_pca: bool = False
    total_expected_pairs: int = 1


@dataclass(frozen=True)
class PairSpec:
    """Minimal per-pair identity used when building KNN batch messages.

    Bundles the pair-identifying fields so ``build_knn_batch_payload``
    stays under the 6-arg smell ceiling.
    """

    coordinator_job_id: str
    pair_id: str
    pair_type: str
    query_version: int
    ref_version: int
    total: int


def build_knn_batch_payload(
    spec: PairSpec,
    p: ExportCoordinatorPayload,
) -> dict[str, Any]:
    """Build the publish dict for a single KNN minijob message."""
    return {
        "coordinator_job_id": spec.coordinator_job_id,
        "pair_id": spec.pair_id,
        "pair_type": spec.pair_type,
        "query_version": spec.query_version,
        "ref_version": spec.ref_version,
        "embedding_config_id": p.embedding_config_id,
        "annotation_set_id": p.annotation_set_id,
        "ontology_snapshot_id": p.ontology_snapshot_id,
        "output_name": p.output_name,
        "k": p.k,
        "search_backend": p.search_backend,
        "annotation_source": p.annotation_source,
        "compute_alignments": p.compute_alignments,
        "compute_taxonomy": p.compute_taxonomy,
        "expand_votes_to_ancestors": p.expand_votes_to_ancestors,
        "use_embedding_pca": p.use_embedding_pca,
        "total_expected_pairs": spec.total,
    }
