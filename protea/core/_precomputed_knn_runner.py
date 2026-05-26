"""Feature-only runner that injects pre-computed KNN neighbors.

Used by ``ExportFeaturesBatchOperation`` to skip the KNN search phase
(already done by ``ExportKnnBatchOperation``) and run only the CPU feature
phases (reranker features, pair features, taxonomy, anc2vec, PCA, records).

``run_with_precomputed_knn`` is the public entry point; it creates a subclass
of ``_KnnTransferRunner`` that overrides ``_run_knn`` to inject the
pre-computed neighbors instead of running KNN search.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from protea.core._knn_transfer_runner import _KnnTransferRunner
from protea.core.training_dump._contexts import (
    KnnTransferContext,
    SequenceContext,
    StreamOutput,
)


@dataclass(frozen=True)
class PrecomputedKnnConfig:
    """Configuration mirroring the payload fields used by ``_KnnTransferRunner``.

    These fields are read directly from ``self.p`` inside the runner phases,
    so we need a compatible object with at least the minimal attribute set.
    """

    limit_per_entry: int
    compute_alignments: bool
    compute_taxonomy: bool
    expand_votes_to_ancestors: bool
    search_backend: str
    metric: str
    faiss_index_type: str
    faiss_nlist: int
    faiss_nprobe: int
    distance_threshold: float | None
    use_embedding_pca: bool


@dataclass(frozen=True)
class _RunnerInputs:
    """Bundle of inputs for ``_PrecomputedKnnRunner.__init__``.

    Groups the DB session, contexts, and pre-computed neighbors together
    so the ``__init__`` signature stays under the §3 6-arg ceiling.
    """

    session: Session
    ctx: KnnTransferContext
    neighbors_by_aspect: dict[str, list[list[tuple[str, float]]]]
    sequence_context: SequenceContext | None
    output_parquet: Path


class _PrecomputedKnnRunner(_KnnTransferRunner):
    """Subclass that injects pre-computed KNN results instead of re-running KNN.

    ``neighbors_by_aspect`` must be a ``{aspect: [[( acc, dist), ...], ...]}``
    mapping with one inner list per query in the same order as
    ``ctx.valid_queries``.
    """

    def __init__(self, inputs: _RunnerInputs, cfg: PrecomputedKnnConfig) -> None:
        from protea.core.training_dump._payload import TrainRerankerAutoPayload

        dummy_payload = TrainRerankerAutoPayload.model_validate(
            {
                "name": "_precomputed",
                "embedding_config_id": "00000000-0000-0000-0000-000000000000",
                "ontology_snapshot_id": "00000000-0000-0000-0000-000000000000",
                "train_versions": [0, 1],
                "test_versions": [2],
                "limit_per_entry": cfg.limit_per_entry,
                "distance_threshold": cfg.distance_threshold,
                "search_backend": cfg.search_backend,
                "metric": cfg.metric,
                "faiss_index_type": cfg.faiss_index_type,
                "faiss_nlist": cfg.faiss_nlist,
                "faiss_nprobe": cfg.faiss_nprobe,
                "compute_alignments": cfg.compute_alignments,
                "compute_taxonomy": cfg.compute_taxonomy,
                "expand_votes_to_ancestors": cfg.expand_votes_to_ancestors,
                "use_embedding_pca": cfg.use_embedding_pca,
                "dump_to": "/tmp",
                "dump_only": True,
            }
        )
        super().__init__(
            session=inputs.session,
            p=dummy_payload,
            ctx=inputs.ctx,
            sequence_context=inputs.sequence_context,
            stream_output=StreamOutput(output_parquet=inputs.output_parquet),
        )
        self._precomputed = inputs.neighbors_by_aspect

    def _run_knn(self) -> None:
        """Inject pre-computed KNN results instead of running KNN search."""
        from protea.core.domain.aspect import ASPECT_CODES as _ASPECTS

        for asp in _ASPECTS:
            if asp in self._precomputed:
                self.neighbors_by_aspect[asp] = self._precomputed[asp]
            else:
                self.neighbors_by_aspect[asp] = [[] for _ in self.valid_queries]


def run_with_precomputed_knn(
    *,
    session: Session,
    ctx: KnnTransferContext,
    neighbors_by_aspect: dict[str, list[list[tuple[str, float]]]],
    cfg: PrecomputedKnnConfig,
    sequence_context: SequenceContext | None,
    output_parquet: Path,
) -> dict[str, Any]:
    """Run the feature pipeline with pre-computed KNN neighbors.

    Returns ``{"parquet_path": str, "n_rows": int}`` from the streaming run.
    """
    inputs = _RunnerInputs(
        session=session,
        ctx=ctx,
        neighbors_by_aspect=neighbors_by_aspect,
        sequence_context=sequence_context,
        output_parquet=output_parquet,
    )
    runner = _PrecomputedKnnRunner(inputs, cfg)
    result = runner.run()
    return dict(result)  # type: ignore[arg-type]
