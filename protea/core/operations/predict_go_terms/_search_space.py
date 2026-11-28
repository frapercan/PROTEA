"""The space a neighbour search runs in: a declared transform of the bank and its queries.

``embedding_transform`` names one of the transforms of
:mod:`protea.core.embedding_space.transforms` (centre, standardise, whiten,
remove the dominant directions by the elbow's count or by the fixed count).
Every transform reads the label-free statistics of the configuration's space at
the run's cut: the ``embedding_space_statistics`` row of (configuration, photo)
that ``compute_embedding_space_statistics`` wrote, its artifact verified against
the sha256 the row records. The photo is the one that loaded the run's
annotation set, so the statistics are of the corpus at that cut, whatever the
donor policy: every breadth level of a pool searches one geometry.

WHERE IT IS APPLIED. The pool in the float32 copy the search reads, in place and
in chunks, after the half-precision vectors are widened and before the cosine
normalisation (``protea.core.disk_cache._dense_f32_views``); the half-precision
pool and its disk cache stay raw, so a transformed run reuses a raw pool's cache.
The queries in ``_load_query_embeddings``, with the same statistics. protea-method
normalises the queries itself and takes the bank as normalised, so it needs no
change: the search sees the transformed space on both sides.

WHAT IT REFUSES. A sparse (k-WTA) bank, which never takes a dense float32 copy;
``compute_v6_features``, whose PCA state is fitted once per configuration on the
raw space; and a run whose cut has no statistics for its configuration. The
coordinator refuses all three before any batch exists, and a batch refuses them
too rather than searching the raw space under the transform's name.
"""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any, NamedTuple

import numpy as np
from sqlalchemy import select
from sqlalchemy.orm import Session

from protea.core._artifact_identity import fetch_and_verify
from protea.core.embedding_space.statistics import read_statistics_artifact
from protea.core.embedding_space.transforms import SpaceParameters, apply_in_place
from protea.core.operations.predict_go_terms._photo_sequences import photo_of
from protea.core.operations.predict_go_terms._sparse_bank import bank_is_sparse
from protea.infrastructure.orm.models.embedding.embedding_space_statistics import (
    EmbeddingSpaceStatistics,
)

__all__ = [
    "SearchSpace",
    "search_space",
]

#: The parameters read from an artifact, by (statistics row, sha256). One
#: configuration's eigenvectors are d x d in float64, 52 MB at 2,560 dimensions,
#: so a batch reads them once per process and not once per batch.
_PARAMETERS: dict[tuple[uuid.UUID, str], SpaceParameters] = {}
_PARAMETERS_KEPT = 2


class SearchSpace(NamedTuple):
    """A transform with the statistics it reads: what the search runs in."""

    transform: str
    statistics_id: uuid.UUID
    statistics_sha256: str
    n: int
    d: int
    parameters: SpaceParameters

    @property
    def identity(self) -> tuple[str, str]:
        """What a cached pool in this space is told apart by: the transform and the statistics."""
        return self.transform, self.statistics_sha256

    def apply(self, vectors: np.ndarray) -> None:
        """Map float32 or float64 rows into the space, in place and in chunks."""
        apply_in_place(vectors, self.transform, self.parameters)

    def record(self) -> dict[str, Any]:
        """The receipt's ``embedding_transform`` block."""
        return {
            "name": self.transform,
            "statistics_id": str(self.statistics_id),
            "statistics_sha256": self.statistics_sha256,
            "n": self.n,
            "d": self.d,
            "shrinkage": self.parameters.shrinkage,
            "removed_directions": self.parameters.removed_directions,
        }


def _store() -> Any:
    from protea.infrastructure.settings import load_settings
    from protea.infrastructure.storage import get_artifact_store

    return get_artifact_store(load_settings(Path(__file__).resolve().parents[4]))


def _statistics_row(
    session: Session, embedding_config_id: object, photo_id: uuid.UUID
) -> EmbeddingSpaceStatistics | None:
    """The statistics of the configuration's space over the photo's members, if computed."""
    return session.execute(
        select(EmbeddingSpaceStatistics).where(
            EmbeddingSpaceStatistics.embedding_config_id == uuid.UUID(str(embedding_config_id)),
            EmbeddingSpaceStatistics.cut_photo_id == photo_id,
        )
    ).scalar_one_or_none()


def _parameters(row: EmbeddingSpaceStatistics) -> SpaceParameters:
    """The row's artifact read once per process, verified against the sha256 it records."""
    key = (row.id, row.artifact_sha256)
    if key not in _PARAMETERS:
        label = f"the statistics {row.id} of configuration {row.embedding_config_id}"
        blob = fetch_and_verify(_store(), row.artifact_uri, row.artifact_sha256, label)
        parameters, _document = read_statistics_artifact(blob)
        if parameters.dimension != row.d:
            raise ValueError(
                f"{label} hold a space of {parameters.dimension} dimensions where the row "
                f"says {row.d}"
            )
        while len(_PARAMETERS) >= _PARAMETERS_KEPT:
            del _PARAMETERS[next(iter(_PARAMETERS))]
        _PARAMETERS[key] = parameters
    return _PARAMETERS[key]


def search_space(session: Session, p: Any) -> SearchSpace | None:
    """The space the payload's search runs in, or None for the configuration's own space.

    The coordinator reads it before any batch exists, so a space the search
    cannot run is refused whole and the receipt records the statistics' values;
    each batch reads it again, the artifact once per process.

    :raises ValueError: beside ``compute_v6_features`` or over a sparse bank; when
        the run's annotation set no photo loaded, or its photo has no statistics
        for the configuration.
    """
    transform = getattr(p, "embedding_transform", None)
    if transform is None:
        return None
    if getattr(p, "compute_v6_features", False):
        raise ValueError(
            "compute_v6_features fits its PCA state once per configuration on the raw "
            "space, so it cannot run beside embedding_transform; run it without the "
            "transform, or the transform without it"
        )
    if bank_is_sparse(session, uuid.UUID(str(p.embedding_config_id))):
        raise ValueError(
            f"configuration {p.embedding_config_id} is a sparse bank, which is searched "
            "without a dense copy to transform, so it cannot run in a declared space"
        )
    photo = photo_of(session, p.annotation_set_id)
    if photo is None:
        raise ValueError(
            f"embedding_transform '{transform}' reads the statistics of a cut photo's "
            f"members, and annotation set {p.annotation_set_id} was loaded by no cut photo"
        )
    row = _statistics_row(session, p.embedding_config_id, photo)
    if row is None:
        raise ValueError(
            f"embedding_transform '{transform}' reads the statistics of configuration "
            f"{p.embedding_config_id} at photo {photo}, and none are computed; run "
            "compute_embedding_space_statistics for them first"
        )
    return SearchSpace(transform, row.id, row.artifact_sha256, row.n, row.d, _parameters(row))
