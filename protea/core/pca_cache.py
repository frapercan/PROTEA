"""Process-shared PCA state for the predict_go_terms pipeline.

The PCA projection of reference embeddings into 16 dims is a feature
input for the lab re-ranker. Fitting it on the full reference pool is
expensive (~50k samples × ~1280 dims) and the result is deterministic
for a given ``EmbeddingConfig`` — so we materialise ``(mean, components)``
into a single ``.npz`` artifact and reuse it across all workers and
prediction sets that share the config.

Artifact layout: one file per ``EmbeddingConfig``:
``{_PCA_ARTIFACTS_DIR}/{embedding_config_id}.npz`` with two arrays
``mean`` (D,) float32 and ``components`` (16, D) float32.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import numpy as np

from protea.core.reranker import EMBEDDING_PCA_DIM, fit_embedding_pca

_PCA_ARTIFACTS_DIR = Path(
    os.environ.get(
        "PROTEA_PCA_ARTIFACTS_DIR",
        str(Path(__file__).resolve().parents[1] / "artifacts" / "pca"),
    )
)


def _pca_state_path(embedding_config_id: uuid.UUID) -> Path:
    return _PCA_ARTIFACTS_DIR / f"{embedding_config_id}.npz"


def _load_pca_state(
    embedding_config_id: uuid.UUID,
) -> tuple[np.ndarray, np.ndarray] | None:
    path = _pca_state_path(embedding_config_id)
    if not path.exists():
        return None
    try:
        data = np.load(path)
        return (
            np.ascontiguousarray(data["mean"], dtype=np.float32),
            np.ascontiguousarray(data["components"], dtype=np.float32),
        )
    except Exception:
        return None


def _save_pca_state(
    embedding_config_id: uuid.UUID,
    mean: np.ndarray,
    components: np.ndarray,
) -> None:
    path = _pca_state_path(embedding_config_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    np.savez(path, mean=mean, components=components)


def _load_or_fit_pca_state(
    embedding_config_id: uuid.UUID,
    unified_embeddings_f32: np.ndarray,
) -> tuple[np.ndarray, np.ndarray] | None:
    """Load PCA state from disk or fit on the reference pool.

    Returns ``None`` when the reference pool is empty (no projection possible).
    The artifact is shared across all workers and every prediction_set that
    uses this ``EmbeddingConfig`` — fit once, reuse forever.
    """
    cached = _load_pca_state(embedding_config_id)
    if cached is not None:
        return cached
    if unified_embeddings_f32.size == 0:
        return None
    mean, components = fit_embedding_pca(unified_embeddings_f32, EMBEDDING_PCA_DIM)
    _save_pca_state(embedding_config_id, mean, components)
    return mean, components


__all__ = [
    "_PCA_ARTIFACTS_DIR",
    "_load_or_fit_pca_state",
    "_load_pca_state",
    "_pca_state_path",
    "_save_pca_state",
]
