"""Process-shared PCA state cache — thin shim over ``protea_method.pca_cache``.

The implementation lives in the standalone ``protea-method`` library
(F2C extraction, 2026-05-07). This module is a backwards-compatible
shim so existing PROTEA call sites that import from
``protea.core.pca_cache`` keep working without changes; new code
should import directly from ``protea_method.pca_cache``.

The PROTEA-specific default artifact directory
(``protea/artifacts/pca/``) is preserved; the underlying helper
accepts an explicit ``artifacts_dir`` kwarg so the path stays under
the PROTEA repo regardless of CWD.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import numpy as np
from protea_method.pca_cache import load_or_fit_pca_state as _lib_load_or_fit

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

    Delegates to ``protea_method.pca_cache.load_or_fit_pca_state``
    with the PROTEA-specific artifact directory; returns ``None``
    when the reference pool is empty.
    """
    return _lib_load_or_fit(
        embedding_config_id,
        unified_embeddings_f32,
        artifacts_dir=_PCA_ARTIFACTS_DIR,
    )


__all__ = [
    "_PCA_ARTIFACTS_DIR",
    "_load_or_fit_pca_state",
    "_load_pca_state",
    "_pca_state_path",
    "_save_pca_state",
]
