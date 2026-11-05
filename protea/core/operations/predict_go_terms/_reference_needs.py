"""Which f32 views of the reference bank a prediction run keeps.

A pure function of the payload, kept out of ``_ReferenceMixin`` so the policy
reads and tests without the loader around it.
"""

from __future__ import annotations

from typing import Any


def reference_dtype_needs(p: Any) -> tuple[bool, bool]:
    """Return ``(need_cos, need_plain)``: which f32 copies this run keeps.

    Both bank paths, disk and database, build their views in
    ``disk_cache._derive_reference_views``. Its dense branch always builds the
    raw copy, because the L2-normalised one is derived from it, and returns
    the raw copy only when ``need_plain``. So ``need_plain=False`` does not
    avoid that allocation. It avoids holding the unified raw copy for the rest
    of the run, and the per-aspect raw slices of ``_assemble_aspect_view``,
    which are then never built.

    The KNN search reads the normalised copy under cosine and the raw one
    otherwise. The only other reader of the raw copy is the embedding-PCA fit,
    which runs only for the v6 features and ignores its pool once its state is
    cached on disk per embedding config. Under cosine the raw copy is therefore
    kept only when ``compute_v6_features`` is on and that state is still
    missing. A payload that does not say keeps it: dropping a copy a reader
    needs is a crash, keeping one nobody reads is only memory.
    """
    from protea.core.pca_cache import _pca_state_path

    need_cos = getattr(p, "metric", "cosine") == "cosine"
    fits_pca = bool(getattr(p, "compute_v6_features", True)) and not (
        _pca_state_path(p.embedding_config_id).exists()
    )
    return need_cos, (not need_cos) or fits_pca
