"""Disk cache for the baseline ``write_predictions`` columnar fetch.

``run_cafa_evaluation`` scores the SAME ``prediction_set`` up to 7 times per
grid cell (one eval per ``scoring_config``; only the score column differs).
The expensive part is the Core columnar fetch + dedup of ~1.2M
``(GOPrediction, GOTerm)`` rows, not the scoring arithmetic. Those 7 evals are
SEPARATE jobs, so the memo has to live on disk.

This module memoises the deduped base frame as parquet keyed by
``(prediction_set_id, max_distance, delta-protein-set hash)`` so the
2nd..7th scoring_config of the same prediction_set reuse it instead of
re-querying. It mirrors the refpool disk-cache pattern in
:mod:`protea.core.disk_cache`: a row-count sidecar is validated against a
fresh ``COUNT(*)`` so a reference re-ingest invalidates the cache without a
manual file delete.
"""

from __future__ import annotations

import hashlib
import os
import uuid
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

_PRED_CACHE_DIR = Path(os.environ.get("PROTEA_PRED_CACHE_DIR", "data/pred_cache"))


def _delta_hash(delta_proteins: Iterable[str]) -> str:
    """Order-independent stable digest of the delta-protein accession set."""
    h = hashlib.sha1(usedforsecurity=False)  # cache key only, not security
    for acc in sorted(delta_proteins):
        h.update(acc.encode("utf-8"))
        h.update(b"\0")
    return h.hexdigest()[:16]


def _cache_paths(
    pred_set_id: uuid.UUID,
    max_distance: float | None,
    delta_proteins: Iterable[str],
) -> tuple[Path, Path]:
    """Return ``(parquet_path, count_path)`` for the deduped base frame."""
    md = "none" if max_distance is None else f"{max_distance:g}"
    key = f"{pred_set_id}__md{md}__{_delta_hash(delta_proteins)}"
    return _PRED_CACHE_DIR / f"{key}.parquet", _PRED_CACHE_DIR / f"{key}.count"


def load_or_build_base(
    pred_set_id: uuid.UUID,
    max_distance: float | None,
    delta_proteins: Iterable[str],
    *,
    count_fn: Callable[[], int],
    build_fn: Callable[[], tuple[Any, int]],
    emit: Callable[[str, dict[str, Any]], None] | None = None,
) -> Any:
    """Return the deduped base DataFrame, from disk cache when valid.

    ``build_fn`` runs the Core columnar fetch + dedup and returns
    ``(df, raw_row_count)`` where ``raw_row_count`` is the pre-dedup match
    count persisted alongside the parquet. ``count_fn`` issues a fresh
    ``COUNT(*)`` over the same filter; a divergence from the cached count
    treats the parquet as stale (drift after a re-ingest). ``emit`` receives
    ``pred_base.cache_hit`` / ``pred_base.cache_write`` audit events; pass
    ``None`` to stay quiet.
    """
    import pandas as pd

    delta = list(delta_proteins)
    parquet_path, count_path = _cache_paths(pred_set_id, max_distance, delta)
    if parquet_path.exists() and count_path.exists():
        cached_count = _read_count(count_path)
        if cached_count is not None and cached_count == count_fn():
            df = pd.read_parquet(parquet_path)
            if emit is not None:
                emit("pred_base.cache_hit", {"path": str(parquet_path), "rows": int(len(df))})
            return df
    df, raw_count = build_fn()
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(parquet_path, index=False)
    count_path.write_text(str(int(raw_count)))
    if emit is not None:
        emit(
            "pred_base.cache_write",
            {"path": str(parquet_path), "rows": int(len(df)), "raw_rows": int(raw_count)},
        )
    return df


def _read_count(count_path: Path) -> int | None:
    """Read the integer row-count sidecar; return ``None`` on any error."""
    try:
        return int(count_path.read_text().strip())
    except (OSError, ValueError):
        return None


__all__ = ["load_or_build_base"]
