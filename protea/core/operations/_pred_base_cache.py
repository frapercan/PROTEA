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
    max_k_position: int | None,
    delta_proteins: Iterable[str],
) -> tuple[Path, Path]:
    """Return ``(parquet_path, count_path)`` for the deduped base frame.

    ``max_k_position`` belongs in the key, not only in the filter. The base
    frame is the candidate set after the cut, so two depths produce different
    frames; sharing one key would serve the deepest arm's parquet to every
    other arm and a depth sweep would come back flat.
    """
    md = "none" if max_distance is None else f"{max_distance:g}"
    mk = "none" if max_k_position is None else str(max_k_position)
    key = f"{pred_set_id}__md{md}__k{mk}__{_delta_hash(delta_proteins)}"
    return _PRED_CACHE_DIR / f"{key}.parquet", _PRED_CACHE_DIR / f"{key}.count"


def load_or_build_base(
    pred_set_id: uuid.UUID,
    max_distance: float | None,
    max_k_position: int | None,
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
    parquet_path, count_path = _cache_paths(pred_set_id, max_distance, max_k_position, delta)
    if parquet_path.exists() and count_path.exists():
        cached_count = _read_count(count_path)
        if cached_count is not None and cached_count == count_fn():
            df = pd.read_parquet(parquet_path)
            if emit is not None:
                emit("pred_base.cache_hit", {"path": str(parquet_path), "rows": int(len(df))})
            return df
    df, raw_count = build_fn()
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    _atomic_write_cache(df, raw_count, parquet_path, count_path)
    if emit is not None:
        emit(
            "pred_base.cache_write",
            {"path": str(parquet_path), "rows": int(len(df)), "raw_rows": int(raw_count)},
        )
    return df


def _atomic_write_cache(df: Any, raw_count: int, parquet_path: Path, count_path: Path) -> None:
    """Write the parquet + count sidecar so concurrent readers never tear.

    Under ``manage.sh scale protea.evaluations N`` several workers re-score the
    same prediction set at once, so the cache writer and reader race. Two
    invariants make that safe:

    1. Each file is written to a unique ``.<pid>.tmp`` sibling and ``os.replace``
       d into place (atomic on POSIX), so a reader never observes a half-written
       parquet.
    2. The count sidecar is the validity gate (``load_or_build_base`` only trusts
       the parquet when the sidecar matches a fresh COUNT). It is renamed LAST,
       so a reader that sees the count is guaranteed the parquet is already
       complete.
    """
    tmp_parquet = parquet_path.with_suffix(parquet_path.suffix + f".{os.getpid()}.tmp")
    tmp_count = count_path.with_suffix(count_path.suffix + f".{os.getpid()}.tmp")
    df.to_parquet(tmp_parquet, index=False)
    os.replace(tmp_parquet, parquet_path)
    tmp_count.write_text(str(int(raw_count)))
    os.replace(tmp_count, count_path)


def _read_count(count_path: Path) -> int | None:
    """Read the integer row-count sidecar; return ``None`` on any error."""
    try:
        return int(count_path.read_text().strip())
    except (OSError, ValueError):
        return None


__all__ = ["load_or_build_base"]
