"""Stable-feature-cache orchestration for ``export_research_dataset``.

NFR-ARCH "export decouple", glue layer. Kept out of the operation module so the
operation methods stay under the smell budget and the default export path is
untouched. Only engaged when the ``stable_feature_cache`` payload flag is set.

Two outcomes, both producing the SAME final staged ``train.parquet`` /
``eval.parquet`` the default path would:

* **cache HIT** -- fetch the cached STABLE parquets into the stage dir, skip the
  ~8h KNN + feature pipeline, then re-stamp the classifier column(s) for the
  current classifier. A classifier A/B costs only the classifier-compute time.
* **cache MISS** -- run the heavy pipeline ONCE with the classifier forced OFF
  (so the staged parquet is purely stable), upload that stable table to the
  cache, then re-stamp the classifier column(s). The next variant on the same
  stable config hits.

The stable columns are produced by the unchanged inline pipeline, so a cached
stable parquet is byte-identical to what the same stable config emits inline.
The classifier re-stamp reuses the predict-path producer
(``_classifier_postpass.apply_classifier_to_frame``) so the values match the
inline applier exactly.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any

from protea.core.contracts.operation import EmitFn, OperationResult
from protea.core.training_dump._stable_feature_cache import (
    cache_manifest,
    compute_stable_cache_key,
    stable_cache_prefix,
)

if TYPE_CHECKING:
    from protea.core.operations.export_research_dataset import (
        ExportResearchDatasetPayload,
    )

# Parquet files the classifier post-pass re-stamps.
_STABLE_PARQUETS = ("train.parquet", "eval.parquet")
# Full set cached / restored as the stable table. ``manifest.json`` is
# stable-config-derived (only ``producer_git_sha`` is a moving part, and that is
# cosmetic provenance), so caching it lets the cache-hit path skip regenerating
# it and keeps ``_upload_outputs``'s manifest-required contract satisfied.
_STABLE_FILES = ("train.parquet", "eval.parquet", "manifest.json")

# Callback the operation hands in so this module never imports the heavy dump
# stack at module load: ``(session, payload, stage_dir, emit, classifier_on)``.
DumpFn = Callable[..., OperationResult]


def run_with_stable_cache(
    p: ExportResearchDatasetPayload,
    factory: Any,
    store: Any,
    stage_dir: Path,
    emit: EmitFn,
    dump_fn: DumpFn,
) -> OperationResult:
    """Produce the final staged parquets via the stable-feature cache.

    Mutates ``stage_dir`` to hold the final ``train.parquet`` / ``eval.parquet``
    (+ ``manifest.json`` from the heavy pass on a miss). Returns the inner dump
    ``OperationResult`` so the caller's merge/register path is unchanged.
    """
    from protea.infrastructure.session import session_scope

    # KNOWN LIMITATION (see PR description): the stable-feature-cache path builds
    # the stable table with the classifier OFF and re-stamps scores onto the
    # frozen frame via ``_restamp_classifier`` (stamp-only, no row union). The
    # native-0.391 parity fix unions NEW classifier candidate rows into the pool
    # on the INLINE export path, which is a STABLE (candidate-pool) concern the
    # variable re-stamp cannot reproduce. A classifier-on export that goes
    # through this cache therefore still emits a KNN-only pool. The parity
    # export does NOT use the stable cache (the default ``_runner`` flow does the
    # inline union); promoting ``compute_classifier`` into the stable cache key
    # is the follow-up that would close this on the cached path.
    key = compute_stable_cache_key(p)
    prefix = stable_cache_prefix(key)
    hit = all(store.exists(prefix + name) for name in _STABLE_PARQUETS)
    emit(
        "export_research_dataset.stable_cache_lookup",
        None,
        {"stable_cache_key": key, "prefix": prefix, "hit": hit},
        "info",
    )
    if hit:
        auto_result = _restore_stable_parquets(store, prefix, stage_dir)
    else:
        with session_scope(factory) as compute_session:
            auto_result = dump_fn(compute_session, p, stage_dir, emit, classifier_on=False)
        _persist_stable_parquets(store, prefix, stage_dir, p, key, emit)
    _restamp_classifier(p, factory, stage_dir, emit)
    return auto_result


def _restore_stable_parquets(store: Any, prefix: str, stage_dir: Path) -> OperationResult:
    """Hydrate the stage dir from the cached stable parquets (cache HIT)."""
    n_train = 0
    n_eval = 0
    for name in _STABLE_FILES:
        if not store.exists(prefix + name):
            continue
        data = store.get(prefix + name)
        (stage_dir / name).write_bytes(data)
    import pandas as pd

    if (stage_dir / "train.parquet").exists():
        n_train = len(pd.read_parquet(stage_dir / "train.parquet", columns=["protein_accession"]))
    if (stage_dir / "eval.parquet").exists():
        n_eval = len(pd.read_parquet(stage_dir / "eval.parquet", columns=["protein_accession"]))
    return OperationResult(
        result={"dumped": True, "n_train_rows": n_train, "n_eval_rows": n_eval, "from_cache": True}
    )


def _persist_stable_parquets(
    store: Any,
    prefix: str,
    stage_dir: Path,
    p: ExportResearchDatasetPayload,
    key: str,
    emit: EmitFn,
) -> None:
    """Upload the freshly-computed stable parquets + cache manifest (cache MISS)."""
    import json

    for name in _STABLE_FILES:
        path = stage_dir / name
        if path.exists():
            store.put(prefix + name, path)
    store.put(prefix + "cache_manifest.json", json.dumps(cache_manifest(p, key), indent=2).encode())
    emit(
        "export_research_dataset.stable_cache_written",
        None,
        {"stable_cache_key": key, "prefix": prefix},
        "info",
    )


def _restamp_classifier(
    p: ExportResearchDatasetPayload, factory: Any, stage_dir: Path, emit: EmitFn
) -> None:
    """Recompute classifier columns on the staged parquets for THIS classifier.

    No-op when the export does not request the classifier family; then the
    staged parquet keeps the zero-fill defaults exactly as a stable-only dump.
    """
    if not p.compute_classifier:
        return
    import pandas as pd

    from protea.core.training_dump._classifier_postpass import apply_classifier_to_frame
    from protea.infrastructure.session import session_scope

    snapshot_id = uuid.UUID(str(p.ontology_snapshot_id))
    with session_scope(factory) as session:
        for name in _STABLE_PARQUETS:
            path = stage_dir / name
            if not path.exists():
                continue
            frame = pd.read_parquet(path)
            apply_classifier_to_frame(session, frame, snapshot_id)
            frame.to_parquet(path, index=False, compression="snappy")
    emit(
        "export_research_dataset.classifier_restamped",
        None,
        {"files": list(_STABLE_PARQUETS)},
        "info",
    )


__all__ = ("run_with_stable_cache",)
