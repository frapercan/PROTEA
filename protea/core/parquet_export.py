"""Shared utility that consolidates per-split parquet shards into the
frozen reranker dataset layout consumed by ``protea-reranker-lab``.

The layout is a directory containing exactly three files:

    train.parquet    concatenated training shards (all splits, all cats)
    eval.parquet     test shards
    manifest.json    metadata compatible with ManifestV1 v2

This module is shared between two producers:

* ``the dump helper`` (operation) — runs KNN + feature generation for
  training and optionally dumps the resulting shards.
* ``export_research_dataset`` (operation) — runs the same generation but
  only to publish the frozen dataset via an ``ArtifactStore`` (local or
  MinIO). No LightGBM is trained.
"""

from __future__ import annotations

import hashlib
import json
import logging
import subprocess
from pathlib import Path
from typing import Any

import pandas as pd

from protea.core.reranker import ALL_FEATURES, LABEL_COLUMN
from protea.infrastructure.storage import ArtifactStore

logger = logging.getLogger(__name__)

_ASPECT_NAMES = {"P": "bpo", "F": "mfo", "C": "cco"}
_CATEGORIES = ("nk", "lk", "pk")


def resolve_protea_git_sha() -> str | None:
    """Best-effort current HEAD sha of the PROTEA repo. Returns None when
    the code is not running inside a git checkout or git is unavailable.
    """
    try:
        repo_root = Path(__file__).resolve().parents[2]
        out = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_root,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=5,
        ).strip()
        return out or None
    except Exception:
        return None


def _reorder(df: pd.DataFrame, reserved: list[str]) -> pd.DataFrame:
    if df.empty:
        return df
    feature_cols = [c for c in ALL_FEATURES if c in df.columns]
    reserved_present = [c for c in reserved if c in df.columns]
    return df[reserved_present + feature_cols]


def _validate_manifest_with_contracts(manifest: dict[str, Any]) -> None:
    """Best-effort validation against the lab's pydantic ManifestV1.

    Dev-time only: when ``protea_reranker_lab`` is installed as an editable
    path dep, validate the manifest dict to catch schema drift before we
    publish. When the import fails (prod image without the dev dep), log
    and skip — this is not a production-path guard.
    """
    try:
        from protea_reranker_lab.contracts import ManifestV1
    except Exception as exc:
        logger.debug("skipping contract validation (lab not importable: %s)", exc)
        return
    ManifestV1.model_validate(manifest)


def export_reranker_parquets(
    *,
    stage_dir: Path,
    split_files: dict[str, list[Path]],
    valid_split_versions: list[tuple[int, int]],
    test_files: dict[str, Path | None],
    test_old_v: int,
    test_new_v: int,
    name: str,
    k: int,
    embedding_config_id: str,
    ontology_snapshot_id: str,
    annotation_source: str,
    store: ArtifactStore | None = None,
    key_prefix: str = "",
    producer_version: str | None = None,
    producer_git_sha: str | None = None,
    validate_with_contracts: bool = True,
) -> dict[str, Any]:
    """Consolidate per-cat per-split parquet shards into the frozen
    dataset layout and optionally publish via an ``ArtifactStore``.

    Parameters
    ----------
    stage_dir
        Directory used as the local staging area. The three output files
        are written here regardless of ``store``; when ``store`` is given
        they are additionally uploaded under ``key_prefix``.
    split_files
        Per-category list of training shard paths, parallel to
        ``valid_split_versions``.
    valid_split_versions
        ``(v_old, v_new)`` pairs for each training shard position.
    test_files
        Per-category test shard path (may be ``None`` when the category
        has no test rows).
    store
        When provided, the three consolidated files are uploaded under
        ``f"{key_prefix}train.parquet"`` etc. The returned dict includes
        the resulting URIs.
    key_prefix
        Prefix for store keys (should typically end with ``/``).
    producer_version
        PROTEA version string recorded in the manifest (optional).
    producer_git_sha
        PROTEA git HEAD at export time, recorded in the manifest
        (optional).
    validate_with_contracts
        If True, best-effort validate the manifest dict against the lab's
        ``ManifestV1`` before writing. Silent if the lab isn't installed.
    """
    stage_dir.mkdir(parents=True, exist_ok=True)
    aspect_norm = dict(_ASPECT_NAMES)

    train_frames: list[pd.DataFrame] = []
    train_snapshot_pairs: list[str] = []
    for cat in _CATEGORIES:
        shards = split_files.get(cat, [])
        for shard_idx, shard_path in enumerate(shards):
            v_old, v_new = valid_split_versions[shard_idx]
            snap_pair = f"v{v_old}-v{v_new}"
            if snap_pair not in train_snapshot_pairs:
                train_snapshot_pairs.append(snap_pair)
            sdf = pd.read_parquet(shard_path)
            sdf["category"] = cat
            sdf["snapshot_pair"] = snap_pair
            if "aspect" in sdf.columns:
                sdf["aspect"] = sdf["aspect"].map(aspect_norm).fillna(sdf["aspect"])
            sdf = sdf.rename(columns={"go_id": "go_term_id"})
            train_frames.append(sdf)
    train_df = (
        pd.concat(train_frames, ignore_index=True) if train_frames else pd.DataFrame()
    )
    del train_frames

    eval_pair = f"v{test_old_v}-v{test_new_v}"
    eval_frames: list[pd.DataFrame] = []
    for cat in _CATEGORIES:
        path = test_files.get(cat)
        if path is None:
            continue
        edf = pd.read_parquet(path)
        edf["category"] = cat
        edf["snapshot_pair"] = eval_pair
        if "aspect" in edf.columns:
            edf["aspect"] = edf["aspect"].map(aspect_norm).fillna(edf["aspect"])
        edf = edf.rename(columns={"go_id": "go_term_id"})
        eval_frames.append(edf)
    eval_df = (
        pd.concat(eval_frames, ignore_index=True) if eval_frames else pd.DataFrame()
    )
    del eval_frames

    reserved = [
        "protein_accession",
        "go_term_id",
        LABEL_COLUMN,
        "category",
        "snapshot_pair",
    ]
    train_df = _reorder(train_df, reserved)
    eval_df = _reorder(eval_df, reserved)

    train_path = stage_dir / "train.parquet"
    eval_path = stage_dir / "eval.parquet"
    manifest_path = stage_dir / "manifest.json"
    if not train_df.empty:
        train_df.to_parquet(train_path, index=False, compression="snappy")
    if not eval_df.empty:
        eval_df.to_parquet(eval_path, index=False, compression="snappy")

    schema_sha = hashlib.sha256(
        json.dumps(list(ALL_FEATURES), sort_keys=True).encode()
    ).hexdigest()[:12]

    manifest: dict[str, Any] = {
        "schema_version": "v2",
        "name": name,
        "k": k,
        "embedding_config_id": embedding_config_id,
        "ontology_snapshot_id": ontology_snapshot_id,
        "annotation_source": annotation_source,
        "train_snapshot_pairs": train_snapshot_pairs,
        "eval_snapshot_pair": eval_pair,
        "schema_sha": schema_sha,
        "n_train_rows": int(len(train_df)),
        "n_eval_rows": int(len(eval_df)),
        "format": "parquet",
        "producer_version": producer_version,
        "producer_git_sha": producer_git_sha,
    }
    if validate_with_contracts:
        _validate_manifest_with_contracts(manifest)
    manifest_path.write_text(json.dumps(manifest, indent=2))

    result: dict[str, Any] = {
        "stage_dir": str(stage_dir),
        "n_train_rows": int(len(train_df)),
        "n_eval_rows": int(len(eval_df)),
        "train_snapshot_pairs": train_snapshot_pairs,
        "eval_snapshot_pair": eval_pair,
        "schema_sha": schema_sha,
    }

    if store is not None:
        prefix = key_prefix or ""
        if train_path.exists():
            result["train_uri"] = store.put(prefix + "train.parquet", train_path)
        if eval_path.exists():
            result["eval_uri"] = store.put(prefix + "eval.parquet", eval_path)
        result["manifest_uri"] = store.put(prefix + "manifest.json", manifest_path)

    return result
