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
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
from protea_contracts import compute_schema_sha as _canonical_schema_sha

from protea.core.reranker import ALL_FEATURES, LABEL_COLUMN
from protea.infrastructure.storage import ArtifactStore

logger = logging.getLogger(__name__)

_ASPECT_NAMES = {"P": "bpo", "F": "mfo", "C": "cco"}
_CATEGORIES = ("nk", "lk", "pk")


@dataclass(frozen=True)
class ParquetExportContext:
    """Bundle of inputs ``export_reranker_parquets`` consumes.

    Groups the 15 per-call inputs (identity, source shards, publishing
    options) so the entry-point signature stays under flake8-bugbear's
    parameter ceiling. Keep this dataclass authoritative when adding
    new options.
    """

    # Source shards
    stage_dir: Path
    split_files: dict[str, list[Path]]
    valid_split_versions: list[tuple[int, int]]
    test_files: dict[str, Path | None]
    test_old_v: int
    test_new_v: int

    # Dataset identity
    name: str
    k: int
    embedding_config_id: str
    ontology_snapshot_id: str
    annotation_source: str

    # Publishing
    store: ArtifactStore | None = None
    key_prefix: str = ""
    producer_version: str | None = None
    producer_git_sha: str | None = None
    validate_with_contracts: bool = True


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


def export_reranker_parquets(ctx: ParquetExportContext) -> dict[str, Any]:
    """Consolidate per-cat per-split parquet shards into the frozen
    dataset layout and optionally publish via an ``ArtifactStore``.

    All inputs live on :class:`ParquetExportContext`. Notable fields:

    - ``stage_dir``: local staging area (always written here; uploaded
      under ``key_prefix`` if ``store`` is set).
    - ``split_files``: per-category training shard paths, parallel to
      ``valid_split_versions``.
    - ``test_files``: per-category test shard path (may be ``None``).
    - ``store`` / ``key_prefix``: optional artifact-store upload.
    - ``producer_version`` / ``producer_git_sha``: manifest provenance.
    - ``validate_with_contracts``: best-effort validate against the
      lab's ``ManifestV1`` before writing.
    """
    ctx.stage_dir.mkdir(parents=True, exist_ok=True)
    aspect_norm = dict(_ASPECT_NAMES)

    train_df, train_snapshot_pairs = _load_train_shards(ctx, aspect_norm)
    eval_pair = f"v{ctx.test_old_v}-v{ctx.test_new_v}"
    eval_df = _load_eval_shards(ctx, eval_pair, aspect_norm)

    reserved = [
        "protein_accession",
        "go_term_id",
        LABEL_COLUMN,
        "category",
        "snapshot_pair",
    ]
    train_df = _reorder(train_df, reserved)
    eval_df = _reorder(eval_df, reserved)
    _assert_canonical_columns(train_df, eval_df, reserved)

    train_path = ctx.stage_dir / "train.parquet"
    eval_path = ctx.stage_dir / "eval.parquet"
    manifest_path = ctx.stage_dir / "manifest.json"
    if not train_df.empty:
        train_df.to_parquet(train_path, index=False, compression="snappy")
    if not eval_df.empty:
        eval_df.to_parquet(eval_path, index=False, compression="snappy")

    # Legacy schema_sha hash kept in the manifest until T1.6 of master
    # plan v3 lands the schema_sha_v2 migration. The T1.8 invariant
    # above already guarantees the column set is correct.
    schema_sha = hashlib.sha256(
        json.dumps(list(ALL_FEATURES), sort_keys=True).encode()
    ).hexdigest()[:12]

    manifest: dict[str, Any] = {
        "schema_version": "v2",
        "name": ctx.name,
        "k": ctx.k,
        "embedding_config_id": ctx.embedding_config_id,
        "ontology_snapshot_id": ctx.ontology_snapshot_id,
        "annotation_source": ctx.annotation_source,
        "train_snapshot_pairs": train_snapshot_pairs,
        "eval_snapshot_pair": eval_pair,
        "schema_sha": schema_sha,
        "n_train_rows": int(len(train_df)),
        "n_eval_rows": int(len(eval_df)),
        "format": "parquet",
        "producer_version": ctx.producer_version,
        "producer_git_sha": ctx.producer_git_sha,
    }
    if ctx.validate_with_contracts:
        _validate_manifest_with_contracts(manifest)
    manifest_path.write_text(json.dumps(manifest, indent=2))

    result: dict[str, Any] = {
        "stage_dir": str(ctx.stage_dir),
        "n_train_rows": int(len(train_df)),
        "n_eval_rows": int(len(eval_df)),
        "train_snapshot_pairs": train_snapshot_pairs,
        "eval_snapshot_pair": eval_pair,
        "schema_sha": schema_sha,
    }
    if ctx.store is not None:
        _publish_to_store(ctx, train_path, eval_path, manifest_path, result)
    return result


def _load_train_shards(
    ctx: ParquetExportContext, aspect_norm: dict[str, str]
) -> tuple[pd.DataFrame, list[str]]:
    """Read every per-cat training shard, stamp ``category`` + ``snapshot_pair``,
    and concat into a single frame. Returns ``(df, snapshot_pairs)`` where the
    snapshot_pairs list keeps insertion order."""
    train_frames: list[pd.DataFrame] = []
    train_snapshot_pairs: list[str] = []
    for cat in _CATEGORIES:
        shards = ctx.split_files.get(cat, [])
        for shard_idx, shard_path in enumerate(shards):
            v_old, v_new = ctx.valid_split_versions[shard_idx]
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
    train_df = pd.concat(train_frames, ignore_index=True) if train_frames else pd.DataFrame()
    return train_df, train_snapshot_pairs


def _load_eval_shards(
    ctx: ParquetExportContext, eval_pair: str, aspect_norm: dict[str, str]
) -> pd.DataFrame:
    """Read each per-cat test shard, stamp ``category`` + ``snapshot_pair``, concat."""
    eval_frames: list[pd.DataFrame] = []
    for cat in _CATEGORIES:
        path = ctx.test_files.get(cat)
        if path is None:
            continue
        edf = pd.read_parquet(path)
        edf["category"] = cat
        edf["snapshot_pair"] = eval_pair
        if "aspect" in edf.columns:
            edf["aspect"] = edf["aspect"].map(aspect_norm).fillna(edf["aspect"])
        edf = edf.rename(columns={"go_id": "go_term_id"})
        eval_frames.append(edf)
    return pd.concat(eval_frames, ignore_index=True) if eval_frames else pd.DataFrame()


def _assert_canonical_columns(
    train_df: pd.DataFrame, eval_df: pd.DataFrame, reserved: list[str]
) -> None:
    """T1.8 boundary check: every non-empty shard's feature columns must equal
    ``ALL_FEATURES`` exactly under the canonical lab schema sha. Raises
    ``ValueError`` with the missing/extras diff before any parquet is written.
    """
    canonical_features_sha = _canonical_schema_sha(list(ALL_FEATURES))
    for shard_name, shard in (("train", train_df), ("eval", eval_df)):
        if shard.empty:
            continue
        present_features = [c for c in shard.columns if c in ALL_FEATURES]
        if _canonical_schema_sha(present_features) == canonical_features_sha:
            continue
        missing = [c for c in ALL_FEATURES if c not in shard.columns]
        extras = [c for c in shard.columns if c not in ALL_FEATURES and c not in reserved]
        raise ValueError(
            f"{shard_name} shard fails the canonical column invariant. "
            f"missing={missing!r} extras={extras!r}. "
            "All ALL_FEATURES columns must be present before write."
        )


def _publish_to_store(
    ctx: ParquetExportContext,
    train_path: Path,
    eval_path: Path,
    manifest_path: Path,
    result: dict[str, Any],
) -> None:
    """Upload train/eval/manifest to ``ctx.store`` and stamp URIs on ``result``."""
    assert ctx.store is not None
    prefix = ctx.key_prefix or ""
    if train_path.exists():
        result["train_uri"] = ctx.store.put(prefix + "train.parquet", train_path)
    if eval_path.exists():
        result["eval_uri"] = ctx.store.put(prefix + "eval.parquet", eval_path)
    result["manifest_uri"] = ctx.store.put(prefix + "manifest.json", manifest_path)
