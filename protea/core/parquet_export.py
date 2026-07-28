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
from typing import Any, NamedTuple

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from protea_contracts import compute_schema_sha as _canonical_schema_sha

from protea.core.features import REGISTRY as _FEATURE_REGISTRY
from protea.core.features._bindings import _POOL_INJECTED_FEATURES
from protea.core.reranker import LABEL_COLUMN
from protea.infrastructure.storage import ArtifactStore

logger = logging.getLogger(__name__)

# T2B.2: drive the schema off the FeatureRegistry singleton instead of
# the legacy hardcoded ``ALL_FEATURES`` constant. Importing
# :mod:`protea.core.features` triggers
# :func:`protea.core.features._bindings.apply_canonical_bindings` so
# every feature has a real compute reference bound (was placeholder
# raiser in T2B.1).
#
# The canonical column list is queried lazily so a registry reset in
# tests (:func:`reset_canonical_registry`) does not strand the
# exporter against a stale tuple captured at import time.


def _registry_feature_names() -> list[str]:
    """Return the canonical feature names this exporter is responsible for.

    Wraps :meth:`FeatureRegistry.names` so the exporter has a single
    seam to swap if T2B.3 makes the registry context-aware (per
    active families).

    Pool-injected columns are excluded. ``plm_id`` and ``k_context`` record
    which PLM and which K a row came from; they are meaningless for a single
    manifest and the lab stamps them only when pooling several sources to
    train a universal booster. PROTEA never writes them, so demanding them at
    the T1.8 boundary would fail every dump this platform produces.

    The exclusion is a statement about who produces a column, not a way to
    make the check pass. Compare ADR-D45: a column with no producer must be
    accounted for explicitly, never filled with a plausible value.
    """
    return [n for n in _FEATURE_REGISTRY.names() if n not in _POOL_INJECTED_FEATURES]


def _produced_family_columns(
    provenance: tuple[FamilyProvenance, ...],
) -> dict[str, list[str]]:
    """Map each ``produced`` family to its registered feature columns (ADR-D45).

    Only families the export recorded as ``produced`` are returned; the
    shard-write degeneracy check iterates this map so a ``declared_absent``
    family (legitimately constant NaN) is never checked. Columns are filtered to
    the canonical registry so a family whose columns are not in this dump's
    schema contributes nothing.
    """
    from protea_contracts import FEATURE_FAMILIES

    names = set(_registry_feature_names())
    out: dict[str, list[str]] = {}
    for fp in provenance:
        if fp.state != PRODUCED:
            continue
        cols = [c for c in FEATURE_FAMILIES.get(fp.family, ()) if c in names]
        if cols:
            out[fp.family] = cols
    return out


_ASPECT_NAMES = {"P": "bpo", "F": "mfo", "C": "cco"}
_CATEGORIES = ("nk", "lk", "pk")

# Number of rows materialised at once while streaming a shard into the
# consolidated split parquet. One batch (~200k rows) is resident at a
# time instead of the whole ~76M-row training set, which keeps the
# final dump-assembly RSS well under 1 GB (twin of the per-split
# streaming fix in #654, which left this final assembly on the old
# ``pd.concat`` path that spiked to ~108 GB committed).
_STREAM_BATCH_ROWS = 200_000

_RESERVED_COLUMNS = [
    "protein_accession",
    "go_term_id",
    LABEL_COLUMN,
    "category",
    "snapshot_pair",
]

#: Feature-family provenance states (ADR-D45).
PRODUCED = "produced"
DECLARED_ABSENT = "declared_absent"


class FamilyProvenance(NamedTuple):
    """Per-family production status recorded in the dataset manifest (ADR-D45).

    A feature family is in exactly one of two recorded states for a given
    export:

    * ``produced``: a producer ran and wrote real values; ``producer`` names it.
    * ``declared_absent``: no producer is wired, so the family's columns ship as
      ``NaN`` (a missing measurement). ``producer`` is ``None``.

    Recording the absence here lets a reader learn it from metadata instead of
    inferring it from a column of zeros, and lets the shard-write degeneracy
    check (:func:`_assert_no_degenerate_families`) skip the declared-absent
    families while still failing loudly on a family that CLAIMS production yet
    ships a constant column.
    """

    family: str
    state: str
    producer: str | None


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

    # ADR-D45: per-family production status. Families marked ``produced`` are
    # subject to the shard-write degeneracy check; families marked
    # ``declared_absent`` are exempt (their columns are legitimately constant
    # NaN). Both are recorded in the manifest. Empty () keeps the legacy
    # behaviour: no family is degeneracy-checked and no provenance is written.
    feature_family_provenance: tuple[FamilyProvenance, ...] = ()


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
    feature_cols = [c for c in _registry_feature_names() if c in df.columns]
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


class _ExportMetrics(NamedTuple):
    """Split metadata threaded through manifest + result builders.

    Carries the streamed row counts (not the frames themselves) so the
    consolidated dataset is never materialised in memory for the
    manifest / result payloads.
    """

    n_train_rows: int
    n_eval_rows: int
    train_snapshot_pairs: list[str]
    eval_pair: str
    schema_sha: str


def export_reranker_parquets(ctx: ParquetExportContext) -> dict[str, Any]:
    """Consolidate per-cat per-split parquet shards into the frozen dataset
    layout and optionally publish via the configured ``ArtifactStore``.

    All inputs live on :class:`ParquetExportContext`. Sub-helpers handle
    shard loading, canonical-column assertion, manifest write, and
    optional store upload.
    """
    ctx.stage_dir.mkdir(parents=True, exist_ok=True)
    aspect_norm = dict(_ASPECT_NAMES)
    eval_pair = f"v{ctx.test_old_v}-v{ctx.test_new_v}"

    train_path = ctx.stage_dir / "train.parquet"
    eval_path = ctx.stage_dir / "eval.parquet"
    manifest_path = ctx.stage_dir / "manifest.json"

    produced_family_columns = _produced_family_columns(ctx.feature_family_provenance)
    n_train_rows, train_snapshot_pairs = _stream_train_shards(
        ctx, aspect_norm, train_path, produced_family_columns
    )
    n_eval_rows = _stream_eval_shards(
        ctx, eval_pair, aspect_norm, eval_path, produced_family_columns
    )

    metrics = _ExportMetrics(
        n_train_rows=n_train_rows,
        n_eval_rows=n_eval_rows,
        train_snapshot_pairs=train_snapshot_pairs,
        eval_pair=eval_pair,
        schema_sha=_compute_schema_sha(),
    )
    _build_and_write_manifest(ctx, manifest_path, metrics)
    result = _build_result(ctx, metrics)
    if ctx.store is not None:
        _publish_to_store(ctx, train_path, eval_path, manifest_path, result)
    return result


def _compute_schema_sha() -> str:
    """Legacy schema_sha hash kept in the manifest until T1.6 of master plan
    v3 lands the schema_sha_v2 migration. The T1.8 invariant guarantees the
    column set is correct."""
    return hashlib.sha256(
        json.dumps(_registry_feature_names(), sort_keys=True).encode()
    ).hexdigest()[:12]


def _build_and_write_manifest(
    ctx: ParquetExportContext, manifest_path: Path, metrics: _ExportMetrics
) -> None:
    """Assemble the ManifestV1 dict, run optional contract validation, write
    JSON to disk."""
    manifest: dict[str, Any] = {
        "schema_version": "v2",
        "name": ctx.name,
        "k": ctx.k,
        "embedding_config_id": ctx.embedding_config_id,
        "ontology_snapshot_id": ctx.ontology_snapshot_id,
        "annotation_source": ctx.annotation_source,
        "train_snapshot_pairs": metrics.train_snapshot_pairs,
        "eval_snapshot_pair": metrics.eval_pair,
        "schema_sha": metrics.schema_sha,
        "n_train_rows": int(metrics.n_train_rows),
        "n_eval_rows": int(metrics.n_eval_rows),
        "format": "parquet",
        "producer_version": ctx.producer_version,
        "producer_git_sha": ctx.producer_git_sha,
    }
    if ctx.feature_family_provenance:
        # ADR-D45: record which feature families a producer wrote and which are
        # declared absent (columns ship NaN). ManifestV1 ignores extra keys, so
        # this rides the existing manifest without a lab-contract bump.
        manifest["feature_family_provenance"] = [
            {"family": fp.family, "state": fp.state, "producer": fp.producer}
            for fp in ctx.feature_family_provenance
        ]
    if ctx.validate_with_contracts:
        _validate_manifest_with_contracts(manifest)
    manifest_path.write_text(json.dumps(manifest, indent=2))


def _build_result(ctx: ParquetExportContext, metrics: _ExportMetrics) -> dict[str, Any]:
    """Compose the function's return payload (URIs are stamped later when the
    optional artefact-store upload runs)."""
    return {
        "stage_dir": str(ctx.stage_dir),
        "n_train_rows": int(metrics.n_train_rows),
        "n_eval_rows": int(metrics.n_eval_rows),
        "train_snapshot_pairs": metrics.train_snapshot_pairs,
        "eval_snapshot_pair": metrics.eval_pair,
        "schema_sha": metrics.schema_sha,
    }


def _stamp_and_reorder_batch(
    pdf: pd.DataFrame,
    cat: str,
    snap_pair: str,
    aspect_norm: dict[str, str],
) -> pd.DataFrame:
    """Apply the per-shard transforms to one batch frame.

    Stamps ``category`` + ``snapshot_pair``, normalises ``aspect``,
    renames ``go_id`` -> ``go_term_id`` and reorders to the canonical
    ``reserved + feature`` layout. Identical to the legacy per-shard
    pandas path, just applied one batch at a time so a whole shard is
    never resident.
    """
    pdf["category"] = cat
    pdf["snapshot_pair"] = snap_pair
    if "aspect" in pdf.columns:
        pdf["aspect"] = pdf["aspect"].map(aspect_norm).fillna(pdf["aspect"])
    pdf = pdf.rename(columns={"go_id": "go_term_id"})
    return _reorder(pdf, _RESERVED_COLUMNS)


class _SplitWriter:
    """Streaming writer for one consolidated split parquet.

    Owns a single :class:`pyarrow.parquet.ParquetWriter` opened lazily on
    the first non-empty batch (so an all-empty split leaves no file on
    disk, matching the legacy ``if not df.empty`` guard). Each shard is
    read in ``_STREAM_BATCH_ROWS`` chunks and only one batch is ever
    resident, replacing the ``pd.concat`` of the whole split.
    """

    def __init__(
        self,
        out_path: Path,
        split_name: str,
        aspect_norm: dict[str, str],
        produced_family_columns: dict[str, list[str]] | None = None,
    ) -> None:
        self._out_path = out_path
        self._split_name = split_name
        self._aspect_norm = aspect_norm
        self._writer: pq.ParquetWriter | None = None
        self.n_rows = 0
        # ADR-D45 degeneracy tracking: per produced-family column, up to two
        # distinct values seen across all batches (capped, so memory is O(1)).
        # A produced family whose every column ends with <= 1 distinct value is
        # constant across the whole split, which means its producer did not run.
        self._produced_family_columns = produced_family_columns or {}
        self._checked_columns: list[str] = sorted(
            {c for cols in self._produced_family_columns.values() for c in cols}
        )
        self._distinct: dict[str, set[Any]] = {c: set() for c in self._checked_columns}

    def write_shard(self, shard_path: Path, cat: str, snap_pair: str) -> None:
        """Stream one shard into the split, batch by batch."""
        pf = pq.ParquetFile(str(shard_path))
        for batch in pf.iter_batches(batch_size=_STREAM_BATCH_ROWS):
            pdf = _stamp_and_reorder_batch(batch.to_pandas(), cat, snap_pair, self._aspect_norm)
            if pdf.empty:
                continue
            _assert_canonical_columns(self._split_name, pdf, _RESERVED_COLUMNS)
            self._track_produced_family_values(pdf)
            table = pa.Table.from_pandas(pdf, preserve_index=False)
            if self._writer is None:
                self._writer = pq.ParquetWriter(
                    str(self._out_path), table.schema, compression="snappy"
                )
            self._writer.write_table(table)
            self.n_rows += len(pdf)

    def _track_produced_family_values(self, pdf: pd.DataFrame) -> None:
        """Accumulate up to two distinct values per checked column for the batch."""
        for col in self._checked_columns:
            seen = self._distinct[col]
            if len(seen) > 1 or col not in pdf.columns:
                continue
            for val in pdf[col].unique():
                # Collapse every NaN to one sentinel so an all-missing column
                # reads as a single distinct value (constant), not many.
                seen.add(_DEGENERATE_NAN if pd.isna(val) else val)
                if len(seen) > 1:
                    break

    def close(self) -> None:
        if self._writer is not None:
            self._writer.close()

    def assert_no_degenerate_families(self) -> None:
        """Fail if any produced family stayed constant across the whole split."""
        if self.n_rows == 0:
            return
        _assert_no_degenerate_families(
            self._split_name, self._distinct, self._produced_family_columns
        )


def _stream_train_shards(
    ctx: ParquetExportContext,
    aspect_norm: dict[str, str],
    out_path: Path,
    produced_family_columns: dict[str, list[str]],
) -> tuple[int, list[str]]:
    """Stream every per-cat training shard into ``out_path``.

    Replaces the legacy ``pd.read_parquet`` + ``pd.concat`` +
    ``to_parquet`` assembly (which materialised the entire ~76M-row
    training set, ~108 GB committed). Only one batch is resident at a
    time. Returns ``(n_rows, snapshot_pairs)`` with snapshot_pairs in
    insertion order. No file is written when no shard yields a row,
    preserving the legacy empty-split behaviour.
    """
    writer = _SplitWriter(out_path, "train", aspect_norm, produced_family_columns)
    snapshot_pairs: list[str] = []
    try:
        for cat in _CATEGORIES:
            shards = ctx.split_files.get(cat, [])
            for shard_idx, shard_path in enumerate(shards):
                v_old, v_new = ctx.valid_split_versions[shard_idx]
                snap_pair = f"v{v_old}-v{v_new}"
                if snap_pair not in snapshot_pairs:
                    snapshot_pairs.append(snap_pair)
                writer.write_shard(shard_path, cat, snap_pair)
    finally:
        writer.close()
    # ADR-D45: only on the success path, after a clean close, so a genuine
    # write error is never masked by the degeneracy assertion.
    writer.assert_no_degenerate_families()
    return writer.n_rows, snapshot_pairs


def _stream_eval_shards(
    ctx: ParquetExportContext,
    eval_pair: str,
    aspect_norm: dict[str, str],
    out_path: Path,
    produced_family_columns: dict[str, list[str]],
) -> int:
    """Stream every per-cat test shard into ``out_path``. Returns row count.

    Streaming twin of the legacy ``_load_eval_shards`` concat path; one
    batch resident at a time, no file written when there are no rows.
    """
    writer = _SplitWriter(out_path, "eval", aspect_norm, produced_family_columns)
    try:
        for cat in _CATEGORIES:
            path = ctx.test_files.get(cat)
            if path is None:
                continue
            writer.write_shard(path, cat, eval_pair)
    finally:
        writer.close()
    writer.assert_no_degenerate_families()
    return writer.n_rows


#: Sentinel every NaN collapses to while tracking distinct values, so an
#: all-missing column reads as one distinct value (constant), not many.
_DEGENERATE_NAN = "__nan__"


def _assert_no_degenerate_families(
    split_name: str,
    distinct: dict[str, set[Any]],
    produced_family_columns: dict[str, list[str]],
) -> None:
    """ADR-D45 boundary check: a family recorded as ``produced`` must vary.

    For every family the export declared PRODUCED, if every one of its columns
    is constant across the whole split (a single distinct value, including the
    all-NaN case), raise: a claimed producer that ships a constant column did
    not actually run. Families recorded ``declared_absent`` are not in
    ``produced_family_columns`` so their legitimately-constant NaN columns are
    never flagged. The error names the family, the constant value per column,
    and the split.
    """
    for family, cols in produced_family_columns.items():
        present = [c for c in cols if c in distinct]
        if not present or any(len(distinct[c]) > 1 for c in present):
            continue
        constant_values = {
            c: (None if _DEGENERATE_NAN in distinct[c] else next(iter(distinct[c]), None))
            for c in present
        }
        raise ValueError(
            f"{split_name!r} split: feature family {family!r} is recorded as "
            f"produced but every column is constant across the shard "
            f"(constant_values={constant_values!r}). A produced family must vary "
            "across the dataset; an all-constant family means its producer did "
            "not run over these records. If the producer is intentionally "
            "unwired, record the family as declared-absent in the export "
            "provenance instead of shipping a constant column (ADR-D45)."
        )


def _assert_canonical_columns(split_name: str, shard: pd.DataFrame, reserved: list[str]) -> None:
    """T1.8 boundary check: a streamed batch's feature columns must equal
    the registry's canonical feature set exactly under the lab schema sha.
    Raises ``ValueError`` with the missing/extras diff before the batch is
    written.
    """
    canonical_features = _registry_feature_names()
    canonical_set = set(canonical_features)
    canonical_features_sha = _canonical_schema_sha(canonical_features)
    if shard.empty:
        return
    present_features = [c for c in shard.columns if c in canonical_set]
    if _canonical_schema_sha(present_features) == canonical_features_sha:
        return
    missing = [c for c in canonical_features if c not in shard.columns]
    extras = [c for c in shard.columns if c not in canonical_set and c not in reserved]
    raise ValueError(
        f"{split_name} shard fails the canonical column invariant. "
        f"missing={missing!r} extras={extras!r}. "
        "All canonical feature columns must be present before write."
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
