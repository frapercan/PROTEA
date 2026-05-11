#!/usr/bin/env python3
"""T2B.2 golden-parquet fixture generator (run-once).

Generates ``tests/fixtures/parquet_export_golden_train.parquet`` and
``tests/fixtures/parquet_export_golden_eval.parquet`` from the
develop@eef6cf4 implementation of ``protea.core.parquet_export``.

Usage::

    python scripts/_t2b2_generate_golden_parquet.py

The script loads the historical ``parquet_export.py`` file via
``git show eef6cf4:protea/core/parquet_export.py`` into a sandbox
module so the fixture is produced by the legacy code path even after
the T2B.2 refactor lands. Output is deterministic for the fixed
:func:`_full_feature_row` inputs.

This is a developer-time tool. It does not run in CI. Re-run only
when the canonical column set or test fixture intentionally
changes (which forces a SemVer bump on ``protea-contracts``).
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys
import tempfile
import types
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = REPO_ROOT / "tests" / "fixtures"
TRAIN_FIXTURE = FIXTURE_DIR / "parquet_export_golden_train.parquet"
EVAL_FIXTURE = FIXTURE_DIR / "parquet_export_golden_eval.parquet"
REFERENCE_REV = "eef6cf4"


def _load_legacy_module() -> types.ModuleType:
    """Materialise the eef6cf4 parquet_export.py as a sandbox module.

    Reads the file via ``git show`` from the working tree so the local
    refactor is bypassed entirely. The sandbox module name is
    ``_parquet_export_eef6cf4`` to avoid colliding with the live
    package under ``protea.core``.
    """
    raw = subprocess.check_output(
        ["git", "show", f"{REFERENCE_REV}:protea/core/parquet_export.py"],
        cwd=str(REPO_ROOT),
        text=True,
    )
    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False, encoding="utf-8") as fh:
        fh.write(raw)
        legacy_path = fh.name
    spec = importlib.util.spec_from_file_location("_parquet_export_eef6cf4", legacy_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    # dataclass() resolves type annotations via sys.modules[cls.__module__];
    # register the sandbox before exec so frozen dataclasses inside the
    # legacy file can introspect their own module.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _full_feature_row() -> dict[str, object]:
    """Deterministic single-row shard input mirroring the fixture in
    :mod:`tests.test_parquet_export_boundary`.

    Numeric features get ``0.0``; string-typed categoricals get
    fixed sentinels; reserved columns get fixed sentinel values.
    """
    from protea_contracts import ALL_FEATURES

    row: dict[str, object] = {
        "protein_accession": "P12345",
        "go_id": "GO:0000001",
        "label": 0,
        "aspect": "P",
    }
    for col in ALL_FEATURES:
        if col in row:
            continue
        if col in {"qualifier", "evidence_code", "taxonomic_relation"}:
            row[col] = "x"
        else:
            row[col] = 0.0
    return row


def _write_shard(path: Path, rows: list[dict[str, object]]) -> Path:
    import pandas as pd

    df = pd.DataFrame(rows) if rows else pd.DataFrame()
    df.to_parquet(path, index=False, compression="snappy")
    return path


def main() -> None:
    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)
    legacy = _load_legacy_module()

    with tempfile.TemporaryDirectory() as workdir:
        stage_dir = Path(workdir)
        train_shard = _write_shard(stage_dir / "_train_nk.parquet", [_full_feature_row()])
        eval_shard = _write_shard(stage_dir / "_eval_nk.parquet", [_full_feature_row()])
        ctx = legacy.ParquetExportContext(
            stage_dir=stage_dir,
            split_files={"nk": [train_shard]},
            valid_split_versions=[(220, 221)],
            test_files={"nk": eval_shard},
            test_old_v=221,
            test_new_v=222,
            name="t2b2-golden",
            k=5,
            embedding_config_id="00000000-0000-0000-0000-000000000001",
            ontology_snapshot_id="00000000-0000-0000-0000-000000000002",
            annotation_source="goa",
            store=None,
            producer_version="t2b2",
            producer_git_sha=REFERENCE_REV,
            validate_with_contracts=False,
        )
        legacy.export_reranker_parquets(ctx)
        produced_train = stage_dir / "train.parquet"
        produced_eval = stage_dir / "eval.parquet"
        TRAIN_FIXTURE.write_bytes(produced_train.read_bytes())
        EVAL_FIXTURE.write_bytes(produced_eval.read_bytes())
        print(f"wrote {TRAIN_FIXTURE} ({TRAIN_FIXTURE.stat().st_size} bytes)")
        print(f"wrote {EVAL_FIXTURE} ({EVAL_FIXTURE.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
    sys.exit(0)
