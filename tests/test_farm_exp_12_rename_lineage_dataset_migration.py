"""FARM-EXP.12 (ADR D36): Alembic data-only rename of the legacy lineage dataset.

Pins:

1. The new revision file is on disk, importable, declares the
   expected revision graph, and exports an idempotent
   ``upgrade`` / ``downgrade`` pair.
2. The migration source carries the literal legacy / canonical
   names, the UNIQUE-name guard against clobbering an existing
   canonical row, and the ``alias_names`` JSONB merge.
3. The downgrade reverts the name swap (string-level check; the
   Postgres round-trip is exercised by the operator on the live DB).

The migration is data-only against an existing Postgres ``dataset``
table; we do not stand up a fake DB for this unit test (consistent
with ``tests/test_schema_sha_v2_migration.py`` style).
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = (
    REPO_ROOT
    / "alembic"
    / "versions"
    / "d8e2f4b6a3c1_farm_exp_12_rename_lineage_dataset_to_prostt5.py"
)

LEGACY_NAME = "bench-v1-K5-v226-lineage"
CANONICAL_NAME = "bench-v1-K5-v226-lineage-prostt5"


def _load_module():
    spec = importlib.util.spec_from_file_location(
        "farm_exp_12_rename", MIGRATION_PATH
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# ---------------------------------------------------------------- file


def test_migration_file_is_on_disk() -> None:
    assert MIGRATION_PATH.is_file(), f"migration file missing at {MIGRATION_PATH}"


def test_migration_module_loads_and_pins_revisions() -> None:
    module = _load_module()
    assert module.revision == "d8e2f4b6a3c1"
    assert module.down_revision == "cf16788285f4"
    assert callable(module.upgrade)
    assert callable(module.downgrade)


def test_migration_exports_canonical_name_constants() -> None:
    module = _load_module()
    assert module.LEGACY_NAME == LEGACY_NAME
    assert module.CANONICAL_NAME == CANONICAL_NAME


# ---------------------------------------------------------------- source


def test_migration_source_renames_dataset_name_atomically() -> None:
    source = MIGRATION_PATH.read_text()
    # Single UPDATE statement swapping the unique name.
    assert "UPDATE dataset SET name = :canonical WHERE name = :legacy" in source
    # Downgrade reverses it (legacy / canonical swapped).
    assert "UPDATE dataset SET name = :legacy WHERE name = :canonical" in source


def test_migration_source_appends_alias_to_meta() -> None:
    source = MIGRATION_PATH.read_text()
    # JSONB alias accumulation: append legacy name to meta.alias_names
    # when not already present (idempotent on rerun).
    assert "alias_names" in source
    assert "jsonb_build_object" in source
    # The merge expression must look like ``COALESCE(meta, '{}'::jsonb) || ...``
    assert "COALESCE(meta, '{}'::jsonb)" in source


def test_migration_source_guards_against_double_canonical_row() -> None:
    source = MIGRATION_PATH.read_text()
    # If both legacy and canonical names exist, raise instead of
    # silently dropping the legacy row.
    assert "Both" in source and "manual resolution required" in source


def test_migration_is_noop_when_legacy_row_missing() -> None:
    source = MIGRATION_PATH.read_text()
    # ``if legacy_row is None: return`` is the no-op short-circuit.
    assert "if legacy_row is None" in source
    assert "return" in source


# ---------------------------------------------------------------- references


def test_adr_d36_file_is_present_on_disk() -> None:
    adr = REPO_ROOT / "docs" / "source" / "adr" / "D36-plm-axis-explicit-in-dataset-naming.rst"
    assert adr.is_file(), f"ADR D36 missing at {adr}"
    body = adr.read_text()
    assert LEGACY_NAME in body
    assert CANONICAL_NAME in body
    assert "FARM-EXP.12" in body


def test_adr_d36_listed_in_index() -> None:
    index = (REPO_ROOT / "docs" / "source" / "adr" / "index.rst").read_text()
    assert "D36-plm-axis-explicit-in-dataset-naming" in index
