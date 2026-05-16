"""FARM-EXP.1: ExperimentRun axis-column declarations + migration pins.

This pins the ORM and the migration file structure so future merges
cannot silently drop the axis columns or the partial-unique index.
Mirrors the style of :mod:`tests.test_schema_sha_v2_column`.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any

from sqlalchemy import Column, Index, Integer, Text
from sqlalchemy.dialects.postgresql import JSONB

from protea.infrastructure.orm.models.experiment_run import ExperimentRun


def _column(table_class: Any, name: str) -> Column[Any]:
    return table_class.__table__.c[name]


# ----------------------------------------------------------------- columns


def test_experiment_run_has_nine_axis_columns() -> None:
    cols = {c.name for c in ExperimentRun.__table__.columns}
    expected = {
        "plm",
        "k",
        "reranker_spec_id",
        "feature_schema_sha",
        "eval_set_name",
        "eval_set_manifest_sha",
        "propagation",
        "ensemble_spec",
        "axis_tuple_shortid",
    }
    missing = expected - cols
    assert not missing, f"missing axis columns: {missing}"


def test_text_axis_columns_are_text_nullable() -> None:
    for name in (
        "plm",
        "reranker_spec_id",
        "feature_schema_sha",
        "eval_set_name",
        "eval_set_manifest_sha",
        "propagation",
        "axis_tuple_shortid",
    ):
        col = _column(ExperimentRun, name)
        assert col.nullable is True, f"{name} must be nullable for backfill"
        assert isinstance(col.type, Text), f"{name} expected Text, got {type(col.type)!r}"


def test_k_column_is_integer_nullable() -> None:
    col = _column(ExperimentRun, "k")
    assert col.nullable is True
    assert isinstance(col.type, Integer)


def test_ensemble_spec_is_jsonb_nullable() -> None:
    col = _column(ExperimentRun, "ensemble_spec")
    assert col.nullable is True
    assert isinstance(col.type, JSONB)


# ----------------------------------------------------------------- index


def _indexes() -> list[Index]:
    return list(ExperimentRun.__table__.indexes)


def test_axis_tuple_shortid_has_partial_unique_index() -> None:
    indexes = _indexes()
    by_name = {ix.name: ix for ix in indexes}
    assert "uq_experiment_run_axis_tuple_shortid" in by_name, (
        f"expected uq_experiment_run_axis_tuple_shortid in {set(by_name)}"
    )
    ix = by_name["uq_experiment_run_axis_tuple_shortid"]
    assert ix.unique is True
    cols = [c.name for c in ix.columns]
    assert cols == ["axis_tuple_shortid"]
    # Partial-where so NULL shortids can coexist (legacy + partial backfill).
    where_clause = ix.dialect_options.get("postgresql", {}).get("where")
    assert where_clause is not None, "partial index must carry postgresql_where"
    assert "axis_tuple_shortid IS NOT NULL" in str(where_clause)


# ----------------------------------------------------------------- migration


def _load_migration(filename: str) -> Any:
    mod_path = (
        Path(__file__).resolve().parents[1]
        / "alembic"
        / "versions"
        / filename
    )
    assert mod_path.is_file(), f"migration file missing at {mod_path}"
    spec = importlib.util.spec_from_file_location(filename, mod_path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_merge_migration_pins_five_parents() -> None:
    module = _load_migration("d9f7a1c3b2e5_merge_pre_farm_exp_1_heads.py")
    assert module.revision == "d9f7a1c3b2e5"
    parents = module.down_revision
    assert isinstance(parents, tuple), "merge migration must carry tuple down_revision"
    assert set(parents) == {
        "2a5e9b3f1c4d",
        "47de89cf6fec",
        "b1c2d3e4f5a6",
        "b8e3f1a7c2d9",
        "d7e4c2b9a1f0",
    }


def test_axis_migration_pins_revision_graph() -> None:
    module = _load_migration("e1c4a7b2d8f3_farm_exp_1_experiment_run_axis.py")
    assert module.revision == "e1c4a7b2d8f3"
    assert module.down_revision == "d9f7a1c3b2e5"
    assert callable(module.upgrade)
    assert callable(module.downgrade)

    source = (
        Path(__file__).resolve().parents[1]
        / "alembic"
        / "versions"
        / "e1c4a7b2d8f3_farm_exp_1_experiment_run_axis.py"
    ).read_text()
    # Both axis columns and the unique index must appear in the migration
    # source so accidental dropping during merges is caught here.
    for name in (
        "plm",
        "axis_tuple_shortid",
        "ensemble_spec",
        "uq_experiment_run_axis_tuple_shortid",
        "axis_tuple_shortid IS NOT NULL",
    ):
        assert name in source, f"migration must reference {name!r}"
