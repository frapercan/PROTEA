"""T1.6: ``schema_sha_v2`` column declaration tests.

Pin the parallel column added by Alembic migration
``a3c1d8e2f4b6_t1_6_add_schema_sha_v2_columns`` so the column is
declared on both ORM models, is nullable, indexed, and typed as
``Text``. The migration is the source of truth for the database; this
test stops the ORM mapping from drifting silently from it.

Also asserts that the migration itself is reachable from the head and
that its ``upgrade``/``downgrade`` references the expected DDL
operations (smoke check against accidental deletions during merges).
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any

from sqlalchemy import Column, Index, Text
from sqlalchemy.sql.sqltypes import String

from protea.infrastructure.orm.models.embedding.dataset import Dataset
from protea.infrastructure.orm.models.embedding.reranker_model import RerankerModel


def _column(table_class: Any, name: str) -> Column[Any]:
    return table_class.__table__.c[name]


def test_dataset_declares_schema_sha_v2_text_nullable() -> None:
    col = _column(Dataset, "schema_sha_v2")
    assert col.nullable is True, "schema_sha_v2 must be nullable for backfill"
    # SQLAlchemy returns the resolved Postgres dialect type; both ``Text``
    # and ``TEXT`` instances satisfy ``isinstance(..., Text)``.
    assert isinstance(col.type, Text), f"expected Text, got {type(col.type)!r}"
    # The legacy ``schema_sha`` stays a ``String(16)`` so we know we are
    # not accidentally reusing the old type.
    legacy = _column(Dataset, "schema_sha")
    assert isinstance(legacy.type, String)


def test_reranker_model_declares_schema_sha_v2_text_nullable() -> None:
    col = _column(RerankerModel, "schema_sha_v2")
    assert col.nullable is True
    assert isinstance(col.type, Text), f"expected Text, got {type(col.type)!r}"


def _indexes(table_class: Any) -> list[Index]:
    return list(table_class.__table__.indexes)


def test_dataset_schema_sha_v2_index_present() -> None:
    indexes = _indexes(Dataset)
    names = {ix.name for ix in indexes}
    assert "ix_dataset_schema_sha_v2" in names, f"expected ix_dataset_schema_sha_v2 in {names}"
    ix = next(ix for ix in indexes if ix.name == "ix_dataset_schema_sha_v2")
    cols = [c.name for c in ix.columns]
    assert cols == ["schema_sha_v2"]


def test_reranker_model_schema_sha_v2_index_present() -> None:
    indexes = _indexes(RerankerModel)
    names = {ix.name for ix in indexes}
    assert "ix_reranker_model_schema_sha_v2" in names, (
        f"expected ix_reranker_model_schema_sha_v2 in {names}"
    )
    ix = next(ix for ix in indexes if ix.name == "ix_reranker_model_schema_sha_v2")
    cols = [c.name for c in ix.columns]
    assert cols == ["schema_sha_v2"]


def test_migration_module_loads_and_pins_revisions() -> None:
    """The migration module must import and declare the expected revision graph.

    Catches accidental rename / deletion of the T1.6 migration file
    during merges.
    """
    mod_path = (
        Path(__file__).resolve().parents[1]
        / "alembic"
        / "versions"
        / "a3c1d8e2f4b6_t1_6_add_schema_sha_v2_columns.py"
    )
    assert mod_path.is_file(), f"migration file missing at {mod_path}"
    spec = importlib.util.spec_from_file_location("t1_6_migration", mod_path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    assert module.revision == "a3c1d8e2f4b6"
    assert module.down_revision == "9d2fb5a07e4c"
    # ``upgrade`` and ``downgrade`` must be callables; we cannot exec
    # them without a live Alembic context, so the smoke check is by
    # source inspection.
    assert callable(module.upgrade)
    assert callable(module.downgrade)
    source = mod_path.read_text()
    assert "schema_sha_v2" in source
    assert "ix_dataset_schema_sha_v2" in source
    assert "ix_reranker_model_schema_sha_v2" in source
