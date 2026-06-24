"""F-EVAL-PROTOCOL .a: ``window_role`` column declaration tests.

Pin the rolling-origin protocol-window marker (ADR D40) added by Alembic
migration ``f2a4c6e8b0d1_add_window_role_to_evaluation_set`` so the
column stays declared on the ORM model, nullable, typed ``String(8)``,
and guarded by the closed-vocabulary CHECK constraint. The migration is
the source of truth for the database; this test stops the ORM mapping
from drifting silently from it.

Also asserts the migration is reachable from the previous head and that
its ``upgrade`` / ``downgrade`` reference the expected DDL (smoke check
against accidental deletion during merges).
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any

from sqlalchemy import CheckConstraint, Column
from sqlalchemy.sql.sqltypes import String

from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet


def _column(name: str) -> Column[Any]:
    return EvaluationSet.__table__.c[name]


def test_window_role_declared_string8_nullable() -> None:
    col = _column("window_role")
    assert col.nullable is True, "window_role must be nullable (legacy + ad-hoc sets)"
    assert isinstance(col.type, String)
    assert col.type.length == 8


def test_window_role_check_constraint_present() -> None:
    constraints = [
        c
        for c in EvaluationSet.__table__.constraints
        if isinstance(c, CheckConstraint)
    ]
    names = {c.name for c in constraints}
    assert "ck_evaluation_set_window_role" in names, (
        f"expected ck_evaluation_set_window_role in {names}"
    )


def test_migration_module_loads_and_pins_revisions() -> None:
    mod_path = (
        Path(__file__).resolve().parents[1]
        / "alembic"
        / "versions"
        / "f2a4c6e8b0d1_add_window_role_to_evaluation_set.py"
    )
    assert mod_path.is_file(), f"migration file missing at {mod_path}"
    spec = importlib.util.spec_from_file_location("window_role_migration", mod_path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    assert module.revision == "f2a4c6e8b0d1"
    assert module.down_revision == "a7b3c8d2e1f4"
    assert callable(module.upgrade)
    assert callable(module.downgrade)
    source = mod_path.read_text()
    assert "window_role" in source
    assert "ck_evaluation_set_window_role" in source
