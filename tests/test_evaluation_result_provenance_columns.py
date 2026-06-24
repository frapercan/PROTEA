"""F-METHOD-EVAL-SURFACE: ``evaluation_result`` provenance column tests.

Pin the method-surface provenance markers (``frame`` / ``temporal_window``
/ ``arms_enabled`` / ``leakage_role``) added by Alembic migration
``c3d5e7f9a1b2_add_method_surface_provenance_to_evaluation_result`` so the
columns stay declared on the ORM model, nullable, correctly typed, and
guarded by their closed-vocabulary CHECK constraints. The migration is the
source of truth for the database; this test stops the ORM mapping from
drifting silently from it.

Also asserts the migration is reachable from the previous head and that
its ``upgrade`` / ``downgrade`` reference the expected DDL (smoke check
against accidental deletion during merges).
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any

from sqlalchemy import CheckConstraint, Column
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql.sqltypes import String

from protea.infrastructure.orm.models.annotation.evaluation_result import EvaluationResult


def _column(name: str) -> Column[Any]:
    return EvaluationResult.__table__.c[name]


def test_frame_declared_string8_nullable() -> None:
    col = _column("frame")
    assert col.nullable is True, "frame must be nullable (legacy rows)"
    assert isinstance(col.type, String)
    assert col.type.length == 8


def test_temporal_window_declared_string32_nullable() -> None:
    col = _column("temporal_window")
    assert col.nullable is True
    assert isinstance(col.type, String)
    assert col.type.length == 32


def test_arms_enabled_declared_jsonb_nullable() -> None:
    col = _column("arms_enabled")
    assert col.nullable is True
    assert isinstance(col.type, JSONB)


def test_leakage_role_declared_string8_nullable() -> None:
    col = _column("leakage_role")
    assert col.nullable is True
    assert isinstance(col.type, String)
    assert col.type.length == 8


def test_provenance_check_constraints_present() -> None:
    names = {
        c.name
        for c in EvaluationResult.__table__.constraints
        if isinstance(c, CheckConstraint)
    }
    assert "ck_evaluation_result_frame" in names, names
    assert "ck_evaluation_result_leakage_role" in names, names


def test_migration_module_loads_and_pins_revisions() -> None:
    mod_path = (
        Path(__file__).resolve().parents[1]
        / "alembic"
        / "versions"
        / "c3d5e7f9a1b2_add_method_surface_provenance_to_evaluation_result.py"
    )
    assert mod_path.is_file(), f"migration file missing at {mod_path}"
    spec = importlib.util.spec_from_file_location("eval_result_provenance_migration", mod_path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    assert module.revision == "c3d5e7f9a1b2"
    assert module.down_revision == "f2a4c6e8b0d1"
    assert callable(module.upgrade)
    assert callable(module.downgrade)
    source = mod_path.read_text()
    assert "frame" in source
    assert "temporal_window" in source
    assert "arms_enabled" in source
    assert "leakage_role" in source
    assert "ck_evaluation_result_frame" in source
    assert "ck_evaluation_result_leakage_role" in source
