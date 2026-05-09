"""Sanity tests for the :class:`ExperimentRun` ORM (T3.8).

We don't exercise the FastAPI surface here — endpoints land in the
follow-up T4.7-T4.9 narrative slices. This file just pins the ORM
shape (column types, defaults, status enum) so a future Alembic edit
can't silently drift the schema away from what F-EXP campaigns expect.
"""
from __future__ import annotations

from sqlalchemy import Column

from protea.infrastructure.orm.models.experiment_run import (
    ExperimentRun,
    ExperimentRunStatus,
)


class TestExperimentRunStatusEnum:
    def test_expected_values(self) -> None:
        assert {s.value for s in ExperimentRunStatus} == {
            "planned",
            "running",
            "done",
            "abandoned",
        }

    def test_string_enum_equality(self) -> None:
        # StrEnum lets cafa-style string comparisons stay ergonomic.
        assert ExperimentRunStatus.PLANNED == "planned"
        assert ExperimentRunStatus.DONE == "done"


class TestExperimentRunMapping:
    def test_table_name(self) -> None:
        assert ExperimentRun.__tablename__ == "experiment_run"

    def test_required_columns_present(self) -> None:
        cols = {c.name for c in ExperimentRun.__table__.columns}
        # Narrative trio mirrored from D11.
        assert {"name", "description", "hypothesis", "findings"} <= cols
        # Lifecycle + provenance.
        assert {"status", "config", "provenance", "tags"} <= cols
        # Time anchors.
        assert {"created_at", "started_at", "finished_at"} <= cols

    def test_name_has_unique_index(self) -> None:
        # The migration creates a UNIQUE index on name; the ORM must
        # reflect it so SQLAlchemy serialisers honour the constraint
        # when emitting CREATE TABLE in tests / dev sandboxes.
        for ix in ExperimentRun.__table__.indexes:
            if ix.name == "ix_experiment_run_name":
                assert ix.unique is True
                cols = [c.name for c in ix.columns]
                assert cols == ["name"]
                return
        raise AssertionError("ix_experiment_run_name index missing on the ORM table")

    def test_status_default_is_planned(self) -> None:
        col: Column[ExperimentRunStatus] = ExperimentRun.__table__.c.status
        assert col.default is not None
        # The ORM stores the python-side default; SQLAlchemy wraps it as
        # ``ScalarElementColumnDefault`` so ``arg`` is the raw value.
        assert col.default.arg == ExperimentRunStatus.PLANNED  # type: ignore[union-attr]

    def test_jsonb_and_array_columns_have_defaults(self) -> None:
        # ``config`` / ``provenance`` default to an empty dict; ``tags``
        # defaults to an empty list. We only assert that a default is
        # present here — exact callable identity is internal SQLAlchemy
        # plumbing.
        for col_name in ("config", "provenance", "tags"):
            col = ExperimentRun.__table__.c[col_name]
            assert col.default is not None


class TestExperimentRunInit:
    def test_constructs_with_only_name(self) -> None:
        # Minimum viable row: the migration handles every other column
        # via server_default. The ORM ``default`` callables fire on
        # flush, so unflushed instances may legitimately leave tags /
        # config / provenance as None — covered by the migration tests.
        run = ExperimentRun(name="ablation-2026-05-09")
        assert run.name == "ablation-2026-05-09"
