"""Postgres-backed regression tests for :class:`ExperimentRun.status`.

The Postgres enum type ``experiment_run_status`` is created by migration
``1e4aee32e677_add_experiment_run_table`` with the **lowercase** labels
``'planned' | 'running' | 'done' | 'abandoned'``. The ORM declaration in
``protea/infrastructure/orm/models/experiment_run.py`` used to bind
``sa.Enum(ExperimentRunStatus, name='experiment_run_status')`` without
``values_callable``, which left SQLAlchemy persisting the member
*names* (uppercase ``'PLANNED'`` etc) and produced two failure modes:

* ``InvalidTextRepresentation`` on ``session.add(...) ; commit()``.
* ``LookupError`` when reading a row inserted via raw SQL with the
  legitimate lowercase label.

FARM-EXP.1's backfill script (``scripts/backfill_experiment_run_axis.py``
on PR #388) sidestepped the bug by going through ``sqlalchemy.text``;
this test file pins the contract that the ORM-only path round-trips
cleanly, so the next FARM-EXP slices can write idiomatic SQLAlchemy
without dropping back to raw text statements.

Gated behind ``--with-postgres`` because we need the real Postgres enum
type (sqlite has no native enum). See ``tests/conftest.py::postgres_url``
for the container plumbing.
"""
from __future__ import annotations

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

import protea.infrastructure.orm.models  # noqa: F401  table registration side-effect
from protea.infrastructure.orm.base import Base
from protea.infrastructure.orm.models.experiment_run import (
    ExperimentRun,
    ExperimentRunStatus,
)


@pytest.fixture()
def pg_session(postgres_url: str):
    """Fresh schema + session bound to the live engine.

    Each test gets a clean slate via ``drop_all`` followed by
    ``create_all`` so ordering between tests does not leak rows /
    enum-value state.
    """
    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    with Session(engine, future=True) as session:
        yield session
    Base.metadata.drop_all(engine)
    engine.dispose()


@pytest.mark.integration
class TestExperimentRunStatusRoundtrip:
    """Pin the per-status round-trip identity through the ORM."""

    def test_default_planned_inserts_and_reads_back(self, pg_session: Session) -> None:
        run = ExperimentRun(name="default-planned")
        pg_session.add(run)
        pg_session.commit()

        row = pg_session.query(ExperimentRun).one()
        assert row.status is ExperimentRunStatus.PLANNED

    @pytest.mark.parametrize(
        "member",
        [
            ExperimentRunStatus.PLANNED,
            ExperimentRunStatus.RUNNING,
            ExperimentRunStatus.DONE,
            ExperimentRunStatus.ABANDONED,
        ],
    )
    def test_each_member_roundtrips(
        self, pg_session: Session, member: ExperimentRunStatus
    ) -> None:
        run = ExperimentRun(name=f"round-trip-{member.value}", status=member)
        pg_session.add(run)
        pg_session.commit()

        row = (
            pg_session.query(ExperimentRun)
            .filter(ExperimentRun.name == f"round-trip-{member.value}")
            .one()
        )
        assert row.status is member

    def test_update_status_persists_lowercase_label(self, pg_session: Session) -> None:
        """Verify the on-disk label matches the StrEnum ``.value``.

        Uses raw SQL on the read side so any future regression that
        switches the ORM back to persisting member *names* is caught
        directly at the storage boundary (not just by the read-back
        path, which would happily round-trip uppercase too if both
        ends drifted in lockstep).
        """
        run = ExperimentRun(name="status-mutation")
        pg_session.add(run)
        pg_session.commit()

        run.status = ExperimentRunStatus.DONE
        pg_session.commit()

        raw = pg_session.execute(
            text("SELECT status::text FROM experiment_run WHERE name = :n"),
            {"n": "status-mutation"},
        ).scalar_one()
        assert raw == "done"


@pytest.mark.integration
class TestExperimentRunStatusLegacyInterop:
    """Cover the read path for rows written outside the ORM.

    The FARM-EXP.1 backfill script inserts axis columns via raw SQL;
    follow-up FARM-EXP.x consumers will read those same rows back
    through the ORM. This pins the bridge.
    """

    def test_orm_reads_row_inserted_via_raw_sql(self, pg_session: Session) -> None:
        pg_session.execute(
            text(
                """
                INSERT INTO experiment_run (
                    id, name, status, config, provenance, tags, created_at
                )
                VALUES (
                    gen_random_uuid(),
                    :name,
                    CAST(:status AS experiment_run_status),
                    '{}'::jsonb,
                    '{}'::jsonb,
                    '{}'::text[],
                    now()
                )
                """
            ),
            {"name": "legacy-row", "status": "running"},
        )
        pg_session.commit()

        row = (
            pg_session.query(ExperimentRun)
            .filter(ExperimentRun.name == "legacy-row")
            .one()
        )
        assert row.status is ExperimentRunStatus.RUNNING

    def test_orm_only_workflow_replaces_text_workaround(
        self, pg_session: Session
    ) -> None:
        """End-to-end ORM workflow that the backfill script could now use.

        Reproduces the legacy raw-SQL row, then mutates + reads it back
        purely through the ORM. If this passes, the
        ``scripts/backfill_experiment_run_axis.py`` ``sqlalchemy.text``
        workaround documented in PR #388 lines 91-95 is provably
        unnecessary and can be removed in a follow-up commit on that
        branch.
        """
        pg_session.execute(
            text(
                """
                INSERT INTO experiment_run (
                    id, name, status, config, provenance, tags, created_at
                )
                VALUES (
                    gen_random_uuid(),
                    :name,
                    CAST(:status AS experiment_run_status),
                    '{}'::jsonb,
                    '{}'::jsonb,
                    '{}'::text[],
                    now()
                )
                """
            ),
            {"name": "orm-only-workflow", "status": "planned"},
        )
        pg_session.commit()

        row = (
            pg_session.query(ExperimentRun)
            .filter(ExperimentRun.name == "orm-only-workflow")
            .one()
        )
        assert row.status is ExperimentRunStatus.PLANNED

        row.status = ExperimentRunStatus.DONE
        pg_session.commit()

        raw = pg_session.execute(
            text("SELECT status::text FROM experiment_run WHERE name = :n"),
            {"n": "orm-only-workflow"},
        ).scalar_one()
        assert raw == "done"
