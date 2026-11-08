"""A window says which code built it, and the backfill only copies what exists.

THE HOLE THIS CLOSES. ``truth_profile``, ``toi_rule`` and ``not_codes`` say
what the delta is by DECLARATION. Nothing said it by IMPLEMENTATION, and the
two are different quantities: on 2026-11-07 a fix to the synonym map's
anchoring changed the delta of every window whose corpus era is not its
closing graph, with all eight identity fields untouched. The only way to ask
which code built a window was to match its ``created_at`` against the dated
blocks of ``agent-farm/plans/DECLARED-REVISION.txt``.

AND WHY THE BACKFILL IS TESTED AT ALL. A data step in a migration that
silently updates zero rows is the same defect class this whole change is
about: a quantity that is computed and not recorded. So the UPDATE is
asserted on both sides -- the row it must fill and the row it must leave
alone -- against the one statement the migration itself runs.
"""

from __future__ import annotations

import importlib.util
import inspect
import pathlib
import uuid

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

import protea.infrastructure.orm.models  # noqa: F401 — register tables
from protea.infrastructure.orm.base import Base
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
from protea.infrastructure.orm.models.job import Job, JobEvent


@pytest.fixture()
def pg_session(postgres_url: str):
    """Fresh schema + a session bound to the live engine."""
    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    with Session(engine, future=True) as session:
        yield session
    Base.metadata.drop_all(engine)


SHA = "637b3517e525a982694fa5ca4b4b6ed6d5b1ea81"

#: Loaded BY PATH, not imported: ``alembic/`` is not a package here and the
#: installed ``alembic`` library shadows the name. Loading it anyway is the
#: point -- the test and the migration must share ONE copy of the statement,
#: or the test passes while the migration runs something else.
_MIGRACION = (
    pathlib.Path(__file__).resolve().parents[1]
    / "alembic/versions/a7d3e9b1c604_evaluation_set_stamps_its_code_revision.py"
)


def _backfill() -> str:
    spec = importlib.util.spec_from_file_location("_a7d3e9b1c604", _MIGRACION)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.BACKFILL


def _window(session: Session, job: Job | None) -> uuid.UUID:
    """One ``evaluation_set`` row with the five ids it needs, and nothing else.

    Through the ORM rather than raw SQL. The first draft used raw INSERTs
    "because this test is about one column", and that reasoning was wrong: a
    raw insert has to satisfy every NOT NULL without a SERVER default, so the
    test became about seven columns in five tables, in three failing runs.
    The models already know their own defaults.
    """
    snapshot = OntologySnapshot(
        obo_version=f"releases/{uuid.uuid4().hex[:8]}",
        obo_url="file:///dev/null",
    )
    session.add(snapshot)
    session.flush()
    aset = AnnotationSet(source="goa", ontology_snapshot_id=snapshot.id)
    session.add(aset)
    session.flush()
    row = EvaluationSet(
        old_annotation_set_id=aset.id,
        new_annotation_set_id=aset.id,
        pivot_snapshot_id=snapshot.id,
        old_native_snapshot_id=snapshot.id,
        new_native_snapshot_id=snapshot.id,
        truth_profile="projected",
        toi_rule="live_in_both_ends",
        not_codes="experimental",
        stats={},
        job_id=None if job is None else job.id,
    )
    session.add(row)
    session.flush()
    return row.id


def _job_with_revision(session: Session, revision: str | None) -> Job:
    """A job, and a ``job.started`` event carrying a revision or not."""
    job = Job(operation="generate_evaluation_set", queue_name="protea.jobs", status="SUCCEEDED")
    session.add(job)
    session.flush()
    session.add(
        JobEvent(
            job_id=job.id,
            event="job.started",
            fields={} if revision is None else {"code_revision": revision},
        )
    )
    session.flush()
    return job


class TestTheBackfillCopiesOnlyWhatIsWrittenDown:
    def test_it_fills_the_row_whose_job_stamped_a_revision(self, pg_session) -> None:
        BACKFILL = _backfill()

        sellada = _window(pg_session, _job_with_revision(pg_session, SHA))
        pg_session.commit()
        pg_session.execute(text(BACKFILL))
        pg_session.expire_all()

        assert pg_session.get(EvaluationSet, sellada).code_revision == SHA

    def test_it_leaves_null_where_nothing_says_which_code_ran(self, pg_session) -> None:
        # Three shapes that must all stay NULL: a job whose started event has
        # no revision field, a window with no job at all, and a window whose
        # job has no started event. NULL means "ask the dated declaration",
        # and inferring a sha from a date would be a guess with the shape of
        # a measurement.
        BACKFILL = _backfill()

        sin_campo = _window(pg_session, _job_with_revision(pg_session, None))
        sin_job = _window(pg_session, None)
        pg_session.commit()
        pg_session.execute(text(BACKFILL))
        pg_session.expire_all()

        for row in (sin_campo, sin_job):
            assert pg_session.get(EvaluationSet, row).code_revision is None

    def test_it_does_not_overwrite_a_revision_already_there(self, pg_session) -> None:
        # The UPDATE is guarded on IS NULL, so re-running the migration, or
        # running it after the operation has started stamping rows itself,
        # cannot replace a value the worker wrote with one derived from an
        # event. A data step that is not idempotent is one nobody can re-run.
        BACKFILL = _backfill()

        row = _window(pg_session, _job_with_revision(pg_session, SHA))
        pg_session.execute(
            text("UPDATE evaluation_set SET code_revision = 'declarado+dirty' WHERE id = :i"),
            {"i": row},
        )
        pg_session.commit()
        pg_session.execute(text(BACKFILL))
        pg_session.expire_all()

        assert pg_session.get(EvaluationSet, row).code_revision == "declarado+dirty"


class TestTheOperationStampsItself:
    def test_it_records_the_revision_loaded_at_import(self) -> None:
        # ``code_revision()`` and never ``tree_revision_now()``: the fleet runs
        # out of a working tree that is edited between runs, so the tree can
        # have moved since this process loaded the code that is running.
        from protea.core.operations import generate_evaluation_set as op

        source = inspect.getsource(op.GenerateEvaluationSetOperation)
        assert "code_revision=code_revision()" in source
        assert "tree_revision_now" not in source

    def test_the_column_is_not_in_the_identity_and_that_is_written_down(self) -> None:
        # It belongs there by this project's own rule, since it decides the
        # delta. It is kept out so a deploy cannot mint a duplicate of every
        # window, and the consequence -- a window built by superseded code
        # cannot be rebuilt in place -- has to be findable from the model.
        constraint = next(
            c for c in EvaluationSet.__table__.constraints
            if getattr(c, "name", "") == "uq_evaluation_set_identity"
        )
        assert "code_revision" not in {c.name for c in constraint.columns}
        doc = EvaluationSet.__table__.c.code_revision.doc or ""
        assert "NOT IN THE IDENTITY" in (doc or inspect.getsource(EvaluationSet))
