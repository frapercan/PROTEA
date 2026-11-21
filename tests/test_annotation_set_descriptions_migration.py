"""The migration that gives the annotation sets loaded before #1213 their words.

Without a database: it follows the head it was written on and its statement
touches only what it is for. Against Postgres (``--with-postgres``, CI): a set
the cut-photo loader made with no description gets exactly the words the
loader writes now, and every other row keeps what it has.
"""

from __future__ import annotations

import importlib.util
import pathlib
import uuid
from datetime import UTC, date, datetime, time
from types import ModuleType

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from protea.core.operations.load_cut_photo import photo_set_description
from protea.infrastructure.orm.base import Base
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot

_VERSIONS = pathlib.Path(__file__).resolve().parents[1] / "alembic" / "versions"
_MIGRATION = _VERSIONS / "e0399c98f515_annotation_sets_loaded_before_1213_say_what_they_are.py"


def _module() -> ModuleType:
    """Loaded by path: ``alembic/`` is not a package and the library shadows the name."""
    spec = importlib.util.spec_from_file_location("_e0399c98f515", _MIGRATION)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_it_follows_the_head_it_was_written_on() -> None:
    module = _module()
    assert module.revision == "e0399c98f515"
    assert module.down_revision == "2ee1c12a5e0c"
    assert list(_VERSIONS.glob("2ee1c12a5e0c_*.py"))


def test_its_statement_touches_only_the_loaders_sets_without_words() -> None:
    statement = " ".join(_module().DESCRIBE.split())
    assert statement.startswith("UPDATE annotation_set AS a SET description =")
    assert "(a.description IS NULL OR btrim(a.description) = '')" in statement
    assert "a.meta->>'created_by' = 'load_cut_photo'" in statement


@pytest.fixture()
def pg_engine(postgres_url: str):
    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    yield engine
    Base.metadata.drop_all(engine)
    engine.dispose()


def _set(session: Session, snapshot: uuid.UUID, **columns) -> uuid.UUID:
    row = AnnotationSet(
        source="goa",
        source_version=columns.pop("cut", "211"),
        ontology_snapshot_id=snapshot,
        source_published_at=datetime.combine(date(2022, 9, 16), time(0), tzinfo=UTC),
        meta={"created_by": columns.pop("created_by", "load_cut_photo")},
        **columns,
    )
    session.add(row)
    session.flush()
    return row.id


def test_a_set_the_photo_loaded_gets_the_loaders_words_and_nothing_else_moves(pg_engine) -> None:
    with Session(pg_engine, future=True) as session:
        snapshot = OntologySnapshot(obo_version="releases/2022-07-01", obo_url="file:///dev/null")
        session.add(snapshot)
        session.flush()
        silent = _set(session, snapshot.id)
        blank = _set(session, snapshot.id, cut="227", description="  ")
        described = _set(session, snapshot.id, description="already says what it is")
        other = _set(session, snapshot.id, created_by="load_goa_annotations")
        session.commit()

    with pg_engine.begin() as connection:
        with Operations.context(MigrationContext.configure(connection)):
            _module().upgrade()

    with Session(pg_engine, future=True) as session:
        words = dict(session.execute(select(AnnotationSet.id, AnnotationSet.description)).all())
    assert words[silent] == photo_set_description(211, date(2022, 9, 16), "releases/2022-07-01")
    assert words[blank] == photo_set_description(227, date(2022, 9, 16), "releases/2022-07-01")
    assert words[described] == "already says what it is"
    assert words[other] is None
