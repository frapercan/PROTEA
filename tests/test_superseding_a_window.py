"""Superseding a window: a pointer to its successor, a date and a note; nothing deleted.

The windows a frame leaves behind keep their rows and their results, which are
records of what was measured; ``supersede_evaluation_sets`` marks them. These pin
what the operation refuses (and that it lists every refusal), what it writes, that
``dry_run`` writes nothing, the check that keeps the three fields together, and the
migration, statically and against Postgres.
"""

from __future__ import annotations

import importlib.util
import pathlib
import uuid
from datetime import datetime
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from pydantic import ValidationError
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session

from protea.core.operations.supersede_evaluation_sets import (
    SupersedeEvaluationSetsOperation,
    SupersedeEvaluationSetsPayload,
    refusals,
)
from protea.core.window_declarations import OCTOBER_HEADLINE_FRAME
from protea.infrastructure.orm.base import Base
from protea.infrastructure.orm.models.annotation import evaluation_set as model
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot

_MIGRATION = (
    pathlib.Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "9e4f2a7c5b13_a_window_points_to_the_window_that_superseded_it.py"
)
NOTE = "2026-11-20: the benchmark takes CAFA's binding rule."
T0, T1 = uuid.uuid4(), uuid.uuid4()


def _window(corpora: tuple[uuid.UUID, uuid.UUID] = (T0, T1), superseded_by=None):
    return SimpleNamespace(
        old_annotation_set_id=corpora[0],
        new_annotation_set_id=corpora[1],
        superseded_by_id=superseded_by,
        superseded_at=None,
        supersession_note=None,
    )


def _payload(successor: uuid.UUID, *windows: uuid.UUID, **extra) -> dict:
    return {
        "successor_id": str(successor),
        "evaluation_set_ids": [str(w) for w in windows],
        "note": NOTE,
        **extra,
    }


def _session(rows: dict[uuid.UUID, object]) -> MagicMock:
    session = MagicMock()
    session.get.side_effect = lambda _model, key: rows.get(key)
    return session


# ------------------------------------------------------------------ the payload


class TestThePayload:
    def test_an_empty_list_is_refused(self) -> None:
        with pytest.raises(ValidationError, match="cannot be empty"):
            SupersedeEvaluationSetsPayload(**_payload(uuid.uuid4()))

    def test_a_repeated_window_is_refused(self) -> None:
        window = uuid.uuid4()
        with pytest.raises(ValidationError, match="repeats"):
            SupersedeEvaluationSetsPayload(**_payload(uuid.uuid4(), window, window))

    @pytest.mark.parametrize("note", ["", "   "])
    def test_a_mark_without_a_reason_is_refused(self, note: str) -> None:
        with pytest.raises(ValidationError, match="says why"):
            SupersedeEvaluationSetsPayload(**{**_payload(uuid.uuid4(), uuid.uuid4()), "note": note})

    def test_an_id_that_is_not_one_is_refused(self) -> None:
        with pytest.raises(ValidationError):
            SupersedeEvaluationSetsPayload(
                **{**_payload(uuid.uuid4(), uuid.uuid4()), "successor_id": "4244033e"}
            )


# ------------------------------------------------------------------ the refusals


class TestTheRefusals:
    def test_a_successor_over_the_same_corpora_is_accepted(self) -> None:
        successor_id, window_id = uuid.uuid4(), uuid.uuid4()
        assert refusals(successor_id, _window(), {window_id: _window()}) == []

    def test_every_refusal_is_listed_not_only_the_first(self) -> None:
        successor_id = uuid.uuid4()
        missing, other_span, marked = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
        found = refusals(
            successor_id,
            _window(superseded_by=uuid.uuid4()),
            {
                missing: None,
                other_span: _window(corpora=(uuid.uuid4(), T1)),
                marked: _window(superseded_by=uuid.uuid4()),
                successor_id: _window(),
            },
        )
        text_found = "\n".join(found)
        assert "is itself superseded" in text_found
        assert f"window {missing} does not exist" in text_found
        assert f"window {other_span} spans other corpora" in text_found
        assert f"window {marked} is already superseded" in text_found
        assert f"window {successor_id} is the successor itself" in text_found

    def test_a_missing_successor_is_refused(self) -> None:
        successor_id = uuid.uuid4()
        assert refusals(successor_id, None, {uuid.uuid4(): _window()}) == [
            f"successor {successor_id} does not exist"
        ]


# ------------------------------------------------------------------ the operation


class TestTheOperation:
    def test_it_marks_each_window_with_the_pointer_the_date_and_the_note(self) -> None:
        successor_id, first, second = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
        rows = {successor_id: _window(), first: _window(), second: _window()}
        emit = MagicMock()

        result = SupersedeEvaluationSetsOperation().execute(
            _session(rows), _payload(successor_id, first, second), emit=emit
        )

        for window_id in (first, second):
            assert rows[window_id].superseded_by_id == successor_id
            assert isinstance(rows[window_id].superseded_at, datetime)
            assert rows[window_id].superseded_at.tzinfo is not None
            assert rows[window_id].supersession_note == NOTE
        assert rows[successor_id].superseded_by_id is None
        assert result.result["superseded"] == [str(first), str(second)]
        assert [call.args[0] for call in emit.call_args_list].count(
            "supersede_evaluation_sets.mark"
        ) == 2

    def test_dry_run_writes_nothing(self) -> None:
        successor_id, window_id = uuid.uuid4(), uuid.uuid4()
        rows = {successor_id: _window(), window_id: _window()}
        session = _session(rows)

        result = SupersedeEvaluationSetsOperation().execute(
            session, _payload(successor_id, window_id, dry_run=True), emit=MagicMock()
        )

        assert rows[window_id].superseded_by_id is None
        assert result.result["dry_run"] is True
        session.flush.assert_not_called()

    def test_a_refusal_writes_nothing_and_says_every_reason(self) -> None:
        successor_id, good, other_span = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
        rows = {
            successor_id: _window(),
            good: _window(),
            other_span: _window(corpora=(uuid.uuid4(), uuid.uuid4())),
        }
        emit = MagicMock()

        with pytest.raises(ValueError, match="spans other corpora"):
            SupersedeEvaluationSetsOperation().execute(
                _session(rows), _payload(successor_id, good, other_span), emit=emit
            )

        assert rows[good].superseded_by_id is None
        assert emit.call_args.args[0] == "supersede_evaluation_sets.refused"


# ------------------------------------------------------------------ the schema


def _migration() -> ModuleType:
    """Loaded by path: ``alembic/`` is not a package and the library shadows the name."""
    spec = importlib.util.spec_from_file_location("_9e4f2a7c5b13", _MIGRATION)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestTheSchema:
    def test_the_migration_carries_the_models_check(self) -> None:
        assert _migration().SUPERSESSION_CHECK == model.SUPERSESSION_CHECK

    def test_the_migration_follows_the_binding_rule(self) -> None:
        assert _migration().down_revision == "7c2e9b4d1a6f"

    def test_the_three_fields_are_nullable_and_the_pointer_restricts_deletes(self) -> None:
        columns = EvaluationSet.__table__.c
        assert all(
            columns[name].nullable
            for name in ("superseded_by_id", "superseded_at", "supersession_note")
        )
        (key,) = columns.superseded_by_id.foreign_keys
        assert key.column.table.name == "evaluation_set"
        assert key.ondelete == "RESTRICT"


# --------------------------------------------------------------- against Postgres


@pytest.fixture()
def pg_engine(postgres_url: str):
    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    yield engine
    Base.metadata.drop_all(engine)
    engine.dispose()


def _graphs(engine) -> tuple[uuid.UUID, uuid.UUID]:
    with Session(engine, future=True) as session:
        snapshot = OntologySnapshot(
            obo_version=f"releases/{uuid.uuid4().hex[:8]}", obo_url="file:///dev/null"
        )
        session.add(snapshot)
        session.flush()
        corpus = AnnotationSet(source="goa", ontology_snapshot_id=snapshot.id)
        session.add(corpus)
        session.commit()
        return snapshot.id, corpus.id


def _insert(engine, corpus: uuid.UUID, snapshot: uuid.UUID, profile: str) -> uuid.UUID:
    row = EvaluationSet(
        old_annotation_set_id=corpus,
        new_annotation_set_id=corpus,
        pivot_snapshot_id=snapshot,
        old_native_snapshot_id=snapshot,
        new_native_snapshot_id=snapshot,
        truth_profile=profile,
        stats={},
        truth_rules=OCTOBER_HEADLINE_FRAME.as_json(),
    )
    with Session(engine, future=True) as session:
        session.add(row)
        session.commit()
        return row.id


def _mark(engine, window: uuid.UUID, columns: str, values: dict) -> None:
    with engine.begin() as connection:
        connection.execute(
            text(f"UPDATE evaluation_set SET {columns} WHERE id = :window"),
            {"window": window, **values},
        )


def test_a_complete_mark_is_stored(pg_engine) -> None:
    snapshot, corpus = _graphs(pg_engine)
    old = _insert(pg_engine, corpus, snapshot, "as_closed")
    new = _insert(pg_engine, corpus, snapshot, "projected")
    _mark(
        pg_engine,
        old,
        "superseded_by_id = :new, superseded_at = now(), supersession_note = :note",
        {"new": new, "note": NOTE},
    )
    with pg_engine.connect() as connection:
        stored = connection.execute(
            text("SELECT superseded_by_id, supersession_note FROM evaluation_set WHERE id = :w"),
            {"w": old},
        ).one()
    assert tuple(stored) == (new, NOTE)


def test_a_partial_mark_or_a_self_pointer_is_refused(pg_engine) -> None:
    snapshot, corpus = _graphs(pg_engine)
    old = _insert(pg_engine, corpus, snapshot, "as_closed")
    new = _insert(pg_engine, corpus, snapshot, "projected")
    with pytest.raises(DBAPIError, match="ck_evaluation_set_supersession"):
        _mark(pg_engine, old, "superseded_by_id = :new", {"new": new})
    with pytest.raises(DBAPIError, match="ck_evaluation_set_supersession"):
        _mark(
            pg_engine,
            old,
            "superseded_by_id = :old, superseded_at = now(), supersession_note = :note",
            {"old": old, "note": NOTE},
        )


def test_a_successor_cannot_be_deleted_while_a_window_points_at_it(pg_engine) -> None:
    snapshot, corpus = _graphs(pg_engine)
    old = _insert(pg_engine, corpus, snapshot, "as_closed")
    new = _insert(pg_engine, corpus, snapshot, "projected")
    _mark(
        pg_engine,
        old,
        "superseded_by_id = :new, superseded_at = now(), supersession_note = :note",
        {"new": new, "note": NOTE},
    )
    with pytest.raises(DBAPIError, match="fk_evaluation_set_superseded_by_id"):
        with pg_engine.begin() as connection:
            connection.execute(text("DELETE FROM evaluation_set WHERE id = :new"), {"new": new})


def test_the_downgrade_refuses_while_a_window_is_marked(pg_engine) -> None:
    snapshot, corpus = _graphs(pg_engine)
    old = _insert(pg_engine, corpus, snapshot, "as_closed")
    new = _insert(pg_engine, corpus, snapshot, "projected")
    _mark(
        pg_engine,
        old,
        "superseded_by_id = :new, superseded_at = now(), supersession_note = :note",
        {"new": new, "note": NOTE},
    )
    with pytest.raises(RuntimeError, match="marked superseded"):
        with pg_engine.begin() as connection:
            with Operations.context(MigrationContext.configure(connection)):
                _migration().downgrade()
    assert "superseded_by_id" in {
        c["name"] for c in inspect(pg_engine).get_columns("evaluation_set")
    }
