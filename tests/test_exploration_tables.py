"""The four tables of an evaluated exploration hold what ADR-D55 says they hold.

The constraints are the contract, so they are exercised as constraints: rows
are written and the database refuses them. The CHECKs are portable and run on
SQLite with only these four tables created; the append-only trigger and the
``RESTRICT`` behaviour need Postgres and run with ``--with-postgres``.
"""

from __future__ import annotations

import importlib.util
import re
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import CheckConstraint, create_engine, text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.orm import Session

import protea.infrastructure.orm.models  # noqa: F401  (registers every table)
from protea.core.explorations import LINK_TARGETS, TRANSITIONS, rules_fingerprint
from protea.infrastructure.orm.base import Base
from protea.infrastructure.orm.models.exploration import exploration_event as event_module
from protea.infrastructure.orm.models.exploration.exploration import Exploration
from protea.infrastructure.orm.models.exploration.exploration_event import ExplorationEvent
from protea.infrastructure.orm.models.exploration.exploration_link import ExplorationLink
from protea.infrastructure.orm.models.exploration.external_reference import ExternalReference
from tests.helpers.windows import make_window

TABLES = ("external_reference", "exploration", "exploration_link", "exploration_event")

_MIGRATION = (
    Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "b2f6c3d8e154_add_explorations.py"
)
_spec = importlib.util.spec_from_file_location("migration_add_explorations", _MIGRATION)
assert _spec and _spec.loader
migration = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(migration)

_TRUNCATE = _MIGRATION.parent / "c5e9a1d3f720_exploration_event_refuses_truncate.py"
_tspec = importlib.util.spec_from_file_location("migration_refuses_truncate", _TRUNCATE)
assert _tspec and _tspec.loader
truncate_migration = importlib.util.module_from_spec(_tspec)
_tspec.loader.exec_module(truncate_migration)


def _squash(sql: str) -> str:
    return re.sub(r"\s+", " ", sql).strip()


class TestTheMigrationCreatesWhatTheModelsDescribe:
    @pytest.mark.parametrize("table", TABLES)
    def test_every_check_agrees(self, table: str) -> None:
        model = {
            c.name: _squash(str(c.sqltext))
            for c in Base.metadata.tables[table].constraints
            if isinstance(c, CheckConstraint)
        }
        frozen = {name: _squash(sql) for name, sql in migration.CHECKS[table].items()}
        assert model == frozen

    def test_the_trigger_is_the_same_text(self) -> None:
        assert _squash(event_module.APPEND_ONLY_FUNCTION) == _squash(migration.APPEND_ONLY_FUNCTION)
        assert _squash(event_module.APPEND_ONLY_TRIGGER) == _squash(migration.APPEND_ONLY_TRIGGER)

    def test_the_truncate_trigger_is_the_same_text(self) -> None:
        assert _squash(event_module.NO_TRUNCATE_TRIGGER) == _squash(
            truncate_migration.NO_TRUNCATE_TRIGGER
        )
        assert truncate_migration.down_revision == "b2f6c3d8e154"

    def test_the_arc_has_the_same_targets(self) -> None:
        frozen = (*(column for column, _ in migration._TARGETS), "store_key")
        assert frozen == LINK_TARGETS

    def test_it_follows_the_current_head(self) -> None:
        assert migration.down_revision == "a7d3f1c9e842"


class TestNothingTheyPointAtCanBeDeletedFromUnderThem:
    @pytest.mark.parametrize("table", TABLES)
    def test_every_foreign_key_restricts(self, table: str) -> None:
        for fk in Base.metadata.tables[table].foreign_keys:
            assert fk.ondelete == "RESTRICT", f"{table}.{fk.parent.name}"

    def test_every_arc_target_is_a_column(self) -> None:
        columns = set(Base.metadata.tables["exploration_link"].columns.keys())
        assert set(LINK_TARGETS) <= columns


class TestTheRulesFingerprint:
    def test_key_order_does_not_change_it(self) -> None:
        assert rules_fingerprint({"a": 1, "b": [1, 2]}) == rules_fingerprint({"b": [1, 2], "a": 1})

    def test_any_change_does(self) -> None:
        assert rules_fingerprint({"a": 1}) != rules_fingerprint({"a": 2})

    def test_the_authors_prose_is_kept_as_written(self) -> None:
        # Escaped and unescaped spellings of one character are one rule.
        assert rules_fingerprint({"pregunta": "¿cambia?"}) == rules_fingerprint(
            {"pregunta": "¿cambia?"}
        )


class TestTheStatesOnlyMoveForward:
    def test_each_state_has_one_successor_and_decided_has_none(self) -> None:
        assert TRANSITIONS == {"draft": "running", "running": "reported", "reported": "decided"}


# ---------------------------------------------------------------------------
# The CHECKs, on SQLite with only these four tables
# ---------------------------------------------------------------------------

NOW = datetime(2026, 11, 4, tzinfo=UTC)


@pytest.fixture()
def session() -> Any:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine, tables=[Base.metadata.tables[t] for t in TABLES])
    with Session(engine) as s:
        yield s


def _exploration(**over: Any) -> Exploration:
    fields: dict[str, Any] = {
        "number": 1,
        "title": "the instrument repeats",
        "question": "q",
        "hypothesis": "h",
        "kind": "instrument",
        "evaluation_set_id": uuid.uuid4(),
        "rules": {},
        "proposer": "lab",
    }
    return Exploration(**{**fields, **over})


def _frozen(**over: Any) -> dict[str, Any]:
    return {"rules_sha256": "a" * 64, "rules_frozen_at": NOW, "rules_frozen_by": "lab", **over}


def _refused(session: Session, row: Any) -> None:
    session.add(row)
    with pytest.raises(IntegrityError):
        session.flush()
    session.rollback()


def _accepted(session: Session, row: Any) -> None:
    session.add(row)
    session.flush()


class TestTheExplorationRow:
    def test_a_draft_is_written(self, session: Session) -> None:
        _accepted(session, _exploration())

    def test_an_unknown_kind_is_refused(self, session: Session) -> None:
        _refused(session, _exploration(kind="survey"))

    def test_a_draft_has_no_frozen_rules(self, session: Session) -> None:
        _refused(session, _exploration(**_frozen()))

    def test_a_running_one_has_them(self, session: Session) -> None:
        _refused(session, _exploration(state="running", launcher="server"))
        _accepted(session, _exploration(state="running", launcher="server", **_frozen()))

    def test_the_rules_are_frozen_with_their_time_and_who(self, session: Session) -> None:
        _refused(session, _exploration(state="running", **_frozen(rules_frozen_at=None)))
        _refused(session, _exploration(state="running", **_frozen(rules_frozen_by=None)))

    def test_only_a_decided_one_carries_a_decision(self, session: Session) -> None:
        _refused(session, _exploration(state="decided", **_frozen()))
        _refused(session, _exploration(state="reported", decision="accepted", **_frozen()))
        _accepted(session, _exploration(state="decided", decision="rejected", **_frozen()))

    def test_the_launcher_is_not_the_proposer(self, session: Session) -> None:
        _refused(session, _exploration(state="running", launcher="lab", **_frozen()))

    def test_archived_is_a_reason_and_a_time(self, session: Session) -> None:
        _refused(session, _exploration(archived_reason="abandoned"))
        _refused(session, _exploration(archived_reason="forgotten", archived_at=NOW))
        _accepted(session, _exploration(archived_reason="superseded", archived_at=NOW))


def _link(exploration_id: uuid.UUID, **over: Any) -> ExplorationLink:
    return ExplorationLink(**{"exploration_id": exploration_id, "role": "arm", **over})


class TestTheLinkIsAnExclusiveArc:
    def test_one_target_is_written(self, session: Session) -> None:
        _accepted(session, _link(uuid.uuid4(), prediction_set_id=uuid.uuid4()))

    def test_no_target_is_refused(self, session: Session) -> None:
        _refused(session, _link(uuid.uuid4()))

    def test_two_targets_are_refused(self, session: Session) -> None:
        _refused(
            session,
            _link(uuid.uuid4(), prediction_set_id=uuid.uuid4(), evaluation_result_id=uuid.uuid4()),
        )

    def test_a_store_key_travels_with_its_sha(self, session: Session) -> None:
        _refused(session, _link(uuid.uuid4(), store_key="eval_artifacts/x"))
        _accepted(session, _link(uuid.uuid4(), store_key="eval_artifacts/x", store_sha256="b" * 64))

    def test_a_temporary_target_has_an_expiry(self, session: Session) -> None:
        _refused(session, _link(uuid.uuid4(), role="temporary", job_id=uuid.uuid4()))
        _accepted(
            session, _link(uuid.uuid4(), role="temporary", job_id=uuid.uuid4(), expires_at=NOW)
        )

    def test_an_exploration_does_not_link_to_itself(self, session: Session) -> None:
        own = uuid.uuid4()
        _refused(session, _link(own, role="supports", target_exploration_id=own))

    def test_an_unknown_role_is_refused(self, session: Session) -> None:
        _refused(session, _link(uuid.uuid4(), role="winner", job_id=uuid.uuid4()))


class TestTheEventAndTheReference:
    def test_an_unknown_event_or_actor_is_refused(self, session: Session) -> None:
        common = {"exploration_id": uuid.uuid4(), "actor": "lab", "body": {}}
        _refused(session, ExplorationEvent(id=1, actor_type="user", event="crowned", **common))
        _refused(session, ExplorationEvent(id=2, actor_type="robot", event="created", **common))
        _accepted(session, ExplorationEvent(id=3, actor_type="api_key", event="created", **common))

    def test_a_reference_keeps_its_copy_with_its_sha(self, session: Session) -> None:
        common = {"title": "LAFA", "kind": "challenge", "captured_by": "lab", "profile": {}}
        _refused(session, ExternalReference(store_key="refs/lafa.html", **common))
        _refused(session, ExternalReference(**{**common, "kind": "rumour"}))
        _accepted(
            session, ExternalReference(store_key="refs/lafa.html", store_sha256="c" * 64, **common)
        )


# ---------------------------------------------------------------------------
# The trigger and RESTRICT, on Postgres
# ---------------------------------------------------------------------------


def _seed_window(session: Session) -> uuid.UUID:
    return make_window(session, snapshot_version="exploration-test")


@pytest.fixture()
def pg(postgres_url: str) -> Any:
    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    with Session(engine) as s:
        yield s


class TestOnPostgres:
    def test_the_log_refuses_an_update_a_delete_and_a_truncate(self, pg: Session) -> None:
        row = _exploration(evaluation_set_id=_seed_window(pg))
        pg.add(row)
        pg.flush()
        pg.add(
            ExplorationEvent(
                exploration_id=row.id, actor="lab", actor_type="user", event="created", body={}
            )
        )
        pg.commit()
        for statement in (
            "UPDATE exploration_event SET actor = 'someone else'",
            "DELETE FROM exploration_event",
            "TRUNCATE exploration_event",
        ):
            with pytest.raises(DBAPIError, match="append only"):
                pg.execute(text(statement))
            pg.rollback()

    def test_the_function_carries_its_message_as_written(self, pg: Session) -> None:
        # The failure this pins was invisible on SQLite and silent in the
        # migration: sqlalchemy.DDL substituted the "% r" of "% refused" and
        # the function never compiled under create_all.
        body = pg.execute(
            text("SELECT prosrc FROM pg_proc WHERE proname = 'exploration_event_is_append_only'")
        ).scalar_one()
        assert "% refused" in body

    def test_a_linked_window_cannot_be_deleted(self, pg: Session) -> None:
        window = _seed_window(pg)
        pg.add(_exploration(evaluation_set_id=window))
        pg.commit()
        with pytest.raises(IntegrityError):
            pg.execute(text("DELETE FROM evaluation_set WHERE id = :w"), {"w": window})
        pg.rollback()
