"""The method tables hold what ADR-D60's decision 3 says they hold, and the migration builds them.

Three layers, cheapest first:

* without a database, the migration is run against a recorder and what it would
  create is compared with the models, column by column, constraint by constraint
  and index by index, so the two cannot drift apart unnoticed;
* on SQLite, with only the three method tables, the derivation is exercised
  through the ORM: a row's id is its spec's, a tampered row is refused, and the
  CHECKs and the one-holdout index refuse what they exist to refuse;
* on Postgres (``--with-postgres``), the revision is upgraded and downgraded on
  a database of its own, with a method's prediction set in the way of the
  downgrade.
"""

from __future__ import annotations

import importlib.util
import json
import re
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import sqlalchemy as sa
from sqlalchemy import CheckConstraint, UniqueConstraint, create_engine, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

import protea.infrastructure.orm.models  # noqa: F401  (registers every table)
from protea.core.contracts.method import (
    Family,
    InstanceCut,
    MethodSpec,
    Pin,
    TrainingWindow,
)
from protea.infrastructure.orm.base import Base
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.method.method import FAMILY_VALUES, Method, MethodPin

_REPO = Path(__file__).resolve().parents[1]
_MIGRATION = _REPO / "alembic/versions/89802d1624f8_a_method_has_an_identity_of_its_own.py"
_spec_module = importlib.util.spec_from_file_location("migration_method_tables", _MIGRATION)
assert _spec_module and _spec_module.loader
migration = importlib.util.module_from_spec(_spec_module)
_spec_module.loader.exec_module(migration)

NEW_TABLES = ("method", "method_training_window", "method_pin", "prediction_explanation")
_SNAPSHOT = uuid.UUID("6b78af68-eb01-477e-a599-fcde11ff0135")
#: Window ids with hex letters in them: SQLite gives a column typed UUID numeric
#: affinity, so an all-digit hex string would come back as an integer.
_W177_205 = uuid.UUID("a177a205-0000-4000-8000-0000000000aa")
_W194_211 = uuid.UUID("a194a211-0000-4000-8000-0000000000bb")
_W_OTHER = (
    uuid.UUID("c0ffee01-0000-4000-8000-0000000000cc"),
    uuid.UUID("c0ffee02-0000-4000-8000-0000000000dd"),
    uuid.UUID("c0ffee03-0000-4000-8000-0000000000ee"),
)


def _squash(sql: str) -> str:
    return re.sub(r"\s+", " ", sql).strip()


def _spec(code: str = "booster-211", **over: Any) -> MethodSpec:
    fields: dict[str, Any] = {
        "family": Family.EMBEDDING_RERANKER,
        "code": code,
        "instance": InstanceCut(211, _SNAPSHOT),
        "recipe": {"k": 5, "rounds": 412, "features": ["identity_nw", "vote_count"]},
        "pins": (
            Pin("plm", "a" * 64, ("s3://protea/weights/ankh-base.tar",)),
            Pin("reranker", "b" * 64, (f"evaluation_set:{_W194_211}",)),
        ),
        "runtime": "native",
        "explains": False,
    }
    fields.update(over)
    return MethodSpec(**fields)


# ---------------------------------------------------------------- the migration, recorded


class _Bind:
    """The connection the downgrade asks how many config-less sets exist."""

    def __init__(self, without_config: int) -> None:
        self._n = without_config

    def execute(self, *_args: Any, **_kwargs: Any) -> Any:
        n = self._n

        class _Result:
            def scalar_one(self) -> int:
                return n

        return _Result()


class _Recorder:
    """Stands in for ``alembic.op`` and keeps what each call would have built."""

    def __init__(self, without_config: int = 0) -> None:
        self.tables: dict[str, sa.Table] = {}
        self.indexes: dict[str, tuple[str, tuple[str, ...], bool, str | None]] = {}
        self.added: dict[tuple[str, str], sa.Column] = {}
        self.foreign_keys: dict[str, tuple[Any, ...]] = {}
        self.altered: dict[tuple[str, str], bool | None] = {}
        self.checks: dict[str, tuple[str, str]] = {}
        self.dropped: list[tuple[str, str]] = []
        self._bind = _Bind(without_config)

    def create_table(self, name: str, *elements: Any, **_kw: Any) -> None:
        self.tables[name] = sa.Table(name, sa.MetaData(), *elements)

    def create_index(
        self, name: str, table: str, columns: list[str], unique: bool = False, **kw: Any
    ) -> None:
        where = kw.get("postgresql_where")
        self.indexes[name] = (table, tuple(columns), unique, None if where is None else str(where))

    def add_column(self, table: str, column: sa.Column) -> None:
        self.added[(table, column.name)] = column

    def create_foreign_key(
        self, name: str, source: str, referent: str, *cols: Any, **kw: Any
    ) -> None:
        local, remote = cols
        self.foreign_keys[name] = (
            source,
            referent,
            tuple(local),
            tuple(remote),
            kw.get("ondelete"),
        )

    def alter_column(
        self, table: str, column: str, nullable: bool | None = None, **_kw: Any
    ) -> None:
        self.altered[(table, column)] = nullable

    def create_check_constraint(self, name: str, table: str, condition: str, **_kw: Any) -> None:
        self.checks[name] = (table, str(condition))

    def drop_table(self, name: str, **_kw: Any) -> None:
        self.dropped.append(("table", name))

    def drop_index(self, name: str, **_kw: Any) -> None:
        self.dropped.append(("index", name))

    def drop_column(self, table: str, column: str, **_kw: Any) -> None:
        self.dropped.append(("column", f"{table}.{column}"))

    def drop_constraint(self, name: str, _table: str, type_: str | None = None, **_kw: Any) -> None:
        self.dropped.append((type_ or "constraint", name))

    def get_bind(self) -> _Bind:
        return self._bind


@pytest.fixture()
def recorded(monkeypatch: pytest.MonkeyPatch) -> _Recorder:
    recorder = _Recorder()
    monkeypatch.setattr(migration, "op", recorder)
    migration.upgrade()
    return recorder


def _shape(table: sa.Table) -> dict[str, Any]:
    """Everything about a table the comparison holds the two sides to."""
    return {
        "columns": {
            c.name: (type(c.type).__name__, getattr(c.type, "length", None), c.nullable)
            for c in table.columns
        },
        "primary_key": [c.name for c in table.primary_key.columns],
        "foreign_keys": sorted(
            (fk.parent.name, fk.target_fullname, fk.ondelete) for fk in table.foreign_keys
        ),
        "unique": sorted(c.name for c in table.constraints if isinstance(c, UniqueConstraint)),
        "checks": {
            c.name: _squash(str(c.sqltext))
            for c in table.constraints
            if isinstance(c, CheckConstraint)
        },
    }


def _orm_indexes(table: str) -> dict[str, tuple[str, tuple[str, ...], bool, str | None]]:
    out = {}
    for index in Base.metadata.tables[table].indexes:
        where = index.dialect_options["postgresql"]["where"]
        out[index.name] = (
            table,
            tuple(c.name for c in index.columns),
            bool(index.unique),
            None if where is None else str(where),
        )
    return out


class TestTheMigrationBuildsWhatTheModelsDescribe:
    @pytest.mark.parametrize("table", NEW_TABLES)
    def test_every_new_table_has_the_models_shape(self, recorded: _Recorder, table: str) -> None:
        assert _shape(recorded.tables[table]) == _shape(Base.metadata.tables[table])

    @pytest.mark.parametrize("table", NEW_TABLES)
    def test_every_new_table_has_the_models_indexes(self, recorded: _Recorder, table: str) -> None:
        made = {name: idx for name, idx in recorded.indexes.items() if idx[0] == table}
        assert made == _orm_indexes(table)

    def test_the_family_column_closes_over_the_contracts_families(self) -> None:
        # The CHECK on method.family is built from this tuple, so a family added
        # to the enum and not to the table would be refused by the database.
        assert FAMILY_VALUES == tuple(f.value for f in Family)
        assert migration.CHECKS["method"]["ck_method_family"] == (
            "family IN ('embedding-reranker', 'world-model', 'explainable-units')"
        )

    def test_the_frozen_checks_are_the_models_checks(self) -> None:
        for table, frozen in migration.CHECKS.items():
            model = _shape(Base.metadata.tables[table])["checks"]
            for name, sql in frozen.items():
                assert model[name] == _squash(sql), f"{table}.{name}"

    def test_a_prediction_set_gains_a_method_and_may_lose_its_configuration(
        self, recorded: _Recorder
    ) -> None:
        orm = Base.metadata.tables["prediction_set"]
        assert recorded.added[("prediction_set", "method_id")].nullable is True
        assert orm.c.method_id.nullable is True
        assert recorded.foreign_keys["fk_prediction_set_method_id"] == (
            "prediction_set",
            "method",
            ("method_id",),
            ("id",),
            "RESTRICT",
        )
        (fk,) = orm.c.method_id.foreign_keys
        assert (fk.constraint.name, fk.target_fullname, fk.ondelete) == (
            "fk_prediction_set_method_id",
            "method.id",
            "RESTRICT",
        )
        assert (
            recorded.indexes["ix_prediction_set_method_id"]
            == _orm_indexes("prediction_set")["ix_prediction_set_method_id"]
        )
        assert recorded.altered[("prediction_set", "embedding_config_id")] is True
        assert orm.c.embedding_config_id.nullable is True
        table, condition = recorded.checks["ck_prediction_set_has_a_producer"]
        assert table == "prediction_set"
        assert _squash(condition) == _shape(orm)["checks"]["ck_prediction_set_has_a_producer"]

    def test_the_downgrade_drops_everything_the_upgrade_made(self, recorded: _Recorder) -> None:
        migration.downgrade()
        dropped = set(recorded.dropped)
        assert {("table", t) for t in recorded.tables} <= dropped
        assert {("index", i) for i in recorded.indexes} <= dropped
        assert ("column", "prediction_set.method_id") in dropped
        assert ("foreignkey", "fk_prediction_set_method_id") in dropped
        assert {("check", name) for name in recorded.checks} <= dropped
        assert recorded.altered[("prediction_set", "embedding_config_id")] is False

    def test_the_downgrade_refuses_a_set_without_a_configuration(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        recorder = _Recorder(without_config=3)
        monkeypatch.setattr(migration, "op", recorder)
        with pytest.raises(RuntimeError, match="3 prediction set"):
            migration.downgrade()
        assert recorder.dropped == []


# ------------------------------------------------------------------- the ORM, on SQLite


@pytest.fixture()
def session() -> Iterator[Session]:
    engine = create_engine("sqlite:///:memory:")
    tables = [Base.metadata.tables[t] for t in ("method", "method_training_window", "method_pin")]
    Base.metadata.create_all(engine, tables=tables)
    with Session(engine) as s:
        yield s


class TestARowIsItsSpec:
    def test_a_row_built_from_a_spec_reads_back_as_that_spec(self, session: Session) -> None:
        spec = _spec()
        windows = [
            TrainingWindow(_W177_205, "fit"),
            TrainingWindow(_W194_211, "holdout"),
        ]
        session.add(Method.from_spec(spec, windows))
        session.commit()
        row = session.get(Method, spec.method_id)
        assert row is not None
        assert row.spec() == spec
        assert row.spec_sha256 == spec.spec_sha256
        assert sorted((w.role, w.evaluation_set_id) for w in row.training_windows) == sorted(
            (w.role, w.evaluation_set_id) for w in windows
        )

    def test_a_row_whose_id_its_spec_does_not_derive_is_refused(self, session: Session) -> None:
        row = Method.from_spec(_spec())
        row.code = "another-method"
        with pytest.raises(ValueError, match="derived, never assigned"):
            session.add(row)
            session.flush()

    def test_a_pin_added_after_the_id_was_derived_is_refused(self, session: Session) -> None:
        row = Method.from_spec(_spec())
        row.pins.append(MethodPin(role="lexicon", sha256="c" * 64, refs=[]))
        with pytest.raises(ValueError, match="derived, never assigned"):
            session.add(row)
            session.flush()

    def test_a_row_without_an_id_gets_the_one_its_spec_derives(self, session: Session) -> None:
        spec = _spec()
        row = Method.from_spec(spec)
        row.id = None  # type: ignore[assignment]
        row.spec_sha256 = None  # type: ignore[assignment]
        session.add(row)
        session.flush()
        assert row.id == spec.method_id
        assert row.spec_sha256 == spec.spec_sha256

    def test_deleting_a_method_takes_its_windows_and_pins(self, session: Session) -> None:
        spec = _spec()
        session.add(Method.from_spec(spec, [TrainingWindow(_W_OTHER[0], "fit")]))
        session.commit()
        row = session.get(Method, spec.method_id)
        session.delete(row)
        session.commit()
        for table in ("method_pin", "method_training_window"):
            assert session.execute(text(f"SELECT count(*) FROM {table}")).scalar_one() == 0


class TestTheDatabaseRefusesOnItsOwn:
    def test_a_second_holdout_window_is_refused(self, session: Session) -> None:
        windows = [
            TrainingWindow(_W_OTHER[1], "holdout"),
            TrainingWindow(_W_OTHER[2], "holdout"),
        ]
        session.add(Method.from_spec(_spec(), windows))
        with pytest.raises(IntegrityError):
            session.flush()

    @pytest.mark.parametrize(
        "column, value",
        [
            ("family", "arm"),
            ("runtime", "docker"),
            ("instance_release", 0),
            ("spec_sha256", "abc"),
        ],
    )
    def test_a_value_outside_its_vocabulary_is_refused(
        self, session: Session, column: str, value: Any
    ) -> None:
        # Written past the ORM, which would refuse it in the contract first.
        values = {
            "id": uuid.uuid4().hex,
            "family": "world-model",
            "code": "jepa-211",
            "instance_release": 211,
            "ontology_snapshot_id": _SNAPSHOT.hex,
            "recipe": "{}",
            "runtime": "container",
            "explains": True,
            "spec_sha256": "e" * 64,
            column: value,
        }
        columns = ", ".join(values)
        params = ", ".join(f":{name}" for name in values)
        with pytest.raises(IntegrityError):
            session.execute(text(f"INSERT INTO method ({columns}) VALUES ({params})"), values)


class TestAPredictionSetNamesWhatProducedIt:
    def test_the_configuration_is_nullable_and_the_method_is_a_restricted_key(self) -> None:
        table = PredictionSet.__table__
        assert table.c.embedding_config_id.nullable
        assert table.c.method_id.nullable
        (fk,) = table.c.method_id.foreign_keys
        assert (fk.target_fullname, fk.ondelete) == ("method.id", "RESTRICT")

    def test_a_set_must_name_a_configuration_or_a_method(self) -> None:
        checks = _shape(PredictionSet.__table__)["checks"]
        assert checks["ck_prediction_set_has_a_producer"] == (
            "embedding_config_id IS NOT NULL OR method_id IS NOT NULL"
        )


# -------------------------------------------------------------- the revision, on Postgres


@pytest.fixture()
def fresh_database(postgres_url: str) -> Iterator[str]:
    """A database of its own, created empty and dropped afterwards.

    The session database is shared and other integration tests reset it with
    ``drop_all()/create_all()``, which can leave ``alembic_version`` stamped over
    a schema that no longer matches it. This test is about the revision, so it
    builds the chain on an empty database it owns, as
    ``tests/test_stage1_standard_configs.py`` does.
    """
    from sqlalchemy.engine import make_url

    base = make_url(postgres_url)
    name = f"method_tables_{uuid.uuid4().hex[:12]}"
    admin = create_engine(base.set(database="postgres"), isolation_level="AUTOCOMMIT")
    with admin.connect() as conn:
        conn.execute(text(f'CREATE DATABASE "{name}"'))
    url = base.set(database=name)
    setup = create_engine(url)
    with setup.begin() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
    setup.dispose()
    try:
        yield url.render_as_string(hide_password=False)
    finally:
        with admin.connect() as conn:
            conn.execute(
                text(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity"
                    " WHERE datname = :name AND pid <> pg_backend_pid()"
                ),
                {"name": name},
            )
            conn.execute(text(f'DROP DATABASE IF EXISTS "{name}"'))
        admin.dispose()


@pytest.fixture()
def alembic_config(fresh_database: str, monkeypatch: pytest.MonkeyPatch) -> Any:
    """Alembic pointed at the fresh database, through the env knob env.py honours."""
    from alembic.config import Config

    monkeypatch.setenv("PROTEA_DB_URL", fresh_database)
    cfg = Config(str(_REPO / "alembic.ini"))
    cfg.set_main_option("script_location", str(_REPO / "alembic"))
    cfg.set_main_option("sqlalchemy.url", fresh_database)
    return cfg


def _seed_a_method_set(engine: sa.Engine) -> uuid.UUID:
    """A snapshot, a corpus, a method and a prediction set the method produced."""
    spec = _spec(instance=InstanceCut(211, _SNAPSHOT))
    pset = uuid.uuid4()
    corpus = uuid.uuid4()
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO ontology_snapshot (id, obo_url, obo_version)"
                " VALUES (:id, 'file:///none', 'method-tables')"
            ),
            {"id": _SNAPSHOT},
        )
        conn.execute(
            text(
                "INSERT INTO annotation_set (id, source, ontology_snapshot_id, meta)"
                " VALUES (:id, 'goa', :snapshot, CAST('{}' AS jsonb))"
            ),
            {"id": corpus, "snapshot": _SNAPSHOT},
        )
        conn.execute(
            text(
                "INSERT INTO method (id, family, code, instance_release, ontology_snapshot_id,"
                " recipe, runtime, explains, spec_sha256) VALUES (:id, :family, :code, 211,"
                " :snapshot, CAST(:recipe AS jsonb), 'native', false, :sha)"
            ),
            {
                "id": spec.method_id,
                "family": spec.family.value,
                "code": spec.code,
                "snapshot": _SNAPSHOT,
                "recipe": json.dumps(spec.document()["recipe"]),
                "sha": spec.spec_sha256,
            },
        )
        conn.execute(
            text(
                "INSERT INTO prediction_set (id, method_id, annotation_set_id,"
                " ontology_snapshot_id, limit_per_entry, meta)"
                " VALUES (:id, :method, :corpus, :snapshot, 30, CAST('{}' AS jsonb))"
            ),
            {"id": pset, "method": spec.method_id, "corpus": corpus, "snapshot": _SNAPSHOT},
        )
    return pset


def test_the_revision_upgrades_and_downgrades_against_postgres(
    alembic_config: Any, fresh_database: str
) -> None:
    """Up to this revision, a method's set in the way of the downgrade, then down and up.

    Upgraded to this revision rather than to head, and downgraded to its parent,
    so the test exercises this revision alone whatever lands after it.
    """
    from alembic import command

    engine = create_engine(fresh_database)
    try:
        command.upgrade(alembic_config, migration.revision)
        inspector = sa.inspect(engine)
        assert set(NEW_TABLES) <= set(inspector.get_table_names())
        columns = {c["name"]: c for c in inspector.get_columns("prediction_set")}
        assert columns["embedding_config_id"]["nullable"] is True
        assert columns["method_id"]["nullable"] is True

        method_set = _seed_a_method_set(engine)
        with pytest.raises(IntegrityError), engine.begin() as conn:
            conn.execute(text("UPDATE prediction_set SET method_id = NULL"))

        with pytest.raises(RuntimeError, match="1 prediction set"):
            command.downgrade(alembic_config, migration.down_revision)
        assert "method" in sa.inspect(engine).get_table_names()

        with engine.begin() as conn:
            conn.execute(text("DELETE FROM prediction_set WHERE id = :id"), {"id": method_set})
        command.downgrade(alembic_config, migration.down_revision)
        inspector = sa.inspect(engine)
        assert not set(NEW_TABLES) & set(inspector.get_table_names())
        columns = {c["name"]: c for c in inspector.get_columns("prediction_set")}
        assert "method_id" not in columns
        assert columns["embedding_config_id"]["nullable"] is False

        command.upgrade(alembic_config, migration.revision)
        assert set(NEW_TABLES) <= set(sa.inspect(engine).get_table_names())
    finally:
        engine.dispose()
