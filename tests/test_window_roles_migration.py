"""The migration that gives ``evaluation_set`` the frame roles and the truth rules.

Two halves. Without a database: the migration names exactly what it undoes,
carries the same CHECK text as the model, and renders the DDL it is meant to.
Against Postgres (``--with-postgres``, CI): the CHECKs refuse what they are for,
a row written under the old schema keeps NULL rules and reads as legacy, and
the downgrade refuses to narrow the roles while a row uses a new one.

The live half applies THIS migration's ``upgrade`` and ``downgrade`` on a schema
built from the models, rather than walking the whole chain to head: the chain
carries data seeds that have nothing to do with this change, and the shared CI
container has already shown it can leave ``alembic_version`` out of step with
the tables. What is exercised is the DDL this file ships, on the engine
production runs.
"""

from __future__ import annotations

import ast
import importlib.util
import io
import pathlib
import re
import uuid
from types import ModuleType

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session

from protea.core.window_declarations import OCTOBER_HEADLINE_FRAME
from protea.infrastructure.orm.base import Base
from protea.infrastructure.orm.models.annotation import evaluation_set as model
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot

_VERSIONS = pathlib.Path(__file__).resolve().parents[1] / "alembic" / "versions"
_MIGRATION = _VERSIONS / "b71e2396e5c3_evaluation_set_window_roles_and_truth_rules.py"


def _module() -> ModuleType:
    """Loaded by path: ``alembic/`` is not a package and the library shadows the name."""
    spec = importlib.util.spec_from_file_location("_b71e2396e5c3", _MIGRATION)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _rendered(step: str) -> str:
    """The DDL ``step`` emits for Postgres, rendered offline: no database needed."""
    buffer = io.StringIO()
    context = MigrationContext.configure(
        dialect_name="postgresql", opts={"as_sql": True, "output_buffer": buffer}
    )
    with Operations.context(context):
        getattr(_module(), step)()
    return buffer.getvalue()


# ------------------------------------------------------------ without a database


def test_the_module_pins_its_revision_and_names_an_existing_parent() -> None:
    module = _module()
    assert module.revision == "b71e2396e5c3"
    # The parent is not pinned by value: parallel changes are re-chained when
    # they merge. It has to name a revision that exists, and not this one.
    assert module.down_revision and module.down_revision != module.revision
    declared = {
        match
        for path in _VERSIONS.glob("*.py")
        for match in re.findall(
            r'^revision(?::\s*str)?\s*=\s*["\'](\w+)["\']', path.read_text(), re.M
        )
    }
    assert module.down_revision in declared


def test_it_carries_the_models_check_text() -> None:
    module = _module()
    assert module.WINDOW_ROLE_CHECK == model.WINDOW_ROLE_CHECK
    assert module.TRUTH_RULES_CHECK == model.TRUTH_RULES_CHECK


def test_the_downgrade_restores_the_check_of_d40() -> None:
    # The text migration f2a4c6e8b0d1 installed, so a downgrade lands where the
    # chain was and not on a third vocabulary.
    previous = (_VERSIONS / "f2a4c6e8b0d1_add_window_role_to_evaluation_set.py").read_text()
    assert f'"{_module().PREVIOUS_WINDOW_ROLE_CHECK}"' in previous


def _touched(function: str) -> set[str]:
    """Every string literal ``function`` passes to an ``op.*`` call."""
    tree = ast.parse(_MIGRATION.read_text())
    target = next(
        n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == function
    )
    names: set[str] = set()
    for node in ast.walk(target):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            if isinstance(node.func.value, ast.Name) and node.func.value.id == "op":
                names |= {
                    a.value
                    for a in node.args
                    if isinstance(a, ast.Constant) and isinstance(a.value, str)
                }
                for arg in node.args:
                    if isinstance(arg, ast.Call):
                        names |= {
                            k.value
                            for k in arg.args
                            if isinstance(k, ast.Constant) and isinstance(k.value, str)
                        }
    return names


def test_the_downgrade_names_exactly_what_the_upgrade_touched() -> None:
    up, down = _touched("upgrade"), _touched("downgrade")
    assert up == down
    assert {"window_role", "truth_rules", "ck_evaluation_set_truth_rules"} <= up


def test_the_upgrade_renders_the_ddl_it_is_meant_to() -> None:
    sql = _rendered("upgrade")
    assert "ALTER COLUMN window_role TYPE VARCHAR(16)" in sql
    assert "ADD COLUMN truth_rules JSONB" in sql
    assert "ADD CONSTRAINT ck_evaluation_set_truth_rules CHECK" in sql
    assert "'test_spent'" in sql and "'direct_terms'" in sql
    # Nothing is backfilled: no UPDATE, so every existing row keeps NULL.
    assert "UPDATE" not in sql


def test_the_downgrade_narrows_the_vocabulary_before_the_width() -> None:
    sql = _rendered("downgrade")
    check = sql.index("ADD CONSTRAINT ck_evaluation_set_window_role")
    width = sql.index("ALTER COLUMN window_role TYPE VARCHAR(8)")
    assert check < width
    assert "DROP COLUMN truth_rules" in sql


# --------------------------------------------------------------- against Postgres


@pytest.fixture()
def pg_engine(postgres_url: str):
    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    yield engine
    Base.metadata.drop_all(engine)
    engine.dispose()


def _apply(engine, step: str) -> None:
    """Run this migration's ``step`` on the live database, in one transaction."""
    with engine.begin() as connection:
        with Operations.context(MigrationContext.configure(connection)):
            getattr(_module(), step)()


def _graphs(engine) -> tuple[uuid.UUID, uuid.UUID]:
    """One snapshot and one corpus to hang windows from."""
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


_RULE_SPELLINGS = (
    ("as_closed", "pivot_terms", "any"),
    ("projected", "live_in_both_ends", "experimental"),
)


def _insert(engine, corpus: uuid.UUID, snapshot: uuid.UUID, **columns) -> uuid.UUID:
    """One window through the ORM, its identity varied by ``spelling`` (0 or 1)."""
    profile, toi_rule, not_codes = _RULE_SPELLINGS[columns.pop("spelling", 0)]
    row = EvaluationSet(
        old_annotation_set_id=corpus,
        new_annotation_set_id=corpus,
        pivot_snapshot_id=snapshot,
        old_native_snapshot_id=snapshot,
        new_native_snapshot_id=snapshot,
        truth_profile=profile,
        toi_rule=toi_rule,
        not_codes=not_codes,
        stats={},
        **columns,
    )
    with Session(engine, future=True) as session:
        session.add(row)
        session.commit()
        return row.id


def _raw_legacy_row(
    engine, corpus: uuid.UUID, snapshot: uuid.UUID, role: str, profile: str = "as_closed"
) -> uuid.UUID:
    """A window written the way the schema BEFORE this migration could write it.

    ``profile`` varies the identity, so two rows over one corpus never meet the
    unique key before the CHECK this file is about.
    """
    row_id = uuid.uuid4()
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO evaluation_set (id, old_annotation_set_id, new_annotation_set_id, "
                "pivot_snapshot_id, old_native_snapshot_id, new_native_snapshot_id, stats, "
                "truth_profile, window_role) VALUES (:id, :corpus, :corpus, :snap, :snap, "
                ":snap, CAST('{}' AS jsonb), :profile, :role)"
            ),
            {"id": row_id, "corpus": corpus, "snap": snapshot, "profile": profile, "role": role},
        )
    return row_id


@pytest.mark.parametrize("role", ["training", "validation", "test_spent", "valid", "test"])
def test_every_role_is_accepted_with_the_october_rules(pg_engine, role: str) -> None:
    snapshot, corpus = _graphs(pg_engine)
    row = _insert(
        pg_engine, corpus, snapshot, window_role=role, truth_rules=OCTOBER_HEADLINE_FRAME.as_json()
    )
    with pg_engine.connect() as connection:
        stored = connection.execute(
            text("SELECT window_role, truth_rules FROM evaluation_set WHERE id = :id"), {"id": row}
        ).one()
    assert tuple(stored) == (role, OCTOBER_HEADLINE_FRAME.as_json())


def test_an_unknown_role_is_refused_by_the_check(pg_engine) -> None:
    snapshot, corpus = _graphs(pg_engine)
    with pytest.raises(DBAPIError, match="ck_evaluation_set_window_role"):
        _insert(pg_engine, corpus, snapshot, window_role="holdout")


_OCTOBER = OCTOBER_HEADLINE_FRAME.as_json()


@pytest.mark.parametrize(
    "rules",
    [
        {k: v for k, v in _OCTOBER.items() if k != "closure"},
        {**_OCTOBER, "closure": None},
        {**_OCTOBER, "closure": "graph_of_t0"},
        {**_OCTOBER, "profile": "remapped"},
        list(_OCTOBER),
        "projected",
    ],
    ids=["missing-key", "json-null", "unknown-closure", "unknown-profile", "array", "scalar"],
)
def test_malformed_rules_are_refused_by_the_check(pg_engine, rules) -> None:
    snapshot, corpus = _graphs(pg_engine)
    with pytest.raises(DBAPIError, match="ck_evaluation_set_truth_rules"):
        _insert(pg_engine, corpus, snapshot, truth_rules=rules)


def test_a_row_from_before_keeps_null_rules_and_its_role(pg_engine) -> None:
    snapshot, corpus = _graphs(pg_engine)
    _apply(pg_engine, "downgrade")
    columns = {c["name"] for c in inspect(pg_engine).get_columns("evaluation_set")}
    assert "truth_rules" not in columns
    legacy = _raw_legacy_row(pg_engine, corpus, snapshot, "valid")
    # Eight characters, so it is the old CHECK and not the old width refusing it.
    with pytest.raises(DBAPIError, match="ck_evaluation_set_window_role"):
        _raw_legacy_row(pg_engine, corpus, snapshot, "training", profile="projected")

    _apply(pg_engine, "upgrade")

    with pg_engine.connect() as connection:
        stored = connection.execute(
            text("SELECT window_role, truth_rules FROM evaluation_set WHERE id = :id"),
            {"id": legacy},
        ).one()
    assert tuple(stored) == ("valid", None)
    _insert(pg_engine, corpus, snapshot, spelling=1, window_role="test_spent")


def test_the_downgrade_refuses_while_a_row_holds_a_frame_role(pg_engine) -> None:
    snapshot, corpus = _graphs(pg_engine)
    _insert(pg_engine, corpus, snapshot, window_role="validation")

    with pytest.raises(DBAPIError, match="ck_evaluation_set_window_role"):
        _apply(pg_engine, "downgrade")

    # One transaction: the failed downgrade left the schema where it was.
    columns = {c["name"] for c in inspect(pg_engine).get_columns("evaluation_set")}
    assert "truth_rules" in columns
