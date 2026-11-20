"""The migration that declares a window's protein-binding rule (7c2e9b4d1a6f).

Two halves. Without a database: the migration installs the model's own texts,
puts the rule in the identity the model declares, and gives each stored row the
rule its path applied when it was built. Against Postgres (``--with-postgres``):
rows written before the migration come out of it declaring that rule, the checks
refuse what they should, the rule separates two windows in the unique key, and
the downgrade refuses while a window was built under CAFA's rule.
"""

from __future__ import annotations

import importlib.util
import json
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

from protea.core._binding_rule import REQUESTABLE_BINDING_RULES
from protea.core.window_declarations import BENCHMARK_FRAME, OCTOBER_HEADLINE_FRAME
from protea.infrastructure.orm.base import Base
from protea.infrastructure.orm.models.annotation import evaluation_set as model
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot

_VERSIONS = pathlib.Path(__file__).resolve().parents[1] / "alembic" / "versions"
_MIGRATION = _VERSIONS / "7c2e9b4d1a6f_a_window_declares_its_protein_binding_rule.py"


def _module() -> ModuleType:
    """Loaded by path: ``alembic/`` is not a package and the library shadows the name."""
    spec = importlib.util.spec_from_file_location("_7c2e9b4d1a6f", _MIGRATION)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_it_follows_a_revision_that_exists() -> None:
    declared = {
        match
        for path in _VERSIONS.glob("*.py")
        for match in re.findall(
            r"^revision(?::\s*str)?\s*=\s*[\"\'](\w+)[\"\']", path.read_text(), re.M
        )
    }
    assert _module().down_revision in declared


def test_it_carries_the_models_check_texts() -> None:
    module = _module()
    assert module.BINDING_RULE_CHECK == model.BINDING_RULE_CHECK
    assert module.TRUTH_RULES_CHECK == model.TRUTH_RULES_CHECK


def test_the_column_check_lists_the_requestable_rules() -> None:
    quoted = re.findall(r"'([^']*)'", model.BINDING_RULE_CHECK)
    assert quoted == list(REQUESTABLE_BINDING_RULES)


def test_the_identity_it_installs_is_the_models() -> None:
    identity = next(
        c
        for c in EvaluationSet.__table__.constraints
        if getattr(c, "name", "") == "uq_evaluation_set_identity"
    )
    assert [c.name for c in identity.columns] == [*_module()._IDENTITY, "binding_rule"]


def test_each_stored_row_is_given_the_rule_its_path_applied() -> None:
    sql = _module()._DECLARE_THE_APPLIED_RULE
    assert "WHEN truth_rules ->> 'closure' = 'direct_terms' THEN 'none'" in sql
    assert "ELSE 'descendants_both_ends'" in sql
    assert "WHERE truth_rules IS NOT NULL" in sql


def test_the_column_defaults_to_proteas_rule_so_no_stored_window_moves() -> None:
    column = EvaluationSet.__table__.c.binding_rule
    assert column.nullable is False
    assert column.server_default.arg == "descendants_both_ends"
    assert column.type.length >= max(len(rule) for rule in REQUESTABLE_BINDING_RULES)


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


def _insert(engine, corpus: uuid.UUID, snapshot: uuid.UUID, **columns) -> uuid.UUID:
    """One window through the ORM, built under the October rules unless told."""
    row = EvaluationSet(
        old_annotation_set_id=corpus,
        new_annotation_set_id=corpus,
        pivot_snapshot_id=snapshot,
        old_native_snapshot_id=snapshot,
        new_native_snapshot_id=snapshot,
        truth_profile="projected",
        toi_rule="live_in_both_ends",
        not_codes="experimental",
        stats={},
        **{"truth_rules": OCTOBER_HEADLINE_FRAME.as_json(), **columns},
    )
    with Session(engine, future=True) as session:
        session.add(row)
        session.commit()
        return row.id


def _raw_row_from_before(
    engine, corpus: uuid.UUID, snapshot: uuid.UUID, closure: str, profile: str
) -> uuid.UUID:
    """A window written the way the schema BEFORE this migration could write it."""
    rules = {k: v for k, v in OCTOBER_HEADLINE_FRAME.as_json().items() if k != "binding"}
    rules["closure"] = closure
    row_id = uuid.uuid4()
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO evaluation_set (id, old_annotation_set_id, new_annotation_set_id, "
                "pivot_snapshot_id, old_native_snapshot_id, new_native_snapshot_id, stats, "
                "truth_profile, truth_rules) VALUES (:id, :corpus, :corpus, :snap, :snap, :snap, "
                "CAST('{}' AS jsonb), :profile, CAST(:rules AS jsonb))"
            ),
            {
                "id": row_id,
                "corpus": corpus,
                "snap": snapshot,
                "profile": profile,
                "rules": json.dumps(rules),
            },
        )
    return row_id


def test_rows_from_before_declare_the_rule_their_path_applied(pg_engine) -> None:
    snapshot, corpus = _graphs(pg_engine)
    _apply(pg_engine, "downgrade")
    assert "binding_rule" not in {
        c["name"] for c in inspect(pg_engine).get_columns("evaluation_set")
    }
    same_snapshot = _raw_row_from_before(pg_engine, corpus, snapshot, "direct_terms", "as_closed")
    reconciled = _raw_row_from_before(pg_engine, corpus, snapshot, "t0_graph", "projected")

    _apply(pg_engine, "upgrade")

    with pg_engine.connect() as connection:
        rows = connection.execute(
            text(
                "SELECT id, binding_rule, truth_rules ->> 'binding' FROM evaluation_set "
                "WHERE id IN (:a, :b)"
            ),
            {"a": same_snapshot, "b": reconciled},
        ).all()
    stored = {row[0]: (row[1], row[2]) for row in rows}
    # The request column takes the default; the applied rule is the path's own.
    assert stored[same_snapshot] == ("descendants_both_ends", "none")
    assert stored[reconciled] == ("descendants_both_ends", "descendants_both_ends")


@pytest.mark.parametrize(
    "columns",
    [
        {"binding_rule": "none"},
        {"binding_rule": "democafa"},
    ],
    ids=["none-is-not-requestable", "unknown"],
)
def test_the_column_check_refuses_what_a_payload_cannot_ask(pg_engine, columns) -> None:
    snapshot, corpus = _graphs(pg_engine)
    with pytest.raises(DBAPIError, match="ck_evaluation_set_binding_rule"):
        _insert(pg_engine, corpus, snapshot, **columns)


@pytest.mark.parametrize(
    "rules",
    [
        {k: v for k, v in BENCHMARK_FRAME.as_json().items() if k != "binding"},
        {**BENCHMARK_FRAME.as_json(), "binding": "democafa"},
        {**BENCHMARK_FRAME.as_json(), "binding": None},
    ],
    ids=["missing-key", "unknown", "json-null"],
)
def test_the_rules_check_refuses_a_missing_or_unknown_binding(pg_engine, rules) -> None:
    snapshot, corpus = _graphs(pg_engine)
    with pytest.raises(DBAPIError, match="ck_evaluation_set_truth_rules"):
        _insert(pg_engine, corpus, snapshot, truth_rules=rules)


def test_the_binding_rule_separates_two_windows_in_the_key(pg_engine) -> None:
    snapshot, corpus = _graphs(pg_engine)
    _insert(pg_engine, corpus, snapshot)
    _insert(pg_engine, corpus, snapshot, binding_rule="cafa", truth_rules=BENCHMARK_FRAME.as_json())
    with pytest.raises(DBAPIError, match="uq_evaluation_set_identity"):
        _insert(
            pg_engine, corpus, snapshot, binding_rule="cafa", truth_rules=BENCHMARK_FRAME.as_json()
        )


def test_the_downgrade_refuses_while_a_window_was_built_under_cafa(pg_engine) -> None:
    snapshot, corpus = _graphs(pg_engine)
    _insert(pg_engine, corpus, snapshot, binding_rule="cafa", truth_rules=BENCHMARK_FRAME.as_json())

    with pytest.raises(RuntimeError, match="CAFA's binding rule"):
        _apply(pg_engine, "downgrade")

    # One transaction: the refused downgrade left the schema where it was.
    assert "binding_rule" in {c["name"] for c in inspect(pg_engine).get_columns("evaluation_set")}
