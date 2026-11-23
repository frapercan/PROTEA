"""The migration that lets a window be built by LAFA's frame (5d3a8c1f7b24).

Without a database: it installs the model's texts, it restores the texts the
earlier migrations installed, and the vocabularies, the model and the
migration list the same values. Against Postgres (``--with-postgres``): a
window of the frame is stored, the downgrade refuses while one exists, and on
an empty table it restores checks that refuse the frame's values again.
"""

from __future__ import annotations

import importlib.util
import pathlib
import re
import uuid
from types import ModuleType

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session

from protea.core._truth_rules import GAIN_RULES, TRUTH_PROFILES
from protea.core._window_population import TARGET_POPULATIONS
from protea.core.window_declarations import CLOSURES, LAFA_FRAME
from protea.infrastructure.orm.base import Base
from protea.infrastructure.orm.models.annotation import evaluation_set as model
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot

_VERSIONS = pathlib.Path(__file__).resolve().parents[1] / "alembic" / "versions"


def _module(revision: str) -> ModuleType:
    """A migration loaded by path: ``alembic/`` is not a package."""
    (path,) = _VERSIONS.glob(f"{revision}_*.py")
    spec = importlib.util.spec_from_file_location(f"_{revision}", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


MIGRATION = _module("5d3a8c1f7b24")


def _quoted(sql: str) -> list[str]:
    return re.findall(r"'([^']*)'", sql)


def test_it_follows_the_supersession_migration() -> None:
    assert MIGRATION.down_revision == "9e4f2a7c5b13"


def test_it_installs_the_models_texts() -> None:
    assert MIGRATION.TRUTH_PROFILE_CHECK == model.TRUTH_PROFILE_CHECK
    assert MIGRATION.TOI_RULE_CHECK == model.TOI_RULE_CHECK
    assert MIGRATION.TARGET_POPULATION_CHECK == model.TARGET_POPULATION_CHECK
    assert MIGRATION.TRUTH_RULES_CHECK == model.TRUTH_RULES_CHECK


def test_it_restores_the_texts_the_chain_installed() -> None:
    # Those two were written inline in their migrations, so they are found there.
    (profile,) = _VERSIONS.glob("a1d7e3b95c62_*.py")
    (gain,) = _VERSIONS.glob("b7c4e1a9f280_*.py")
    assert MIGRATION.PREVIOUS_TRUTH_PROFILE_CHECK in profile.read_text()
    assert MIGRATION.PREVIOUS_TOI_RULE_CHECK in gain.read_text()
    assert (
        MIGRATION.PREVIOUS_TARGET_POPULATION_CHECK
        == _module("e5b1c9d3a7f2").TARGET_POPULATION_CHECK
    )
    assert MIGRATION.PREVIOUS_TRUTH_RULES_CHECK == _module("7c2e9b4d1a6f").TRUTH_RULES_CHECK


def test_the_vocabularies_and_the_checks_list_the_same_values() -> None:
    assert _quoted(model.TRUTH_PROFILE_CHECK) == list(TRUTH_PROFILES)
    assert _quoted(model.TOI_RULE_CHECK) == list(GAIN_RULES)
    assert _quoted(model.TARGET_POPULATION_CHECK) == list(TARGET_POPULATIONS)
    closures = model.TRUTH_RULES_CHECK.split("'closure', '') IN ")[1].split(")")[0]
    assert _quoted(closures) == list(CLOSURES)


# --------------------------------------------------------------- against Postgres


@pytest.fixture()
def pg_engine(postgres_url: str):
    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    yield engine
    Base.metadata.drop_all(engine)
    engine.dispose()


def _store_a_window_of_the_frame(engine) -> uuid.UUID:
    with Session(engine, future=True) as session:
        snapshot = OntologySnapshot(
            obo_version=f"releases/{uuid.uuid4().hex[:8]}", obo_url="file:///dev/null"
        )
        session.add(snapshot)
        session.flush()
        corpus = AnnotationSet(source="goa", ontology_snapshot_id=snapshot.id)
        session.add(corpus)
        session.flush()
        row = EvaluationSet(
            old_annotation_set_id=corpus.id,
            new_annotation_set_id=corpus.id,
            pivot_snapshot_id=snapshot.id,
            old_native_snapshot_id=snapshot.id,
            new_native_snapshot_id=snapshot.id,
            truth_profile="own_graph",
            toi_rule="both_ends_trimmed",
            not_codes="experimental",
            target_population="swissprot_same_sequence",
            binding_rule="cafa",
            stats={},
            truth_rules=LAFA_FRAME.as_json(),
        )
        session.add(row)
        session.commit()
        return row.id


def _migrate(engine, step: str) -> None:
    with engine.begin() as connection:
        with Operations.context(MigrationContext.configure(connection)):
            getattr(MIGRATION, step)()


@pytest.mark.integration
def test_a_window_of_the_frame_is_stored(pg_engine) -> None:
    assert _store_a_window_of_the_frame(pg_engine)


@pytest.mark.integration
def test_the_downgrade_refuses_while_a_window_of_the_frame_exists(pg_engine) -> None:
    _store_a_window_of_the_frame(pg_engine)
    with pytest.raises(RuntimeError, match="LAFA's frame"):
        _migrate(pg_engine, "downgrade")


@pytest.mark.integration
def test_on_an_empty_table_the_downgrade_restores_checks_that_refuse_the_frame(pg_engine) -> None:
    _migrate(pg_engine, "downgrade")
    with pytest.raises(DBAPIError, match="ck_evaluation_set_"):
        _store_a_window_of_the_frame(pg_engine)
    _migrate(pg_engine, "upgrade")
    assert _store_a_window_of_the_frame(pg_engine)
