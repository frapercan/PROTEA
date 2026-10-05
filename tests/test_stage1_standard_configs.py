"""The stage-1 seed migration names the configs it claims to name (ADR-D48).

Migration ``3cd5f76d282f`` restates the embedding-config identity derivation
instead of importing it, so that it keeps producing the same rows after the
application code moves on. A restated derivation can drift from the real one
without anything failing: the migration would seed rows whose pinned ids no
longer derive from their own recipes, and every later reader that re-derives an
id would land on a different row. These tests are what tie the two together.

They also pin the roster. The rung-1 seeds were replaced because their roster
had drifted from the eight PLMs of ADR-D35, and a roster check that lives only
in a docstring is how that happened in the first place.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest

from protea.core.embedding_identity import (
    IDENTITY_FIELDS,
    NAMESPACE,
    derive_embedding_config_id,
)

_REPO_ROOT = Path(__file__).resolve().parents[1]
_MIGRATION = (
    _REPO_ROOT / "alembic" / "versions" / "3cd5f76d282f_stage1_standard_configs_replace_rung1.py"
)

if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


@pytest.fixture(scope="module")
def migration() -> ModuleType:
    spec = importlib.util.spec_from_file_location("stage1_standard_configs", _MIGRATION)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _recipe(base: dict, model_name: str, backend: str) -> dict:
    return dict(base, model_name=model_name, model_backend=backend)


class TestTheMigrationIsWiredIn:
    def test_it_follows_the_previous_head(self, migration):
        assert migration.revision == "3cd5f76d282f"
        assert migration.down_revision == "f3a05d81c6e2"
        assert callable(migration.upgrade) and callable(migration.downgrade)


class TestTheRestatedDerivationIsTheRealOne:
    def test_the_namespace_is_the_application_namespace(self, migration):
        assert migration._NS == NAMESPACE

    def test_the_identity_columns_are_the_application_identity_fields(self, migration):
        assert set(migration._IDENTITY_COLS) == set(IDENTITY_FIELDS)

    def test_every_stage1_pin_is_what_the_application_derives(self, migration):
        for pinned, key, model_name, backend, _family in migration.NEW:
            derived = derive_embedding_config_id(_recipe(migration.STAGE1, model_name, backend))
            assert str(derived) == pinned, f"{key}: pinned {pinned}, application derives {derived}"

    def test_every_restored_rung1_pin_is_what_the_application_derives(self, migration):
        """The downgrade restores rows whose ids still derive from their recipes."""
        for pinned, key, model_name, backend, *_ in migration.OLD:
            derived = derive_embedding_config_id(_recipe(migration.RUNG1, model_name, backend))
            assert str(derived) == pinned, f"{key}: pinned {pinned}, application derives {derived}"


class TestTheRosterIsTheD35Roster:
    def test_the_stage1_checkpoints_are_the_eight_of_d35(self, migration):
        from apps.lafa_knn_8plm.plm_encoders import PLM_SPECS

        seeded = {model_name for _id, _key, model_name, _backend, _family in migration.NEW}
        assert seeded == {spec.checkpoint for spec in PLM_SPECS}

    def test_the_two_cells_that_were_never_on_the_axis_are_retired(self, migration):
        retired = {key for _id, key, *_ in migration.OLD}
        seeded = {key for _id, key, *_ in migration.NEW}
        assert {"esm2_8m", "protst"} <= retired
        assert not {"esm2_8m", "protst"} & seeded

    def test_no_stage1_id_reuses_a_rung1_id(self, migration):
        """A changed recipe must land on a new row, never relabel an old one."""
        assert not {row[0] for row in migration.NEW} & {row[0] for row in migration.OLD}


class TestTheStage1RecipeIsTheStandardOne:
    def test_one_recipe_for_all_eight(self, migration):
        assert migration.STAGE1 == {
            "layer_indices": [0],
            "layer_agg": "mean",
            "pooling": "mean",
            "normalize_residues": False,
            "normalize": True,
            "embedding_scale": 1.0,
            "use_chunking": False,
            "chunk_size": 512,
            "chunk_overlap": 0,
            "max_length": 1022,
        }

    def test_the_retired_recipe_differs_only_in_max_length(self, migration):
        assert {k: v for k, v in migration.RUNG1.items() if k != "max_length"} == {
            k: v for k, v in migration.STAGE1.items() if k != "max_length"
        }
        assert migration.RUNG1["max_length"] == 2048


class TestTheGuardCoversEveryForeignKey:
    def test_every_table_that_references_embedding_config_is_checked(self, migration):
        import protea.infrastructure.orm.models  # noqa: F401  (registers every table)
        from protea.infrastructure.orm.base import Base

        referencing = {
            (table.name, column.name)
            for table in Base.metadata.tables.values()
            for column in table.columns
            for fk in column.foreign_keys
            if fk.column.table.name == "embedding_config"
        }
        assert referencing == set(migration._DEPENDENTS)


# ---------------------------------------------------------------- live migration


@pytest.fixture()
def _fresh_database(postgres_url: str):
    """A database of its own, created empty and dropped afterwards.

    The session database is shared: other integration tests reset it with
    ``Base.metadata.drop_all()/create_all()`` and can leave ``alembic_version``
    stamped at head over a schema that no longer matches it. ``upgrade head``
    is then a no-op over missing tables. This test is about the migration chain
    itself, so it builds that chain from an empty database it owns.
    """
    import uuid

    from sqlalchemy import create_engine, text
    from sqlalchemy.engine import make_url

    base = make_url(postgres_url)
    name = f"stage1_d48_{uuid.uuid4().hex[:12]}"
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
def _alembic_config(_fresh_database: str, monkeypatch: pytest.MonkeyPatch):
    """Alembic pointed at the fresh database, through the env knob env.py honours."""
    from alembic.config import Config

    monkeypatch.setenv("PROTEA_DB_URL", _fresh_database)
    cfg = Config(str(_REPO_ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(_REPO_ROOT / "alembic"))
    cfg.set_main_option("sqlalchemy.url", _fresh_database)
    return cfg


def test_it_replaces_restores_and_refuses_against_postgres(
    _alembic_config, _fresh_database: str, migration
) -> None:
    """Upgrade seeds the eight, downgrade restores the rung-1 rows exactly, and a
    dependent row turns the upgrade into a refusal instead of a cascade.

    Runs the whole chain from an empty database. Skipped without
    ``--with-postgres``.
    """
    import uuid

    from sqlalchemy import create_engine, text

    from alembic import command

    new_ids = {row[0] for row in migration.NEW}
    old_ids = {row[0] for row in migration.OLD}
    engine = create_engine(_fresh_database)

    def present() -> set[str]:
        with engine.connect() as conn:
            return {str(r[0]) for r in conn.execute(text("SELECT id FROM embedding_config"))}

    try:
        command.upgrade(_alembic_config, "head")
        assert new_ids <= present()
        assert not old_ids & present()

        command.downgrade(_alembic_config, migration.down_revision)
        assert old_ids <= present()
        assert not new_ids & present()
        with engine.connect() as conn:
            rows = {
                str(r.id): r
                for r in conn.execute(
                    text(
                        "SELECT id, display_name, family, kind, param_count, description"
                        " FROM embedding_config"
                    )
                )
            }
        for old_id, key, _name, _backend, family, param_count, description in migration.OLD:
            row = rows[old_id]
            assert (row.display_name, row.family, row.kind) == (key, family, "pretrained")
            assert row.param_count == param_count
            assert row.description == description

        derived_id = str(uuid.uuid4())
        with engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO embedding_config (id, model_name, model_backend,"
                    " layer_indices, layer_agg, pooling, normalize, max_length, kind,"
                    " derived_from_embedding_config_id) VALUES (CAST(:id AS uuid),"
                    " 'guard-probe', 'learned-code', CAST('[0]' AS jsonb), 'mean', 'mean',"
                    " true, 1022, 'learned', CAST(:parent AS uuid))"
                ),
                {"id": derived_id, "parent": migration.OLD[0][0]},
            )
        try:
            with pytest.raises(RuntimeError, match="refusing to retire"):
                command.upgrade(_alembic_config, "head")
            assert old_ids <= present(), "a refused upgrade must leave the rung-1 rows in place"
        finally:
            with engine.begin() as conn:
                conn.execute(
                    text("DELETE FROM embedding_config WHERE id = CAST(:id AS uuid)"),
                    {"id": derived_id},
                )
    finally:
        command.upgrade(_alembic_config, "head")
        engine.dispose()

    assert new_ids <= present()
