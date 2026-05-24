"""Unit tests for :mod:`protea.api.auth.passwords` (FARM-AUTH.1).

Covers the contract documented in the slice acceptance criteria:

* hash is not the plaintext
* :func:`verify_password` returns True on match
* :func:`verify_password` returns False on mismatch
* two ``hash_password`` calls with the same input produce different
  outputs (random salt per call)
* corrupted / malformed hashes degrade to ``False`` rather than
  raising, so a bad DB row cannot turn a login attempt into a 500
* type-checking on the inputs

Pure-Python tests — no DB / Postgres / FastAPI dependency. argon2-cffi
is a hard dep declared in ``pyproject.toml`` so these tests run in
the default ``pytest`` invocation without extras.
"""

from __future__ import annotations

import pytest

from protea.api.auth.passwords import hash_password, verify_password


class TestHashPassword:
    def test_hash_is_not_plaintext(self):
        raw = "correct horse battery staple"
        h = hash_password(raw)
        assert h != raw
        # PHC-encoded Argon2id hashes start with the algorithm tag.
        assert h.startswith("$argon2id$")

    def test_two_hashes_of_same_input_differ(self):
        raw = "deterministic input"
        h1 = hash_password(raw)
        h2 = hash_password(raw)
        assert h1 != h2, "salts must be random per call"

    def test_hash_rejects_non_string(self):
        with pytest.raises(TypeError):
            hash_password(b"bytes-not-str")  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            hash_password(12345)  # type: ignore[arg-type]

    def test_empty_string_is_hashable(self):
        # Empty-password rejection is a higher-layer concern (signup
        # validator); the primitive must not crash on it.
        h = hash_password("")
        assert h.startswith("$argon2id$")


class TestVerifyPassword:
    def test_verify_matches(self):
        raw = "p@ssw0rd-with-symbols!"
        h = hash_password(raw)
        assert verify_password(raw, h) is True

    def test_verify_rejects_wrong_password(self):
        h = hash_password("real password")
        assert verify_password("wrong password", h) is False

    def test_verify_rejects_off_by_one(self):
        h = hash_password("abcdef")
        assert verify_password("abcdeF", h) is False
        assert verify_password("abcdef ", h) is False
        assert verify_password("", h) is False

    def test_verify_against_malformed_hash_returns_false(self):
        # A corrupted DB row must not crash the login endpoint with a
        # 500; verify_password collapses InvalidHashError to ``False``.
        assert verify_password("anything", "not-a-valid-hash") is False
        assert verify_password("anything", "") is False

    def test_verify_rejects_non_string(self):
        h = hash_password("x")
        with pytest.raises(TypeError):
            verify_password(b"x", h)  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            verify_password("x", 42)  # type: ignore[arg-type]

    def test_round_trip_across_many_inputs(self):
        # Property-flavoured spot check: each input verifies against
        # its own hash and not against any other input's hash.
        inputs = [
            "alpha",
            "beta-with-dashes",
            "tres palabras separadas",
            "unicode: \N{LATIN SMALL LETTER N WITH TILDE}andu",
            "longer " * 20,
        ]
        hashes = [hash_password(s) for s in inputs]
        for i, raw in enumerate(inputs):
            assert verify_password(raw, hashes[i]) is True
            for j, other_hash in enumerate(hashes):
                if i == j:
                    continue
                assert verify_password(raw, other_hash) is False


class TestModelImportSideEffects:
    """Importing models exposes the User class — FARM-AUTH.1 acceptance."""

    def test_user_orm_model_is_registered(self):
        from protea.infrastructure.orm import models

        assert hasattr(models, "User")
        assert hasattr(models, "UserRole")
        assert hasattr(models, "UserStatus")
        # The four roles required by ADR D37.
        assert {m.value for m in models.UserRole} == {
            "guest",
            "researcher",
            "operator",
            "admin",
        }
        assert {m.value for m in models.UserStatus} == {
            "pending",
            "active",
            "deactivated",
        }

    def test_user_table_metadata(self):
        from protea.infrastructure.orm.models.user import User

        cols = {c.name for c in User.__table__.columns}
        # Slice acceptance enumerates the required columns explicitly.
        for required in (
            "id",
            "email",
            "username",
            "display_name",
            "password_hash",
            "role",
            "status",
            "intended_use",
            "created_at",
            "last_login_at",
            "deactivated_at",
        ):
            assert required in cols, f"missing column {required}"


# ---------------------------------------------------------------------------
# Migration smoke test (file-on-disk + alembic revision graph)
# ---------------------------------------------------------------------------


class TestMigrationModule:
    """File-level pins for the FARM-AUTH.1 migration."""

    def test_migration_module_loads_and_pins_revisions(self):
        import importlib.util
        from pathlib import Path

        repo_root = Path(__file__).resolve().parents[1]
        path = repo_root / "alembic" / "versions" / "d6e5a7b8c9f1_farm_auth_1_add_user_table.py"
        assert path.is_file(), f"migration file missing at {path}"
        spec = importlib.util.spec_from_file_location("farm_auth_1_migration", path)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        assert module.revision == "d6e5a7b8c9f1"
        assert module.down_revision == "a9c1d2e7f8b0"
        assert callable(module.upgrade)
        assert callable(module.downgrade)

    def test_migration_source_references_required_ddl(self):
        from pathlib import Path

        repo_root = Path(__file__).resolve().parents[1]
        source = (
            repo_root / "alembic" / "versions" / "d6e5a7b8c9f1_farm_auth_1_add_user_table.py"
        ).read_text()
        # Table + enum types
        assert 'create_table(\n        "user"' in source
        assert "user_role" in source
        assert "user_status" in source
        # Role and status enum values must all appear
        for value in ("guest", "researcher", "operator", "admin"):
            assert f'"{value}"' in source, f"missing role value {value}"
        for value in ("pending", "active", "deactivated"):
            assert f'"{value}"' in source, f"missing status value {value}"
        # Required columns
        for col in (
            "email",
            "username",
            "display_name",
            "password_hash",
            "role",
            "status",
            "intended_use",
            "created_at",
            "last_login_at",
            "deactivated_at",
        ):
            assert col in source, f"missing column reference {col}"
        # Indexes
        assert "uq_user_email" in source
        assert "uq_user_username" in source
        assert "ix_user_status_created_at" in source


# ---------------------------------------------------------------------------
# End-to-end migration against a temporary Postgres (--with-postgres only).
# ---------------------------------------------------------------------------


@pytest.fixture()
def _alembic_config(postgres_url, monkeypatch):
    """Build an Alembic Config pointing at the temporary Postgres URL.

    Mirrors the pattern in :mod:`tests.test_schema_sha_v2_migration`:
    ``alembic/env.py`` reads ``PROTEA_DB_URL`` via ``load_settings``, so
    we set both the env var and the alembic config so the upgrade
    cannot accidentally hit the developer's live Postgres.
    """
    from pathlib import Path

    from alembic.config import Config

    monkeypatch.setenv("PROTEA_DB_URL", postgres_url)
    repo_root = Path(__file__).resolve().parents[1]
    cfg = Config(str(repo_root / "alembic.ini"))
    cfg.set_main_option("script_location", str(repo_root / "alembic"))
    cfg.set_main_option("sqlalchemy.url", postgres_url)
    return cfg


def test_user_migration_applies_against_postgres(_alembic_config, postgres_url):
    """``alembic upgrade head`` + introspect + idempotent re-upgrade.

    Skipped without ``--with-postgres``; gated on the
    :func:`tests.conftest.postgres_url` fixture.
    """
    from sqlalchemy import create_engine, inspect, text

    from alembic import command

    command.upgrade(_alembic_config, "head")

    engine = create_engine(postgres_url)
    try:
        insp = inspect(engine)
        # Quoted reserved word "user" is the canonical Postgres table name.
        assert "user" in insp.get_table_names(), insp.get_table_names()

        cols = {c["name"]: c for c in insp.get_columns("user")}
        for required in (
            "id",
            "email",
            "username",
            "display_name",
            "password_hash",
            "role",
            "status",
            "intended_use",
            "created_at",
            "last_login_at",
            "deactivated_at",
        ):
            assert required in cols, f"missing column {required}"

        idx_names = {ix["name"] for ix in insp.get_indexes("user")}
        assert {"uq_user_email", "uq_user_username", "ix_user_status_created_at"} <= idx_names

        # Idempotent: re-running upgrade head must be a no-op.
        command.upgrade(_alembic_config, "head")

        # Functional insert + read round-trip using the ORM model so the
        # enum ``values_callable`` wiring is exercised end-to-end (the
        # ExperimentRun bug FARM-EXP.1 worked around).
        from sqlalchemy.orm import Session

        from protea.api.auth.passwords import hash_password
        from protea.infrastructure.orm.models.user import (
            User,
            UserRole,
            UserStatus,
        )

        with Session(engine) as session:
            row = User(
                email="alice@example.test",
                username="alice",
                display_name="Alice",
                password_hash=hash_password("supersecret"),
                role=UserRole.RESEARCHER,
                status=UserStatus.PENDING,
                intended_use="benchmarking",
            )
            session.add(row)
            session.commit()
            session.refresh(row)
            assert row.id is not None
            assert row.role is UserRole.RESEARCHER
            assert row.status is UserStatus.PENDING

        # Unique constraints hold: a second insert with the same email
        # must raise IntegrityError (the SQLAlchemy wrapper around the
        # underlying psycopg ``UniqueViolation``).
        from sqlalchemy.exc import IntegrityError

        with Session(engine) as session, pytest.raises(IntegrityError):
            dup = User(
                email="alice@example.test",
                username="alice2",
                password_hash=hash_password("x"),
            )
            session.add(dup)
            session.commit()

        # Cleanup so the fixture's session-scoped engine can be reused.
        with engine.begin() as conn:
            conn.execute(text('DELETE FROM "user"'))
    finally:
        engine.dispose()
