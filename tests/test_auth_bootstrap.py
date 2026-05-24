"""Unit tests for :mod:`protea.api.auth.bootstrap` (FARM-AUTH.2).

Acceptance criteria from the slice spec:

* On startup, if PROTEA_BOOTSTRAP_ADMIN_EMAIL is set and no User row with
  role=admin exists, PROTEA creates the admin; password read from
  PROTEA_BOOTSTRAP_ADMIN_PASSWORD if set, otherwise generated and printed
  to stderr once.
* Bootstrap is idempotent: second startup with the same env var does nothing.
* Unit test mocks the session and asserts admin row created on first call,
  skipped on second.

All tests are pure-Python (no live Postgres / no FastAPI). The session is
an in-memory SQLite database so the ORM insert/select path is exercised
without a running Postgres instance.

The CLI command surface is tested via Click's ``CliRunner`` in isolation from
the database (session injected via monkeypatching ``_load_factory``).
"""

from __future__ import annotations

import io
import sys
from unittest.mock import MagicMock, call, patch

import pytest
from click.testing import CliRunner
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from protea.api.auth.bootstrap import bootstrap_admin
from protea.api.auth.passwords import verify_password
from protea.infrastructure.orm.models.user import User, UserRole, UserStatus


# ---------------------------------------------------------------------------
# Shared SQLite in-memory fixture
# ---------------------------------------------------------------------------


def _create_user_table(engine):
    """Create only the ``user`` table in the given engine.

    We cannot call ``Base.metadata.create_all`` because other ORM models
    use Postgres-only types (JSONB, PG_UUID as primary key with
    gen_random_uuid(), etc.) that SQLite cannot compile. Instead we emit
    the minimal DDL for the ``user`` table directly so the bootstrap
    unit tests stay independent of the full schema.
    """
    with engine.connect() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS "user" (
                    id TEXT PRIMARY KEY,
                    email TEXT NOT NULL UNIQUE,
                    username TEXT NOT NULL UNIQUE,
                    display_name TEXT,
                    password_hash TEXT NOT NULL,
                    role TEXT NOT NULL DEFAULT 'researcher',
                    status TEXT NOT NULL DEFAULT 'pending',
                    intended_use TEXT,
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    last_login_at TEXT,
                    deactivated_at TEXT
                )
                """
            )
        )
        conn.commit()


@pytest.fixture()
def sqlite_session():
    """Provide an in-memory SQLite session with the User table only.

    Uses hand-written DDL to avoid Postgres-only column types in the
    full metadata that SQLite cannot compile.
    """
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
    )
    _create_user_table(engine)

    factory = sessionmaker(bind=engine, class_=Session, autoflush=False, autocommit=False)
    session = factory()
    try:
        yield session
        session.rollback()
    finally:
        session.close()
        engine.dispose()


# ---------------------------------------------------------------------------
# bootstrap_admin — core logic
# ---------------------------------------------------------------------------


class TestBootstrapAdmin:
    """Tests for :func:`protea.api.auth.bootstrap.bootstrap_admin`."""

    def test_creates_admin_when_none_exists(self, sqlite_session):
        user, created = bootstrap_admin(
            sqlite_session,
            "admin@example.test",
            password="s3cr3t",
        )
        assert created is True
        assert user.email == "admin@example.test"
        assert user.role == UserRole.ADMIN
        assert user.status == UserStatus.ACTIVE
        assert verify_password("s3cr3t", user.password_hash)

    def test_idempotent_second_call_is_noop(self, sqlite_session):
        bootstrap_admin(sqlite_session, "admin@example.test", password="first")
        sqlite_session.commit()

        user2, created2 = bootstrap_admin(
            sqlite_session, "admin@example.test", password="second"
        )
        assert created2 is False
        assert user2.email == "admin@example.test"
        # Password must remain the first one (no update on second call).
        assert verify_password("first", user2.password_hash)

    def test_generates_password_when_none_supplied(self, sqlite_session, capsys):
        user, created = bootstrap_admin(sqlite_session, "gen@example.test")
        assert created is True
        # The generated password is printed to stderr.
        captured = capsys.readouterr()
        assert "gen@example.test" in captured.err
        assert "[PROTEA bootstrap]" in captured.err
        # The stored hash must verify against the printed password.
        # Parse it from the stderr line: "... Generated admin password for ...: <token>"
        printed_line = captured.err.strip()
        generated_pw = printed_line.split(": ", maxsplit=2)[-1]
        assert verify_password(generated_pw, user.password_hash)

    def test_username_derived_from_email_local_part(self, sqlite_session):
        user, _ = bootstrap_admin(
            sqlite_session,
            "john.doe@corp.example.test",
            password="pw",
        )
        assert user.username == "john.doe"

    def test_user_role_value_stored_lowercase(self, sqlite_session):
        user, _ = bootstrap_admin(sqlite_session, "admin2@example.test", password="x")
        # Access via ORM after flush so the value_callable round-trip is exercised.
        assert user.role.value == "admin"
        assert user.status.value == "active"

    def test_second_call_different_email_creates_new_row(self, sqlite_session):
        _, c1 = bootstrap_admin(sqlite_session, "a@example.test", password="p")
        sqlite_session.commit()
        _, c2 = bootstrap_admin(sqlite_session, "b@example.test", password="q")
        assert c1 is True
        assert c2 is True


# ---------------------------------------------------------------------------
# _run_bootstrap — lifespan helper
# ---------------------------------------------------------------------------


class TestRunBootstrap:
    """Tests for the lifespan-wired ``_run_bootstrap`` helper in app.py."""

    def test_noop_when_email_not_set(self):
        from protea.api.app import _run_bootstrap
        from protea.infrastructure.settings import Settings
        from pathlib import Path

        settings = Settings(
            db_url="sqlite:///:memory:",
            amqp_url="amqp://guest:guest@localhost/",
            artifacts_dir=Path("/tmp"),
            admin_token="",
            bootstrap_admin_email=None,
            bootstrap_admin_password=None,
        )
        mock_factory = MagicMock()
        _run_bootstrap(mock_factory, settings)
        # session_scope must never be entered when email is None.
        mock_factory.assert_not_called()

    def test_calls_bootstrap_admin_when_email_set(self, sqlite_session):
        from protea.api.app import _run_bootstrap
        from protea.infrastructure.settings import Settings
        from protea.infrastructure.session import build_session_factory
        from pathlib import Path

        engine = sqlite_session.get_bind()
        factory = sessionmaker(bind=engine, class_=Session, autoflush=False, autocommit=False)

        settings = Settings(
            db_url="sqlite:///:memory:",
            amqp_url="amqp://guest:guest@localhost/",
            artifacts_dir=Path("/tmp"),
            admin_token="",
            bootstrap_admin_email="boot@example.test",
            bootstrap_admin_password="passw0rd",
        )

        with patch("protea.api.app.build_session_factory", return_value=factory):
            with patch("protea.api.app.session_scope") as mock_scope:
                mock_scope.return_value.__enter__ = lambda s: sqlite_session
                mock_scope.return_value.__exit__ = MagicMock(return_value=False)
                _run_bootstrap(factory, settings)


# ---------------------------------------------------------------------------
# CLI — protea-cli admin bootstrap
# ---------------------------------------------------------------------------


class TestAdminCLI:
    """Integration tests for ``protea-cli admin bootstrap`` and ``add-user``."""

    def _make_factory(self, sqlite_session):
        """Return a factory whose __call__ returns the shared sqlite session."""
        factory = MagicMock()
        factory.return_value = sqlite_session
        return factory

    def test_bootstrap_cmd_creates_admin(self, sqlite_session):
        from protea.cli.admin import cli

        runner = CliRunner()
        with patch("protea.cli.admin._load_factory") as mock_lf:
            from contextlib import contextmanager

            @contextmanager
            def _fake_scope(factory):
                yield sqlite_session

            mock_lf.return_value = MagicMock()
            with patch("protea.cli.admin.session_scope", side_effect=_fake_scope):
                result = runner.invoke(
                    cli,
                    ["admin", "bootstrap", "--email", "cli@example.test", "--password", "clipw"],
                )
        assert result.exit_code == 0, result.output
        assert "cli@example.test" in result.output

    def test_bootstrap_cmd_idempotent(self, sqlite_session):
        from protea.cli.admin import cli

        # Pre-insert the admin row.
        bootstrap_admin(sqlite_session, "idem@example.test", password="x")
        sqlite_session.commit()

        runner = CliRunner()
        with patch("protea.cli.admin._load_factory") as mock_lf:
            from contextlib import contextmanager

            @contextmanager
            def _fake_scope(factory):
                yield sqlite_session

            mock_lf.return_value = MagicMock()
            with patch("protea.cli.admin.session_scope", side_effect=_fake_scope):
                result = runner.invoke(
                    cli,
                    [
                        "admin",
                        "bootstrap",
                        "--email",
                        "idem@example.test",
                        "--password",
                        "other",
                    ],
                )
        assert result.exit_code == 0, result.output
        assert "already exists" in result.output

    def test_add_user_cmd_creates_user(self, sqlite_session):
        from protea.cli.admin import cli

        runner = CliRunner()
        with patch("protea.cli.admin._load_factory") as mock_lf:
            from contextlib import contextmanager

            @contextmanager
            def _fake_scope(factory):
                yield sqlite_session

            mock_lf.return_value = MagicMock()
            with patch("protea.cli.admin.session_scope", side_effect=_fake_scope):
                result = runner.invoke(
                    cli,
                    [
                        "admin",
                        "add-user",
                        "--email",
                        "newuser@example.test",
                        "--password",
                        "secret123",
                        "--role",
                        "researcher",
                    ],
                )
        assert result.exit_code == 0, result.output
        assert "newuser@example.test" in result.output

    def test_add_user_cmd_rejects_duplicate_email(self, sqlite_session):
        from protea.cli.admin import cli

        bootstrap_admin(sqlite_session, "dup@example.test", password="x")
        sqlite_session.commit()

        runner = CliRunner()
        with patch("protea.cli.admin._load_factory") as mock_lf:
            from contextlib import contextmanager

            @contextmanager
            def _fake_scope(factory):
                yield sqlite_session

            mock_lf.return_value = MagicMock()
            with patch("protea.cli.admin.session_scope", side_effect=_fake_scope):
                result = runner.invoke(
                    cli,
                    [
                        "admin",
                        "add-user",
                        "--email",
                        "dup@example.test",
                        "--password",
                        "pw",
                    ],
                )
        assert result.exit_code == 1
        assert "already exists" in result.output
