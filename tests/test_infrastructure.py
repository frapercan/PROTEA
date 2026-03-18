"""
Unit tests for infrastructure layer: session, engine, settings, and app factory.
No real database or broker required — SQLAlchemy and pika are mocked.
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from protea.infrastructure.session import build_session_factory, session_scope

# ---------------------------------------------------------------------------
# session_scope
# ---------------------------------------------------------------------------

class TestSessionScope:
    def _make_factory(self):
        session = MagicMock()
        factory = MagicMock(return_value=session)
        return factory, session

    def test_commits_on_success(self):
        factory, session = self._make_factory()
        with session_scope(factory) as s:
            assert s is session
        session.commit.assert_called_once()
        session.rollback.assert_not_called()

    def test_closes_on_success(self):
        factory, session = self._make_factory()
        with session_scope(factory):
            pass
        session.close.assert_called_once()

    def test_rolls_back_on_exception(self):
        factory, session = self._make_factory()
        with pytest.raises(ValueError):
            with session_scope(factory):
                raise ValueError("oops")
        session.rollback.assert_called_once()
        session.commit.assert_not_called()

    def test_closes_on_exception(self):
        factory, session = self._make_factory()
        with pytest.raises(RuntimeError):
            with session_scope(factory):
                raise RuntimeError("boom")
        session.close.assert_called_once()

    def test_reraises_exception(self):
        factory, session = self._make_factory()
        with pytest.raises(KeyError, match="missing"):
            with session_scope(factory):
                raise KeyError("missing")


# ---------------------------------------------------------------------------
# build_session_factory
# ---------------------------------------------------------------------------

class TestBuildSessionFactory:
    def test_returns_sessionmaker(self):
        with patch("protea.infrastructure.session.build_engine") as mock_engine:
            mock_engine.return_value = MagicMock()
            factory = build_session_factory("sqlite:///:memory:")
        assert callable(factory)

    def test_calls_build_engine_with_url(self):
        url = "postgresql+psycopg://user:pw@localhost/db"
        with patch("protea.infrastructure.session.build_engine") as mock_engine:
            mock_engine.return_value = MagicMock()
            build_session_factory(url)
        mock_engine.assert_called_once_with(url)


# ---------------------------------------------------------------------------
# build_engine
# ---------------------------------------------------------------------------

class TestBuildEngine:
    def test_returns_engine(self):
        from protea.infrastructure.database.engine import build_engine
        with patch("protea.infrastructure.database.engine.create_engine") as mock_create:
            mock_create.return_value = MagicMock()
            engine = build_engine("sqlite:///:memory:")
        mock_create.assert_called_once_with(
            "sqlite:///:memory:",
            future=True,
            pool_pre_ping=True,
            pool_size=20,
            max_overflow=40,
            pool_recycle=3600,
        )
        assert engine is mock_create.return_value


# ---------------------------------------------------------------------------
# create_app
# ---------------------------------------------------------------------------

class TestCreateApp:
    def test_sets_session_factory_on_state(self):
        from protea.api.app import create_app
        mock_factory = MagicMock()
        mock_settings = MagicMock()
        mock_settings.db_url = "sqlite:///:memory:"
        mock_settings.amqp_url = "amqp://guest:guest@localhost/"

        with patch("protea.api.app.load_settings", return_value=mock_settings), \
             patch("protea.api.app.build_session_factory", return_value=mock_factory):
            app = create_app(Path("/fake/root"))

        assert app.state.session_factory is mock_factory

    def test_sets_amqp_url_on_state(self):
        from protea.api.app import create_app
        mock_factory = MagicMock()
        mock_settings = MagicMock()
        mock_settings.db_url = "sqlite:///:memory:"
        mock_settings.amqp_url = "amqp://guest:guest@localhost/"

        with patch("protea.api.app.load_settings", return_value=mock_settings), \
             patch("protea.api.app.build_session_factory", return_value=mock_factory):
            app = create_app(Path("/fake/root"))

        assert app.state.amqp_url == "amqp://guest:guest@localhost/"

    def test_jobs_router_is_registered(self):
        from protea.api.app import create_app
        mock_settings = MagicMock()
        mock_settings.db_url = "sqlite:///:memory:"
        mock_settings.amqp_url = "amqp://guest:guest@localhost/"

        with patch("protea.api.app.load_settings", return_value=mock_settings), \
             patch("protea.api.app.build_session_factory", return_value=MagicMock()):
            app = create_app(Path("/fake/root"))

        routes = [r.path for r in app.routes]
        assert any("/jobs" in p for p in routes)

    def test_health_endpoint_registered(self):
        from protea.api.app import create_app

        mock_settings = MagicMock()
        mock_settings.db_url = "sqlite:///:memory:"
        mock_settings.amqp_url = "amqp://guest:guest@localhost/"

        with patch("protea.api.app.load_settings", return_value=mock_settings), \
             patch("protea.api.app.build_session_factory", return_value=MagicMock()):
            app = create_app(Path("/fake/root"))

        routes = [r.path for r in app.routes]
        assert "/health" in routes
        assert "/health/ready" in routes

    def test_health_endpoint_returns_ok(self):
        """GET /health returns 200 with status ok."""
        from fastapi.testclient import TestClient
        from protea.api.app import create_app

        mock_settings = MagicMock()
        mock_settings.db_url = "sqlite:///:memory:"
        mock_settings.amqp_url = "amqp://guest:guest@localhost/"

        with patch("protea.api.app.load_settings", return_value=mock_settings), \
             patch("protea.api.app.build_session_factory", return_value=MagicMock()):
            app = create_app(Path("/fake/root"))

        client = TestClient(app)
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json() == {"status": "ok"}

    def test_readiness_check_succeeds(self):
        """GET /health/ready returns 200 when DB and RabbitMQ are reachable."""
        from fastapi.testclient import TestClient
        from protea.api.app import create_app

        mock_settings = MagicMock()
        mock_settings.db_url = "sqlite:///:memory:"
        mock_settings.amqp_url = "amqp://guest:guest@localhost/"

        mock_factory = MagicMock()
        mock_session = MagicMock()
        mock_factory.return_value = mock_session
        mock_session.__enter__ = lambda s: s
        mock_session.__exit__ = MagicMock(return_value=False)

        with patch("protea.api.app.load_settings", return_value=mock_settings), \
             patch("protea.api.app.build_session_factory", return_value=mock_factory):
            app = create_app(Path("/fake/root"))

        mock_conn = MagicMock()
        with patch("protea.infrastructure.session.session_scope") as mock_scope, \
             patch("pika.BlockingConnection", return_value=mock_conn) as mock_pika:
            mock_scope.return_value.__enter__ = lambda s: mock_session
            mock_scope.return_value.__exit__ = MagicMock(return_value=False)
            client = TestClient(app)
            resp = client.get("/health/ready")

        assert resp.status_code == 200
        assert resp.json() == {"status": "ready"}

    def test_readiness_check_fails_when_rabbitmq_down(self):
        """GET /health/ready returns 503 when RabbitMQ is unreachable."""
        from fastapi.testclient import TestClient
        from protea.api.app import create_app

        mock_settings = MagicMock()
        mock_settings.db_url = "sqlite:///:memory:"
        mock_settings.amqp_url = "amqp://guest:guest@localhost/"

        mock_factory = MagicMock()
        mock_session = MagicMock()
        mock_factory.return_value = mock_session
        mock_session.__enter__ = lambda s: s
        mock_session.__exit__ = MagicMock(return_value=False)

        with patch("protea.api.app.load_settings", return_value=mock_settings), \
             patch("protea.api.app.build_session_factory", return_value=mock_factory):
            app = create_app(Path("/fake/root"))

        with patch("protea.infrastructure.session.session_scope") as mock_scope, \
             patch("pika.BlockingConnection", side_effect=Exception("Connection refused")):
            mock_scope.return_value.__enter__ = lambda s: mock_session
            mock_scope.return_value.__exit__ = MagicMock(return_value=False)
            client = TestClient(app)
            resp = client.get("/health/ready")

        assert resp.status_code == 503
        assert "RabbitMQ unreachable" in resp.json()["detail"]

    def test_project_root_defaults_to_parents_2(self):
        """When project_root is None, it defaults to Path(__file__).parents[2]."""
        from protea.api.app import create_app

        mock_settings = MagicMock()
        mock_settings.db_url = "sqlite:///:memory:"
        mock_settings.amqp_url = "amqp://guest:guest@localhost/"

        with patch("protea.api.app.load_settings", return_value=mock_settings) as mock_load, \
             patch("protea.api.app.build_session_factory", return_value=MagicMock()):
            app = create_app()  # project_root=None

        # load_settings should have been called with the resolved parents[2] path
        called_root = mock_load.call_args[0][0]
        assert isinstance(called_root, Path)
        assert called_root.is_absolute()

    def test_sphinx_mount_when_directory_exists(self, tmp_path):
        """When docs/build/html exists, /sphinx is mounted."""
        from protea.api.app import create_app

        sphinx_dir = tmp_path / "docs" / "build" / "html"
        sphinx_dir.mkdir(parents=True)
        (sphinx_dir / "index.html").write_text("<html></html>")

        mock_settings = MagicMock()
        mock_settings.db_url = "sqlite:///:memory:"
        mock_settings.amqp_url = "amqp://guest:guest@localhost/"

        with patch("protea.api.app.load_settings", return_value=mock_settings), \
             patch("protea.api.app.build_session_factory", return_value=MagicMock()):
            app = create_app(project_root=tmp_path)

        route_paths = [r.path for r in app.routes]
        assert any("/sphinx" in p for p in route_paths)

    def test_static_mount_when_directory_exists(self, tmp_path):
        """When static/ exists, /static is mounted."""
        from protea.api.app import create_app

        static_dir = tmp_path / "static"
        static_dir.mkdir()
        (static_dir / "test.txt").write_text("hello")

        mock_settings = MagicMock()
        mock_settings.db_url = "sqlite:///:memory:"
        mock_settings.amqp_url = "amqp://guest:guest@localhost/"

        with patch("protea.api.app.load_settings", return_value=mock_settings), \
             patch("protea.api.app.build_session_factory", return_value=MagicMock()):
            app = create_app(project_root=tmp_path)

        route_paths = [r.path for r in app.routes]
        assert any("/static" in p for p in route_paths)
