"""Unit tests for the /admin router.

Database and subprocess calls are fully mocked -- no real infrastructure required.
"""
from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from protea.api.routers.admin import router


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_app():
    app = FastAPI()
    app.state.session_factory = MagicMock()
    app.include_router(router)
    return app


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def mock_psycopg():
    """Ensure psycopg is available as a mock in sys.modules for the local import."""
    mock_mod = MagicMock()
    conn_ctx = MagicMock()
    mock_mod.connect.return_value.__enter__ = MagicMock(return_value=conn_ctx)
    mock_mod.connect.return_value.__exit__ = MagicMock(return_value=False)
    with patch.dict(sys.modules, {"psycopg": mock_mod}):
        yield mock_mod, conn_ctx


@pytest.fixture()
def client(mock_psycopg):
    app = _make_app()
    with TestClient(app) as c:
        yield c, app, mock_psycopg


# ---------------------------------------------------------------------------
# POST /admin/reset-db
# ---------------------------------------------------------------------------

class TestResetDB:
    @patch("protea.api.routers.admin.build_session_factory")
    @patch("protea.api.routers.admin.subprocess.run")
    @patch("protea.api.routers.admin.load_settings")
    def test_reset_db_success(self, mock_settings, mock_run, mock_build, client):
        c, app, (mock_psycopg_mod, conn_ctx) = client
        settings = MagicMock()
        settings.db_url = "postgresql+psycopg://u:p@localhost/db"
        mock_settings.return_value = settings

        mock_run.return_value = MagicMock(returncode=0)
        mock_build.return_value = MagicMock()

        resp = c.post("/admin/reset-db")
        assert resp.status_code == 200
        assert resp.json()["ok"] is True
        mock_build.assert_called_once()

    @patch("protea.api.routers.admin.build_session_factory")
    @patch("protea.api.routers.admin.subprocess.run")
    @patch("protea.api.routers.admin.load_settings")
    def test_reset_db_migration_failure(self, mock_settings, mock_run, mock_build, client):
        c, app, (mock_psycopg_mod, conn_ctx) = client
        settings = MagicMock()
        settings.db_url = "postgresql+psycopg://u:p@localhost/db"
        mock_settings.return_value = settings

        mock_run.return_value = MagicMock(returncode=1, stderr="migration error")

        resp = c.post("/admin/reset-db")
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is False
        assert "migration error" in data["error"]
        mock_build.assert_not_called()

    @patch("protea.api.routers.admin.build_session_factory")
    @patch("protea.api.routers.admin.subprocess.run")
    @patch("protea.api.routers.admin.load_settings")
    def test_reset_db_drops_and_recreates_schema(self, mock_settings, mock_run, mock_build, client):
        c, app, (mock_psycopg_mod, conn_ctx) = client
        settings = MagicMock()
        settings.db_url = "postgresql+psycopg://u:p@localhost/db"
        mock_settings.return_value = settings

        mock_run.return_value = MagicMock(returncode=0)

        resp = c.post("/admin/reset-db")
        assert resp.status_code == 200
        conn_ctx.execute.assert_any_call("DROP SCHEMA public CASCADE")
        conn_ctx.execute.assert_any_call("CREATE SCHEMA public")

    @patch("protea.api.routers.admin.build_session_factory")
    @patch("protea.api.routers.admin.subprocess.run")
    @patch("protea.api.routers.admin.load_settings")
    def test_reset_db_replaces_psycopg_in_url(self, mock_settings, mock_run, mock_build, client):
        c, app, (mock_psycopg_mod, conn_ctx) = client
        settings = MagicMock()
        settings.db_url = "postgresql+psycopg://u:p@localhost/db"
        mock_settings.return_value = settings

        mock_run.return_value = MagicMock(returncode=0)

        resp = c.post("/admin/reset-db")
        assert resp.status_code == 200
        # Verify psycopg.connect was called with the URL without +psycopg
        mock_psycopg_mod.connect.assert_called_once_with(
            "postgresql://u:p@localhost/db", autocommit=True
        )
