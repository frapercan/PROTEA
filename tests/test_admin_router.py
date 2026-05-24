"""Unit tests for the /admin router (FARM-AUTH.4).

Auth is now enforced by ``require_role("admin")`` via the FEAT-AUTH JWT
flow. Database and subprocess calls are fully mocked -- no real
infrastructure required.
"""

from __future__ import annotations

import sys
import time
from unittest.mock import MagicMock, patch

import jwt
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from protea.api.routers.admin import router

_SECRET = "feat-auth-test-secret-padding-padding-padding"  # >32 bytes
_ALG = "HS256"


def _mint(role: str | None, *, ttl: int = 60) -> str:
    now = int(time.time())
    payload: dict[str, object] = {
        "sub": "00000000-0000-0000-0000-000000000001",
        "iat": now,
        "exp": now + ttl,
    }
    if role is not None:
        payload["role"] = role
    return jwt.encode(payload, _SECRET, algorithm=_ALG)


def _make_app(monkeypatch) -> FastAPI:
    monkeypatch.setenv("PROTEA_AUTHN_REQUIRED", "true")
    monkeypatch.setenv("PROTEA_JWT_SECRET", _SECRET)
    app = FastAPI()
    app.state.session_factory = MagicMock()
    app.include_router(router)
    return app


# ---------------------------------------------------------------------------
# Helpers
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


# ---------------------------------------------------------------------------
# POST /admin/reset-db -- role gate
# ---------------------------------------------------------------------------


class TestResetDBAuth:
    def test_unauthenticated_returns_401(self, monkeypatch):
        app = _make_app(monkeypatch)
        client = TestClient(app)
        resp = client.post("/admin/reset-db")
        assert resp.status_code == 401

    def test_operator_jwt_rejected_with_403(self, monkeypatch):
        app = _make_app(monkeypatch)
        client = TestClient(app)
        resp = client.post(
            "/admin/reset-db",
            headers={"Authorization": f"Bearer {_mint('operator')}"},
        )
        assert resp.status_code == 403
        assert "insufficient" in resp.json()["detail"]

    def test_viewer_jwt_rejected_with_403(self, monkeypatch):
        app = _make_app(monkeypatch)
        client = TestClient(app)
        resp = client.post(
            "/admin/reset-db",
            headers={"Authorization": f"Bearer {_mint('viewer')}"},
        )
        assert resp.status_code == 403

    def test_invalid_token_rejected_with_401(self, monkeypatch):
        app = _make_app(monkeypatch)
        client = TestClient(app)
        resp = client.post(
            "/admin/reset-db",
            headers={"Authorization": "Bearer not-a-real-jwt"},
        )
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# POST /admin/reset-db -- success path (admin role)
# ---------------------------------------------------------------------------


class TestResetDB:
    @patch("protea.api.routers.admin.build_session_factory")
    @patch("protea.api.routers.admin.subprocess.run")
    @patch("protea.api.routers.admin.load_settings")
    def test_reset_db_success(self, mock_settings, mock_run, mock_build, monkeypatch, mock_psycopg):
        mock_psycopg_mod, conn_ctx = mock_psycopg
        app = _make_app(monkeypatch)
        settings = MagicMock()
        settings.db_url = "postgresql+psycopg://u:p@localhost/db"
        mock_settings.return_value = settings
        mock_run.return_value = MagicMock(returncode=0)
        mock_build.return_value = MagicMock()

        client = TestClient(app)
        resp = client.post(
            "/admin/reset-db",
            headers={"Authorization": f"Bearer {_mint('admin')}"},
        )
        assert resp.status_code == 200
        assert resp.json()["ok"] is True
        mock_build.assert_called_once()

    @patch("protea.api.routers.admin.build_session_factory")
    @patch("protea.api.routers.admin.subprocess.run")
    @patch("protea.api.routers.admin.load_settings")
    def test_reset_db_migration_failure(self, mock_settings, mock_run, mock_build, monkeypatch, mock_psycopg):
        mock_psycopg_mod, conn_ctx = mock_psycopg
        app = _make_app(monkeypatch)
        settings = MagicMock()
        settings.db_url = "postgresql+psycopg://u:p@localhost/db"
        mock_settings.return_value = settings
        mock_run.return_value = MagicMock(returncode=1, stderr="migration error")

        client = TestClient(app)
        resp = client.post(
            "/admin/reset-db",
            headers={"Authorization": f"Bearer {_mint('admin')}"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is False
        assert "migration error" in data["error"]
        mock_build.assert_not_called()

    @patch("protea.api.routers.admin.build_session_factory")
    @patch("protea.api.routers.admin.subprocess.run")
    @patch("protea.api.routers.admin.load_settings")
    def test_reset_db_drops_and_recreates_schema(self, mock_settings, mock_run, mock_build, monkeypatch, mock_psycopg):
        mock_psycopg_mod, conn_ctx = mock_psycopg
        app = _make_app(monkeypatch)
        settings = MagicMock()
        settings.db_url = "postgresql+psycopg://u:p@localhost/db"
        mock_settings.return_value = settings
        mock_run.return_value = MagicMock(returncode=0)

        client = TestClient(app)
        resp = client.post(
            "/admin/reset-db",
            headers={"Authorization": f"Bearer {_mint('admin')}"},
        )
        assert resp.status_code == 200
        conn_ctx.execute.assert_any_call("DROP SCHEMA public CASCADE")
        conn_ctx.execute.assert_any_call("CREATE SCHEMA public")

    @patch("protea.api.routers.admin.build_session_factory")
    @patch("protea.api.routers.admin.subprocess.run")
    @patch("protea.api.routers.admin.load_settings")
    def test_reset_db_replaces_psycopg_in_url(self, mock_settings, mock_run, mock_build, monkeypatch, mock_psycopg):
        mock_psycopg_mod, conn_ctx = mock_psycopg
        app = _make_app(monkeypatch)
        settings = MagicMock()
        settings.db_url = "postgresql+psycopg://u:p@localhost/db"
        mock_settings.return_value = settings
        mock_run.return_value = MagicMock(returncode=0)

        client = TestClient(app)
        resp = client.post(
            "/admin/reset-db",
            headers={"Authorization": f"Bearer {_mint('admin')}"},
        )
        assert resp.status_code == 200
        mock_psycopg_mod.connect.assert_called_once_with(
            "postgresql://u:p@localhost/db", autocommit=True
        )
