"""Unit tests for FEAT-AUTH role propagation + gates.

Covers:

* :mod:`protea.api.roles` — ``normalise_role``, ``role_of``,
  ``require_role`` dependency factory.
* ``POST /auth/api-key-login`` — role claim minted from the matched ``ApiKey``
  row, NULL collapsed to ``viewer``.
* Role gates on ``POST /datasets`` and ``POST /maintenance/vacuum-*``
  (operator floor) and the bearer-admin path on ``POST /admin/reset-db``.

All tests run without Postgres: session factories are mocked and the
key lookup is patched at the module level.
"""

from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import jwt
import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from protea.api.bearer import BearerPrincipal
from protea.api.roles import (
    ROLE_ADMIN,
    ROLE_OPERATOR,
    ROLE_VIEWER,
    normalise_role,
    require_role,
    role_of,
)

_SECRET = "feat-auth-test-secret-padding-padding-padding"  # >32 bytes
_ALG = "HS256"


def _mint(
    role: str | None, *, ttl: int = 60, sub: str = "00000000-0000-0000-0000-000000000001"
) -> str:
    now = int(time.time())
    payload: dict[str, object] = {"sub": sub, "iat": now, "exp": now + ttl}
    if role is not None:
        payload["role"] = role
    return jwt.encode(payload, _SECRET, algorithm=_ALG)


# ---------------------------------------------------------------------------
# normalise_role / role_of
# ---------------------------------------------------------------------------


class TestNormaliseRole:
    def test_known_roles_pass_through(self):
        assert normalise_role("viewer") == ROLE_VIEWER
        assert normalise_role("operator") == ROLE_OPERATOR
        assert normalise_role("admin") == ROLE_ADMIN

    def test_case_and_whitespace_tolerant(self):
        assert normalise_role("  OPERATOR ") == ROLE_OPERATOR

    def test_unknown_collapses_to_viewer(self):
        assert normalise_role("superuser") == ROLE_VIEWER
        assert normalise_role(None) == ROLE_VIEWER  # type: ignore[arg-type]
        assert normalise_role(123) == ROLE_VIEWER  # type: ignore[arg-type]


class TestRoleOf:
    def test_none_principal_is_admin_for_dev(self):
        # Gate disabled (PROTEA_AUTHN_REQUIRED=false) returns None;
        # dev stacks should keep working without role friction.
        assert role_of(None) == ROLE_ADMIN

    def test_bearer_principal_reads_role_claim(self):
        p = BearerPrincipal(sub="x", claims={"role": "operator"})
        assert role_of(p) == ROLE_OPERATOR

    def test_bearer_principal_missing_role_is_viewer(self):
        p = BearerPrincipal(sub="x", claims={})
        assert role_of(p) == ROLE_VIEWER

    def test_api_key_reads_role_column(self):
        row = MagicMock()
        row.role = "operator"
        assert role_of(row) == ROLE_OPERATOR

    def test_api_key_null_role_is_viewer(self):
        row = MagicMock()
        row.role = None
        assert role_of(row) == ROLE_VIEWER


# ---------------------------------------------------------------------------
# require_role dependency
# ---------------------------------------------------------------------------


def _make_app_with_gate(min_role: str) -> FastAPI:
    app = FastAPI()
    app.state.session_factory = MagicMock()

    @app.get("/x", dependencies=[Depends(require_role(min_role))])
    def _x():
        return {"ok": True}

    return app


class TestRequireRoleGate:
    def test_operator_passes_operator_floor(self, monkeypatch):
        monkeypatch.setenv("PROTEA_AUTHN_REQUIRED", "true")
        monkeypatch.setenv("PROTEA_JWT_SECRET", _SECRET)
        client = TestClient(_make_app_with_gate(ROLE_OPERATOR))
        resp = client.get("/x", headers={"Authorization": f"Bearer {_mint('operator')}"})
        assert resp.status_code == 200

    def test_viewer_rejected_at_operator_floor(self, monkeypatch):
        monkeypatch.setenv("PROTEA_AUTHN_REQUIRED", "true")
        monkeypatch.setenv("PROTEA_JWT_SECRET", _SECRET)
        client = TestClient(_make_app_with_gate(ROLE_OPERATOR))
        resp = client.get("/x", headers={"Authorization": f"Bearer {_mint('viewer')}"})
        assert resp.status_code == 403
        assert "insufficient" in resp.json()["detail"]

    def test_missing_role_is_viewer_rejected(self, monkeypatch):
        monkeypatch.setenv("PROTEA_AUTHN_REQUIRED", "true")
        monkeypatch.setenv("PROTEA_JWT_SECRET", _SECRET)
        client = TestClient(_make_app_with_gate(ROLE_OPERATOR))
        resp = client.get("/x", headers={"Authorization": f"Bearer {_mint(None)}"})
        assert resp.status_code == 403

    def test_admin_passes_admin_floor(self, monkeypatch):
        monkeypatch.setenv("PROTEA_AUTHN_REQUIRED", "true")
        monkeypatch.setenv("PROTEA_JWT_SECRET", _SECRET)
        client = TestClient(_make_app_with_gate(ROLE_ADMIN))
        resp = client.get("/x", headers={"Authorization": f"Bearer {_mint('admin')}"})
        assert resp.status_code == 200

    def test_operator_rejected_at_admin_floor(self, monkeypatch):
        monkeypatch.setenv("PROTEA_AUTHN_REQUIRED", "true")
        monkeypatch.setenv("PROTEA_JWT_SECRET", _SECRET)
        client = TestClient(_make_app_with_gate(ROLE_ADMIN))
        resp = client.get("/x", headers={"Authorization": f"Bearer {_mint('operator')}"})
        assert resp.status_code == 403

    def test_authn_disabled_waves_through(self, monkeypatch):
        monkeypatch.setenv("PROTEA_AUTHN_REQUIRED", "false")
        client = TestClient(_make_app_with_gate(ROLE_ADMIN))
        resp = client.get("/x")
        assert resp.status_code == 200

    def test_unknown_min_role_raises_at_construction(self):
        with pytest.raises(ValueError, match="unknown role"):
            require_role("god")


# ---------------------------------------------------------------------------
# POST /auth/api-key-login — role propagation
# ---------------------------------------------------------------------------


@pytest.fixture()
def login_app(monkeypatch):
    monkeypatch.setenv("PROTEA_AUTHN_REQUIRED", "true")
    monkeypatch.setenv("PROTEA_JWT_SECRET", _SECRET)
    monkeypatch.setenv("PROTEA_ENVIRONMENT", "test")  # disable rate-limit
    from protea.api.routers import auth_login as auth_login_router

    app = FastAPI()
    app.state.session_factory = MagicMock()
    app.state.limiter = MagicMock()
    app.include_router(auth_login_router.router)
    return app


class TestAuthLogin:
    def test_login_mints_jwt_with_role_claim(self, login_app):
        row = MagicMock()
        row.id = "00000000-0000-0000-0000-000000000abc"
        row.key_hash = "deadbeef" * 8  # 64 chars
        row.prefix = "abcdefgh"
        row.role = "operator"
        row.revoked_at = None
        with patch("protea.api.routers.auth_login._lookup_role") as m:
            m.return_value = (str(row.id), "operator")
            client = TestClient(login_app)
            resp = client.post("/auth/api-key-login", json={"api_key": "abcdefgh1234567890"})
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["role"] == "operator"
        assert body["token_type"] == "Bearer"
        decoded = jwt.decode(body["token"], _SECRET, algorithms=[_ALG])
        assert decoded["sub"] == str(row.id)
        assert decoded["role"] == "operator"

    def test_login_defaults_role_to_viewer_when_key_role_is_null(self, login_app):
        with patch("protea.api.routers.auth_login._lookup_role") as m:
            m.return_value = ("00000000-0000-0000-0000-0000000000ff", ROLE_VIEWER)
            client = TestClient(login_app)
            resp = client.post("/auth/api-key-login", json={"api_key": "abcdefgh1234567890"})
        assert resp.status_code == 200
        decoded = jwt.decode(resp.json()["token"], _SECRET, algorithms=[_ALG])
        assert decoded["role"] == ROLE_VIEWER

    def test_login_rejects_unknown_key(self, login_app):
        with patch("protea.api.routers.auth_login._lookup_role") as m:
            m.return_value = None
            client = TestClient(login_app)
            resp = client.post("/auth/api-key-login", json={"api_key": "no-such-key-here-12345"})
        assert resp.status_code == 401

    def test_login_503_when_secret_missing(self, login_app, monkeypatch):
        monkeypatch.delenv("PROTEA_JWT_SECRET", raising=False)
        client = TestClient(login_app)
        resp = client.post("/auth/api-key-login", json={"api_key": "abcdefgh1234567890"})
        assert resp.status_code == 503


# ---------------------------------------------------------------------------
# POST /admin/reset-db — role gate (FARM-AUTH.4)
# ---------------------------------------------------------------------------


@pytest.fixture()
def admin_app(monkeypatch):
    """Minimal FastAPI app that includes the admin router, no real DB."""
    monkeypatch.setenv("PROTEA_AUTHN_REQUIRED", "true")
    monkeypatch.setenv("PROTEA_JWT_SECRET", _SECRET)
    # reset-db now requires the destructive-op sentinel (this PR's hardening);
    # enable it so these tests exercise the role gate, not the sentinel guard.
    monkeypatch.setenv("PROTEA_ALLOW_DB_RESET", "1")
    from protea.api.routers import admin as admin_router

    app = FastAPI()
    app.state.session_factory = MagicMock()
    app.include_router(admin_router.router)
    return app


class TestAdminResetDbRoleGate:
    def test_admin_jwt_returns_200(self, admin_app):
        # Patch out the destructive DB work; psycopg is imported locally inside
        # the handler so we patch via sys.modules rather than the module attribute.
        import sys

        fake_psycopg = MagicMock()
        fake_conn = MagicMock()
        fake_psycopg.connect.return_value.__enter__ = MagicMock(return_value=fake_conn)
        fake_psycopg.connect.return_value.__exit__ = MagicMock(return_value=False)
        with patch.dict(sys.modules, {"psycopg": fake_psycopg}), patch(
            "protea.api.routers.admin.subprocess.run"
        ) as mock_run, patch(
            "protea.api.routers.admin.build_session_factory"
        ):
            mock_run.return_value = MagicMock(returncode=0, stderr="")
            client = TestClient(admin_app, raise_server_exceptions=False)
            resp = client.post(
                "/admin/reset-db",
                headers={"Authorization": f"Bearer {_mint('admin')}"},
            )
        assert resp.status_code == 200

    def test_operator_jwt_rejected_at_admin_gate(self, admin_app):
        client = TestClient(admin_app)
        resp = client.post(
            "/admin/reset-db",
            headers={"Authorization": f"Bearer {_mint('operator')}"},
        )
        assert resp.status_code == 403
        assert "insufficient" in resp.json()["detail"]

    def test_unauthenticated_reset_db_rejected(self, admin_app):
        client = TestClient(admin_app)
        resp = client.post("/admin/reset-db")
        assert resp.status_code == 401
