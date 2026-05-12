"""Postgres-backed integration tests for ``/auth/api-keys`` (T5.6a).

Exercises the full router against a real database engine via the
``--with-postgres`` fixture. Covers create / list / revoke and the
end-to-end behaviour of :func:`require_api_key` mounted on a protected
endpoint: missing key → 401, valid key → 200, revoked key → 401.
"""

from __future__ import annotations

import pytest
from fastapi import APIRouter, Depends, FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import protea.infrastructure.orm.models  # noqa: F401 — import-side-effect: register tables
from protea.api.auth import require_api_key
from protea.api.routers.auth_api_keys import router as auth_router
from protea.infrastructure.orm.base import Base
from protea.infrastructure.orm.models.api_key import ApiKey


@pytest.fixture()
def app_client(postgres_url: str, monkeypatch):
    """Build a fresh schema + a FastAPI app wired to the live engine."""
    monkeypatch.setenv("PROTEA_AUTHN_REQUIRED", "true")

    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, future=True)

    app = FastAPI()
    app.state.session_factory = factory
    app.include_router(auth_router)

    # Protected endpoint to exercise the dependency end-to-end.
    protected = APIRouter()

    @protected.get("/protected", dependencies=[Depends(require_api_key)])
    def _protected():
        return {"ok": True}

    app.include_router(protected)

    client = TestClient(app, raise_server_exceptions=True)
    yield client, factory
    Base.metadata.drop_all(engine)


@pytest.mark.integration
class TestApiKeyLifecycle:
    def test_create_returns_raw_key_once(self, app_client):
        client, _ = app_client
        resp = client.post("/auth/api-keys", json={"name": "lab-runner"})
        assert resp.status_code == 201, resp.text
        body = resp.json()
        assert body["name"] == "lab-runner"
        assert len(body["prefix"]) == 8
        assert body["key"].startswith(body["prefix"])
        assert body["revoked_at"] is None

    def test_list_hides_revoked_by_default(self, app_client):
        client, _ = app_client
        # Mint two keys, revoke one.
        a = client.post("/auth/api-keys", json={"name": "alpha"}).json()
        b = client.post("/auth/api-keys", json={"name": "beta"}).json()
        client.delete(f"/auth/api-keys/{b['id']}")

        rows = client.get("/auth/api-keys").json()
        names = [r["name"] for r in rows]
        assert "alpha" in names
        assert "beta" not in names

        # include_revoked=true brings beta back.
        rows_all = client.get("/auth/api-keys?include_revoked=true").json()
        names_all = [r["name"] for r in rows_all]
        assert {"alpha", "beta"} <= set(names_all)
        # And the response never leaks the key.
        for r in rows_all:
            assert "key" not in r
            assert "key_hash" not in r
        assert a["id"] != b["id"]

    def test_revoke_is_idempotent(self, app_client):
        client, _ = app_client
        k = client.post("/auth/api-keys", json={"name": "double-revoke"}).json()
        r1 = client.delete(f"/auth/api-keys/{k['id']}")
        assert r1.status_code == 200
        assert r1.json()["revoked_at"] is not None
        # Second call returns the same shape, status 200.
        r2 = client.delete(f"/auth/api-keys/{k['id']}")
        assert r2.status_code == 200
        assert r2.json()["revoked_at"] == r1.json()["revoked_at"]

    def test_revoke_unknown_returns_404(self, app_client):
        client, _ = app_client
        resp = client.delete("/auth/api-keys/00000000-0000-0000-0000-000000000000")
        assert resp.status_code == 404


@pytest.mark.integration
class TestProtectedEndpoint:
    def test_missing_key_returns_401(self, app_client):
        client, _ = app_client
        resp = client.get("/protected")
        assert resp.status_code == 401
        assert resp.headers["www-authenticate"] == "ApiKey"

    def test_valid_key_returns_200(self, app_client):
        client, _ = app_client
        k = client.post("/auth/api-keys", json={"name": "good"}).json()
        # Authorization header path.
        resp1 = client.get(
            "/protected", headers={"Authorization": f"ApiKey {k['key']}"}
        )
        assert resp1.status_code == 200, resp1.text
        # X-Api-Key header path also works.
        resp2 = client.get("/protected", headers={"X-Api-Key": k["key"]})
        assert resp2.status_code == 200

    def test_invalid_key_returns_401(self, app_client):
        client, _ = app_client
        resp = client.get("/protected", headers={"X-Api-Key": "not-a-real-key"})
        assert resp.status_code == 401

    def test_revoked_key_returns_401(self, app_client):
        client, _ = app_client
        k = client.post("/auth/api-keys", json={"name": "to-revoke"}).json()
        client.delete(f"/auth/api-keys/{k['id']}")
        resp = client.get("/protected", headers={"X-Api-Key": k["key"]})
        assert resp.status_code == 401

    def test_valid_key_updates_last_used_at(self, app_client):
        client, factory = app_client
        k = client.post("/auth/api-keys", json={"name": "ticked"}).json()
        client.get("/protected", headers={"X-Api-Key": k["key"]})

        # Background task ran by the time TestClient returns
        # (Starlette runs sync background tasks before yielding control
        # back to the caller).
        with factory() as session:
            row = (
                session.query(ApiKey).filter(ApiKey.prefix == k["prefix"]).one()
            )
            assert row.last_used_at is not None
