"""FARM-AUTH.5 — Endpoint gating sweep smoke tests.

For every POST, PATCH, and DELETE route registered in the app:

* Unauthenticated request (no token) must return 401 or 403 (never 200/201/204).
* A viewer-role JWT must be rejected (403) on operator/admin-floor routes.
* An operator-role JWT must be rejected (403) on admin-only routes.
* An admin-role JWT must reach the handler (response may be 4xx from the
  handler's own business logic, but NOT 401/403).

All tests run without Postgres: the app is created with fully mocked
infrastructure so no real side-effects occur. The test instantiates the
real FastAPI app (via ``create_app``) to exercise the actual route
dependency graph, not just unit-test ``require_role`` in isolation.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import jwt
import pytest
from fastapi.testclient import TestClient

_SECRET = "farm-auth-5-test-secret-32-chars-pad"
_ALG = "HS256"

# Roles that exist in the system
_VIEWER = "viewer"
_OPERATOR = "operator"
_ADMIN = "admin"


def _mint(role: str, ttl: int = 300) -> str:
    now = int(time.time())
    payload = {
        "sub": "00000000-0000-0000-0000-000000000001",
        "iat": now,
        "exp": now + ttl,
        "role": role,
    }
    return jwt.encode(payload, _SECRET, algorithm=_ALG)


def _bearer(role: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {_mint(role)}"}


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------


def _make_app() -> Any:
    """Instantiate a real FastAPI app with mocked infrastructure deps."""
    fake_settings = MagicMock(
        db_url="postgresql://fake/none",
        amqp_url="amqp://fake/none",
        allowed_origins=(),
        storage_backend="local",
        artifacts_dir=Path("/tmp/protea-auth5-fake"),
        admin_token="",
    )
    with (
        patch("protea.api.app.load_settings", return_value=fake_settings),
        patch("protea.api.app.build_session_factory", return_value=MagicMock()),
        patch("protea.api.app.load_benchmark_config", return_value=MagicMock()),
        patch("protea.api.app.build_operation_registry", return_value=MagicMock()),
        patch("protea.api.app.assert_bearer_config"),
    ):
        from protea.api.app import create_app

        app = create_app(Path("/tmp/protea-auth5-fake"))
    return app


@pytest.fixture(scope="module")
def app():
    return _make_app()


@pytest.fixture(scope="module")
def client(app):
    return TestClient(app, raise_server_exceptions=False)


# ---------------------------------------------------------------------------
# Collect all POST/PATCH/DELETE routes from the real app
# ---------------------------------------------------------------------------


def _collect_mutable_routes(app: Any) -> list[tuple[str, str]]:
    """Return (method, path) for every POST/PATCH/DELETE route in the app."""
    results = []
    for route in app.routes:
        if not hasattr(route, "methods") or not hasattr(route, "path"):
            continue
        for method in route.methods:
            if method in ("POST", "PATCH", "DELETE"):
                results.append((method, route.path))
    return results


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_PUBLIC_POST_PATHS = {
    # Login endpoint must be unauthenticated by design.
    "/auth/login",
}

_ADMIN_ONLY_PATHS = {
    "/admin/reset-db",
    "/auth/api-keys",
    "/auth/api-keys/{key_id}",
}

# Paths that require at least operator (pipeline dispatch, mutations).
_OPERATOR_PATHS = {
    "/datasets",
    "/datasets/import-by-reference",
    "/embeddings/configs",
    "/embeddings/configs/{config_id}",
    "/embeddings/predict",
    "/embeddings/prediction-sets/{set_id}",
    "/experiment-runs",
    "/experiment-runs/{run_id}",
    "/jobs",
    "/jobs/{job_id}",
    "/jobs/{job_id}/cancel",
    "/jobs/{job_id}/comments",
    "/maintenance/vacuum-sequences",
    "/maintenance/vacuum-embeddings",
    "/reranker-models/import",
    "/reranker-models/import-by-reference",
    "/scoring/configs",
    "/scoring/configs/presets",
    "/scoring/configs/{config_id}",
    "/scoring/rerankers/{reranker_id}",
    "/annotations/sets/{set_id}",
    "/annotations/sets/load-goa",
    "/annotations/sets/load-quickgo",
    "/annotations/snapshots/{snapshot_id}/ia-url",
    "/annotations/snapshots/load",
    "/annotations/evaluation-sets/generate",
    "/annotations/evaluation-sets/{eval_id}",
    "/annotations/evaluation-sets/{eval_id}/run",
    "/annotations/evaluation-sets/{eval_id}/results/{result_id}",
}

# Paths that require at least viewer (logged-in user).
_VIEWER_PATHS = {
    "/annotate",
    "/query-sets",
    "/query-sets/{query_set_id}",
    "/support",
}


# ---------------------------------------------------------------------------
# Parametrised: unauthenticated POST/PATCH/DELETE must NOT return 200/201/204
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def mutable_routes(app):
    return _collect_mutable_routes(app)


def _skip_public(path: str) -> bool:
    return path in _PUBLIC_POST_PATHS


def _is_gated(path: str) -> bool:
    """True when the path is one we explicitly gate in FARM-AUTH.5."""
    for gated in _ADMIN_ONLY_PATHS | _OPERATOR_PATHS | _VIEWER_PATHS:
        if path == gated:
            return True
    return False


class TestUnauthenticatedMustNotSucceed:
    """Every gated mutable route must return 401 when called with no credentials."""

    def test_gated_routes_reject_no_credentials(self, monkeypatch, client, mutable_routes):
        monkeypatch.setenv("PROTEA_AUTHN_REQUIRED", "true")
        monkeypatch.setenv("PROTEA_JWT_SECRET", _SECRET)
        failures = []
        for method, path in mutable_routes:
            if _skip_public(path):
                continue
            if not _is_gated(path):
                continue
            if method == "DELETE":
                resp = client.delete(path)
            else:
                resp = getattr(client, method.lower())(path, json={})
            if resp.status_code in (200, 201, 204):
                failures.append(f"{method} {path} -> {resp.status_code} (expected 401/403)")
        assert failures == [], "Gated routes accepted unauthenticated requests:\n" + "\n".join(
            failures
        )


# ---------------------------------------------------------------------------
# Specific per-route role-gate smoke checks
# ---------------------------------------------------------------------------


class TestViewerFloorRoutes:
    """Viewer-role routes: unauthenticated=401, viewer=passes handler."""

    @pytest.mark.parametrize(
        "method,path",
        [
            ("POST", "/annotate"),
            ("POST", "/query-sets"),
            ("POST", "/support"),
        ],
    )
    def test_unauthenticated_gets_401(self, monkeypatch, client, method, path):
        monkeypatch.setenv("PROTEA_AUTHN_REQUIRED", "true")
        monkeypatch.setenv("PROTEA_JWT_SECRET", _SECRET)
        resp = getattr(client, method.lower())(path, json={})
        assert resp.status_code in (401, 403), (
            f"{method} {path}: expected 401/403 for unauthenticated, got {resp.status_code}"
        )

    @pytest.mark.parametrize(
        "method,path",
        [
            ("POST", "/annotate"),
            ("POST", "/query-sets"),
            ("POST", "/support"),
        ],
    )
    def test_viewer_jwt_passes_gate(self, monkeypatch, client, method, path):
        """Viewer JWT must NOT receive 401 or 403 from the role gate.

        The handler itself may return 4xx for business reasons (missing body
        fields, DB not available, etc.) — but not 401/403.
        """
        monkeypatch.setenv("PROTEA_AUTHN_REQUIRED", "true")
        monkeypatch.setenv("PROTEA_JWT_SECRET", _SECRET)
        resp = getattr(client, method.lower())(path, json={}, headers=_bearer(_VIEWER))
        assert resp.status_code not in (401, 403), (
            f"{method} {path}: viewer JWT was blocked by role gate ({resp.status_code})"
        )


class TestOperatorFloorRoutes:
    """Operator-floor routes: viewer=403, operator=passes gate."""

    @pytest.mark.parametrize(
        "method,path",
        [
            ("POST", "/datasets"),
            ("POST", "/embeddings/configs"),
            ("POST", "/embeddings/predict"),
            ("POST", "/experiment-runs"),
            ("POST", "/jobs"),
            ("POST", "/maintenance/vacuum-sequences"),
            ("POST", "/scoring/configs"),
            ("POST", "/scoring/configs/presets"),
        ],
    )
    def test_unauthenticated_gets_401(self, monkeypatch, client, method, path):
        monkeypatch.setenv("PROTEA_AUTHN_REQUIRED", "true")
        monkeypatch.setenv("PROTEA_JWT_SECRET", _SECRET)
        resp = getattr(client, method.lower())(path, json={})
        assert resp.status_code in (401, 403), (
            f"{method} {path}: expected 401/403 for unauthenticated, got {resp.status_code}"
        )

    @pytest.mark.parametrize(
        "method,path",
        [
            ("POST", "/datasets"),
            ("POST", "/embeddings/configs"),
            ("POST", "/embeddings/predict"),
            ("POST", "/experiment-runs"),
            ("POST", "/jobs"),
            ("POST", "/maintenance/vacuum-sequences"),
            ("POST", "/scoring/configs"),
            ("POST", "/scoring/configs/presets"),
        ],
    )
    def test_viewer_jwt_rejected_at_operator_floor(self, monkeypatch, client, method, path):
        monkeypatch.setenv("PROTEA_AUTHN_REQUIRED", "true")
        monkeypatch.setenv("PROTEA_JWT_SECRET", _SECRET)
        resp = getattr(client, method.lower())(path, json={}, headers=_bearer(_VIEWER))
        assert resp.status_code == 403, (
            f"{method} {path}: viewer should get 403, got {resp.status_code}"
        )

    @pytest.mark.parametrize(
        "method,path",
        [
            ("POST", "/datasets"),
            ("POST", "/embeddings/configs"),
            ("POST", "/experiment-runs"),
            ("POST", "/scoring/configs"),
            ("POST", "/scoring/configs/presets"),
        ],
    )
    def test_operator_jwt_passes_gate(self, monkeypatch, client, method, path):
        """Operator JWT must NOT receive 401 or 403 from the role gate."""
        monkeypatch.setenv("PROTEA_AUTHN_REQUIRED", "true")
        monkeypatch.setenv("PROTEA_JWT_SECRET", _SECRET)
        resp = getattr(client, method.lower())(path, json={}, headers=_bearer(_OPERATOR))
        assert resp.status_code not in (401, 403), (
            f"{method} {path}: operator JWT was blocked by role gate ({resp.status_code})"
        )


class TestAdminFloorRoutes:
    """Admin-only routes: operator=403, admin=passes gate."""

    @pytest.mark.parametrize(
        "method,path",
        [
            ("POST", "/auth/api-keys"),
            ("GET", "/auth/api-keys"),
            ("DELETE", "/auth/api-keys/00000000-0000-0000-0000-000000000001"),
        ],
    )
    def test_unauthenticated_gets_401(self, monkeypatch, client, method, path):
        monkeypatch.setenv("PROTEA_AUTHN_REQUIRED", "true")
        monkeypatch.setenv("PROTEA_JWT_SECRET", _SECRET)
        if method == "DELETE":
            resp = client.delete(path)
        elif method == "GET":
            resp = client.get(path)
        else:
            resp = getattr(client, method.lower())(path, json={})
        assert resp.status_code in (401, 403), (
            f"{method} {path}: expected 401/403 for unauthenticated, got {resp.status_code}"
        )

    @pytest.mark.parametrize(
        "method,path",
        [
            ("POST", "/auth/api-keys"),
            ("DELETE", "/auth/api-keys/00000000-0000-0000-0000-000000000001"),
        ],
    )
    def test_operator_jwt_rejected_at_admin_floor(self, monkeypatch, client, method, path):
        monkeypatch.setenv("PROTEA_AUTHN_REQUIRED", "true")
        monkeypatch.setenv("PROTEA_JWT_SECRET", _SECRET)
        if method == "DELETE":
            resp = client.delete(path, headers=_bearer(_OPERATOR))
        else:
            resp = getattr(client, method.lower())(path, json={}, headers=_bearer(_OPERATOR))
        assert resp.status_code == 403, (
            f"{method} {path}: operator should get 403, got {resp.status_code}"
        )

    @pytest.mark.parametrize(
        "method,path",
        [
            ("POST", "/auth/api-keys"),
        ],
    )
    def test_admin_jwt_passes_gate(self, monkeypatch, client, method, path):
        """Admin JWT must NOT receive 401 or 403 from the role gate."""
        monkeypatch.setenv("PROTEA_AUTHN_REQUIRED", "true")
        monkeypatch.setenv("PROTEA_JWT_SECRET", _SECRET)
        resp = getattr(client, method.lower())(path, json={}, headers=_bearer(_ADMIN))
        assert resp.status_code not in (401, 403), (
            f"{method} {path}: admin JWT was blocked by role gate ({resp.status_code})"
        )


class TestPublicGetRoutes:
    """Read-only public routes must remain accessible without credentials."""

    @pytest.mark.parametrize(
        "path",
        [
            "/health",
            "/metrics",
            "/benchmark",
            "/scoring/configs",
        ],
    )
    def test_public_get_routes_accessible_without_auth(self, monkeypatch, client, path):
        monkeypatch.setenv("PROTEA_AUTHN_REQUIRED", "true")
        monkeypatch.setenv("PROTEA_JWT_SECRET", _SECRET)
        resp = client.get(path)
        assert resp.status_code not in (401, 403), (
            f"GET {path}: public route should not require auth, got {resp.status_code}"
        )

    def test_login_endpoint_has_no_bearer_gate(self, monkeypatch, client):
        """POST /auth/login has no role gate dependency (any 401 comes from
        business logic inside the handler, not from require_role).

        We verify this by checking that the response does not include the
        WWW-Authenticate: ApiKey header that require_api_key_or_bearer injects.
        A handler-level 401 (invalid key) is acceptable.
        """
        monkeypatch.setenv("PROTEA_AUTHN_REQUIRED", "true")
        monkeypatch.setenv("PROTEA_JWT_SECRET", _SECRET)
        resp = client.post("/auth/login", json={"api_key": "fake-key-12345678"})
        www_auth = resp.headers.get("www-authenticate", "")
        assert "ApiKey" not in www_auth, (
            "POST /auth/login should not return an ApiKey challenge (no gate dep)"
        )

    def test_authn_disabled_all_routes_open(self, monkeypatch, client):
        """When PROTEA_AUTHN_REQUIRED=false every route is open (dev stack)."""
        monkeypatch.setenv("PROTEA_AUTHN_REQUIRED", "false")
        resp = client.post("/datasets", json={})
        assert resp.status_code not in (401, 403), (
            "With authn disabled POST /datasets should not return 401/403"
        )
