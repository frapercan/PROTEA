"""Admin gating sweep — per-route role-floor audit (PR #567 follow-up).

PR #567 added the client-side gate that hides ``/es/admin/maintenance`` &
co. from non-admin sessions. This module is the matching server-side
guarantee: it enumerates every backend admin route, asserts the JWT
floor it must enforce, and fails when a new endpoint is added without
being declared here. That single-source-of-truth shape is what stops a
future router file from silently shipping a destructive endpoint with
the wrong (or missing) ``require_role``.

The sweep covers three families of routes:

* DESTRUCTIVE (``/admin/reset-db``, ``/admin/dlq/replay``,
  ``/admin/dlq/purge``, ``/maintenance/vacuum-sequences``,
  ``/maintenance/vacuum-embeddings``, ``/auth/api-keys`` lifecycle,
  ``/auth/admin/revoke-sessions/{user_id}``) MUST be gated at
  ``require_role("admin")`` — viewer and operator both get 403.
* OPERATOR-FLOOR READS (``/admin/dlq/summary``) MUST reject viewers but
  let operators through. The DLQ summary is read-only (peek + requeue)
  and is needed by on-call operators to triage backlog before deciding
  whether to escalate to a destructive replay/purge.
* PREVIEW READS (``/maintenance/vacuum-sequences/preview``,
  ``/maintenance/vacuum-embeddings/preview``) are non-destructive and
  intentionally open to viewers — anyone with a session can inspect
  housekeeping counts without being able to act on them.

All tests run without Postgres: the app is created with fully mocked
infrastructure so no real side-effects occur. The real FastAPI app is
exercised (via ``create_app``) so the actual dependency graph is hit,
not just ``require_role`` in isolation.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import jwt
import pytest
from fastapi.testclient import TestClient

_SECRET = "admin-sweep-test-secret-32-chars-pad"
_ALG = "HS256"

_VIEWER = "viewer"
_OPERATOR = "operator"
_ADMIN = "admin"

_FAKE_UUID = "00000000-0000-0000-0000-000000000001"


def _mint(role: str, ttl: int = 300) -> str:
    """Mint an HS256 JWT carrying ``role`` for the duration of one test."""
    now = int(time.time())
    payload = {
        "sub": _FAKE_UUID,
        "iat": now,
        "exp": now + ttl,
        "role": role,
    }
    return jwt.encode(payload, _SECRET, algorithm=_ALG)


def _bearer(role: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {_mint(role)}"}


# ---------------------------------------------------------------------------
# App factory (mirrors tests/test_endpoint_gating_sweep.py)
# ---------------------------------------------------------------------------


def _make_app() -> Any:
    """Instantiate a real FastAPI app with mocked infrastructure deps."""
    fake_settings = MagicMock(
        db_url="postgresql://fake/none",
        amqp_url="amqp://fake/none",
        allowed_origins=(),
        storage_backend="local",
        artifacts_dir=Path("/tmp/protea-admin-sweep-fake"),
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

        app = create_app(Path("/tmp/protea-admin-sweep-fake"))
    return app


@pytest.fixture(scope="module")
def app():
    return _make_app()


@pytest.fixture(scope="module")
def client(app):
    return TestClient(app, raise_server_exceptions=False)


# ---------------------------------------------------------------------------
# Admin route catalogue — single source of truth
# ---------------------------------------------------------------------------
#
# Adding a new admin route?  Add it here.  The
# ``test_every_admin_route_in_catalog`` discovery check below walks the
# live router list and fails if anything under ``/admin``, ``/maintenance``,
# or ``/auth/api-keys`` is missing from one of the buckets.

# (method, path) pairs that MUST be gated at admin (viewer + operator => 403).
_ADMIN_DESTRUCTIVE_ROUTES: list[tuple[str, str]] = [
    ("POST", "/admin/reset-db"),
    ("POST", "/admin/dlq/replay"),
    ("POST", "/admin/dlq/purge"),
    ("POST", "/maintenance/vacuum-sequences"),
    ("POST", "/maintenance/vacuum-embeddings"),
    ("POST", "/auth/api-keys"),
    ("GET", "/auth/api-keys"),
    ("DELETE", f"/auth/api-keys/{_FAKE_UUID}"),
    ("POST", f"/auth/admin/revoke-sessions/{_FAKE_UUID}"),
]

# (method, path) pairs that are operator-floor reads: viewer => 403, operator
# passes the gate (handler may still 4xx/5xx for business reasons).
_OPERATOR_READS: list[tuple[str, str]] = [
    ("GET", "/admin/dlq/summary"),
]

# (method, path) pairs that are intentionally open to viewers (non-destructive
# previews / observation). Anonymous callers may still be challenged depending
# on whether the route declares a gate at all; the contract here is just
# "viewer JWT must not be 403".
_VIEWER_OPEN_READS: list[tuple[str, str]] = [
    ("GET", "/maintenance/vacuum-sequences/preview"),
    ("GET", "/maintenance/vacuum-embeddings/preview"),
]


def _all_catalog_paths() -> set[str]:
    """Flatten the three buckets into a set of paths for discovery checks."""
    return (
        {p for _, p in _ADMIN_DESTRUCTIVE_ROUTES}
        | {p for _, p in _OPERATOR_READS}
        | {p for _, p in _VIEWER_OPEN_READS}
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _call(client: TestClient, method: str, path: str, **kwargs: Any) -> Any:
    """Dispatch ``method`` through ``client`` with a JSON body for mutations."""
    method = method.upper()
    if method == "GET":
        return client.get(path, **kwargs)
    if method == "DELETE":
        return client.delete(path, **kwargs)
    # POST / PATCH / PUT — always pass a JSON body so FastAPI's body-required
    # routes can short-circuit on the auth gate before validating the schema.
    kwargs.setdefault("json", {})
    return getattr(client, method.lower())(path, **kwargs)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestAdminDestructiveRoutes:
    """Every destructive admin route must require ``require_role('admin')``."""

    @pytest.mark.parametrize("method,path", _ADMIN_DESTRUCTIVE_ROUTES)
    def test_unauthenticated_rejected(self, monkeypatch, client, method: str, path: str) -> None:
        monkeypatch.setenv("PROTEA_AUTHN_REQUIRED", "true")
        monkeypatch.setenv("PROTEA_JWT_SECRET", _SECRET)
        resp = _call(client, method, path)
        assert resp.status_code in (401, 403), (
            f"{method} {path}: expected 401/403 for unauthenticated caller, got {resp.status_code}"
        )

    @pytest.mark.parametrize("method,path", _ADMIN_DESTRUCTIVE_ROUTES)
    def test_viewer_jwt_rejected(self, monkeypatch, client, method: str, path: str) -> None:
        monkeypatch.setenv("PROTEA_AUTHN_REQUIRED", "true")
        monkeypatch.setenv("PROTEA_JWT_SECRET", _SECRET)
        resp = _call(client, method, path, headers=_bearer(_VIEWER))
        assert resp.status_code == 403, (
            f"{method} {path}: viewer JWT should be 403 at admin floor, got {resp.status_code}"
        )

    @pytest.mark.parametrize("method,path", _ADMIN_DESTRUCTIVE_ROUTES)
    def test_operator_jwt_rejected(self, monkeypatch, client, method: str, path: str) -> None:
        monkeypatch.setenv("PROTEA_AUTHN_REQUIRED", "true")
        monkeypatch.setenv("PROTEA_JWT_SECRET", _SECRET)
        resp = _call(client, method, path, headers=_bearer(_OPERATOR))
        assert resp.status_code == 403, (
            f"{method} {path}: operator JWT should be 403 at admin floor, got {resp.status_code}"
        )

    @pytest.mark.parametrize("method,path", _ADMIN_DESTRUCTIVE_ROUTES)
    def test_admin_jwt_passes_gate(self, monkeypatch, client, method: str, path: str) -> None:
        """Admin JWT must clear the role gate. Handlers may still 4xx/5xx
        for business reasons (missing body, mocked DB) but never 401/403.
        """
        monkeypatch.setenv("PROTEA_AUTHN_REQUIRED", "true")
        monkeypatch.setenv("PROTEA_JWT_SECRET", _SECRET)
        # reset-db carries an extra destructive-op sentinel on top of the
        # role gate; opt in so the role-gate assertion below is exercised.
        monkeypatch.setenv("PROTEA_ALLOW_DB_RESET", "1")
        resp = _call(client, method, path, headers=_bearer(_ADMIN))
        assert resp.status_code not in (401, 403), (
            f"{method} {path}: admin JWT was blocked by role gate ({resp.status_code})"
        )


class TestOperatorFloorReads:
    """Read-only admin observation: viewer rejected, operator passes."""

    @pytest.mark.parametrize("method,path", _OPERATOR_READS)
    def test_viewer_jwt_rejected(self, monkeypatch, client, method: str, path: str) -> None:
        monkeypatch.setenv("PROTEA_AUTHN_REQUIRED", "true")
        monkeypatch.setenv("PROTEA_JWT_SECRET", _SECRET)
        resp = _call(client, method, path, headers=_bearer(_VIEWER))
        assert resp.status_code == 403, (
            f"{method} {path}: viewer JWT should be 403 at operator floor, got {resp.status_code}"
        )

    @pytest.mark.parametrize("method,path", _OPERATOR_READS)
    def test_operator_jwt_passes_gate(self, monkeypatch, client, method: str, path: str) -> None:
        monkeypatch.setenv("PROTEA_AUTHN_REQUIRED", "true")
        monkeypatch.setenv("PROTEA_JWT_SECRET", _SECRET)
        resp = _call(client, method, path, headers=_bearer(_OPERATOR))
        assert resp.status_code not in (401, 403), (
            f"{method} {path}: operator JWT was blocked by role gate ({resp.status_code})"
        )


class TestViewerOpenReads:
    """Non-destructive previews are intentionally open to viewers."""

    @pytest.mark.parametrize("method,path", _VIEWER_OPEN_READS)
    def test_viewer_jwt_not_blocked(self, monkeypatch, client, method: str, path: str) -> None:
        monkeypatch.setenv("PROTEA_AUTHN_REQUIRED", "true")
        monkeypatch.setenv("PROTEA_JWT_SECRET", _SECRET)
        resp = _call(client, method, path, headers=_bearer(_VIEWER))
        assert resp.status_code not in (401, 403), (
            f"{method} {path}: viewer-open read returned {resp.status_code}; "
            "must not require admin/operator role."
        )


class TestAdminRouteDiscovery:
    """Fail when a new admin/maintenance/api-keys route is added off-catalog.

    This is the single-source-of-truth guarantee: any new route that lives
    under one of the audited prefixes must be declared in this module before
    it can ship. If you are landing this test failing on your branch, add
    the new route to the appropriate bucket above.
    """

    _AUDITED_PREFIXES: tuple[str, ...] = (
        "/admin/",
        "/maintenance/",
        "/auth/api-keys",
        "/auth/admin/",
    )

    def _normalise(self, raw_path: str) -> str:
        """Replace concrete UUIDs in catalog entries with the path-param shape.

        FastAPI exposes routes as ``/auth/api-keys/{key_id}``; our catalog
        entries inline a fake UUID so the parametrised client.delete call
        actually has a path to hit. We canonicalise both to a single shape
        for the discovery diff.
        """
        return raw_path.replace(_FAKE_UUID, "{id}")

    def test_every_admin_route_is_catalogued(self, app) -> None:
        live: set[tuple[str, str]] = set()
        for route in app.routes:
            if not hasattr(route, "methods") or not hasattr(route, "path"):
                continue
            path: str = route.path
            # Skip the dual-mount alias under /v1 — every router is mounted
            # twice (canonical /v1 + legacy /), so we only inspect the
            # legacy (unprefixed) view.
            if path.startswith("/v1/"):
                continue
            if not any(path.startswith(prefix) for prefix in self._AUDITED_PREFIXES):
                continue
            for method in route.methods:
                if method in {"HEAD", "OPTIONS"}:
                    continue
                live.add((method, path))

        # Normalise concrete UUIDs in the catalog to ``{id}`` paths so we
        # compare against FastAPI's path-param shape.
        catalogued: set[tuple[str, str]] = set()
        for method, path in _ADMIN_DESTRUCTIVE_ROUTES + _OPERATOR_READS + _VIEWER_OPEN_READS:
            # Rewrite the concrete UUID into the path-param shape FastAPI uses.
            if _FAKE_UUID in path:
                if path.endswith(_FAKE_UUID):
                    if path.startswith("/auth/api-keys/"):
                        path = "/auth/api-keys/{key_id}"
                    elif path.startswith("/auth/admin/revoke-sessions/"):
                        path = "/auth/admin/revoke-sessions/{user_id}"
            catalogued.add((method, path))

        missing = live - catalogued
        # Don't flag stray catalog entries: a removed route in the source
        # would show up as a hard test failure elsewhere (404 with a
        # WWW-Authenticate Bearer challenge would not be a 401/403 gate
        # response), and keeping the catalog is the load-bearing direction.
        assert not missing, (
            "New admin-prefixed routes were added without updating the "
            "admin gating sweep catalog:\n"
            + "\n".join(f"  {m} {p}" for m, p in sorted(missing))
            + "\nAdd each entry to _ADMIN_DESTRUCTIVE_ROUTES, "
            "_OPERATOR_READS, or _VIEWER_OPEN_READS in tests/"
            "test_admin_gating_sweep.py."
        )

    def test_catalog_buckets_are_disjoint(self) -> None:
        """A route may only appear in one bucket."""
        a = {(m, p) for m, p in _ADMIN_DESTRUCTIVE_ROUTES}
        o = {(m, p) for m, p in _OPERATOR_READS}
        v = {(m, p) for m, p in _VIEWER_OPEN_READS}
        assert not (a & o), f"overlap admin/operator: {a & o}"
        assert not (a & v), f"overlap admin/viewer: {a & v}"
        assert not (o & v), f"overlap operator/viewer: {o & v}"

    def test_catalog_paths_unique(self) -> None:
        """A (method, path) tuple may only appear once across all buckets.

        Some paths legitimately appear under multiple methods (e.g.
        ``/auth/api-keys`` is both POST=create and GET=list); the
        idempotent gate is on the full (method, path) tuple.
        """
        tuples = list(_ADMIN_DESTRUCTIVE_ROUTES) + list(_OPERATOR_READS) + list(_VIEWER_OPEN_READS)
        duplicates = {t for t in tuples if tuples.count(t) > 1}
        assert not duplicates, f"(method, path) appears in multiple buckets: {duplicates}"
        # _all_catalog_paths is the canonical flattened set used by other
        # callers / future tests; ensure it stays in sync with the tuples.
        assert len(_all_catalog_paths()) == len({p for _, p in tuples})
