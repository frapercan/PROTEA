"""Integration tests for the T5.5 CORS allowlist contract.

The settings layer is covered by ``tests/test_settings.py``; this module
exercises the FastAPI app produced by :func:`protea.api.app.create_app`
to confirm the resolved allowlist actually drives the response headers
returned by ``CORSMiddleware``.

Coverage matrix
---------------
* default (env unset) — built-in dev origins (localhost + ngrok) are
  echoed when the request advertises one of them, and the cross-origin
  header is absent when the ``Origin`` is unknown
* custom env list — comma-separated parsing reaches all the way to the
  middleware
* empty env — CORS middleware is not installed at all, so the response
  carries no ``Access-Control-Allow-Origin`` header for any ``Origin``
* wildcard ``*`` — origins set to ``["*"]`` and credentials disabled,
  matching the Fetch spec (credentials + wildcard is forbidden)
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from protea.api.app import create_app


def _build_app(allowed_origins: tuple[str, ...]):
    """Construct a real FastAPI app with stubbed infra deps.

    Patches ``load_settings`` so we can drive the CORS allowlist
    directly, and ``build_session_factory`` so we never reach for a
    database. The middleware chain (including ``CORSMiddleware``) is
    wired exactly as in production.
    """
    mock_settings = MagicMock()
    mock_settings.db_url = "sqlite:///:memory:"
    mock_settings.amqp_url = "amqp://guest:guest@localhost/"
    mock_settings.allowed_origins = allowed_origins

    with (
        patch("protea.api.app.load_settings", return_value=mock_settings),
        patch("protea.api.app.build_session_factory", return_value=MagicMock()),
    ):
        return create_app(Path("/fake/root"))


# ---------------------------------------------------------------------------
# Default behaviour — built-in dev origins.
# ---------------------------------------------------------------------------


class TestDefaultOrigins:
    def test_allowed_origin_echoed(self):
        app = _build_app(
            (
                "http://localhost:3000",
                "http://127.0.0.1:3000",
                "https://protea.ngrok.app",
            )
        )
        client = TestClient(app)

        resp = client.get("/health", headers={"Origin": "http://localhost:3000"})

        assert resp.status_code == 200
        assert resp.headers.get("access-control-allow-origin") == "http://localhost:3000"
        assert resp.headers.get("access-control-allow-credentials") == "true"

    def test_unknown_origin_gets_no_cors_header(self):
        app = _build_app(
            (
                "http://localhost:3000",
                "https://protea.ngrok.app",
            )
        )
        client = TestClient(app)

        resp = client.get("/health", headers={"Origin": "https://evil.example"})

        # Starlette returns 200 (CORS rejection is browser-side); the
        # contract surface is the absence of the allow-origin header.
        assert resp.status_code == 200
        assert "access-control-allow-origin" not in {k.lower() for k in resp.headers}


# ---------------------------------------------------------------------------
# Explicit env-driven allowlist.
# ---------------------------------------------------------------------------


class TestCustomAllowlist:
    def test_listed_origin_is_echoed(self):
        app = _build_app(("https://foo.example", "https://bar.example"))
        client = TestClient(app)

        resp = client.get("/health", headers={"Origin": "https://bar.example"})

        assert resp.status_code == 200
        assert resp.headers.get("access-control-allow-origin") == "https://bar.example"

    def test_origin_outside_list_blocked(self):
        app = _build_app(("https://foo.example",))
        client = TestClient(app)

        resp = client.get("/health", headers={"Origin": "https://bar.example"})

        assert resp.status_code == 200
        assert "access-control-allow-origin" not in {k.lower() for k in resp.headers}

    def test_preflight_returns_allowed_methods(self):
        app = _build_app(("https://foo.example",))
        client = TestClient(app)

        resp = client.options(
            "/health",
            headers={
                "Origin": "https://foo.example",
                "Access-Control-Request-Method": "POST",
                "Access-Control-Request-Headers": "content-type",
            },
        )

        assert resp.status_code == 200
        assert resp.headers.get("access-control-allow-origin") == "https://foo.example"
        # ``allow_methods=["*"]`` is rewritten by Starlette to the
        # explicit method list; just confirm POST is permitted.
        allow_methods = resp.headers.get("access-control-allow-methods", "")
        assert "POST" in allow_methods or "*" in allow_methods


# ---------------------------------------------------------------------------
# Empty allowlist — middleware skipped entirely.
# ---------------------------------------------------------------------------


class TestEmptyAllowlist:
    def test_no_cors_header_for_any_origin(self):
        app = _build_app(())
        client = TestClient(app)

        resp = client.get("/health", headers={"Origin": "http://localhost:3000"})

        assert resp.status_code == 200
        assert "access-control-allow-origin" not in {k.lower() for k in resp.headers}


# ---------------------------------------------------------------------------
# Wildcard — credentials must be off (Fetch spec).
# ---------------------------------------------------------------------------


class TestWildcard:
    def test_any_origin_echoed_as_wildcard(self):
        app = _build_app(("*",))
        client = TestClient(app)

        resp = client.get("/health", headers={"Origin": "https://anything.example"})

        assert resp.status_code == 200
        assert resp.headers.get("access-control-allow-origin") == "*"

    def test_credentials_header_absent(self):
        """With wildcard, ``allow_credentials`` must be False so
        Starlette never emits ``Access-Control-Allow-Credentials: true``.

        Sending that header alongside ``*`` is rejected by every
        major browser (CORS spec violation)."""
        app = _build_app(("*",))
        client = TestClient(app)

        resp = client.get("/health", headers={"Origin": "https://anything.example"})

        # Header must be missing entirely (Starlette omits it when
        # allow_credentials is False), not just false-valued.
        header_names = {k.lower() for k in resp.headers}
        assert "access-control-allow-credentials" not in header_names

    def test_wildcard_mixed_with_origins_still_collapses(self):
        """If the resolved list contains both ``*`` and named origins
        the wildcard branch wins so the response stays spec-compliant.
        """
        app = _build_app(("https://foo.example", "*"))
        client = TestClient(app)

        resp = client.get("/health", headers={"Origin": "https://anything.example"})

        assert resp.headers.get("access-control-allow-origin") == "*"
        header_names = {k.lower() for k in resp.headers}
        assert "access-control-allow-credentials" not in header_names


# ---------------------------------------------------------------------------
# Settings-layer smoke — confirm the env-var contract still wires through
# without needing the full app fixture.
# ---------------------------------------------------------------------------


class TestEnvParsing:
    def test_comma_separated_env_parses(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from protea.infrastructure.settings import load_settings

        monkeypatch.setenv(
            "PROTEA_ALLOWED_ORIGINS",
            "https://foo.example,https://bar.example",
        )
        s = load_settings(tmp_path)
        assert s.allowed_origins == ("https://foo.example", "https://bar.example")

    def test_wildcard_env_value_propagates(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from protea.infrastructure.settings import load_settings

        monkeypatch.setenv("PROTEA_ALLOWED_ORIGINS", "*")
        s = load_settings(tmp_path)
        assert s.allowed_origins == ("*",)
