"""Tests for the slowapi rate limiter (T5.6b).

We stand up a minimal FastAPI app with the limiter installed and a
single rate-limited route. slowapi's bucketing is in-process and per
:class:`Limiter` instance, so we reset it between scenarios via the
public ``reset`` method to keep tests independent.
"""

from __future__ import annotations

import pytest
from fastapi import APIRouter, FastAPI, Request, Response
from fastapi.testclient import TestClient

from protea.api.rate_limit import (
    DEFAULT_API_KEYS,
    DEFAULT_DATASETS,
    DEFAULT_JOBS,
    api_keys_limit,
    datasets_limit,
    install_rate_limiter,
    jobs_limit,
    limiter,
)


# ---------------------------------------------------------------------------
# Env knob resolution
# ---------------------------------------------------------------------------


class TestLimitEnvOverrides:
    def test_jobs_default(self, monkeypatch):
        monkeypatch.delenv("PROTEA_RATELIMIT_JOBS", raising=False)
        assert jobs_limit() == DEFAULT_JOBS

    def test_jobs_override(self, monkeypatch):
        monkeypatch.setenv("PROTEA_RATELIMIT_JOBS", "100/minute")
        assert jobs_limit() == "100/minute"

    def test_api_keys_default(self, monkeypatch):
        monkeypatch.delenv("PROTEA_RATELIMIT_API_KEYS", raising=False)
        assert api_keys_limit() == DEFAULT_API_KEYS

    def test_datasets_default(self, monkeypatch):
        monkeypatch.delenv("PROTEA_RATELIMIT_DATASETS", raising=False)
        assert datasets_limit() == DEFAULT_DATASETS


# ---------------------------------------------------------------------------
# Limiter behaviour (in-memory bucket)
# ---------------------------------------------------------------------------


def _hard_reset_limiter() -> None:
    """Wipe slowapi's in-memory state between tests.

    ``Limiter.reset()`` clears the counter map, but the route-level
    registry (``_route_limits``) accumulates one entry per decorator
    application. When a single test module re-creates the FastAPI app
    inside multiple fixtures, the same route ends up with N copies of
    the limit, multiplying the counter on every hit. Clearing the
    registry between tests keeps the bucket honest.
    """
    limiter.reset()
    storage = limiter._storage
    for attr in ("storage", "events", "expirations"):
        target = getattr(storage, attr, None)
        if hasattr(target, "clear"):
            target.clear()
    route_limits = getattr(limiter, "_route_limits", None)
    if isinstance(route_limits, dict):
        route_limits.clear()


@pytest.fixture()
def limited_client():
    """Build a tiny app that throttles a single POST at 2/minute."""
    _hard_reset_limiter()

    app = FastAPI()
    install_rate_limiter(app)

    router = APIRouter()

    @router.post("/ping")
    @limiter.limit("2/minute")
    def _ping(request: Request, response: Response) -> dict:
        return {"ok": True}

    app.include_router(router)
    client = TestClient(app, raise_server_exceptions=True)
    yield client
    _hard_reset_limiter()


class TestRateLimitEnforcement:
    def test_under_quota_passes(self, limited_client):
        for _ in range(2):
            resp = limited_client.post("/ping", json={})
            assert resp.status_code == 200, resp.text

    def test_third_call_within_window_is_429(self, limited_client):
        # Same caller (no headers → IP bucket).
        for _ in range(2):
            assert limited_client.post("/ping", json={}).status_code == 200
        resp = limited_client.post("/ping", json={})
        assert resp.status_code == 429
        # RFC 7807 shape.
        body = resp.json()
        assert body["status"] == 429
        assert body["title"] == "Too Many Requests"

    def test_separate_api_keys_have_separate_buckets(self, limited_client):
        for _ in range(2):
            r = limited_client.post(
                "/ping",
                json={},
                headers={"X-Api-Key": "aaaaaaaa-key-1"},
            )
            assert r.status_code == 200
        # Different bucket → still allowed.
        r2 = limited_client.post(
            "/ping",
            json={},
            headers={"X-Api-Key": "bbbbbbbb-key-2"},
        )
        assert r2.status_code == 200


# ---------------------------------------------------------------------------
# Combined with authn-bypass: PROTEA_AUTHN_REQUIRED=false still rate-limits
# (the limiter is independent of the auth gate but bucketed by IP fallback).
# ---------------------------------------------------------------------------


class TestRateLimitIndependentOfAuthn:
    def test_authn_off_still_rate_limits(self, monkeypatch):
        monkeypatch.setenv("PROTEA_AUTHN_REQUIRED", "false")
        _hard_reset_limiter()

        app = FastAPI()
        install_rate_limiter(app)

        router = APIRouter()

        @router.post("/open")
        @limiter.limit("1/minute")
        def _open(request: Request, response: Response) -> dict:
            return {"ok": True}

        app.include_router(router)
        client = TestClient(app, raise_server_exceptions=False)

        assert client.post("/open", json={}).status_code == 200
        resp = client.post("/open", json={})
        assert resp.status_code == 429
        _hard_reset_limiter()
