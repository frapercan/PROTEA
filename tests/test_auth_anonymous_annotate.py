"""Tests for the anonymous quick-annotate IP-hash quota (FARM-AUTH.6).

Covers:
- counter increments on each anonymous call,
- 429 returned once the daily limit is exhausted (with Retry-After header),
- retry_after_seconds present in the 429 body,
- mocked new day resets the counter (new row for new day),
- authenticated caller bypasses the quota gate entirely.

All tests run without Postgres: the session factory is mocked with an
in-memory SQLite engine.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from protea.api.auth.anon_quota import (
    _hash_ip,
    _is_authenticated,
    _seconds_until_midnight_utc,
    require_anon_quota,
)
from protea.api.deps import get_session_factory
from protea.infrastructure.orm.models.anon_quota import AnonQuota

# ---------------------------------------------------------------------------
# SQLite-backed in-memory session factory
# ---------------------------------------------------------------------------


@pytest.fixture()
def sqlite_factory(tmp_path):
    """Yield a sessionmaker backed by a file-based SQLite engine.

    Uses a temporary file so every session shares the same on-disk DB.
    Only creates the ``anon_quota`` table (avoids Postgres-specific types
    in other models that are incompatible with SQLite).
    """
    db_path = tmp_path / "test_quota.db"
    engine = create_engine(f"sqlite:///{db_path}")
    AnonQuota.__table__.create(engine)
    factory = sessionmaker(bind=engine)
    yield factory
    AnonQuota.__table__.drop(engine)


# ---------------------------------------------------------------------------
# Tiny FastAPI app that only mounts require_anon_quota
# ---------------------------------------------------------------------------


@pytest.fixture()
def app_and_client(sqlite_factory, monkeypatch):
    """Spin up a minimal FastAPI app with the quota dep on POST /annotate."""
    monkeypatch.setenv("PROTEA_ANON_QUOTA_PER_DAY", "3")

    test_app = FastAPI()
    test_app.state.session_factory = sqlite_factory

    from fastapi import APIRouter, Depends

    anon_router = APIRouter()

    @anon_router.post("/annotate", dependencies=[Depends(require_anon_quota)])
    def _annotate():
        return {"ok": True}

    test_app.include_router(anon_router)
    test_app.dependency_overrides[get_session_factory] = lambda: sqlite_factory

    client = TestClient(test_app, raise_server_exceptions=False)
    return test_app, client, sqlite_factory


# ---------------------------------------------------------------------------
# Pure-unit helpers
# ---------------------------------------------------------------------------


class TestHashIp:
    def test_sha256_len(self):
        h = _hash_ip("1.2.3.4")
        assert len(h) == 64

    def test_same_ip_same_hash(self):
        assert _hash_ip("10.0.0.1") == _hash_ip("10.0.0.1")

    def test_different_ips_differ(self):
        assert _hash_ip("1.1.1.1") != _hash_ip("2.2.2.2")


class TestSecondsUntilMidnight:
    def test_positive(self):
        assert _seconds_until_midnight_utc() > 0


class TestIsAuthenticated:
    def _make_request(self, headers: dict) -> MagicMock:
        req = MagicMock()
        req.headers = headers
        return req

    def test_bearer_token_is_authed(self):
        req = self._make_request({"authorization": "Bearer some.token"})
        assert _is_authenticated(req) is True

    def test_apikey_header_is_authed(self):
        req = self._make_request({"authorization": "ApiKey some-key"})
        assert _is_authenticated(req) is True

    def test_x_api_key_is_authed(self):
        req = self._make_request({"x-api-key": "some-key"})
        assert _is_authenticated(req) is True

    def test_no_headers_is_anon(self):
        req = self._make_request({})
        assert _is_authenticated(req) is False


# ---------------------------------------------------------------------------
# Integration: quota counting + 429
# ---------------------------------------------------------------------------


class TestAnonQuotaCounting:
    def test_first_call_succeeds(self, app_and_client):
        _, client, _ = app_and_client
        resp = client.post("/annotate")
        assert resp.status_code == 200

    def test_count_increments_per_call(self, app_and_client, monkeypatch):
        _, client, factory = app_and_client
        # limit is 3 (set via monkeypatch in fixture)
        for _ in range(3):
            resp = client.post("/annotate")
            assert resp.status_code == 200

    def test_limit_hit_returns_429(self, app_and_client):
        _, client, _ = app_and_client
        # Exhaust the limit (3 calls)
        for _ in range(3):
            client.post("/annotate")
        # 4th call must be blocked
        resp = client.post("/annotate")
        assert resp.status_code == 429

    def test_429_has_retry_after_header(self, app_and_client):
        _, client, _ = app_and_client
        for _ in range(3):
            client.post("/annotate")
        resp = client.post("/annotate")
        assert resp.status_code == 429
        assert "retry-after" in resp.headers

    def test_429_body_has_retry_after_seconds(self, app_and_client):
        _, client, _ = app_and_client
        for _ in range(3):
            client.post("/annotate")
        resp = client.post("/annotate")
        assert resp.status_code == 429
        body = resp.json()
        detail = body.get("detail", {})
        assert "retry_after_seconds" in detail
        assert detail["retry_after_seconds"] > 0

    def test_new_day_resets_counter(self, app_and_client):
        """A new UTC day starts a fresh quota bucket (count 0 on new date key)."""
        _, client, factory = app_and_client
        # Exhaust today's quota
        for _ in range(3):
            client.post("/annotate")
        resp = client.post("/annotate")
        assert resp.status_code == 429

        # Simulate midnight by patching datetime.now so the dependency
        # resolves 'today' as a future date (2099-01-01).
        def _fake_now(_tz=None):
            return datetime(2099, 1, 1, 0, 0, 0, tzinfo=UTC)

        with patch("protea.api.auth.anon_quota.datetime") as mock_dt:
            mock_dt.now.side_effect = _fake_now
            # The dependency calls datetime.now(UTC).date() -- simulate it.
            resp_next_day = client.post("/annotate")

        # A new day has no row yet, so count starts at 0 and the call succeeds.
        assert resp_next_day.status_code == 200


class TestAnonQuotaAuthBypass:
    def test_authenticated_bearer_bypasses_quota(self, app_and_client, monkeypatch):
        """Bearer-authenticated callers must not hit the quota gate."""
        _, client, _ = app_and_client
        # Exhaust anonymous quota
        for _ in range(3):
            client.post("/annotate")
        # Authenticated call must NOT be blocked even though anon quota is full
        resp = client.post(
            "/annotate", headers={"Authorization": "Bearer fake.jwt.token"}
        )
        # The quota gate is bypassed; the endpoint itself returns 200
        assert resp.status_code == 200

    def test_authenticated_x_api_key_bypasses_quota(self, app_and_client):
        _, client, _ = app_and_client
        for _ in range(3):
            client.post("/annotate")
        resp = client.post("/annotate", headers={"X-Api-Key": "some-key"})
        assert resp.status_code == 200


class TestAnonQuotaFailOpen:
    def test_db_error_allows_request(self, monkeypatch):
        """If the DB is unavailable the request proceeds (fail-open)."""
        test_app = FastAPI()
        bad_factory = MagicMock(side_effect=Exception("db gone"))
        test_app.state.session_factory = bad_factory

        from fastapi import APIRouter, Depends

        r = APIRouter()

        @r.post("/annotate", dependencies=[Depends(require_anon_quota)])
        def _ok():
            return {"ok": True}

        test_app.include_router(r)
        test_app.dependency_overrides[get_session_factory] = lambda: bad_factory

        client = TestClient(test_app, raise_server_exceptions=False)
        resp = client.post("/annotate")
        # Fail-open: must not 500; should succeed
        assert resp.status_code == 200
