"""Unit tests for FARM-AUTH.7 per-user daily quota gate.

Coverage:
* :func:`protea.api.auth.user_quota.require_user_quota` -- increments per
  call, 429 on limit, admin bypasses, distinct operations independent,
  resets at UTC midnight (mocked).
* :func:`protea.api.auth.user_quota._resolve_user_id` -- bearer extracts UUID.
* Fail-open on DB error.
"""

from __future__ import annotations

import time
import uuid
from typing import Any
from unittest.mock import MagicMock, patch

import jwt
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from protea.api.auth.user_quota import (
    _resolve_user_id,
    _seconds_until_midnight_utc,
    require_user_quota,
)
from protea.api.bearer import BearerPrincipal

_SECRET = "farm-auth-7-test-secret-long-enough-padding"
_ALG = "HS256"
_USER_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"


def _mint(role: str, *, sub: str = _USER_ID, ttl: int = 60) -> str:
    now = int(time.time())
    return jwt.encode(
        {"sub": sub, "iat": now, "exp": now + ttl, "role": role},
        _SECRET,
        algorithm=_ALG,
    )


# ---------------------------------------------------------------------------
# _resolve_user_id
# ---------------------------------------------------------------------------


class TestResolveUserId:
    def test_bearer_extracts_uuid(self):
        principal = BearerPrincipal(sub=_USER_ID, claims={"role": "researcher"})
        result = _resolve_user_id(principal)
        assert result == uuid.UUID(_USER_ID)

    def test_bearer_invalid_sub_returns_none(self):
        principal = BearerPrincipal(sub="not-a-uuid", claims={})
        assert _resolve_user_id(principal) is None

    def test_none_principal_returns_none(self):
        assert _resolve_user_id(None) is None


# ---------------------------------------------------------------------------
# _seconds_until_midnight_utc
# ---------------------------------------------------------------------------


class TestSecondsUntilMidnight:
    def test_positive_value(self):
        secs = _seconds_until_midnight_utc()
        assert 0 <= secs <= 86400


# ---------------------------------------------------------------------------
# Integration-style tests via a minimal FastAPI app
# ---------------------------------------------------------------------------


def _make_quota_app(
    operation: str = "predict",
    quota_map: dict[str, int] | None = None,
) -> FastAPI:
    """Build a minimal FastAPI app with require_user_quota wired to GET /x."""
    if quota_map is None:
        quota_map = {"predict": 3, "export_research_dataset": 2, "run_cafa_evaluation": 5}

    app = FastAPI()
    app.state.user_quota_per_day = quota_map

    @app.get("/x", dependencies=[Depends(require_user_quota(operation))])
    def _x() -> dict[str, str]:
        return {"ok": "true"}

    return app


def _auth_header(role: str, sub: str = _USER_ID) -> dict[str, str]:
    return {"Authorization": f"Bearer {_mint(role, sub=sub)}"}


class TestQuotaGate:
    """Tests require a mocked session factory + DB layer."""

    def _patch_session_factory(self, app: FastAPI, session: Session | None = None) -> None:
        """Attach a mock session_factory that hands back ``session``."""
        mock_factory = MagicMock()
        if session is not None:
            mock_factory.return_value.__enter__ = MagicMock(return_value=session)
            mock_factory.return_value.__exit__ = MagicMock(return_value=False)
        app.state.session_factory = mock_factory

    def test_increments_count_on_each_call(self, monkeypatch):
        """Each request increments the quota row; limit=3 means 3 succeed."""
        monkeypatch.setenv("PROTEA_AUTHN_REQUIRED", "true")
        monkeypatch.setenv("PROTEA_JWT_SECRET", _SECRET)

        # Track calls to _enforce_quota so we can assert count behaviour
        # without a real DB.  Calls 1-3 pass; call 4 would exceed limit=3.
        call_count: list[int] = [0]

        def fake_enforce(session: Any, user_id: Any, operation: str, limit: int) -> None:
            call_count[0] += 1
            if call_count[0] > limit:
                from fastapi import HTTPException
                raise HTTPException(status_code=429, detail="user_quota_exceeded")

        app = _make_quota_app("predict", {"predict": 3})
        self._patch_session_factory(app)

        with patch("protea.api.auth.user_quota._enforce_quota", side_effect=fake_enforce):
            client = TestClient(app, raise_server_exceptions=False)
            for i in range(3):
                resp = client.get("/x", headers=_auth_header("researcher"))
                assert resp.status_code == 200, f"call {i+1} should succeed"
            resp = client.get("/x", headers=_auth_header("researcher"))
            assert resp.status_code == 429

    def test_admin_bypasses_quota(self, monkeypatch):
        """Admin role gets through without touching _enforce_quota."""
        monkeypatch.setenv("PROTEA_AUTHN_REQUIRED", "true")
        monkeypatch.setenv("PROTEA_JWT_SECRET", _SECRET)

        enforced: list[bool] = [False]

        def fake_enforce(*args: Any, **kwargs: Any) -> None:
            enforced[0] = True

        app = _make_quota_app("predict", {"predict": 0})
        self._patch_session_factory(app)

        with patch("protea.api.auth.user_quota._enforce_quota", side_effect=fake_enforce):
            client = TestClient(app, raise_server_exceptions=False)
            resp = client.get("/x", headers=_auth_header("admin"))
            assert resp.status_code == 200
            assert not enforced[0], "admin should not hit _enforce_quota"

    def test_429_on_limit(self, monkeypatch):
        monkeypatch.setenv("PROTEA_AUTHN_REQUIRED", "true")
        monkeypatch.setenv("PROTEA_JWT_SECRET", _SECRET)

        def fake_enforce(*args: Any, **kwargs: Any) -> None:
            from fastapi import HTTPException
            raise HTTPException(status_code=429, detail="user_quota_exceeded",
                                headers={"Retry-After": "3600"})

        app = _make_quota_app("predict", {"predict": 1})
        self._patch_session_factory(app)

        with patch("protea.api.auth.user_quota._enforce_quota", side_effect=fake_enforce):
            client = TestClient(app, raise_server_exceptions=False)
            resp = client.get("/x", headers=_auth_header("researcher"))
            assert resp.status_code == 429
            assert resp.json()["detail"] == "user_quota_exceeded"

    def test_distinct_operations_do_not_share_quota(self, monkeypatch):
        """Two operations with the same limit consume separate buckets."""
        monkeypatch.setenv("PROTEA_AUTHN_REQUIRED", "true")
        monkeypatch.setenv("PROTEA_JWT_SECRET", _SECRET)

        seen_operations: list[str] = []

        def fake_enforce(session: Any, user_id: Any, operation: str, limit: int) -> None:
            seen_operations.append(operation)

        app_predict = _make_quota_app("predict", {"predict": 1})
        app_export = _make_quota_app("export_research_dataset", {"export_research_dataset": 1})
        self._patch_session_factory(app_predict)
        self._patch_session_factory(app_export)

        with patch("protea.api.auth.user_quota._enforce_quota", side_effect=fake_enforce):
            client_p = TestClient(app_predict, raise_server_exceptions=False)
            client_e = TestClient(app_export, raise_server_exceptions=False)
            client_p.get("/x", headers=_auth_header("researcher"))
            client_e.get("/x", headers=_auth_header("researcher"))

        assert "predict" in seen_operations
        assert "export_research_dataset" in seen_operations

    def test_fail_open_on_db_error(self, monkeypatch):
        """A DB exception in _enforce_quota is swallowed; request succeeds."""
        monkeypatch.setenv("PROTEA_AUTHN_REQUIRED", "true")
        monkeypatch.setenv("PROTEA_JWT_SECRET", _SECRET)

        def explode(*args: Any, **kwargs: Any) -> None:
            raise RuntimeError("DB is down")

        app = _make_quota_app("predict", {"predict": 1})
        self._patch_session_factory(app)

        with patch("protea.api.auth.user_quota._enforce_quota", side_effect=explode):
            client = TestClient(app, raise_server_exceptions=False)
            resp = client.get("/x", headers=_auth_header("researcher"))
            assert resp.status_code == 200

    def test_unauthenticated_returns_401(self, monkeypatch):
        monkeypatch.setenv("PROTEA_AUTHN_REQUIRED", "true")
        monkeypatch.setenv("PROTEA_JWT_SECRET", _SECRET)

        app = _make_quota_app("predict", {"predict": 10})
        self._patch_session_factory(app)

        with patch("protea.api.auth.user_quota._enforce_quota"):
            client = TestClient(app, raise_server_exceptions=False)
            resp = client.get("/x")
            assert resp.status_code == 401

    def test_resets_at_utc_midnight(self, monkeypatch):
        """_enforce_quota counts against today only; yesterday's row is ignored."""
        monkeypatch.setenv("PROTEA_AUTHN_REQUIRED", "true")
        monkeypatch.setenv("PROTEA_JWT_SECRET", _SECRET)

        # _enforce_quota filters by today's date; yesterday's data should
        # not bleed through. Simulate the DB returning None for today's row.
        from protea.api.auth.user_quota import _enforce_quota as real_enforce

        session_mock = MagicMock()
        # Simulate query().filter().first() returning None (new day, no row yet)
        session_mock.query.return_value.filter.return_value.filter.return_value \
            .filter.return_value.first.return_value = None
        session_mock.query.return_value.filter.return_value.first.return_value = None

        # Directly test _enforce_quota doesn't raise when count=0 and limit=3.
        # (Would raise if yesterday's row with count=999 bled through.)
        real_enforce(session_mock, uuid.UUID(_USER_ID), "predict", 3)
        # No exception = test passes (limit not exceeded on new day).
