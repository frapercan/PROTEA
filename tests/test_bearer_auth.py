"""Unit tests for the Bearer JWT layer (T5.6b).

Exercises :mod:`protea.api.bearer` in isolation (no Postgres) and the
combined :func:`require_api_key_or_bearer` dependency through a tiny
FastAPI app.
"""

from __future__ import annotations

import time
from unittest.mock import MagicMock

import jwt
import pytest
from fastapi import APIRouter, Depends, FastAPI
from fastapi.testclient import TestClient

from protea.api.auth import require_api_key_or_bearer
from protea.api.bearer import (
    assert_bearer_config,
    decode_bearer_token,
    extract_bearer_token,
)

_SECRET = "test-secret-not-for-prod-padding-padding-padding"  # >32 bytes
_ALG = "HS256"


def _mint(
    sub: str = "operator@example.org",
    *,
    secret: str = _SECRET,
    ttl_seconds: int = 60,
    extra: dict | None = None,
) -> str:
    """Mint an HS256 token with the default required claims."""
    now = int(time.time())
    payload = {"sub": sub, "iat": now, "exp": now + ttl_seconds}
    if extra:
        payload.update(extra)
    return jwt.encode(payload, secret, algorithm=_ALG)


# ---------------------------------------------------------------------------
# extract_bearer_token
# ---------------------------------------------------------------------------


class TestExtractBearerToken:
    def test_returns_token_for_bearer_header(self):
        assert extract_bearer_token("Bearer abc.def.ghi") == "abc.def.ghi"

    def test_case_insensitive_scheme(self):
        assert extract_bearer_token("bearer xyz") == "xyz"
        assert extract_bearer_token("BEARER xyz") == "xyz"

    def test_apikey_scheme_returns_none(self):
        assert extract_bearer_token("ApiKey secret") is None

    def test_missing_returns_none(self):
        assert extract_bearer_token(None) is None
        assert extract_bearer_token("") is None
        assert extract_bearer_token("Bearer  ") is None


# ---------------------------------------------------------------------------
# decode_bearer_token (signature, exp, claims)
# ---------------------------------------------------------------------------


class TestDecodeBearerToken:
    def test_happy_path_returns_principal(self, monkeypatch):
        monkeypatch.setenv("PROTEA_JWT_SECRET", _SECRET)
        token = _mint(sub="frapercan@example.org")
        principal = decode_bearer_token(token)
        assert principal.sub == "frapercan@example.org"
        assert principal.claims["sub"] == "frapercan@example.org"
        assert "exp" in principal.claims

    def test_expired_token_rejected(self, monkeypatch):
        monkeypatch.setenv("PROTEA_JWT_SECRET", _SECRET)
        token = _mint(ttl_seconds=-1)
        with pytest.raises(Exception) as exc_info:
            decode_bearer_token(token)
        assert getattr(exc_info.value, "status_code", None) == 401

    def test_bad_signature_rejected(self, monkeypatch):
        monkeypatch.setenv("PROTEA_JWT_SECRET", _SECRET)
        token = _mint(secret="wrong-secret-padding-padding-padding-padding")
        with pytest.raises(Exception) as exc_info:
            decode_bearer_token(token)
        assert getattr(exc_info.value, "status_code", None) == 401

    def test_missing_required_claim_rejected(self, monkeypatch):
        monkeypatch.setenv("PROTEA_JWT_SECRET", _SECRET)
        # Hand-craft a token without ``sub``.
        token = jwt.encode(
            {"iat": int(time.time()), "exp": int(time.time()) + 60},
            _SECRET,
            algorithm=_ALG,
        )
        with pytest.raises(Exception) as exc_info:
            decode_bearer_token(token)
        assert getattr(exc_info.value, "status_code", None) == 401


# ---------------------------------------------------------------------------
# assert_bearer_config (startup gate)
# ---------------------------------------------------------------------------


class TestAssertBearerConfig:
    def test_raises_when_authn_on_and_secret_missing(self, monkeypatch):
        monkeypatch.setenv("PROTEA_AUTHN_REQUIRED", "true")
        monkeypatch.delenv("PROTEA_JWT_SECRET", raising=False)
        with pytest.raises(RuntimeError, match="PROTEA_JWT_SECRET"):
            assert_bearer_config()

    def test_passes_when_authn_off(self, monkeypatch):
        monkeypatch.setenv("PROTEA_AUTHN_REQUIRED", "false")
        monkeypatch.delenv("PROTEA_JWT_SECRET", raising=False)
        assert_bearer_config()  # no raise

    def test_passes_when_secret_present(self, monkeypatch):
        monkeypatch.setenv("PROTEA_AUTHN_REQUIRED", "true")
        monkeypatch.setenv("PROTEA_JWT_SECRET", _SECRET)
        assert_bearer_config()  # no raise


# ---------------------------------------------------------------------------
# require_api_key_or_bearer end-to-end
# ---------------------------------------------------------------------------


@pytest.fixture()
def combined_client(monkeypatch):
    """Spin up a FastAPI app that mounts a route behind the combined dep."""
    monkeypatch.setenv("PROTEA_AUTHN_REQUIRED", "true")
    monkeypatch.setenv("PROTEA_JWT_SECRET", _SECRET)

    app = FastAPI()
    app.state.session_factory = MagicMock()

    router = APIRouter()

    @router.get(
        "/protected",
        dependencies=[Depends(require_api_key_or_bearer)],
    )
    def _protected():
        return {"ok": True}

    app.include_router(router)
    return TestClient(app, raise_server_exceptions=True)


class TestRequireApiKeyOrBearer:
    def test_valid_bearer_passes(self, combined_client):
        token = _mint()
        resp = combined_client.get(
            "/protected",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert resp.status_code == 200, resp.text

    def test_missing_header_returns_401(self, combined_client):
        resp = combined_client.get("/protected")
        assert resp.status_code == 401
        assert "bearer" in resp.headers["www-authenticate"].lower()

    def test_expired_bearer_returns_401(self, combined_client):
        token = _mint(ttl_seconds=-1)
        resp = combined_client.get(
            "/protected",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert resp.status_code == 401

    def test_bad_signature_returns_401(self, combined_client):
        token = _mint(secret="not-the-real-secret-padding-padding-padding")
        resp = combined_client.get(
            "/protected",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert resp.status_code == 401

    def test_authn_disabled_bypasses(self, monkeypatch):
        """``PROTEA_AUTHN_REQUIRED=false`` waves bearer routes through too."""
        monkeypatch.setenv("PROTEA_AUTHN_REQUIRED", "false")

        app = FastAPI()
        app.state.session_factory = MagicMock()

        router = APIRouter()

        @router.get(
            "/open",
            dependencies=[Depends(require_api_key_or_bearer)],
        )
        def _open():
            return {"ok": True}

        app.include_router(router)
        client = TestClient(app, raise_server_exceptions=True)
        resp = client.get("/open")
        assert resp.status_code == 200
