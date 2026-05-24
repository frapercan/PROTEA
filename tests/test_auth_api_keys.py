"""Unit tests for the API key authentication primitives (T5.6a).

Covers the in-isolation behaviour of :mod:`protea.api.auth` without
needing a Postgres container:

* key generation entropy + prefix length
* hash + compare round-trip
* header extraction precedence
* ``PROTEA_AUTHN_REQUIRED`` env knob short-circuit
* 401 / revoked / missing flows via FastAPI ``TestClient``

The companion :mod:`tests.test_auth_api_keys_pg` exercises the same
endpoints against a real Postgres engine.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC
from unittest.mock import MagicMock, patch

import pytest
from fastapi import APIRouter, Depends, FastAPI
from fastapi.testclient import TestClient

from protea.api.auth import (
    PREFIX_LEN,
    _authn_required,
    _extract_raw_key,
    _lookup_and_validate,
    generate_raw_key,
    hash_key,
    prefix_of,
    require_api_key,
)

# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


class TestKeyGeneration:
    def test_generate_raw_key_is_url_safe_and_long(self):
        k = generate_raw_key()
        # ``token_urlsafe(32)`` returns roughly 43 chars; lower bound is enough.
        assert len(k) >= PREFIX_LEN + 8
        # url-safe = letters + digits + '-' + '_'.
        assert all(c.isalnum() or c in "-_" for c in k)

    def test_each_key_is_distinct(self):
        keys = {generate_raw_key() for _ in range(50)}
        assert len(keys) == 50

    def test_hash_key_round_trip(self):
        raw = generate_raw_key()
        h1 = hash_key(raw)
        h2 = hash_key(raw)
        assert h1 == h2
        assert len(h1) == 64  # sha256 hex digest
        assert hash_key(raw + "x") != h1

    def test_prefix_of_takes_first_n_chars(self):
        raw = "abcdefghij1234567890"
        assert prefix_of(raw) == raw[:PREFIX_LEN]
        assert len(prefix_of(raw)) == PREFIX_LEN


# ---------------------------------------------------------------------------
# Header extraction
# ---------------------------------------------------------------------------


class TestHeaderExtraction:
    def test_authorization_apikey_header_wins(self):
        assert _extract_raw_key("ApiKey abcd", None) == "abcd"

    def test_apikey_scheme_is_case_insensitive(self):
        assert _extract_raw_key("apikey xyz", None) == "xyz"
        assert _extract_raw_key("APIKEY xyz", None) == "xyz"

    def test_x_api_key_header_used_when_no_authorization(self):
        assert _extract_raw_key(None, "secret") == "secret"

    def test_authorization_precedence_over_x_api_key(self):
        assert _extract_raw_key("ApiKey aaa", "bbb") == "aaa"

    def test_bearer_scheme_is_ignored(self):
        # Bearer JWT lands in T5.6b; the ApiKey dep must not eat it.
        assert _extract_raw_key("Bearer some.jwt.token", None) is None

    def test_empty_headers_return_none(self):
        assert _extract_raw_key(None, None) is None
        assert _extract_raw_key("", "") is None
        assert _extract_raw_key("ApiKey  ", None) is None


# ---------------------------------------------------------------------------
# Env knob
# ---------------------------------------------------------------------------


class TestAuthnRequiredEnv:
    def test_default_is_on(self, monkeypatch):
        monkeypatch.delenv("PROTEA_AUTHN_REQUIRED", raising=False)
        assert _authn_required() is True

    @pytest.mark.parametrize("v", ["0", "false", "no", "off", "FALSE"])
    def test_falsy_values_disable(self, monkeypatch, v):
        monkeypatch.setenv("PROTEA_AUTHN_REQUIRED", v)
        assert _authn_required() is False

    @pytest.mark.parametrize("v", ["1", "true", "yes", "on", "True"])
    def test_truthy_values_enable(self, monkeypatch, v):
        monkeypatch.setenv("PROTEA_AUTHN_REQUIRED", v)
        assert _authn_required() is True


# ---------------------------------------------------------------------------
# End-to-end: require_api_key behind a stub session_factory
# ---------------------------------------------------------------------------


@contextmanager
def _scope(session):
    yield session


@pytest.fixture()
def protected_client(monkeypatch):
    """Mount a tiny protected endpoint behind ``require_api_key``."""
    # Force authn on for this client even if the shell has it off.
    monkeypatch.setenv("PROTEA_AUTHN_REQUIRED", "true")

    session = MagicMock()

    app = FastAPI()
    app.state.session_factory = MagicMock()

    router = APIRouter()

    @router.get("/secret", dependencies=[Depends(require_api_key)])
    def _secret():
        return {"ok": True}

    app.include_router(router)

    with patch(
        "protea.api.auth.session_scope",
        side_effect=lambda _: _scope(session),
    ):
        yield TestClient(app, raise_server_exceptions=True), session


class TestRequireApiKey:
    def test_missing_header_returns_401(self, protected_client):
        client, _ = protected_client
        resp = client.get("/secret")
        assert resp.status_code == 401
        assert resp.headers["www-authenticate"] == "ApiKey"

    def test_invalid_key_returns_401(self, protected_client):
        client, session = protected_client
        # No row matches that prefix.
        session.query.return_value.filter.return_value.all.return_value = []
        resp = client.get("/secret", headers={"X-Api-Key": "bogus-key-value"})
        assert resp.status_code == 401

    def test_valid_key_returns_200(self, protected_client):
        client, session = protected_client
        raw = generate_raw_key()
        row = MagicMock()
        row.id = "00000000-0000-0000-0000-000000000001"
        row.key_hash = hash_key(raw)
        row.prefix = prefix_of(raw)
        row.name = "test-key"
        row.revoked_at = None
        row.created_at = None
        row.last_used_at = None
        session.query.return_value.filter.return_value.all.return_value = [row]

        resp = client.get("/secret", headers={"X-Api-Key": raw})
        assert resp.status_code == 200, resp.text
        assert resp.json() == {"ok": True}

    def test_revoked_key_returns_401(self, protected_client):
        client, session = protected_client
        raw = generate_raw_key()
        row = MagicMock()
        row.id = "00000000-0000-0000-0000-000000000002"
        row.key_hash = hash_key(raw)
        row.prefix = prefix_of(raw)
        row.name = "revoked-key"
        # revoked timestamp present → reject.
        from datetime import datetime

        row.revoked_at = datetime.now(UTC)
        row.created_at = None
        row.last_used_at = None
        session.query.return_value.filter.return_value.all.return_value = [row]

        resp = client.get("/secret", headers={"X-Api-Key": raw})
        assert resp.status_code == 401
        assert "revoked" in resp.json().get("detail", "")

    def test_snapshot_carries_role_column(self, monkeypatch):
        """Regression: `_lookup_and_validate` must copy ``role`` into the
        detached snapshot so downstream `role_of()` sees the real value
        instead of collapsing every api_key request to viewer.

        Without ``role=matched.role`` in the snapshot ctor, operator/admin
        api_keys silently degraded to viewer at every gate, breaking
        FARM-EXP.13 dispatch (the bug surfaced 2026-05-25).
        """
        monkeypatch.setenv("PROTEA_AUTHN_REQUIRED", "true")
        raw = generate_raw_key()
        row = MagicMock()
        row.id = "00000000-0000-0000-0000-000000000010"
        row.key_hash = hash_key(raw)
        row.prefix = prefix_of(raw)
        row.name = "operator-key"
        row.role = "operator"
        row.revoked_at = None
        row.created_at = None
        row.last_used_at = None

        session = MagicMock()
        session.query.return_value.filter.return_value.all.return_value = [row]
        factory = MagicMock()
        with patch(
            "protea.api.auth.session_scope",
            side_effect=lambda _: _scope(session),
        ):
            snapshot, err = _lookup_and_validate(factory, raw)
        assert err is None
        assert snapshot is not None
        assert snapshot.role == "operator"

    def test_authn_disabled_short_circuits(self, monkeypatch):
        """``PROTEA_AUTHN_REQUIRED=false`` waves every request through."""
        monkeypatch.setenv("PROTEA_AUTHN_REQUIRED", "false")

        app = FastAPI()
        app.state.session_factory = MagicMock()

        router = APIRouter()

        @router.get("/open", dependencies=[Depends(require_api_key)])
        def _open():
            return {"ok": True}

        app.include_router(router)
        client = TestClient(app, raise_server_exceptions=True)
        resp = client.get("/open")
        assert resp.status_code == 200
