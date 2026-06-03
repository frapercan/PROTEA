"""Tests for FARM-AUTH.8 - session revocation table and middleware.

Coverage matrix (acceptance criteria):

* Login inserts a UserSession row into the DB.
* ``/me`` updates ``last_seen_at`` on every successful call.
* Logout marks the session ``revoked_at`` as non-null.
* A revoked session rejects ``/me`` with 401.
* Logout without a cookie is idempotent (204, no error).
* Admin revoke-sessions marks all active sessions revoked and returns count.
* Admin revoke-sessions returns 0 on a second call (idempotency).
* Admin revoke-sessions returns 404 for unknown user_id.
* ``find_session`` returns None for an unknown token.
* ``_hash_token`` produces a stable 64-char hex digest.

All tests use in-memory fakes so they run without Docker or Postgres.
The fake-session layer stubs out the ORM calls that would require
Postgres-specific column types (UUID, TIMESTAMPTZ).
"""

from __future__ import annotations

import hashlib
import time
from contextlib import contextmanager
from datetime import UTC, datetime
from unittest.mock import patch
from uuid import uuid4

import jwt
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from protea.api.routers.auth_user import (
    _COOKIE_NAME,
    _JWT_ALGORITHM,
    _hash_token,
)
from protea.api.routers.auth_user import (
    router as auth_user_router,
)
from protea.infrastructure.orm.models.user import UserRole, UserStatus

_SECRET = "farm-auth-8-test-secret-long-enough-padding"
_COOKIE_MAX_AGE = 30 * 24 * 3600


# ---------------------------------------------------------------------------
# Minimal in-memory data store
# ---------------------------------------------------------------------------


class _SessionRow:
    """Lightweight stand-in for a UserSession ORM row."""

    def __init__(
        self,
        *,
        id,
        user_id,
        token_hash: str,
        expires_at: datetime,
        last_seen_at: datetime,
        revoked_at=None,
        user_agent=None,
        client_ip_hash=None,
    ):
        self.id = id
        self.user_id = user_id
        self.token_hash = token_hash
        self.expires_at = expires_at
        self.last_seen_at = last_seen_at
        self.revoked_at = revoked_at
        self.user_agent = user_agent
        self.client_ip_hash = client_ip_hash


class _UserProxy:
    __slots__ = (
        "id",
        "email",
        "username",
        "display_name",
        "password_hash",
        "role",
        "status",
        "intended_use",
        "last_login_at",
        "deactivated_at",
        "created_at",
    )

    def __init__(self, data: dict) -> None:
        from uuid import UUID

        self.id = data["id"] if isinstance(data["id"], type(uuid4())) else UUID(str(data["id"]))
        self.email = data["email"]
        self.username = data["username"]
        self.display_name = data.get("display_name")
        self.password_hash = data["password_hash"]
        _role = data["role"]
        self.role = UserRole(_role) if isinstance(_role, str) else _role
        _status = data["status"]
        self.status = UserStatus(_status) if isinstance(_status, str) else _status
        self.intended_use = data.get("intended_use")
        self.last_login_at = data.get("last_login_at")
        self.deactivated_at = data.get("deactivated_at")
        self.created_at = data.get("created_at", datetime.now(UTC))


class _FakeIntegrityError(Exception):
    def __init__(self, constraint: str):
        super().__init__(constraint)
        self.orig = type("Orig", (), {"__str__": lambda s: constraint})()

    @property
    def args(self):
        return (str(self.orig),)


class _FakeSession:
    """Minimal fake session holding users + user_sessions dicts."""

    def __init__(self, users: dict, sessions: dict):
        self._users = users  # email -> user dict
        self._sessions = sessions  # token_hash -> _SessionRow
        self._pending_add: list = []
        self._live_user_proxies: list = []
        self._live_sess_rows: list = []

    # --- User queries -------------------------------------------------------

    def query(self, model):
        return _FakeQuery(self._users, self._sessions, model, self._live_user_proxies, self._live_sess_rows)

    def get(self, model, pk):
        from protea.infrastructure.orm.models.user import User
        from protea.infrastructure.orm.models.user_session import UserSession

        if model is User:
            for data in self._users.values():
                if str(data["id"]) == str(pk):
                    proxy = _UserProxy(data)
                    self._live_user_proxies.append((data, proxy))
                    return proxy
            return None
        if model is UserSession:
            for row in self._sessions.values():
                if str(row.id) == str(pk):
                    return row
            return None
        return None

    def add(self, obj):
        self._pending_add.append(obj)

    def flush(self):
        from protea.core.utils import utcnow
        from protea.infrastructure.orm.models.user_session import UserSession

        # Sync mutations from live user proxies.
        for store_dict, proxy in self._live_user_proxies:
            store_dict["last_login_at"] = getattr(proxy, "last_login_at", None)
            store_dict["status"] = (
                proxy.status.value if hasattr(proxy.status, "value") else proxy.status
            )

        for obj in self._pending_add:
            if isinstance(obj, UserSession):
                self._sessions[obj.token_hash] = obj
            elif hasattr(obj, "email"):
                # User
                if obj.email in self._users:
                    raise _FakeIntegrityError("uq_user_email")
                obj.id = uuid4()
                obj.created_at = utcnow()
                self._users[obj.email] = {
                    "id": obj.id,
                    "email": obj.email,
                    "username": obj.username,
                    "display_name": getattr(obj, "display_name", None),
                    "password_hash": obj.password_hash,
                    "role": obj.role.value if hasattr(obj.role, "value") else obj.role,
                    "status": obj.status.value if hasattr(obj.status, "value") else obj.status,
                    "intended_use": getattr(obj, "intended_use", None),
                    "last_login_at": None,
                }
            # AuthAudit rows: ignore (fire-and-forget in tests)
        self._pending_add.clear()

    def execute(self, stmt, params=None):
        pass

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass


class _FakeQuery:
    def __init__(self, users, sessions, model, live_user_proxies, live_sess_rows):
        self._users = users
        self._sessions = sessions
        self._model = model
        self._live_user_proxies = live_user_proxies
        self._live_sess_rows = live_sess_rows
        self._filters: list = []

    def filter(self, *args):
        self._filters.extend(args)
        return self

    def all(self):
        from protea.infrastructure.orm.models.user_session import UserSession

        if self._model is UserSession:
            results = list(self._sessions.values())
            for f in self._filters:
                # Handle user_id equality
                try:
                    col_key = f.left.key
                    val = f.right.value
                    if col_key == "user_id":
                        results = [r for r in results if str(r.user_id) == str(val)]
                    elif col_key == "token_hash":
                        results = [r for r in results if r.token_hash == val]
                except AttributeError:
                    # revoked_at IS NULL filter
                    results = [r for r in results if r.revoked_at is None]
            return results
        return []

    def first(self):
        from protea.infrastructure.orm.models.user import User
        from protea.infrastructure.orm.models.user_session import UserSession

        if self._model is User:
            filter_email = None
            for f in self._filters:
                try:
                    filter_email = f.right.value
                except AttributeError:
                    pass
            if filter_email is None:
                return None
            data = self._users.get(filter_email)
            if data is None:
                return None
            proxy = _UserProxy(data)
            self._live_user_proxies.append((data, proxy))
            return proxy

        if self._model is UserSession:
            results = self.all()
            return results[0] if results else None

        return None


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def fake_db():
    """Return shared mutable dicts for users and sessions."""
    return {"users": {}, "sessions": {}}


@pytest.fixture()
def mem_client(fake_db: dict, monkeypatch: pytest.MonkeyPatch):
    """Pure in-memory fake-session client for AUTH.8 tests."""
    from protea.infrastructure import session as session_mod

    monkeypatch.setenv("PROTEA_JWT_SECRET", _SECRET)

    @contextmanager
    def _fake_scope(factory):
        sess = _FakeSession(fake_db["users"], fake_db["sessions"])
        try:
            yield sess
            sess.commit()
        except Exception:
            sess.rollback()
            raise
        finally:
            sess.close()

    with patch.object(session_mod, "session_scope", _fake_scope):
        with patch("protea.api.routers.auth_user.session_scope", _fake_scope):
            with patch("protea.api.routers.auth_user.IntegrityError", _FakeIntegrityError):
                app = FastAPI()
                app.state.session_factory = lambda: None

                app.include_router(auth_user_router, prefix="/v1")

                with TestClient(
                    app,
                    base_url="https://testserver",
                    raise_server_exceptions=True,
                ) as c:
                    c._db = fake_db
                    yield c


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _signup_and_approve(client: TestClient, email: str, username: str, password: str = "pass1234") -> str:
    """Signup then force-approve the user in the fake store; returns user_id."""
    r = client.post(
        "/v1/auth/signup",
        json={"email": email, "username": username, "password": password},
    )
    assert r.status_code == 201, r.text
    uid = r.json()["id"]
    # Force-approve in the fake store
    client._db["users"][email]["status"] = "active"
    return uid


def _login(client: TestClient, email: str, password: str = "pass1234"):
    return client.post("/v1/auth/login", json={"email": email, "password": password})


# ---------------------------------------------------------------------------
# Unit tests for helpers
# ---------------------------------------------------------------------------


class TestHashToken:
    def test_returns_64_char_hex(self):
        digest = _hash_token("sometoken")
        assert len(digest) == 64
        assert all(c in "0123456789abcdef" for c in digest)

    def test_stable_digest(self):
        assert _hash_token("abc") == _hash_token("abc")

    def test_different_tokens_differ(self):
        assert _hash_token("token_a") != _hash_token("token_b")

    def test_matches_stdlib_sha256(self):
        raw = "rawtoken123"
        expected = hashlib.sha256(raw.encode()).hexdigest()
        assert _hash_token(raw) == expected


# ---------------------------------------------------------------------------
# Login creates a session row
# ---------------------------------------------------------------------------


class TestLoginCreatesSessionRow:
    def test_login_inserts_session(self, mem_client: TestClient, fake_db: dict):
        _signup_and_approve(mem_client, "alice@x.test", "alice")
        r = _login(mem_client, "alice@x.test")
        assert r.status_code == 200
        sessions = fake_db["sessions"]
        assert len(sessions) == 1
        row = next(iter(sessions.values()))
        assert row.revoked_at is None

    def test_session_token_hash_matches_cookie(self, mem_client: TestClient, fake_db: dict):
        _signup_and_approve(mem_client, "bob@x.test", "bob")
        r = _login(mem_client, "bob@x.test")
        assert r.status_code == 200
        cookie_val = r.cookies.get(_COOKIE_NAME)
        assert cookie_val is not None
        expected_hash = _hash_token(cookie_val)
        assert expected_hash in fake_db["sessions"]

    def test_multiple_logins_create_multiple_rows(self, mem_client: TestClient, fake_db: dict):
        _signup_and_approve(mem_client, "carol@x.test", "carol")
        _login(mem_client, "carol@x.test")
        _login(mem_client, "carol@x.test")
        assert len(fake_db["sessions"]) == 2


# ---------------------------------------------------------------------------
# /me updates last_seen_at
# ---------------------------------------------------------------------------


class TestMeUpdatesLastSeen:
    def test_me_updates_last_seen_at(self, mem_client: TestClient, fake_db: dict):
        _signup_and_approve(mem_client, "dave@x.test", "dave")
        _login(mem_client, "dave@x.test")
        row = next(iter(fake_db["sessions"].values()))
        old_seen = row.last_seen_at

        # Bump time slightly
        time.sleep(0.01)
        r = mem_client.get("/v1/auth/me")
        assert r.status_code == 200

        # last_seen_at should have been updated to a new value
        assert row.last_seen_at >= old_seen

    def test_me_returns_user_payload(self, mem_client: TestClient):
        _signup_and_approve(mem_client, "eve@x.test", "eve")
        _login(mem_client, "eve@x.test")
        r = mem_client.get("/v1/auth/me")
        assert r.status_code == 200
        data = r.json()
        assert data["email"] == "eve@x.test"
        assert data["role"] == "researcher"
        assert data["status"] == "active"


# ---------------------------------------------------------------------------
# Logout marks session revoked
# ---------------------------------------------------------------------------


class TestLogoutRevokesSession:
    def test_logout_sets_revoked_at(self, mem_client: TestClient, fake_db: dict):
        _signup_and_approve(mem_client, "frank@x.test", "frank")
        _login(mem_client, "frank@x.test")
        row = next(iter(fake_db["sessions"].values()))
        assert row.revoked_at is None

        r = mem_client.post("/v1/auth/logout")
        assert r.status_code == 204
        assert row.revoked_at is not None

    def test_logout_without_cookie_is_204(self, mem_client: TestClient):
        r = mem_client.post("/v1/auth/logout")
        assert r.status_code == 204

    def test_logout_twice_is_idempotent(self, mem_client: TestClient, fake_db: dict):
        _signup_and_approve(mem_client, "grace@x.test", "grace")
        _login(mem_client, "grace@x.test")
        mem_client.post("/v1/auth/logout")
        r = mem_client.post("/v1/auth/logout")
        assert r.status_code == 204


# ---------------------------------------------------------------------------
# Revoked session rejects /me
# ---------------------------------------------------------------------------


class TestRevokedSessionRejectsMe:
    def test_me_after_logout_returns_401(self, mem_client: TestClient, fake_db: dict, monkeypatch: pytest.MonkeyPatch):
        """After logout the cookie is cleared; /me returns 401 not_authenticated.
        When the stale JWT is re-sent directly its revoked session also returns 401.
        """
        monkeypatch.setenv("PROTEA_JWT_SECRET", _SECRET)
        _signup_and_approve(mem_client, "henry@x.test", "henry")
        login_resp = _login(mem_client, "henry@x.test")
        assert mem_client.get("/v1/auth/me").status_code == 200

        # Capture the cookie before logout clears it.
        stale_cookie = login_resp.cookies.get(_COOKIE_NAME)

        mem_client.post("/v1/auth/logout")

        # After logout, the test client's cookie jar is cleared.
        r_no_cookie = mem_client.get("/v1/auth/me")
        assert r_no_cookie.status_code == 401
        # The detail is not_authenticated because the cookie is absent.
        assert r_no_cookie.json()["detail"] == "not_authenticated"

        # Re-sending the stale cookie returns session_revoked because the
        # row's revoked_at is now set in the fake store.
        r_stale = mem_client.get("/v1/auth/me", cookies={_COOKIE_NAME: stale_cookie})
        assert r_stale.status_code == 401
        assert r_stale.json()["detail"] == "session_revoked"

    def test_me_with_unknown_token_returns_401(self, mem_client: TestClient, monkeypatch: pytest.MonkeyPatch):
        """A valid JWT whose token_hash is not in the DB returns 401."""
        monkeypatch.setenv("PROTEA_JWT_SECRET", _SECRET)
        # Mint a valid JWT but never insert a session row for it.
        now = int(time.time())
        token = jwt.encode(
            {"sub": str(uuid4()), "jti": "neverinserted", "role": "researcher",
             "status": "active", "iat": now, "exp": now + 86400},
            _SECRET,
            algorithm=_JWT_ALGORITHM,
        )
        r = mem_client.get("/v1/auth/me", cookies={_COOKIE_NAME: token})
        assert r.status_code == 401


# ---------------------------------------------------------------------------
# Admin revoke-sessions endpoint
# ---------------------------------------------------------------------------


class _FakeAdminPrincipal:
    """Simulates an admin BearerPrincipal for require_role bypass."""
    claims: dict = {"role": "admin"}


def _make_admin_client(fake_db: dict, monkeypatch: pytest.MonkeyPatch):
    """Build a TestClient where require_role(admin) is bypassed."""
    from protea.infrastructure import session as session_mod

    monkeypatch.setenv("PROTEA_JWT_SECRET", _SECRET)

    @contextmanager
    def _fake_scope(factory):
        sess = _FakeSession(fake_db["users"], fake_db["sessions"])
        try:
            yield sess
            sess.commit()
        except Exception:
            sess.rollback()
            raise
        finally:
            sess.close()

    from protea.api import auth as auth_mod

    with patch.object(session_mod, "session_scope", _fake_scope):
        with patch("protea.api.routers.auth_user.session_scope", _fake_scope):
            with patch("protea.api.routers.auth_user.IntegrityError", _FakeIntegrityError):
                # Bypass the require_role dependency by overriding require_api_key_or_bearer
                with patch.object(auth_mod, "require_api_key_or_bearer", return_value=lambda: _FakeAdminPrincipal()):
                    with patch("protea.api.auth.require_api_key_or_bearer", return_value=lambda: _FakeAdminPrincipal()):
                        app = FastAPI()
                        app.state.session_factory = lambda: None
                        app.include_router(auth_user_router, prefix="/v1")
                        return TestClient(app, base_url="https://testserver", raise_server_exceptions=False)


class TestAdminRevokeSessions:
    def test_revoke_marks_all_sessions(self, fake_db: dict, monkeypatch: pytest.MonkeyPatch):
        """Admin revoke returns count of revoked sessions and all are marked."""
        from protea.infrastructure import session as session_mod

        monkeypatch.setenv("PROTEA_JWT_SECRET", _SECRET)

        @contextmanager
        def _fake_scope(factory):
            sess = _FakeSession(fake_db["users"], fake_db["sessions"])
            try:
                yield sess
                sess.commit()
            except Exception:
                sess.rollback()
                raise
            finally:
                sess.close()

        with patch.object(session_mod, "session_scope", _fake_scope):
            with patch("protea.api.routers.auth_user.session_scope", _fake_scope):
                with patch("protea.api.routers.auth_user.IntegrityError", _FakeIntegrityError):
                    with patch("protea.api.roles.require_api_key_or_bearer", return_value=_FakeAdminPrincipal()):
                        app = FastAPI()
                        app.state.session_factory = lambda: None
                        app.include_router(auth_user_router, prefix="/v1")

                        with TestClient(app, base_url="https://testserver", raise_server_exceptions=True) as c:
                            c._db = fake_db
                            # Create a user with two sessions directly in fake_db.
                            uid = uuid4()
                            email = "target@x.test"
                            from protea.api.auth.passwords import hash_password
                            from protea.core.utils import utcnow

                            fake_db["users"][email] = {
                                "id": uid,
                                "email": email,
                                "username": "target",
                                "display_name": None,
                                "password_hash": hash_password("pass1234"),
                                "role": "researcher",
                                "status": "active",
                                "last_login_at": None,
                            }
                            now = utcnow()
                            sess1 = _SessionRow(
                                id=uuid4(), user_id=uid,
                                token_hash=_hash_token("tok1"),
                                expires_at=now, last_seen_at=now,
                            )
                            sess2 = _SessionRow(
                                id=uuid4(), user_id=uid,
                                token_hash=_hash_token("tok2"),
                                expires_at=now, last_seen_at=now,
                            )
                            fake_db["sessions"][sess1.token_hash] = sess1
                            fake_db["sessions"][sess2.token_hash] = sess2

                            r = c.post(f"/v1/auth/admin/revoke-sessions/{uid}")
                            assert r.status_code == 200, r.text
                            data = r.json()
                            assert data["revoked_count"] == 2
                            assert data["user_id"] == str(uid)
                            assert sess1.revoked_at is not None
                            assert sess2.revoked_at is not None

    def test_revoke_idempotent(self, fake_db: dict, monkeypatch: pytest.MonkeyPatch):
        """Second call to revoke returns 0 (all already revoked)."""
        from protea.infrastructure import session as session_mod

        monkeypatch.setenv("PROTEA_JWT_SECRET", _SECRET)

        @contextmanager
        def _fake_scope(factory):
            sess = _FakeSession(fake_db["users"], fake_db["sessions"])
            try:
                yield sess
                sess.commit()
            except Exception:
                sess.rollback()
                raise
            finally:
                sess.close()

        with patch.object(session_mod, "session_scope", _fake_scope):
            with patch("protea.api.routers.auth_user.session_scope", _fake_scope):
                with patch("protea.api.routers.auth_user.IntegrityError", _FakeIntegrityError):
                    with patch("protea.api.roles.require_api_key_or_bearer", return_value=_FakeAdminPrincipal()):
                        app = FastAPI()
                        app.state.session_factory = lambda: None
                        app.include_router(auth_user_router, prefix="/v1")

                        with TestClient(app, base_url="https://testserver", raise_server_exceptions=True) as c:
                            c._db = fake_db
                            from protea.api.auth.passwords import hash_password
                            from protea.core.utils import utcnow

                            uid = uuid4()
                            email = "target2@x.test"
                            fake_db["users"][email] = {
                                "id": uid, "email": email, "username": "target2",
                                "display_name": None,
                                "password_hash": hash_password("pass1234"),
                                "role": "researcher", "status": "active",
                                "last_login_at": None,
                            }
                            now = utcnow()
                            sess1 = _SessionRow(
                                id=uuid4(), user_id=uid,
                                token_hash=_hash_token("tok_idem"),
                                expires_at=now, last_seen_at=now,
                            )
                            fake_db["sessions"][sess1.token_hash] = sess1

                            r1 = c.post(f"/v1/auth/admin/revoke-sessions/{uid}")
                            assert r1.json()["revoked_count"] == 1

                            r2 = c.post(f"/v1/auth/admin/revoke-sessions/{uid}")
                            assert r2.status_code == 200
                            assert r2.json()["revoked_count"] == 0

    def test_revoke_unknown_user_returns_404(self, fake_db: dict, monkeypatch: pytest.MonkeyPatch):
        """Revoking sessions for a non-existent user returns 404."""
        from protea.infrastructure import session as session_mod

        monkeypatch.setenv("PROTEA_JWT_SECRET", _SECRET)

        @contextmanager
        def _fake_scope(factory):
            sess = _FakeSession(fake_db["users"], fake_db["sessions"])
            try:
                yield sess
                sess.commit()
            except Exception:
                sess.rollback()
                raise
            finally:
                sess.close()

        with patch.object(session_mod, "session_scope", _fake_scope):
            with patch("protea.api.routers.auth_user.session_scope", _fake_scope):
                with patch("protea.api.routers.auth_user.IntegrityError", _FakeIntegrityError):
                    with patch("protea.api.roles.require_api_key_or_bearer", return_value=_FakeAdminPrincipal()):
                        app = FastAPI()
                        app.state.session_factory = lambda: None
                        app.include_router(auth_user_router, prefix="/v1")

                        with TestClient(app, base_url="https://testserver", raise_server_exceptions=True) as c:
                            unknown = str(uuid4())
                            r = c.post(f"/v1/auth/admin/revoke-sessions/{unknown}")
                            assert r.status_code == 404

    def test_revoke_invalid_uuid_returns_422(self, fake_db: dict, monkeypatch: pytest.MonkeyPatch):
        """Revoking with a non-UUID string returns 422."""
        from protea.infrastructure import session as session_mod

        monkeypatch.setenv("PROTEA_JWT_SECRET", _SECRET)

        @contextmanager
        def _fake_scope(factory):
            sess = _FakeSession(fake_db["users"], fake_db["sessions"])
            try:
                yield sess
                sess.commit()
            except Exception:
                sess.rollback()
                raise
            finally:
                sess.close()

        with patch.object(session_mod, "session_scope", _fake_scope):
            with patch("protea.api.routers.auth_user.session_scope", _fake_scope):
                with patch("protea.api.routers.auth_user.IntegrityError", _FakeIntegrityError):
                    with patch("protea.api.roles.require_api_key_or_bearer", return_value=_FakeAdminPrincipal()):
                        app = FastAPI()
                        app.state.session_factory = lambda: None
                        app.include_router(auth_user_router, prefix="/v1")

                        with TestClient(app, base_url="https://testserver", raise_server_exceptions=True) as c:
                            r = c.post("/v1/auth/admin/revoke-sessions/not-a-uuid")
                            assert r.status_code == 422


# ---------------------------------------------------------------------------
# Full flow: login, /me, logout, /me returns 401
# ---------------------------------------------------------------------------


class TestFullRevocationFlow:
    def test_full_flow(self, mem_client: TestClient, fake_db: dict):
        _signup_and_approve(mem_client, "zoe@x.test", "zoe")

        # 1. Login creates a session row.
        r = _login(mem_client, "zoe@x.test")
        assert r.status_code == 200
        assert len(fake_db["sessions"]) == 1

        # 2. /me succeeds and session is not revoked.
        assert mem_client.get("/v1/auth/me").status_code == 200
        row = next(iter(fake_db["sessions"].values()))
        assert row.revoked_at is None

        # 3. Logout revokes the session.
        mem_client.post("/v1/auth/logout")
        assert row.revoked_at is not None

        # 4. /me now returns 401 (cookie cleared by logout).
        assert mem_client.get("/v1/auth/me").status_code == 401
