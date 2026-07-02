"""Unit tests for the reranker-model management endpoints.

Covers the operator-controlled serve-selection surface added to
``protea/api/routers/reranker_models.py``:

* ``POST /reranker-models/{id}/activate`` — pins a booster active and
  deactivates any sibling already active in the same ``(category,
  aspect)`` serve slot (the exclusivity the ``active_or_latest_reranker``
  selector and the ``uq_reranker_model_active_slot`` partial unique index
  both key on).
* ``POST /reranker-models/{id}/deactivate`` — clears the flag.
* ``DELETE /reranker-models/{id}`` — removes a row, refusing with 409
  while the row is active.
* Authz: the operator role floor rejects a viewer bearer token.

No Postgres required: a hand-rolled fake session models ``get`` /
``query().filter().all()`` / ``delete`` so the slot logic is exercised
without SQL.
"""

from __future__ import annotations

import time
import uuid
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import jwt
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from protea.api.routers.reranker_models import router as reranker_models_router

_SECRET = "reranker-mgmt-test-secret-padding-padding-padding"  # >32 bytes
_ALG = "HS256"


def _mint(role: str | None, *, ttl: int = 60) -> str:
    now = int(time.time())
    payload: dict[str, object] = {
        "sub": "00000000-0000-0000-0000-000000000001",
        "iat": now,
        "exp": now + ttl,
    }
    if role is not None:
        payload["role"] = role
    return jwt.encode(payload, _SECRET, algorithm=_ALG)


def _model(*, category="pk", aspect="bpo", is_active=False, name="run-x"):
    return SimpleNamespace(
        id=uuid.uuid4(),
        name=name,
        category=category,
        aspect=aspect,
        is_active=is_active,
    )


class _FakeQuery:
    def __init__(self, rows):
        self._rows = rows

    def filter(self, *_args, **_kwargs):
        return self

    def all(self):
        return list(self._rows)


class _FakeSession:
    """Minimal session double for the management handlers.

    ``get`` returns the target by id; ``query`` returns whatever sibling
    list the test wires up (the handler already narrows to the same slot
    via SQL, which we stub out). ``delete`` records removed rows.
    """

    def __init__(self, target, siblings=None):
        self._target = target
        self._siblings = siblings or []
        self.deleted: list[object] = []

    def get(self, _model, id_):
        if self._target is not None and self._target.id == id_:
            return self._target
        return None

    def query(self, *_args):
        return _FakeQuery(self._siblings)

    def delete(self, obj):
        self.deleted.append(obj)

    def flush(self):
        pass


@contextmanager
def _scope(session):
    yield session


def _client(session: _FakeSession) -> TestClient:
    app = FastAPI()
    app.state.session_factory = MagicMock()
    app.include_router(reranker_models_router)
    return TestClient(app, raise_server_exceptions=True), app


class TestActivate:
    def test_activate_flips_is_active_and_deactivates_siblings(self):
        target = _model(is_active=False, name="pk-bpo-new")
        sib_a = _model(is_active=True, name="pk-bpo-old-a")
        sib_b = _model(is_active=True, name="pk-bpo-old-b")
        session = _FakeSession(target, siblings=[sib_a, sib_b])

        client, _ = _client(session)
        with patch(
            "protea.api.routers.reranker_models.session_scope",
            side_effect=lambda _f: _scope(session),
        ):
            resp = client.post(f"/reranker-models/{target.id}/activate")

        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["activated"]["id"] == str(target.id)
        assert body["activated"]["is_active"] is True
        assert set(body["deactivated"]) == {str(sib_a.id), str(sib_b.id)}
        # The target is active; the same-slot siblings were flipped off.
        assert target.is_active is True
        assert sib_a.is_active is False
        assert sib_b.is_active is False

    def test_activate_with_no_siblings_returns_empty_deactivated(self):
        target = _model(is_active=False)
        session = _FakeSession(target, siblings=[])

        client, _ = _client(session)
        with patch(
            "protea.api.routers.reranker_models.session_scope",
            side_effect=lambda _f: _scope(session),
        ):
            resp = client.post(f"/reranker-models/{target.id}/activate")

        assert resp.status_code == 200, resp.text
        assert resp.json()["deactivated"] == []
        assert target.is_active is True

    def test_activate_unknown_id_404(self):
        session = _FakeSession(None)
        client, _ = _client(session)
        with patch(
            "protea.api.routers.reranker_models.session_scope",
            side_effect=lambda _f: _scope(session),
        ):
            resp = client.post(f"/reranker-models/{uuid.uuid4()}/activate")
        assert resp.status_code == 404


class TestDeactivate:
    def test_deactivate_clears_flag(self):
        target = _model(is_active=True)
        session = _FakeSession(target)
        client, _ = _client(session)
        with patch(
            "protea.api.routers.reranker_models.session_scope",
            side_effect=lambda _f: _scope(session),
        ):
            resp = client.post(f"/reranker-models/{target.id}/deactivate")
        assert resp.status_code == 200, resp.text
        assert resp.json()["is_active"] is False
        assert target.is_active is False

    def test_deactivate_unknown_id_404(self):
        session = _FakeSession(None)
        client, _ = _client(session)
        with patch(
            "protea.api.routers.reranker_models.session_scope",
            side_effect=lambda _f: _scope(session),
        ):
            resp = client.post(f"/reranker-models/{uuid.uuid4()}/deactivate")
        assert resp.status_code == 404


class TestDelete:
    def test_delete_removes_inactive_row(self):
        target = _model(is_active=False)
        session = _FakeSession(target)
        client, _ = _client(session)
        with patch(
            "protea.api.routers.reranker_models.session_scope",
            side_effect=lambda _f: _scope(session),
        ):
            resp = client.delete(f"/reranker-models/{target.id}")
        assert resp.status_code == 200, resp.text
        assert resp.json()["id"] == str(target.id)
        assert session.deleted == [target]

    def test_delete_active_row_refused_409(self):
        target = _model(is_active=True)
        session = _FakeSession(target)
        client, _ = _client(session)
        with patch(
            "protea.api.routers.reranker_models.session_scope",
            side_effect=lambda _f: _scope(session),
        ):
            resp = client.delete(f"/reranker-models/{target.id}")
        assert resp.status_code == 409
        assert "deactivate" in resp.json()["detail"]
        # Row must NOT have been removed.
        assert session.deleted == []

    def test_delete_unknown_id_404(self):
        session = _FakeSession(None)
        client, _ = _client(session)
        with patch(
            "protea.api.routers.reranker_models.session_scope",
            side_effect=lambda _f: _scope(session),
        ):
            resp = client.delete(f"/reranker-models/{uuid.uuid4()}")
        assert resp.status_code == 404


class TestManagementAuthz:
    """The operator role floor guards every mutating management route."""

    def _guarded_client(self, session: _FakeSession, monkeypatch) -> TestClient:
        monkeypatch.setenv("PROTEA_AUTHN_REQUIRED", "true")
        monkeypatch.setenv("PROTEA_JWT_SECRET", _SECRET)
        client, _ = _client(session)
        return client

    def test_viewer_rejected_on_activate(self, monkeypatch):
        target = _model(is_active=False)
        session = _FakeSession(target)
        client = self._guarded_client(session, monkeypatch)
        with patch(
            "protea.api.routers.reranker_models.session_scope",
            side_effect=lambda _f: _scope(session),
        ):
            resp = client.post(
                f"/reranker-models/{target.id}/activate",
                headers={"Authorization": f"Bearer {_mint('viewer')}"},
            )
        assert resp.status_code == 403

    def test_viewer_rejected_on_delete(self, monkeypatch):
        target = _model(is_active=False)
        session = _FakeSession(target)
        client = self._guarded_client(session, monkeypatch)
        with patch(
            "protea.api.routers.reranker_models.session_scope",
            side_effect=lambda _f: _scope(session),
        ):
            resp = client.delete(
                f"/reranker-models/{target.id}",
                headers={"Authorization": f"Bearer {_mint('viewer')}"},
            )
        assert resp.status_code == 403

    def test_operator_allowed_on_activate(self, monkeypatch):
        target = _model(is_active=False)
        session = _FakeSession(target)
        client = self._guarded_client(session, monkeypatch)
        with patch(
            "protea.api.routers.reranker_models.session_scope",
            side_effect=lambda _f: _scope(session),
        ):
            resp = client.post(
                f"/reranker-models/{target.id}/activate",
                headers={"Authorization": f"Bearer {_mint('operator')}"},
            )
        assert resp.status_code == 200, resp.text


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-v"]))
