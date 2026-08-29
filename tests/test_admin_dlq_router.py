"""Unit tests for GET /admin/dlq/summary, POST /admin/dlq/replay, POST /admin/dlq/purge.

Auth enforcement and service-layer calls are fully mocked. No real
RabbitMQ or pika connection is opened.
"""

from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import jwt
from fastapi import FastAPI
from fastapi.testclient import TestClient

from protea.api.routers.admin import router

_SECRET = "feat-auth-test-secret-padding-padding-padding"
_ALG = "HS256"


def _mint(role: str) -> str:
    now = int(time.time())
    return jwt.encode(
        {"sub": "00000000-0000-0000-0000-000000000001", "iat": now, "exp": now + 60, "role": role},
        _SECRET,
        algorithm=_ALG,
    )


def _make_app(monkeypatch) -> FastAPI:
    monkeypatch.setenv("PROTEA_AUTHN_REQUIRED", "true")
    monkeypatch.setenv("PROTEA_JWT_SECRET", _SECRET)
    app = FastAPI()
    app.state.session_factory = MagicMock()
    app.state.amqp_url = "amqp://guest:guest@localhost:5672/"
    app.include_router(router)
    return app


# ---------------------------------------------------------------------------
# GET /admin/dlq/summary
# ---------------------------------------------------------------------------


class TestDlqSummaryEndpoint:
    def test_unauthenticated_returns_401(self, monkeypatch):
        app = _make_app(monkeypatch)
        resp = TestClient(app).get("/admin/dlq/summary")
        assert resp.status_code == 401

    @patch("protea.api.routers.admin.dlq_summary")
    def test_operator_allowed_for_read_only_summary(self, mock_summary, monkeypatch):
        # /admin/dlq/summary is a read-only observation endpoint;
        # the role floor was downgraded from admin to operator so
        # the on-call rotation can triage backlog before deciding to
        # escalate to a destructive replay/purge (which stay admin).
        mock_summary.return_value = {"total_peeked": 0, "queue_message_count": 0, "groups": []}
        app = _make_app(monkeypatch)
        resp = TestClient(app).get(
            "/admin/dlq/summary",
            headers={"Authorization": f"Bearer {_mint('operator')}"},
        )
        assert resp.status_code == 200

    @patch("protea.api.routers.admin.dlq_summary")
    def test_admin_gets_summary(self, mock_summary, monkeypatch):
        mock_summary.return_value = {
            "total_peeked": 10,
            "queue_message_count": 100,
            "groups": [
                {
                    "operation": "store_embeddings",
                    "first_death_queue": "protea.embeddings.write",
                    "age_bucket": ">7d",
                    "count": 10,
                }
            ],
        }
        app = _make_app(monkeypatch)
        resp = TestClient(app).get(
            "/admin/dlq/summary",
            headers={"Authorization": f"Bearer {_mint('admin')}"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["queue_message_count"] == 100
        assert data["groups"][0]["operation"] == "store_embeddings"
        mock_summary.assert_called_once()

    @patch("protea.api.routers.admin.dlq_summary")
    def test_service_exception_returns_503(self, mock_summary, monkeypatch):
        mock_summary.side_effect = RuntimeError("RabbitMQ down")
        app = _make_app(monkeypatch)
        resp = TestClient(app).get(
            "/admin/dlq/summary",
            headers={"Authorization": f"Bearer {_mint('admin')}"},
        )
        assert resp.status_code == 503


# ---------------------------------------------------------------------------
# POST /admin/dlq/replay
# ---------------------------------------------------------------------------


class TestDlqReplayEndpoint:
    def test_unauthenticated_returns_401(self, monkeypatch):
        app = _make_app(monkeypatch)
        resp = TestClient(app).post("/admin/dlq/replay", json={})
        assert resp.status_code == 401

    @patch("protea.api.routers.admin.dlq_replay")
    def test_admin_replay_dry_run(self, mock_replay, monkeypatch):
        mock_replay.return_value = {
            "replayed": 0,
            "would_replay": 50,
            "skipped": 5,
            "dry_run": True,
        }
        app = _make_app(monkeypatch)
        resp = TestClient(app).post(
            "/admin/dlq/replay",
            json={"operation": "store_embeddings", "dry_run": True},
            headers={"Authorization": f"Bearer {_mint('admin')}"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["dry_run"] is True
        assert data["would_replay"] == 50
        mock_replay.assert_called_once_with(
            "amqp://guest:guest@localhost:5672/",
            operation_filter="store_embeddings",
            queue_filter=None,
            target_queue=None,
            dry_run=True,
            max_messages=1000,
        )

    @patch("protea.api.routers.admin.dlq_replay")
    def test_service_exception_returns_503(self, mock_replay, monkeypatch):
        mock_replay.side_effect = Exception("broker gone")
        app = _make_app(monkeypatch)
        resp = TestClient(app).post(
            "/admin/dlq/replay",
            json={},
            headers={"Authorization": f"Bearer {_mint('admin')}"},
        )
        assert resp.status_code == 503


# ---------------------------------------------------------------------------
# POST /admin/dlq/purge
# ---------------------------------------------------------------------------


class TestDlqPurgeEndpoint:
    def test_unauthenticated_returns_401(self, monkeypatch):
        app = _make_app(monkeypatch)
        resp = TestClient(app).post("/admin/dlq/purge", json={})
        assert resp.status_code == 401

    def test_operator_rejected(self, monkeypatch):
        app = _make_app(monkeypatch)
        resp = TestClient(app).post(
            "/admin/dlq/purge",
            json={},
            headers={"Authorization": f"Bearer {_mint('operator')}"},
        )
        assert resp.status_code == 403

    @patch("protea.api.routers.admin.dlq_purge")
    def test_admin_purge_dry_run(self, mock_purge, monkeypatch):
        mock_purge.return_value = {
            "purged": 0,
            "would_purge": 7272,
            "skipped": 0,
            "dry_run": True,
        }
        app = _make_app(monkeypatch)
        resp = TestClient(app).post(
            "/admin/dlq/purge",
            json={"operation": "store_embeddings", "dry_run": True},
            headers={"Authorization": f"Bearer {_mint('admin')}"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["would_purge"] == 7272
        assert data["dry_run"] is True

    @patch("protea.api.routers.admin.dlq_purge")
    def test_admin_purge_live(self, mock_purge, monkeypatch):
        mock_purge.return_value = {
            "purged": 7272,
            "would_purge": 7272,
            "skipped": 0,
            "dry_run": False,
        }
        app = _make_app(monkeypatch)
        resp = TestClient(app).post(
            "/admin/dlq/purge",
            json={"operation": "store_embeddings", "dry_run": False},
            headers={"Authorization": f"Bearer {_mint('admin')}"},
        )
        assert resp.status_code == 200
        assert resp.json()["purged"] == 7272

    @patch("protea.api.routers.admin.dlq_purge")
    def test_service_exception_returns_503(self, mock_purge, monkeypatch):
        mock_purge.side_effect = Exception("connection lost")
        app = _make_app(monkeypatch)
        resp = TestClient(app).post(
            "/admin/dlq/purge",
            json={},
            headers={"Authorization": f"Bearer {_mint('admin')}"},
        )
        assert resp.status_code == 503


class TestSayingNothingAsksAndDoesNotAct:
    """An empty body is a question, not an instruction.

    Every field on both requests is optional, so ``{}`` is valid, and it used
    to mean "replay up to a thousand messages" and "discard up to ten
    thousand". This deployment runs with ``PROTEA_AUTHN_REQUIRED=false`` and
    ``role_of(None)`` returning admin, so one unauthenticated request with an
    empty body was enough. A dead letter is often the only surviving copy of
    the payload that produced it, and there is no undo for the purge.

    These assert the default itself rather than the endpoint, because the
    default is the whole change and a test that posts ``{"dry_run": true}``
    would pass whatever the default said.
    """

    def test_replay_defaults_to_asking(self) -> None:
        from protea.api.routers.admin import DlqReplayRequest

        assert DlqReplayRequest().dry_run is True

    def test_purge_defaults_to_asking(self) -> None:
        from protea.api.routers.admin import DlqPurgeRequest

        assert DlqPurgeRequest().dry_run is True

    def test_acting_still_takes_saying_so(self) -> None:
        """The safe default must not become an inability to act."""
        from protea.api.routers.admin import DlqPurgeRequest, DlqReplayRequest

        assert DlqReplayRequest(dry_run=False).dry_run is False
        assert DlqPurgeRequest(dry_run=False).dry_run is False
