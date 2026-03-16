"""Unit tests for support and maintenance API routers — no real DB required."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from protea.api.routers.support import router as support_router
from protea.api.routers.maintenance import router as maintenance_router


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@contextmanager
def _mock_scope(session):
    yield session


def _make_app_with_router(router, session):
    app = FastAPI()
    factory = MagicMock()
    app.state.session_factory = factory
    app.include_router(router)
    return app, factory


# ---------------------------------------------------------------------------
# Support router
# ---------------------------------------------------------------------------

@pytest.fixture()
def support_session():
    return MagicMock()


@pytest.fixture()
def support_client(support_session):
    app, _ = _make_app_with_router(support_router, support_session)
    with patch("protea.api.routers.support.session_scope",
               side_effect=lambda _: _mock_scope(support_session)):
        yield TestClient(app, raise_server_exceptions=True)


class TestGetSupport:
    def test_returns_count_and_comments(self, support_client, support_session):
        support_session.query.return_value.count.return_value = 5
        support_session.query.return_value.filter.return_value.order_by.return_value.limit.return_value.all.return_value = []
        resp = support_client.get("/support")
        assert resp.status_code == 200
        data = resp.json()
        assert "count" in data
        assert "comments" in data

    def test_all_comments_flag(self, support_client, support_session):
        support_session.query.return_value.count.return_value = 3
        support_session.query.return_value.filter.return_value.order_by.return_value.limit.return_value.all.return_value = []
        resp = support_client.get("/support?all_comments=true")
        assert resp.status_code == 200

    def test_comments_serialized(self, support_client, support_session):
        entry = MagicMock()
        entry.id = uuid4()
        entry.comment = "Great tool!"
        entry.created_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
        support_session.query.return_value.count.return_value = 1
        support_session.query.return_value.filter.return_value.order_by.return_value.limit.return_value.all.return_value = [entry]
        resp = support_client.get("/support")
        assert resp.status_code == 200
        comments = resp.json()["comments"]
        assert len(comments) == 1
        assert comments[0]["comment"] == "Great tool!"


class TestPostSupport:
    def test_submit_with_comment(self, support_client, support_session):
        entry = MagicMock()
        entry.id = uuid4()

        def add_side(obj):
            obj.id = entry.id

        support_session.add.side_effect = add_side
        support_session.flush = MagicMock()
        support_session.query.return_value.count.return_value = 10

        resp = support_client.post("/support", json={"comment": "Nice work!"})
        assert resp.status_code == 201
        assert resp.json()["count"] == 10

    def test_submit_without_comment(self, support_client, support_session):
        entry = MagicMock()
        entry.id = uuid4()

        def add_side(obj):
            obj.id = entry.id

        support_session.add.side_effect = add_side
        support_session.flush = MagicMock()
        support_session.query.return_value.count.return_value = 1

        resp = support_client.post("/support", json={})
        assert resp.status_code == 201

    def test_empty_string_comment_stored_as_none(self, support_client, support_session):
        entry = MagicMock()
        entry.id = uuid4()

        captured = {}

        def add_side(obj):
            obj.id = entry.id
            captured["comment"] = obj.comment

        support_session.add.side_effect = add_side
        support_session.flush = MagicMock()
        support_session.query.return_value.count.return_value = 1

        support_client.post("/support", json={"comment": "   "})
        # Whitespace-only comment should be stored as None
        assert captured.get("comment") is None


# ---------------------------------------------------------------------------
# Maintenance router
# ---------------------------------------------------------------------------

@pytest.fixture()
def maint_session():
    return MagicMock()


@pytest.fixture()
def maint_client(maint_session):
    app, _ = _make_app_with_router(maintenance_router, maint_session)
    with patch("protea.api.routers.maintenance.session_scope",
               side_effect=lambda _: _mock_scope(maint_session)):
        yield TestClient(app, raise_server_exceptions=True)


class TestVacuumSequencesPreview:
    def test_returns_counts(self, maint_client, maint_session):
        maint_session.query.return_value.count.return_value = 100
        maint_session.execute.return_value.scalar.return_value = 10
        resp = maint_client.get("/maintenance/vacuum-sequences/preview")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_sequences"] == 100
        assert data["orphan_sequences"] == 10
        assert data["referenced_sequences"] == 90


class TestVacuumSequences:
    def test_no_orphans(self, maint_client, maint_session):
        maint_session.execute.return_value.fetchall.return_value = []
        resp = maint_client.post("/maintenance/vacuum-sequences")
        assert resp.status_code == 200
        assert resp.json()["deleted_sequences"] == 0

    def test_with_orphans(self, maint_client, maint_session):
        maint_session.execute.return_value.fetchall.return_value = [(1,), (2,), (3,)]
        maint_session.query.return_value.filter.return_value.delete.return_value = 3
        resp = maint_client.post("/maintenance/vacuum-sequences")
        assert resp.status_code == 200
        assert resp.json()["deleted_sequences"] == 3


class TestVacuumEmbeddingsPreview:
    def test_returns_counts(self, maint_client, maint_session):
        maint_session.query.return_value.count.return_value = 500
        maint_session.execute.return_value.scalar.return_value = 50
        resp = maint_client.get("/maintenance/vacuum-embeddings/preview")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_embeddings"] == 500
        assert data["unindexed_embeddings"] == 50
        assert data["indexed_embeddings"] == 450


class TestVacuumEmbeddings:
    def test_no_unindexed(self, maint_client, maint_session):
        maint_session.execute.return_value.fetchall.return_value = []
        resp = maint_client.post("/maintenance/vacuum-embeddings")
        assert resp.status_code == 200
        assert resp.json()["deleted_embeddings"] == 0

    def test_with_unindexed(self, maint_client, maint_session):
        maint_session.execute.return_value.fetchall.return_value = [(10,), (20,)]
        maint_session.query.return_value.filter.return_value.delete.return_value = 2
        resp = maint_client.post("/maintenance/vacuum-embeddings")
        assert resp.status_code == 200
        assert resp.json()["deleted_embeddings"] == 2


class TestMaintenanceSessionFactoryMissing:
    def test_raises_when_no_factory(self):
        from protea.api.routers.maintenance import router as maint_router
        app = FastAPI()
        # Intentionally do NOT set app.state.session_factory
        app.include_router(maint_router)
        client = TestClient(app, raise_server_exceptions=False)
        resp = client.get("/maintenance/vacuum-sequences/preview")
        assert resp.status_code == 500
