"""
Unit tests for the FastAPI annotations router.
Database and pika are fully mocked — no real infrastructure required.
"""
from __future__ import annotations

import uuid
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from protea.api.routers.annotations import router

FAKE_AMQP = "amqp://guest:guest@localhost/"
_SNAPSHOT_ID = uuid.uuid4()
_SET_ID = uuid.uuid4()


def _make_snapshot(snapshot_id=None):
    s = MagicMock()
    s.id = snapshot_id or uuid.uuid4()
    s.obo_url = "https://purl.obolibrary.org/obo/go.obo"
    s.obo_version = "releases/2024-01-17"
    s.loaded_at = MagicMock()
    s.loaded_at.isoformat.return_value = "2024-01-17T00:00:00"
    return s


def _make_annotation_set(set_id=None, snapshot_id=None):
    a = MagicMock()
    a.id = set_id or uuid.uuid4()
    a.source = "quickgo"
    a.source_version = "2024-01-11"
    a.ontology_snapshot_id = snapshot_id or uuid.uuid4()
    a.job_id = None
    a.created_at = MagicMock()
    a.created_at.isoformat.return_value = "2024-01-11T00:00:00"
    a.meta = {}
    return a


def _make_job(job_id=None, operation="load_ontology_snapshot"):
    job = MagicMock()
    job.id = job_id or uuid.uuid4()
    job.operation = operation
    return job


def _make_app(session_factory, amqp_url=FAKE_AMQP):
    app = FastAPI()
    app.state.session_factory = session_factory
    app.state.amqp_url = amqp_url
    app.include_router(router)
    return app


@contextmanager
def _mock_scope(session):
    yield session


@pytest.fixture()
def session():
    return MagicMock()


@pytest.fixture()
def client(session):
    factory = MagicMock()
    app = _make_app(factory)
    with patch("protea.api.routers.annotations.session_scope", side_effect=lambda _: _mock_scope(session)):
        yield TestClient(app, raise_server_exceptions=True)


# ---------------------------------------------------------------------------
# GET /annotations/snapshots
# ---------------------------------------------------------------------------

class TestListSnapshots:
    def test_returns_list(self, session):
        s = _make_snapshot(_SNAPSHOT_ID)
        session.query.return_value.order_by.return_value.all.return_value = [s]

        factory = MagicMock()
        app = _make_app(factory)
        with patch("protea.api.routers.annotations.session_scope", side_effect=lambda _: _mock_scope(session)):
            resp = TestClient(app).get("/annotations/snapshots")

        assert resp.status_code == 200
        body = resp.json()
        assert isinstance(body, list)
        assert body[0]["obo_version"] == "releases/2024-01-17"

    def test_empty_list(self, session):
        session.query.return_value.order_by.return_value.all.return_value = []

        factory = MagicMock()
        app = _make_app(factory)
        with patch("protea.api.routers.annotations.session_scope", side_effect=lambda _: _mock_scope(session)):
            resp = TestClient(app).get("/annotations/snapshots")

        assert resp.status_code == 200
        assert resp.json() == []


# ---------------------------------------------------------------------------
# GET /annotations/snapshots/{id}
# ---------------------------------------------------------------------------

class TestGetSnapshot:
    def test_returns_snapshot_with_term_count(self, session):
        s = _make_snapshot(_SNAPSHOT_ID)
        session.get.return_value = s
        session.query.return_value.filter.return_value.scalar.return_value = 47000

        factory = MagicMock()
        app = _make_app(factory)
        with patch("protea.api.routers.annotations.session_scope", side_effect=lambda _: _mock_scope(session)):
            resp = TestClient(app).get(f"/annotations/snapshots/{_SNAPSHOT_ID}")

        assert resp.status_code == 200
        body = resp.json()
        assert body["id"] == str(_SNAPSHOT_ID)
        assert body["go_term_count"] == 47000

    def test_not_found_returns_404(self, session):
        session.get.return_value = None

        factory = MagicMock()
        app = _make_app(factory)
        with patch("protea.api.routers.annotations.session_scope", side_effect=lambda _: _mock_scope(session)):
            resp = TestClient(app, raise_server_exceptions=False).get(f"/annotations/snapshots/{uuid.uuid4()}")

        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# POST /annotations/snapshots/load
# ---------------------------------------------------------------------------

class TestLoadOntologySnapshot:
    def test_valid_payload_creates_job(self, session):
        job = _make_job(operation="load_ontology_snapshot")
        session.flush = MagicMock()

        factory = MagicMock()
        app = _make_app(factory)
        with patch("protea.api.routers.annotations.session_scope", side_effect=lambda _: _mock_scope(session)), \
             patch("protea.api.routers.annotations.publish_job"), \
             patch("protea.api.routers.annotations.Job", return_value=job):
            resp = TestClient(app).post(
                "/annotations/snapshots/load",
                json={"obo_url": "https://purl.obolibrary.org/obo/go.obo"},
            )

        assert resp.status_code == 200
        body = resp.json()
        assert "id" in body
        assert body["status"] == "queued"

    def test_invalid_payload_returns_422(self, session):
        factory = MagicMock()
        app = _make_app(factory)
        with patch("protea.api.routers.annotations.session_scope", side_effect=lambda _: _mock_scope(session)):
            resp = TestClient(app, raise_server_exceptions=False).post(
                "/annotations/snapshots/load",
                json={},  # missing obo_url
            )

        assert resp.status_code == 422

    def test_publish_called_after_commit(self, session):
        job = _make_job(operation="load_ontology_snapshot")
        session.flush = MagicMock()

        factory = MagicMock()
        app = _make_app(factory)
        with patch("protea.api.routers.annotations.session_scope", side_effect=lambda _: _mock_scope(session)), \
             patch("protea.api.routers.annotations.publish_job") as mock_publish, \
             patch("protea.api.routers.annotations.Job", return_value=job):
            TestClient(app).post(
                "/annotations/snapshots/load",
                json={"obo_url": "https://purl.obolibrary.org/obo/go.obo"},
            )

        mock_publish.assert_called_once_with(FAKE_AMQP, "protea.jobs", job.id)


# ---------------------------------------------------------------------------
# GET /annotations/sets
# ---------------------------------------------------------------------------

class TestListAnnotationSets:
    def test_returns_list(self, session):
        a = _make_annotation_set(_SET_ID, _SNAPSHOT_ID)
        q = MagicMock()
        q.filter.return_value = q
        q.order_by.return_value.all.return_value = [a]
        session.query.return_value = q

        factory = MagicMock()
        app = _make_app(factory)
        with patch("protea.api.routers.annotations.session_scope", side_effect=lambda _: _mock_scope(session)):
            resp = TestClient(app).get("/annotations/sets")

        assert resp.status_code == 200
        body = resp.json()
        assert body[0]["source"] == "quickgo"

    def test_filter_by_source(self, session):
        q = MagicMock()
        q.filter.return_value = q
        q.order_by.return_value.all.return_value = []
        session.query.return_value = q

        factory = MagicMock()
        app = _make_app(factory)
        with patch("protea.api.routers.annotations.session_scope", side_effect=lambda _: _mock_scope(session)):
            resp = TestClient(app).get("/annotations/sets?source=goa")

        assert resp.status_code == 200
        q.filter.assert_called()


# ---------------------------------------------------------------------------
# GET /annotations/sets/{id}
# ---------------------------------------------------------------------------

class TestGetAnnotationSet:
    def test_returns_set_with_annotation_count(self, session):
        a = _make_annotation_set(_SET_ID, _SNAPSHOT_ID)
        session.get.return_value = a
        session.query.return_value.filter.return_value.scalar.return_value = 1500000

        factory = MagicMock()
        app = _make_app(factory)
        with patch("protea.api.routers.annotations.session_scope", side_effect=lambda _: _mock_scope(session)):
            resp = TestClient(app).get(f"/annotations/sets/{_SET_ID}")

        assert resp.status_code == 200
        body = resp.json()
        assert body["annotation_count"] == 1500000

    def test_not_found_returns_404(self, session):
        session.get.return_value = None

        factory = MagicMock()
        app = _make_app(factory)
        with patch("protea.api.routers.annotations.session_scope", side_effect=lambda _: _mock_scope(session)):
            resp = TestClient(app, raise_server_exceptions=False).get(f"/annotations/sets/{uuid.uuid4()}")

        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# POST /annotations/sets/load-goa
# ---------------------------------------------------------------------------

class TestLoadGOAAnnotations:
    _VALID_PAYLOAD = {
        "ontology_snapshot_id": str(_SNAPSHOT_ID),
        "gaf_url": "https://ftp.ebi.ac.uk/pub/databases/GO/goa/UNIPROT/goa_uniprot_all.gaf.gz",
        "source_version": "2024-01-11",
    }

    def test_valid_payload_creates_job(self, session):
        job = _make_job(operation="load_goa_annotations")
        session.flush = MagicMock()

        factory = MagicMock()
        app = _make_app(factory)
        with patch("protea.api.routers.annotations.session_scope", side_effect=lambda _: _mock_scope(session)), \
             patch("protea.api.routers.annotations.publish_job"), \
             patch("protea.api.routers.annotations.Job", return_value=job):
            resp = TestClient(app).post("/annotations/sets/load-goa", json=self._VALID_PAYLOAD)

        assert resp.status_code == 200
        assert resp.json()["status"] == "queued"

    def test_missing_fields_returns_422(self, session):
        factory = MagicMock()
        app = _make_app(factory)
        with patch("protea.api.routers.annotations.session_scope", side_effect=lambda _: _mock_scope(session)):
            resp = TestClient(app, raise_server_exceptions=False).post(
                "/annotations/sets/load-goa",
                json={"ontology_snapshot_id": str(_SNAPSHOT_ID)},  # missing gaf_url + source_version
            )

        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# POST /annotations/sets/load-quickgo
# ---------------------------------------------------------------------------

class TestLoadQuickGOAnnotations:
    _VALID_PAYLOAD = {
        "ontology_snapshot_id": str(_SNAPSHOT_ID),
        "source_version": "2024-01-11",
    }

    def test_valid_payload_creates_job(self, session):
        job = _make_job(operation="load_quickgo_annotations")
        session.flush = MagicMock()

        factory = MagicMock()
        app = _make_app(factory)
        with patch("protea.api.routers.annotations.session_scope", side_effect=lambda _: _mock_scope(session)), \
             patch("protea.api.routers.annotations.publish_job"), \
             patch("protea.api.routers.annotations.Job", return_value=job):
            resp = TestClient(app).post("/annotations/sets/load-quickgo", json=self._VALID_PAYLOAD)

        assert resp.status_code == 200
        assert resp.json()["status"] == "queued"

    def test_missing_source_version_returns_422(self, session):
        factory = MagicMock()
        app = _make_app(factory)
        with patch("protea.api.routers.annotations.session_scope", side_effect=lambda _: _mock_scope(session)):
            resp = TestClient(app, raise_server_exceptions=False).post(
                "/annotations/sets/load-quickgo",
                json={"ontology_snapshot_id": str(_SNAPSHOT_ID)},  # missing source_version
            )

        assert resp.status_code == 422

    def test_publish_called_with_correct_queue(self, session):
        job = _make_job(operation="load_quickgo_annotations")
        session.flush = MagicMock()

        factory = MagicMock()
        app = _make_app(factory)
        with patch("protea.api.routers.annotations.session_scope", side_effect=lambda _: _mock_scope(session)), \
             patch("protea.api.routers.annotations.publish_job") as mock_publish, \
             patch("protea.api.routers.annotations.Job", return_value=job):
            TestClient(app).post("/annotations/sets/load-quickgo", json=self._VALID_PAYLOAD)

        mock_publish.assert_called_once_with(FAKE_AMQP, "protea.jobs", job.id)
