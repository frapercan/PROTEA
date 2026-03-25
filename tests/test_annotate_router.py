"""Unit tests for the /annotate router.

Database and queue are fully mocked — no real infrastructure required.
"""
from __future__ import annotations

from contextlib import contextmanager
from io import BytesIO
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from protea.api.routers.annotate import router


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_app(session_factory, amqp_url="amqp://guest:guest@localhost:5672/"):
    app = FastAPI()
    app.state.session_factory = session_factory
    app.state.amqp_url = amqp_url
    app.include_router(router)
    return app


@contextmanager
def _mock_scope(session):
    yield session


def _fasta_content(records: list[tuple[str, str]]) -> str:
    lines = []
    for acc, seq in records:
        lines.append(f">{acc}")
        lines.append(seq)
    return "\n".join(lines)


def _mock_embedding_config(session, has_embeddings=True):
    config = MagicMock()
    config.id = uuid4()
    if has_embeddings:
        row = (config, 100)
    else:
        row = (config, 0)
    q = session.query.return_value
    q.outerjoin.return_value.group_by.return_value.order_by.return_value.all.return_value = [row]
    return config


def _mock_annotation_set(session):
    ann = MagicMock()
    ann.id = uuid4()
    return ann


def _mock_ontology_snapshot(session):
    snap = MagicMock()
    snap.id = uuid4()
    return snap


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def session():
    return MagicMock()


@pytest.fixture()
def factory(session):
    return MagicMock()


@pytest.fixture()
def client(session, factory):
    app = _make_app(factory)
    with patch(
        "protea.api.routers.annotate.session_scope",
        side_effect=lambda _: _mock_scope(session),
    ), patch(
        "protea.api.routers.annotate.publish_job",
    ) as mock_publish:
        with TestClient(app) as c:
            yield c, session, mock_publish


# ---------------------------------------------------------------------------
# POST /annotate — input validation
# ---------------------------------------------------------------------------

class TestAnnotateInputValidation:
    def test_no_input_returns_422(self, client):
        c, session, _ = client
        resp = c.post("/annotate")
        assert resp.status_code == 422

    def test_empty_fasta_text_returns_422(self, client):
        c, session, _ = client
        resp = c.post("/annotate", data={"fasta_text": ""})
        assert resp.status_code == 422

    def test_invalid_fasta_returns_422(self, client):
        c, session, _ = client
        resp = c.post("/annotate", data={"fasta_text": "not a fasta"})
        assert resp.status_code == 422

    def test_duplicate_accession_returns_422(self, client):
        c, session, _ = client
        fasta = _fasta_content([("P12345", "MKVL"), ("P12345", "MKVL")])
        resp = c.post("/annotate", data={"fasta_text": fasta})
        assert resp.status_code == 422
        assert "Duplicate" in resp.json()["detail"]

    def test_file_upload_non_utf8_returns_422(self, client):
        c, session, _ = client
        resp = c.post(
            "/annotate",
            files={"file": ("test.fasta", b"\x80\x81\x82\x83", "text/plain")},
        )
        assert resp.status_code == 422
        assert "UTF-8" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# POST /annotate — missing prerequisites
# ---------------------------------------------------------------------------

class TestAnnotatePrerequisites:
    def _setup_session(self, session, has_config=True, has_ann=True, has_snap=True):
        """Configure mock session for the annotate flow."""
        # Sequence upsert: no existing sequences
        query_mock = MagicMock()
        session.query.return_value = query_mock
        query_mock.filter.return_value.all.return_value = []
        query_mock.outerjoin.return_value.group_by.return_value.order_by.return_value.all.return_value = []

        # Sequence hash computation
        sequence_mock = MagicMock()
        sequence_mock.id = 1

        # Make session.add assign an id to new objects
        def add_side_effect(obj):
            if not hasattr(obj, "id") or obj.id is None:
                obj.id = uuid4()
        session.add.side_effect = add_side_effect
        session.flush.return_value = None

        # Config
        if has_config:
            config = MagicMock()
            config.id = uuid4()
            query_mock.outerjoin.return_value.group_by.return_value.order_by.return_value.all.return_value = [(config, 10)]
        else:
            query_mock.outerjoin.return_value.group_by.return_value.order_by.return_value.all.return_value = []

        # Annotation set
        if has_ann:
            ann = MagicMock()
            ann.id = uuid4()
            query_mock.order_by.return_value.first.return_value = ann
        else:
            query_mock.order_by.return_value.first.return_value = None

        # Ontology snapshot — separate query
        if has_snap:
            snap = MagicMock()
            snap.id = uuid4()
        else:
            snap = None

        return query_mock

    def test_no_annotation_set_returns_409(self, client):
        c, session, _ = client
        fasta = _fasta_content([("P12345", "MKVLWAGS")])

        # Setup: config exists, but no annotation set
        query_mock = MagicMock()
        session.query.return_value = query_mock
        query_mock.filter.return_value.all.return_value = []

        # Sequence mock
        def add_side_effect(obj):
            if not hasattr(obj, "id") or obj.id is None:
                obj.id = uuid4()
        session.add.side_effect = add_side_effect

        config = MagicMock()
        config.id = uuid4()
        query_mock.outerjoin.return_value.group_by.return_value.order_by.return_value.all.return_value = [(config, 10)]
        # No annotation set
        query_mock.order_by.return_value.first.return_value = None

        resp = c.post("/annotate", data={"fasta_text": fasta})
        assert resp.status_code == 409
        assert "annotation" in resp.json()["detail"].lower()


# ---------------------------------------------------------------------------
# POST /annotate — successful flow
# ---------------------------------------------------------------------------

class TestAnnotateSuccess:
    def test_fasta_text_happy_path(self, client):
        c, session, mock_publish = client
        fasta = _fasta_content([("P12345", "MKVLWAGS"), ("Q99999", "ACDEF")])

        config = MagicMock()
        config.id = uuid4()
        ann = MagicMock()
        ann.id = uuid4()
        snap = MagicMock()
        snap.id = uuid4()
        reranker = MagicMock()
        reranker.id = uuid4()

        first_results = iter([ann, snap, reranker])

        def query_side_effect(*args):
            q = MagicMock()
            q.filter.return_value.all.return_value = []
            q.outerjoin.return_value.group_by.return_value.order_by.return_value.all.return_value = [(config, 10)]
            q.order_by.return_value.first.side_effect = lambda: next(first_results)
            return q

        session.query.side_effect = query_side_effect

        def add_side_effect(obj):
            if not hasattr(obj, "id") or obj.id is None:
                obj.id = uuid4()
        session.add.side_effect = add_side_effect
        session.flush.return_value = None

        resp = c.post("/annotate", data={"fasta_text": fasta, "name": "Test annotation"})
        assert resp.status_code == 200
        data = resp.json()
        assert "query_set_id" in data
        assert "embedding_config_id" in data
        assert "annotation_set_id" in data
        assert "embedding_job_id" in data
        assert "predict_payload" in data
        assert data["sequence_count"] == 2
        mock_publish.assert_called_once()

    def test_file_upload_happy_path(self, client):
        c, session, mock_publish = client
        fasta = _fasta_content([("P12345", "MKVLWAGS")])

        config = MagicMock()
        config.id = uuid4()
        ann = MagicMock()
        ann.id = uuid4()
        snap = MagicMock()
        snap.id = uuid4()

        first_results = iter([ann, snap, None])

        def query_side_effect(*args):
            q = MagicMock()
            q.filter.return_value.all.return_value = []
            q.outerjoin.return_value.group_by.return_value.order_by.return_value.all.return_value = [(config, 10)]
            q.order_by.return_value.first.side_effect = lambda: next(first_results)
            return q

        session.query.side_effect = query_side_effect

        def add_side_effect(obj):
            if not hasattr(obj, "id") or obj.id is None:
                obj.id = uuid4()
        session.add.side_effect = add_side_effect

        resp = c.post(
            "/annotate",
            files={"file": ("test.fasta", fasta.encode(), "text/plain")},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["sequence_count"] == 1
        assert data["reranker_id"] is None


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

class TestBestEmbeddingConfig:
    def test_returns_config_with_most_embeddings(self):
        from protea.api.routers.annotate import _best_embedding_config

        session = MagicMock()
        config_a = MagicMock()
        config_a.id = uuid4()
        config_b = MagicMock()
        config_b.id = uuid4()

        session.query.return_value.outerjoin.return_value.group_by.return_value.order_by.return_value.all.return_value = [
            (config_a, 100),
            (config_b, 50),
        ]

        result = _best_embedding_config(session)
        assert result is config_a

    def test_returns_none_when_no_configs(self):
        from protea.api.routers.annotate import _best_embedding_config

        session = MagicMock()
        session.query.return_value.outerjoin.return_value.group_by.return_value.order_by.return_value.all.return_value = []

        result = _best_embedding_config(session)
        assert result is None

    def test_returns_config_with_zero_embeddings_if_only_option(self):
        from protea.api.routers.annotate import _best_embedding_config

        session = MagicMock()
        config = MagicMock()
        config.id = uuid4()
        session.query.return_value.outerjoin.return_value.group_by.return_value.order_by.return_value.all.return_value = [
            (config, 0),
        ]

        result = _best_embedding_config(session)
        assert result is config


class TestNewestAnnotationSet:
    def test_returns_newest(self):
        from protea.api.routers.annotate import _newest_annotation_set

        session = MagicMock()
        ann = MagicMock()
        session.query.return_value.order_by.return_value.first.return_value = ann
        assert _newest_annotation_set(session) is ann

    def test_returns_none_when_empty(self):
        from protea.api.routers.annotate import _newest_annotation_set

        session = MagicMock()
        session.query.return_value.order_by.return_value.first.return_value = None
        assert _newest_annotation_set(session) is None


class TestNewestOntologySnapshot:
    def test_returns_newest(self):
        from protea.api.routers.annotate import _newest_ontology_snapshot

        session = MagicMock()
        snap = MagicMock()
        session.query.return_value.order_by.return_value.first.return_value = snap
        assert _newest_ontology_snapshot(session) is snap

    def test_returns_none_when_empty(self):
        from protea.api.routers.annotate import _newest_ontology_snapshot

        session = MagicMock()
        session.query.return_value.order_by.return_value.first.return_value = None
        assert _newest_ontology_snapshot(session) is None


class TestDeriveMethod:
    def test_derive_method_used_in_showcase(self):
        from protea.api.routers.showcase import _derive_method

        assert _derive_method(None, None) == ("knn_baseline", "KNN (embedding distance)")
        assert _derive_method(uuid4(), None) == ("knn_scored", "KNN + Scoring")
        assert _derive_method(None, uuid4()) == ("knn_reranker", "KNN + Re-ranker")
        assert _derive_method(uuid4(), uuid4()) == ("knn_reranker", "KNN + Re-ranker")
