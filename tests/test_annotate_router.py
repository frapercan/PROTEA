"""Unit tests for the /annotate router.

Database and queue are fully mocked — no real infrastructure required.
"""

from __future__ import annotations

from contextlib import contextmanager
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
    q = session.query.return_value
    q.order_by.return_value.all.return_value = [config]
    q.scalar.return_value = bool(has_embeddings)
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


@pytest.fixture(autouse=True)
def _clear_best_config_cache():
    """The best-config id is memoised in a process-local TTL store; reset it
    between tests so cross-test state never leaks."""
    from protea.api.routers.annotate import invalidate_best_config_cache

    invalidate_best_config_cache()
    yield
    invalidate_best_config_cache()


@pytest.fixture()
def session():
    return MagicMock()


@pytest.fixture()
def factory(session):
    return MagicMock()


@pytest.fixture()
def client(session, factory):
    app = _make_app(factory)
    with (
        patch(
            "protea.api.routers.annotate.session_scope",
            side_effect=lambda _: _mock_scope(session),
        ),
        patch(
            "protea.api.routers.annotate.publish_job",
        ) as mock_publish,
    ):
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
        query_mock.order_by.return_value.all.return_value = []
        query_mock.scalar.return_value = False

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
            query_mock.order_by.return_value.all.return_value = [config]
            query_mock.scalar.return_value = True
            session.get.return_value = config
        else:
            query_mock.order_by.return_value.all.return_value = []

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
        query_mock.order_by.return_value.all.return_value = [config]
        query_mock.scalar.return_value = True
        session.get.return_value = config
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
            q.order_by.return_value.all.return_value = [config]
            q.scalar.return_value = True
            q.order_by.return_value.first.side_effect = lambda: next(first_results)
            return q

        session.query.side_effect = query_side_effect
        session.get.return_value = config

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
            q.order_by.return_value.all.return_value = [config]
            q.scalar.return_value = True
            q.order_by.return_value.first.side_effect = lambda: next(first_results)
            return q

        session.query.side_effect = query_side_effect
        session.get.return_value = config

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

    def _setup_happy_session(self, session):
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
            q.order_by.return_value.all.return_value = [config]
            q.scalar.return_value = True
            q.order_by.return_value.first.side_effect = lambda: next(first_results)
            return q

        session.query.side_effect = query_side_effect
        session.get.return_value = config

        def add_side_effect(obj):
            if not hasattr(obj, "id") or obj.id is None:
                obj.id = uuid4()

        session.add.side_effect = add_side_effect
        session.flush.return_value = None

    def test_compute_reranker_features_true_round_trips(self, client):
        """compute_reranker_features=True must appear as True in predict_payload."""
        c, session, _ = client
        fasta = _fasta_content([("P12345", "MKVLWAGS")])
        self._setup_happy_session(session)
        resp = c.post(
            "/annotate",
            data={"fasta_text": fasta, "compute_reranker_features": "true"},
        )
        assert resp.status_code == 200
        assert resp.json()["predict_payload"]["compute_reranker_features"] is True

    def test_compute_reranker_features_false_round_trips(self, client):
        """compute_reranker_features=False must appear as False in predict_payload."""
        c, session, _ = client
        fasta = _fasta_content([("P12345", "MKVLWAGS")])
        self._setup_happy_session(session)
        resp = c.post(
            "/annotate",
            data={"fasta_text": fasta, "compute_reranker_features": "false"},
        )
        assert resp.status_code == 200
        assert resp.json()["predict_payload"]["compute_reranker_features"] is False

    def test_compute_reranker_features_defaults_to_true(self, client):
        """When compute_reranker_features is omitted, predict_payload carries True."""
        c, session, _ = client
        fasta = _fasta_content([("P12345", "MKVLWAGS")])
        self._setup_happy_session(session)
        resp = c.post("/annotate", data={"fasta_text": fasta})
        assert resp.status_code == 200
        assert resp.json()["predict_payload"]["compute_reranker_features"] is True

    def test_lafa_flags_round_trip(self, client):
        """compute_classifier / self_prior / association flow into predict_payload."""
        c, session, _ = client
        fasta = _fasta_content([("P12345", "MKVLWAGS")])
        self._setup_happy_session(session)
        resp = c.post(
            "/annotate",
            data={
                "fasta_text": fasta,
                "compute_classifier": "true",
                "compute_self_prior": "true",
                "compute_association": "true",
            },
        )
        assert resp.status_code == 200
        payload = resp.json()["predict_payload"]
        assert payload["compute_classifier"] is True
        assert payload["compute_self_prior"] is True
        assert payload["compute_association"] is True

    def test_lafa_flags_default_false(self, client):
        """When omitted, the LAFA levers default to False in predict_payload."""
        c, session, _ = client
        fasta = _fasta_content([("P12345", "MKVLWAGS")])
        self._setup_happy_session(session)
        resp = c.post("/annotate", data={"fasta_text": fasta})
        assert resp.status_code == 200
        payload = resp.json()["predict_payload"]
        assert payload["compute_classifier"] is False
        assert payload["compute_self_prior"] is False
        assert payload["compute_association"] is False


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


class TestBestEmbeddingConfig:
    @staticmethod
    def _wire(session, configs, has_embeddings):
        """Drive the two query shapes used by ``_best_embedding_config``:
        ``query(EmbeddingConfig).order_by(...).all()`` returns ``configs``;
        each ``query(exists()...).scalar()`` returns the next ``has_embeddings``
        flag in order."""
        flags = iter(has_embeddings)

        def query_side_effect(*args):
            q = MagicMock()
            q.order_by.return_value.all.return_value = configs
            q.scalar.side_effect = lambda: next(flags)
            return q

        session.query.side_effect = query_side_effect

    def test_returns_smallest_config_that_has_embeddings(self):
        from protea.api.routers.annotate import _best_embedding_config

        session = MagicMock()
        config_a = MagicMock()
        config_a.id = uuid4()
        config_b = MagicMock()
        config_b.id = uuid4()
        # Smallest-first ordering is done in SQL; first config has embeddings.
        self._wire(session, [config_a, config_b], [True])

        result = _best_embedding_config(session)
        assert result is config_a

    def test_skips_config_without_embeddings(self):
        from protea.api.routers.annotate import _best_embedding_config

        session = MagicMock()
        config_a = MagicMock()
        config_a.id = uuid4()
        config_b = MagicMock()
        config_b.id = uuid4()
        # Smaller config has no embeddings; fall through to the next one.
        self._wire(session, [config_a, config_b], [False, True])

        result = _best_embedding_config(session)
        assert result is config_b

    def test_returns_none_when_no_configs(self):
        from protea.api.routers.annotate import _best_embedding_config

        session = MagicMock()
        self._wire(session, [], [])

        result = _best_embedding_config(session)
        assert result is None

    def test_returns_first_config_when_none_have_embeddings(self):
        from protea.api.routers.annotate import _best_embedding_config

        session = MagicMock()
        config = MagicMock()
        config.id = uuid4()
        self._wire(session, [config], [False])

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


class TestBestEmbeddingConfigIdCached:
    """The TTL cache is what makes /annotate fast: the 5.8M-row scan must run
    once, then subsequent calls serve the memoised id without touching the DB."""

    def test_second_call_does_not_rescan(self):
        from protea.api.routers import annotate

        annotate.invalidate_best_config_cache()
        config = MagicMock()
        config.id = uuid4()
        session = MagicMock()

        def query_side_effect(*args):
            q = MagicMock()
            q.order_by.return_value.all.return_value = [config]
            q.scalar.return_value = True
            return q

        session.query.side_effect = query_side_effect
        factory = MagicMock()

        with patch.object(
            annotate, "session_scope", side_effect=lambda _: _mock_scope(session)
        ):
            first = annotate._best_embedding_config_id_cached(factory, ttl=300.0)
            calls_after_first = session.query.call_count
            second = annotate._best_embedding_config_id_cached(factory, ttl=300.0)

        assert first == config.id
        assert second == config.id
        # No new queries on the cache hit.
        assert session.query.call_count == calls_after_first
        annotate.invalidate_best_config_cache()

    def test_none_result_is_not_cached(self):
        from protea.api.routers import annotate

        annotate.invalidate_best_config_cache()
        session = MagicMock()

        def query_side_effect(*args):
            q = MagicMock()
            q.order_by.return_value.all.return_value = []
            return q

        session.query.side_effect = query_side_effect
        factory = MagicMock()

        with patch.object(
            annotate, "session_scope", side_effect=lambda _: _mock_scope(session)
        ):
            result = annotate._best_embedding_config_id_cached(factory, ttl=300.0)
            # A None answer (empty DB) must NOT be cached, so a later
            # default-create is picked up: the second call rescans.
            calls_after_first = session.query.call_count
            annotate._best_embedding_config_id_cached(factory, ttl=300.0)

        assert result is None
        assert session.query.call_count > calls_after_first
        annotate.invalidate_best_config_cache()
