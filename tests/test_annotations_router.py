"""Unit tests for the /annotations router.

Database and queue are fully mocked -- no real infrastructure required.
"""
from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.exc import IntegrityError

from protea.api.routers.annotations import router

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_app(session_factory, amqp_url="amqp://guest:guest@localhost", artifacts_dir=None):
    app = FastAPI()
    app.state.session_factory = session_factory
    app.state.amqp_url = amqp_url
    app.state.artifacts_dir = artifacts_dir or Path("/tmp/protea-test-artifacts")
    app.include_router(router)
    return app


@contextmanager
def _mock_scope(session):
    yield session


def _make_snapshot(snap_id=None, obo_url="http://obo", obo_version="2024-01-01", ia_url=None):
    s = MagicMock()
    s.id = snap_id or uuid4()
    s.obo_url = obo_url
    s.obo_version = obo_version
    s.ia_url = ia_url
    s.loaded_at = MagicMock()
    s.loaded_at.isoformat.return_value = "2024-01-01T00:00:00"
    return s


def _make_annotation_set(set_id=None, source="goa", source_version="2024-01", snap_id=None, job_id=None):
    a = MagicMock()
    a.id = set_id or uuid4()
    a.source = source
    a.source_version = source_version
    a.ontology_snapshot_id = snap_id or uuid4()
    a.job_id = job_id
    a.created_at = MagicMock()
    a.created_at.isoformat.return_value = "2024-01-01T00:00:00"
    a.meta = {"key": "value"}
    return a


def _make_evaluation_set(eval_id=None, old_id=None, new_id=None, job_id=None, stats=None):
    e = MagicMock()
    e.id = eval_id or uuid4()
    e.old_annotation_set_id = old_id or uuid4()
    e.new_annotation_set_id = new_id or uuid4()
    e.job_id = job_id
    e.created_at = MagicMock()
    e.created_at.isoformat.return_value = "2024-06-01T00:00:00"
    e.stats = stats or {"nk": 10, "lk": 5}
    return e


def _make_evaluation_result(result_id=None, eval_set_id=None, pred_set_id=None, scoring_id=None, job_id=None, results=None):
    r = MagicMock()
    r.id = result_id or uuid4()
    r.evaluation_set_id = eval_set_id or uuid4()
    r.prediction_set_id = pred_set_id or uuid4()
    r.scoring_config_id = scoring_id
    r.job_id = job_id
    r.created_at = MagicMock()
    r.created_at.isoformat.return_value = "2024-07-01T00:00:00"
    r.results = results or {}
    return r


@pytest.fixture()
def session():
    return MagicMock()


@pytest.fixture()
def factory(session):
    return MagicMock()


@pytest.fixture()
def client(session, factory):
    app = _make_app(factory)
    with patch("protea.api.routers.annotations.session_scope", side_effect=lambda _: _mock_scope(session)):
        with TestClient(app) as c:
            yield c, session


@pytest.fixture()
def client_with_artifacts(session, factory, tmp_path):
    app = _make_app(factory, artifacts_dir=tmp_path)
    with patch("protea.api.routers.annotations.session_scope", side_effect=lambda _: _mock_scope(session)):
        with TestClient(app) as c:
            yield c, session, tmp_path


# ---------------------------------------------------------------------------
# GET /annotations/snapshots (lines 71-86)
# ---------------------------------------------------------------------------


class TestListSnapshots:
    def test_returns_list(self, client):
        c, session = client
        snap = _make_snapshot()
        # Simulate the subquery join: session.query(...).outerjoin(...).order_by(...).all()
        session.query.return_value.group_by.return_value.subquery.return_value = MagicMock()
        session.query.return_value.outerjoin.return_value.order_by.return_value.all.return_value = [
            (snap, 42)
        ]

        resp = c.get("/annotations/snapshots")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["obo_version"] == "2024-01-01"
        assert data[0]["go_term_count"] == 42

    def test_empty_list(self, client):
        c, session = client
        session.query.return_value.group_by.return_value.subquery.return_value = MagicMock()
        session.query.return_value.outerjoin.return_value.order_by.return_value.all.return_value = []

        resp = c.get("/annotations/snapshots")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_null_count_defaults_to_zero(self, client):
        c, session = client
        snap = _make_snapshot()
        session.query.return_value.group_by.return_value.subquery.return_value = MagicMock()
        session.query.return_value.outerjoin.return_value.order_by.return_value.all.return_value = [
            (snap, None)
        ]

        resp = c.get("/annotations/snapshots")
        assert resp.status_code == 200
        assert resp.json()[0]["go_term_count"] == 0


# ---------------------------------------------------------------------------
# GET /annotations/snapshots/{snapshot_id} (lines 105-116)
# ---------------------------------------------------------------------------


class TestGetSnapshot:
    def test_returns_snapshot(self, client):
        c, session = client
        snap = _make_snapshot()
        session.get.return_value = snap
        session.query.return_value.filter.return_value.scalar.return_value = 99

        resp = c.get(f"/annotations/snapshots/{snap.id}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["obo_version"] == "2024-01-01"
        assert data["go_term_count"] == 99

    def test_not_found(self, client):
        c, session = client
        session.get.return_value = None

        resp = c.get(f"/annotations/snapshots/{uuid4()}")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# POST /annotations/snapshots/load (lines 176-195)
# ---------------------------------------------------------------------------


class TestLoadOntologySnapshot:
    def test_success(self, client):
        c, session = client

        def add_side(obj):
            from protea.infrastructure.orm.models.job import Job
            if isinstance(obj, Job):
                obj.id = uuid4()
        session.add.side_effect = add_side

        with patch("protea.api.routers.annotations.publish_job"):
            resp = c.post(
                "/annotations/snapshots/load",
                json={"obo_url": "http://example.com/go.obo"},
            )
        assert resp.status_code == 200
        assert resp.json()["status"] == "queued"

    def test_invalid_payload(self, client):
        c, session = client
        resp = c.post("/annotations/snapshots/load", json={})
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# GET /annotations/sets (lines 207-222)
# ---------------------------------------------------------------------------


class TestListAnnotationSets:
    def test_returns_list(self, client):
        c, session = client
        aset = _make_annotation_set()
        session.query.return_value.group_by.return_value.subquery.return_value = MagicMock()
        q_mock = session.query.return_value.outerjoin.return_value
        q_mock.filter.return_value.order_by.return_value.all.return_value = [(aset, 10)]
        q_mock.order_by.return_value.all.return_value = [(aset, 10)]

        resp = c.get("/annotations/sets")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["source"] == "goa"

    def test_filter_by_source(self, client):
        c, session = client
        aset = _make_annotation_set(source="quickgo")
        session.query.return_value.group_by.return_value.subquery.return_value = MagicMock()
        q_mock = session.query.return_value.outerjoin.return_value
        q_mock.filter.return_value.order_by.return_value.all.return_value = [(aset, 5)]

        resp = c.get("/annotations/sets?source=quickgo")
        assert resp.status_code == 200

    def test_empty(self, client):
        c, session = client
        session.query.return_value.group_by.return_value.subquery.return_value = MagicMock()
        q_mock = session.query.return_value.outerjoin.return_value
        q_mock.order_by.return_value.all.return_value = []

        resp = c.get("/annotations/sets")
        assert resp.status_code == 200
        assert resp.json() == []


# ---------------------------------------------------------------------------
# GET /annotations/sets/{set_id} (lines 243-254)
# ---------------------------------------------------------------------------


class TestGetAnnotationSet:
    def test_returns_set(self, client):
        c, session = client
        aset = _make_annotation_set(job_id=uuid4())
        session.get.return_value = aset
        session.query.return_value.filter.return_value.scalar.return_value = 100

        resp = c.get(f"/annotations/sets/{aset.id}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["annotation_count"] == 100
        assert data["job_id"] is not None

    def test_not_found(self, client):
        c, session = client
        session.get.return_value = None

        resp = c.get(f"/annotations/sets/{uuid4()}")
        assert resp.status_code == 404

    def test_no_job_id(self, client):
        c, session = client
        aset = _make_annotation_set(job_id=None)
        session.get.return_value = aset
        session.query.return_value.filter.return_value.scalar.return_value = 0

        resp = c.get(f"/annotations/sets/{aset.id}")
        assert resp.status_code == 200
        assert resp.json()["job_id"] is None


# ---------------------------------------------------------------------------
# POST /annotations/sets/load-goa (lines 300-319)
# ---------------------------------------------------------------------------


class TestLoadGOAAnnotations:
    def test_success(self, client):
        c, session = client

        def add_side(obj):
            from protea.infrastructure.orm.models.job import Job
            if isinstance(obj, Job):
                obj.id = uuid4()
        session.add.side_effect = add_side

        with patch("protea.api.routers.annotations.publish_job"):
            resp = c.post(
                "/annotations/sets/load-goa",
                json={
                    "ontology_snapshot_id": str(uuid4()),
                    "gaf_url": "http://example.com/goa.gaf.gz",
                    "source_version": "2024-01",
                },
            )
        assert resp.status_code == 200
        assert resp.json()["status"] == "queued"

    def test_invalid_payload(self, client):
        c, session = client
        resp = c.post("/annotations/sets/load-goa", json={})
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# POST /annotations/sets/load-quickgo (lines 330-349)
# ---------------------------------------------------------------------------


class TestLoadQuickGOAnnotations:
    def test_success(self, client):
        c, session = client

        def add_side(obj):
            from protea.infrastructure.orm.models.job import Job
            if isinstance(obj, Job):
                obj.id = uuid4()
        session.add.side_effect = add_side

        with patch("protea.api.routers.annotations.publish_job"):
            resp = c.post(
                "/annotations/sets/load-quickgo",
                json={
                    "ontology_snapshot_id": str(uuid4()),
                    "source_version": "2024-01",
                },
            )
        assert resp.status_code == 200
        assert resp.json()["status"] == "queued"

    def test_invalid_payload(self, client):
        c, session = client
        resp = c.post("/annotations/sets/load-quickgo", json={})
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Dependency edge cases (lines 45, 52, 57-60)
# ---------------------------------------------------------------------------


class TestDependencyGuards:
    def test_missing_session_factory_raises(self):
        app = FastAPI()
        app.include_router(router)
        with TestClient(app, raise_server_exceptions=False) as c:
            resp = c.get("/annotations/snapshots")
        assert resp.status_code == 500

    def test_missing_amqp_url_raises(self, session):
        app = FastAPI()
        app.state.session_factory = MagicMock()
        # no amqp_url set
        app.include_router(router)
        with patch("protea.api.routers.annotations.session_scope", side_effect=lambda _: _mock_scope(session)):
            with TestClient(app, raise_server_exceptions=False) as c:
                resp = c.post("/annotations/snapshots/load", json={"obo_url": "http://example.com/go.obo"})
        assert resp.status_code == 500

    def test_missing_artifacts_dir_raises(self, session):
        app = FastAPI()
        app.state.session_factory = MagicMock()
        # no artifacts_dir set
        app.include_router(router)
        eval_id = uuid4()
        with patch("protea.api.routers.annotations.session_scope", side_effect=lambda _: _mock_scope(session)):
            with TestClient(app, raise_server_exceptions=False) as c:
                resp = c.delete(f"/annotations/evaluation-sets/{eval_id}")
        assert resp.status_code == 500


# ---------------------------------------------------------------------------
# PATCH /annotations/snapshots/{snapshot_id}/ia-url (lines 146-158)
# ---------------------------------------------------------------------------


class TestSetSnapshotIaUrl:
    def test_set_ia_url_success(self, client):
        c, session = client
        snap = _make_snapshot()
        session.get.return_value = snap

        resp = c.patch(
            f"/annotations/snapshots/{snap.id}/ia-url",
            json={"ia_url": "http://example.com/ia.tsv"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["id"] == str(snap.id)
        assert data["obo_version"] == snap.obo_version

    def test_set_ia_url_null_clears(self, client):
        c, session = client
        snap = _make_snapshot(ia_url="http://old.com/ia.tsv")
        session.get.return_value = snap

        resp = c.patch(
            f"/annotations/snapshots/{snap.id}/ia-url",
            json={"ia_url": None},
        )
        assert resp.status_code == 200

    def test_missing_ia_url_key_returns_422(self, client):
        c, session = client
        snap = _make_snapshot()

        resp = c.patch(
            f"/annotations/snapshots/{snap.id}/ia-url",
            json={"wrong_key": "value"},
        )
        assert resp.status_code == 422

    def test_snapshot_not_found_returns_404(self, client):
        c, session = client
        session.get.return_value = None

        resp = c.patch(
            f"/annotations/snapshots/{uuid4()}/ia-url",
            json={"ia_url": "http://example.com/ia.tsv"},
        )
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# DELETE /annotations/sets/{set_id} (lines 272-289)
# ---------------------------------------------------------------------------


class TestDeleteAnnotationSet:
    def test_delete_success(self, client):
        c, session = client
        aset = _make_annotation_set()
        session.get.return_value = aset
        session.query.return_value.filter.return_value.scalar.return_value = 42

        resp = c.delete(f"/annotations/sets/{aset.id}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["deleted"] == str(aset.id)
        assert data["annotations_deleted"] == 42
        session.delete.assert_called_once_with(aset)

    def test_delete_not_found(self, client):
        c, session = client
        session.get.return_value = None

        resp = c.delete(f"/annotations/sets/{uuid4()}")
        assert resp.status_code == 404

    def test_delete_integrity_error_returns_409(self, client):
        c, session = client
        aset = _make_annotation_set()
        session.get.return_value = aset
        session.query.return_value.filter.return_value.scalar.return_value = 10
        session.flush.side_effect = IntegrityError("stmt", "params", Exception("fk"))

        resp = c.delete(f"/annotations/sets/{aset.id}")
        assert resp.status_code == 409
        assert "referenced" in resp.json()["detail"].lower()


# ---------------------------------------------------------------------------
# POST /annotations/evaluation-sets/generate (lines 367-386)
# ---------------------------------------------------------------------------


class TestGenerateEvaluationSet:
    def test_success(self, client):
        c, session = client
        old_id, new_id = str(uuid4()), str(uuid4())

        # Mock Job creation
        def add_side(obj):
            from protea.infrastructure.orm.models.job import Job
            if isinstance(obj, Job):
                obj.id = uuid4()
        session.add.side_effect = add_side

        with patch("protea.api.routers.annotations.publish_job"):
            resp = c.post(
                "/annotations/evaluation-sets/generate",
                json={"old_annotation_set_id": old_id, "new_annotation_set_id": new_id},
            )
        assert resp.status_code == 200
        assert resp.json()["status"] == "queued"

    def test_invalid_payload_returns_422(self, client):
        c, session = client
        resp = c.post("/annotations/evaluation-sets/generate", json={})
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# GET /annotations/evaluation-sets (lines 394-396)
# ---------------------------------------------------------------------------


class TestListEvaluationSets:
    def test_returns_list(self, client):
        c, session = client
        ev = _make_evaluation_set()
        session.query.return_value.order_by.return_value.all.return_value = [ev]

        resp = c.get("/annotations/evaluation-sets")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["id"] == str(ev.id)
        assert data[0]["stats"] == ev.stats

    def test_empty_list(self, client):
        c, session = client
        session.query.return_value.order_by.return_value.all.return_value = []

        resp = c.get("/annotations/evaluation-sets")
        assert resp.status_code == 200
        assert resp.json() == []


# ---------------------------------------------------------------------------
# DELETE /annotations/evaluation-sets/{eval_id} (lines 416-434)
# ---------------------------------------------------------------------------


class TestDeleteEvaluationSet:
    def test_delete_success(self, client_with_artifacts):
        c, session, tmp_path = client_with_artifacts
        ev = _make_evaluation_set()
        session.get.side_effect = lambda model, id_: ev if id_ == ev.id else None

        # Create a fake result with an artifact directory
        result_mock = MagicMock()
        result_mock.id = uuid4()
        result_dir = tmp_path / str(result_mock.id)
        result_dir.mkdir()
        (result_dir / "output.tsv").write_text("test")

        session.query.return_value.filter.return_value.all.return_value = [result_mock]

        resp = c.delete(f"/annotations/evaluation-sets/{ev.id}")
        assert resp.status_code == 204
        session.delete.assert_called_once_with(ev)
        # Artifact directory should be removed
        assert not result_dir.exists()

    def test_delete_not_found(self, client_with_artifacts):
        c, session, _ = client_with_artifacts
        session.get.return_value = None

        resp = c.delete(f"/annotations/evaluation-sets/{uuid4()}")
        assert resp.status_code == 404

    def test_delete_no_artifact_dir(self, client_with_artifacts):
        c, session, tmp_path = client_with_artifacts
        ev = _make_evaluation_set()
        session.get.side_effect = lambda model, id_: ev if id_ == ev.id else None
        session.query.return_value.filter.return_value.all.return_value = []

        resp = c.delete(f"/annotations/evaluation-sets/{ev.id}")
        assert resp.status_code == 204


# ---------------------------------------------------------------------------
# GET /annotations/evaluation-sets/{eval_id} (lines 442-446)
# ---------------------------------------------------------------------------


class TestGetEvaluationSet:
    def test_success(self, client):
        c, session = client
        ev = _make_evaluation_set(job_id=uuid4())
        session.get.return_value = ev

        resp = c.get(f"/annotations/evaluation-sets/{ev.id}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["id"] == str(ev.id)
        assert data["job_id"] == str(ev.job_id)

    def test_not_found(self, client):
        c, session = client
        session.get.return_value = None

        resp = c.get(f"/annotations/evaluation-sets/{uuid4()}")
        assert resp.status_code == 404

    def test_no_job_id(self, client):
        c, session = client
        ev = _make_evaluation_set(job_id=None)
        session.get.return_value = ev

        resp = c.get(f"/annotations/evaluation-sets/{ev.id}")
        assert resp.status_code == 200
        assert resp.json()["job_id"] is None


# ---------------------------------------------------------------------------
# _eval_set_or_404 helper (lines 457-460) -- tested indirectly via GT endpoints
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Ground-truth TSV downloads (lines 475-591)
# ---------------------------------------------------------------------------


class _EvalData:
    """Fake result of compute_evaluation_data."""
    def __init__(self, nk=None, lk=None, pk=None, known=None):
        self.nk = nk or {}
        self.lk = lk or {}
        self.pk = pk or {}
        self.known = known or {}


class TestDownloadGroundTruthNK:
    def test_success(self, client):
        c, session = client
        ev = _make_evaluation_set()
        ann_old = _make_annotation_set(snap_id=uuid4())

        def get_side(model, id_):
            from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
            from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
            if model is EvaluationSet:
                return ev
            if model is AnnotationSet:
                return ann_old
            return None
        session.get.side_effect = get_side

        fake_data = _EvalData(nk={"P12345": {"GO:0003674", "GO:0008150"}})
        with patch("protea.api.routers.annotations.compute_evaluation_data", return_value=fake_data):
            resp = c.get(f"/annotations/evaluation-sets/{ev.id}/ground-truth-NK.tsv")
        assert resp.status_code == 200
        assert "text/tab-separated-values" in resp.headers["content-type"]
        lines = resp.text.strip().split("\n")
        assert len(lines) == 2
        assert "P12345" in lines[0]

    def test_not_found(self, client):
        c, session = client
        session.get.return_value = None

        resp = c.get(f"/annotations/evaluation-sets/{uuid4()}/ground-truth-NK.tsv")
        assert resp.status_code == 404


class TestDownloadGroundTruthLK:
    def test_success(self, client):
        c, session = client
        ev = _make_evaluation_set()
        ann_old = _make_annotation_set(snap_id=uuid4())

        def get_side(model, id_):
            from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
            from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
            if model is EvaluationSet:
                return ev
            if model is AnnotationSet:
                return ann_old
            return None
        session.get.side_effect = get_side

        fake_data = _EvalData(lk={"Q99999": {"GO:0005575"}})
        with patch("protea.api.routers.annotations.compute_evaluation_data", return_value=fake_data):
            resp = c.get(f"/annotations/evaluation-sets/{ev.id}/ground-truth-LK.tsv")
        assert resp.status_code == 200
        lines = resp.text.strip().split("\n")
        assert len(lines) == 1
        assert "Q99999\tGO:0005575" in lines[0]


class TestDownloadGroundTruthPK:
    def test_success(self, client):
        c, session = client
        ev = _make_evaluation_set()
        ann_old = _make_annotation_set(snap_id=uuid4())

        def get_side(model, id_):
            from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
            from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
            if model is EvaluationSet:
                return ev
            if model is AnnotationSet:
                return ann_old
            return None
        session.get.side_effect = get_side

        fake_data = _EvalData(pk={"A00001": {"GO:0003674"}})
        with patch("protea.api.routers.annotations.compute_evaluation_data", return_value=fake_data):
            resp = c.get(f"/annotations/evaluation-sets/{ev.id}/ground-truth-PK.tsv")
        assert resp.status_code == 200
        assert "A00001\tGO:0003674" in resp.text


class TestDownloadKnownTerms:
    def test_success(self, client):
        c, session = client
        ev = _make_evaluation_set()
        ann_old = _make_annotation_set(snap_id=uuid4())

        def get_side(model, id_):
            from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
            from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
            if model is EvaluationSet:
                return ev
            if model is AnnotationSet:
                return ann_old
            return None
        session.get.side_effect = get_side

        fake_data = _EvalData(known={"P12345": {"GO:0003674"}, "Q99999": {"GO:0005575"}})
        with patch("protea.api.routers.annotations.compute_evaluation_data", return_value=fake_data):
            resp = c.get(f"/annotations/evaluation-sets/{ev.id}/known-terms.tsv")
        assert resp.status_code == 200
        lines = resp.text.strip().split("\n")
        assert len(lines) == 2


# ---------------------------------------------------------------------------
# GET /annotations/evaluation-sets/{eval_id}/delta-proteins.fasta (lines 615-672)
# ---------------------------------------------------------------------------


class TestDownloadDeltaFasta:
    def _setup_session(self, session, ev, ann_old, fake_data, protein_rows=None):
        def get_side(model, id_):
            from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
            from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
            if model is EvaluationSet:
                return ev
            if model is AnnotationSet:
                return ann_old
            return None
        session.get.side_effect = get_side

        if protein_rows is not None:
            session.query.return_value.join.return_value.filter.return_value.order_by.return_value.all.return_value = protein_rows

    def test_all_category(self, client):
        c, session = client
        ev = _make_evaluation_set()
        ann_old = _make_annotation_set(snap_id=uuid4())

        protein = MagicMock()
        protein.accession = "P12345"
        protein.entry_name = "P12345_HUMAN"
        protein.organism = "Homo sapiens"
        protein.taxonomy_id = 9606
        seq = MagicMock()
        seq.sequence = "ACDEFGHIKLMNPQRST"

        fake_data = _EvalData(nk={"P12345": {"GO:0003674"}}, lk={})
        self._setup_session(session, ev, ann_old, fake_data, protein_rows=[(protein, seq)])

        with patch("protea.api.routers.annotations.compute_evaluation_data", return_value=fake_data):
            resp = c.get(f"/annotations/evaluation-sets/{ev.id}/delta-proteins.fasta")
        assert resp.status_code == 200
        assert ">P12345" in resp.text
        assert "ACDEFGHIKLMNPQRST" in resp.text
        assert "(NK)" in resp.text

    def test_nk_category_filter(self, client):
        c, session = client
        ev = _make_evaluation_set()
        ann_old = _make_annotation_set(snap_id=uuid4())

        protein = MagicMock()
        protein.accession = "P12345"
        protein.entry_name = None
        protein.organism = None
        protein.taxonomy_id = None
        seq = MagicMock()
        seq.sequence = "ACDEF"

        fake_data = _EvalData(nk={"P12345": {"GO:0003674"}}, lk={"Q99999": {"GO:0005575"}})
        self._setup_session(session, ev, ann_old, fake_data, protein_rows=[(protein, seq)])

        with patch("protea.api.routers.annotations.compute_evaluation_data", return_value=fake_data):
            resp = c.get(f"/annotations/evaluation-sets/{ev.id}/delta-proteins.fasta?category=nk")
        assert resp.status_code == 200
        assert ">P12345" in resp.text

    def test_empty_delta_returns_empty_fasta(self, client):
        c, session = client
        ev = _make_evaluation_set()
        ann_old = _make_annotation_set(snap_id=uuid4())

        fake_data = _EvalData()
        self._setup_session(session, ev, ann_old, fake_data, protein_rows=[])

        with patch("protea.api.routers.annotations.compute_evaluation_data", return_value=fake_data):
            resp = c.get(f"/annotations/evaluation-sets/{ev.id}/delta-proteins.fasta")
        assert resp.status_code == 200
        assert resp.text == ""

    def test_long_sequence_wraps_at_60(self, client):
        c, session = client
        ev = _make_evaluation_set()
        ann_old = _make_annotation_set(snap_id=uuid4())

        protein = MagicMock()
        protein.accession = "P12345"
        protein.entry_name = None
        protein.organism = None
        protein.taxonomy_id = None
        seq = MagicMock()
        seq.sequence = "A" * 120  # should wrap to two lines of 60

        fake_data = _EvalData(nk={"P12345": {"GO:0003674"}})
        self._setup_session(session, ev, ann_old, fake_data, protein_rows=[(protein, seq)])

        with patch("protea.api.routers.annotations.compute_evaluation_data", return_value=fake_data):
            resp = c.get(f"/annotations/evaluation-sets/{ev.id}/delta-proteins.fasta")
        lines = resp.text.strip().split("\n")
        # header + 2 sequence lines
        assert len(lines) == 3
        assert len(lines[1]) == 60
        assert len(lines[2]) == 60

    def test_pk_category(self, client):
        c, session = client
        ev = _make_evaluation_set()
        ann_old = _make_annotation_set(snap_id=uuid4())

        protein = MagicMock()
        protein.accession = "X00001"
        protein.entry_name = "X_MOUSE"
        protein.organism = "Mus musculus"
        protein.taxonomy_id = 10090
        seq = MagicMock()
        seq.sequence = "MMLLL"

        fake_data = _EvalData(pk={"X00001": {"GO:0005575"}})
        self._setup_session(session, ev, ann_old, fake_data, protein_rows=[(protein, seq)])

        with patch("protea.api.routers.annotations.compute_evaluation_data", return_value=fake_data):
            resp = c.get(f"/annotations/evaluation-sets/{ev.id}/delta-proteins.fasta?category=pk")
        assert resp.status_code == 200
        assert "(PK)" in resp.text

    def test_all_category_includes_lk(self, client):
        """Ensure LK proteins are included when category=all (covers line 632)."""
        c, session = client
        ev = _make_evaluation_set()
        ann_old = _make_annotation_set(snap_id=uuid4())

        protein = MagicMock()
        protein.accession = "Q99999"
        protein.entry_name = None
        protein.organism = None
        protein.taxonomy_id = None
        seq = MagicMock()
        seq.sequence = "MMMM"

        fake_data = _EvalData(nk={}, lk={"Q99999": {"GO:0005575"}})
        self._setup_session(session, ev, ann_old, fake_data, protein_rows=[(protein, seq)])

        with patch("protea.api.routers.annotations.compute_evaluation_data", return_value=fake_data):
            resp = c.get(f"/annotations/evaluation-sets/{ev.id}/delta-proteins.fasta?category=all")
        assert resp.status_code == 200
        assert "(LK)" in resp.text


# ---------------------------------------------------------------------------
# POST /annotations/evaluation-sets/{eval_id}/run (lines 698-720)
# ---------------------------------------------------------------------------


class TestRunCafaEvaluation:
    def test_success(self, client):
        c, session = client
        eval_id = uuid4()
        pred_set_id = str(uuid4())
        ev = _make_evaluation_set(eval_id=eval_id)
        session.get.return_value = ev

        def add_side(obj):
            from protea.infrastructure.orm.models.job import Job
            if isinstance(obj, Job):
                obj.id = uuid4()
        session.add.side_effect = add_side

        with patch("protea.api.routers.annotations.publish_job"):
            resp = c.post(
                f"/annotations/evaluation-sets/{eval_id}/run",
                json={"prediction_set_id": pred_set_id},
            )
        assert resp.status_code == 200
        assert resp.json()["status"] == "queued"

    def test_invalid_payload_returns_422(self, client):
        c, session = client
        eval_id = uuid4()

        resp = c.post(f"/annotations/evaluation-sets/{eval_id}/run", json={})
        assert resp.status_code == 422

    def test_evaluation_set_not_found(self, client):
        c, session = client
        eval_id = uuid4()
        pred_set_id = str(uuid4())
        session.get.return_value = None

        with patch("protea.api.routers.annotations.publish_job"):
            resp = c.post(
                f"/annotations/evaluation-sets/{eval_id}/run",
                json={"prediction_set_id": pred_set_id},
            )
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# GET .../results/{result_id}/metrics.tsv (lines 732-751)
# ---------------------------------------------------------------------------


class TestDownloadEvaluationMetrics:
    def test_success_with_results(self, client):
        c, session = client
        eval_id = uuid4()
        result = _make_evaluation_result(
            eval_set_id=eval_id,
            results={
                "NK": {
                    "BPO": {"fmax": 0.42, "precision": 0.5, "recall": 0.35, "tau": 0.3, "coverage": 0.8, "n_proteins": 100},
                    "MFO": {"fmax": 0.55, "precision": 0.6, "recall": 0.5, "tau": 0.4, "coverage": 0.9, "n_proteins": 80},
                },
                "LK": {},
            },
        )
        session.get.return_value = result

        resp = c.get(f"/annotations/evaluation-sets/{eval_id}/results/{result.id}/metrics.tsv")
        assert resp.status_code == 200
        assert "text/tab-separated-values" in resp.headers["content-type"]
        lines = resp.text.strip().split("\n")
        # header + 2 data lines (NK/BPO and NK/MFO)
        assert len(lines) == 3
        assert lines[0].startswith("setting")
        assert "NK\tBPO" in lines[1]

    def test_result_not_found(self, client):
        c, session = client
        eval_id = uuid4()
        session.get.return_value = None

        resp = c.get(f"/annotations/evaluation-sets/{eval_id}/results/{uuid4()}/metrics.tsv")
        assert resp.status_code == 404

    def test_result_wrong_eval_set(self, client):
        c, session = client
        eval_id = uuid4()
        result = _make_evaluation_result(eval_set_id=uuid4())  # different eval set
        session.get.return_value = result

        resp = c.get(f"/annotations/evaluation-sets/{eval_id}/results/{result.id}/metrics.tsv")
        assert resp.status_code == 404

    def test_empty_results(self, client):
        c, session = client
        eval_id = uuid4()
        result = _make_evaluation_result(eval_set_id=eval_id, results={})
        session.get.return_value = result

        resp = c.get(f"/annotations/evaluation-sets/{eval_id}/results/{result.id}/metrics.tsv")
        assert resp.status_code == 200
        lines = resp.text.strip().split("\n")
        assert len(lines) == 1  # header only


# ---------------------------------------------------------------------------
# GET .../results/{result_id}/artifacts.zip (lines 768-785)
# ---------------------------------------------------------------------------


class TestDownloadEvaluationArtifacts:
    def test_success(self, client_with_artifacts):
        c, session, tmp_path = client_with_artifacts
        eval_id = uuid4()
        result = _make_evaluation_result(eval_set_id=eval_id)
        session.get.return_value = result

        # Create artifact directory with files
        result_dir = tmp_path / str(result.id)
        result_dir.mkdir()
        (result_dir / "pr_curve.tsv").write_text("threshold\tprecision\trecall\n0.5\t0.8\t0.6")
        (result_dir / "metrics.json").write_text('{"fmax": 0.42}')

        resp = c.get(f"/annotations/evaluation-sets/{eval_id}/results/{result.id}/artifacts.zip")
        assert resp.status_code == 200
        assert "application/zip" in resp.headers["content-type"]
        assert len(resp.content) > 0

        # Verify it's a valid zip
        import io
        import zipfile
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            names = zf.namelist()
            assert "pr_curve.tsv" in names
            assert "metrics.json" in names

    def test_result_not_found(self, client_with_artifacts):
        c, session, _ = client_with_artifacts
        eval_id = uuid4()
        session.get.return_value = None

        resp = c.get(f"/annotations/evaluation-sets/{eval_id}/results/{uuid4()}/artifacts.zip")
        assert resp.status_code == 404

    def test_no_artifacts_directory(self, client_with_artifacts):
        c, session, tmp_path = client_with_artifacts
        eval_id = uuid4()
        result = _make_evaluation_result(eval_set_id=eval_id)
        session.get.return_value = result
        # No directory created for this result

        resp = c.get(f"/annotations/evaluation-sets/{eval_id}/results/{result.id}/artifacts.zip")
        assert resp.status_code == 404
        assert "No artifacts found" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# GET .../results (lines 800-809)
# ---------------------------------------------------------------------------


class TestListEvaluationResults:
    def test_success(self, client):
        c, session = client
        eval_id = uuid4()
        ev = _make_evaluation_set(eval_id=eval_id)
        result = _make_evaluation_result(eval_set_id=eval_id, scoring_id=uuid4(), job_id=uuid4())

        # First call: session.get(EvaluationSet, eval_id) returns ev
        session.get.return_value = ev
        session.query.return_value.filter.return_value.order_by.return_value.all.return_value = [result]

        resp = c.get(f"/annotations/evaluation-sets/{eval_id}/results")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["id"] == str(result.id)

    def test_eval_set_not_found(self, client):
        c, session = client
        session.get.return_value = None

        resp = c.get(f"/annotations/evaluation-sets/{uuid4()}/results")
        assert resp.status_code == 404

    def test_empty_results(self, client):
        c, session = client
        eval_id = uuid4()
        ev = _make_evaluation_set(eval_id=eval_id)
        session.get.return_value = ev
        session.query.return_value.filter.return_value.order_by.return_value.all.return_value = []

        resp = c.get(f"/annotations/evaluation-sets/{eval_id}/results")
        assert resp.status_code == 200
        assert resp.json() == []


# ---------------------------------------------------------------------------
# DELETE .../results/{result_id} (lines 834-845)
# ---------------------------------------------------------------------------


class TestDeleteEvaluationResult:
    def test_success(self, client_with_artifacts):
        c, session, tmp_path = client_with_artifacts
        eval_id = uuid4()
        result = _make_evaluation_result(eval_set_id=eval_id)
        session.get.return_value = result

        # Create artifact dir
        result_dir = tmp_path / str(result.id)
        result_dir.mkdir()
        (result_dir / "output.tsv").write_text("data")

        resp = c.delete(f"/annotations/evaluation-sets/{eval_id}/results/{result.id}")
        assert resp.status_code == 204
        session.delete.assert_called_once_with(result)
        assert not result_dir.exists()

    def test_not_found(self, client_with_artifacts):
        c, session, _ = client_with_artifacts
        eval_id = uuid4()
        session.get.return_value = None

        resp = c.delete(f"/annotations/evaluation-sets/{eval_id}/results/{uuid4()}")
        assert resp.status_code == 404

    def test_wrong_eval_set(self, client_with_artifacts):
        c, session, _ = client_with_artifacts
        eval_id = uuid4()
        result = _make_evaluation_result(eval_set_id=uuid4())
        session.get.return_value = result

        resp = c.delete(f"/annotations/evaluation-sets/{eval_id}/results/{result.id}")
        assert resp.status_code == 404

    def test_no_artifact_dir(self, client_with_artifacts):
        c, session, tmp_path = client_with_artifacts
        eval_id = uuid4()
        result = _make_evaluation_result(eval_set_id=eval_id)
        session.get.return_value = result

        resp = c.delete(f"/annotations/evaluation-sets/{eval_id}/results/{result.id}")
        assert resp.status_code == 204


# ---------------------------------------------------------------------------
# GET /annotations/snapshots/{snapshot_id}/subgraph (lines 859-927)
# ---------------------------------------------------------------------------


class TestGetGoSubgraph:
    def _make_go_term(self, db_id, go_id, name="term", aspect="F"):
        t = MagicMock()
        t.id = db_id
        t.go_id = go_id
        t.name = name
        t.aspect = aspect
        t.ontology_snapshot_id = None
        return t

    def _make_rel(self, child_id, parent_id, relation_type="is_a"):
        r = MagicMock()
        r.child_go_term_id = child_id
        r.parent_go_term_id = parent_id
        r.relation_type = relation_type
        r.ontology_snapshot_id = None
        return r

    def test_basic_subgraph(self, client):
        c, session = client
        snap_id = uuid4()
        snap = _make_snapshot(snap_id=snap_id)

        seed = self._make_go_term(1, "GO:0003674", "molecular_function")
        parent = self._make_go_term(2, "GO:0005488", "binding")
        rel = self._make_rel(1, 2, "is_a")

        # session.get for snapshot
        session.get.return_value = snap
        # session.query(GOTerm).filter(...).all() for seed terms
        # session.query(GOTermRelationship).filter(...).all() for rels
        # session.query(GOTerm).filter(...).all() for parents
        query_mock = session.query.return_value
        filter_mock = query_mock.filter.return_value
        filter_mock.all.side_effect = [
            [seed],   # seed terms query
            [rel],    # first BFS level relationships
            [parent], # parent terms fetch
            [],       # second BFS level relationships (no more)
        ]

        resp = c.get(f"/annotations/snapshots/{snap_id}/subgraph?go_ids=GO:0003674")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["nodes"]) == 2
        assert len(data["edges"]) == 1
        # Check that the seed term is marked as is_query
        seed_node = [n for n in data["nodes"] if n["go_id"] == "GO:0003674"][0]
        assert seed_node["is_query"] is True
        parent_node = [n for n in data["nodes"] if n["go_id"] == "GO:0005488"][0]
        assert parent_node["is_query"] is False

    def test_snapshot_not_found(self, client):
        c, session = client
        session.get.return_value = None

        resp = c.get(f"/annotations/snapshots/{uuid4()}/subgraph?go_ids=GO:0003674")
        assert resp.status_code == 404

    def test_no_matching_terms_returns_empty(self, client):
        c, session = client
        snap = _make_snapshot()
        session.get.return_value = snap
        session.query.return_value.filter.return_value.all.return_value = []

        resp = c.get(f"/annotations/snapshots/{snap.id}/subgraph?go_ids=GO:9999999")
        assert resp.status_code == 200
        data = resp.json()
        assert data == {"nodes": [], "edges": []}

    def test_multiple_go_ids(self, client):
        c, session = client
        snap = _make_snapshot()
        session.get.return_value = snap

        t1 = self._make_go_term(1, "GO:0003674")
        t2 = self._make_go_term(2, "GO:0008150")

        query_mock = session.query.return_value
        filter_mock = query_mock.filter.return_value
        filter_mock.all.side_effect = [
            [t1, t2],  # seed terms
            [],         # no relationships
        ]

        resp = c.get(f"/annotations/snapshots/{snap.id}/subgraph?go_ids=GO:0003674,GO:0008150")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["nodes"]) == 2
        assert data["edges"] == []

    def test_bfs_stops_when_frontier_empty(self, client):
        """After one BFS level with parents, next level has rels but no new parents -> frontier empty -> break (line 887)."""
        c, session = client
        snap = _make_snapshot()
        session.get.return_value = snap

        seed = self._make_go_term(1, "GO:0003674")
        parent = self._make_go_term(2, "GO:0005488")
        rel1 = self._make_rel(1, 2, "is_a")

        query_mock = session.query.return_value
        filter_mock = query_mock.filter.return_value
        filter_mock.all.side_effect = [
            [seed],    # seed terms
            [rel1],    # first BFS: rel from 1->2
            [parent],  # fetch parent 2
            [],        # second BFS: no rels from frontier {2}
        ]

        resp = c.get(f"/annotations/snapshots/{snap.id}/subgraph?go_ids=GO:0003674&depth=5")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["nodes"]) == 2
