"""Unit tests for the scoring API router — no real DB required."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from protea.api.routers.scoring import router
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.embedding.dataset import Dataset
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.embedding.reranker_model import RerankerModel
from protea.infrastructure.orm.models.embedding.scoring_config import (
    FORMULA_LINEAR,
    ScoringConfig,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_config(name="test", formula=FORMULA_LINEAR, weights=None, ev_weights=None):
    cfg = MagicMock(spec=ScoringConfig)
    cfg.id = uuid4()
    cfg.name = name
    cfg.formula = formula
    cfg.weights = weights or {"embedding_similarity": 1.0}
    cfg.evidence_weights = ev_weights
    cfg.params = None
    cfg.description = None
    cfg.created_at = datetime(2026, 1, 1, tzinfo=UTC)
    return cfg


@contextmanager
def _mock_scope(session):
    yield session


def _make_app(session):
    app = FastAPI()
    factory = MagicMock()
    app.state.session_factory = factory
    app.include_router(router)
    return app, factory, session


@pytest.fixture()
def session():
    return MagicMock()


@pytest.fixture()
def client(session):
    app, factory, _ = _make_app(session)
    with patch(
        "protea.api.routers.scoring.session_scope", side_effect=lambda _: _mock_scope(session)
    ):
        yield TestClient(app, raise_server_exceptions=True)


# ---------------------------------------------------------------------------
# GET /configs
# ---------------------------------------------------------------------------


class TestListScoringConfigs:
    def test_empty_list(self, client, session):
        session.query.return_value.order_by.return_value.all.return_value = []
        resp = client.get("/scoring/configs")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_returns_configs(self, client, session):
        cfg = _make_config("my-config")
        session.query.return_value.order_by.return_value.all.return_value = [cfg]
        resp = client.get("/scoring/configs")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["name"] == "my-config"


# ---------------------------------------------------------------------------
# POST /configs
# ---------------------------------------------------------------------------


class TestCreateScoringConfig:
    def test_create_valid(self, client, session):
        cfg = _make_config("new-config")
        session.flush = MagicMock()

        def add_side_effect(obj):
            obj.id = cfg.id
            obj.created_at = cfg.created_at

        session.add.side_effect = add_side_effect

        resp = client.post(
            "/scoring/configs",
            json={
                "name": "new-config",
                "formula": "linear",
                "weights": {"embedding_similarity": 1.0},
            },
        )
        assert resp.status_code == 201
        assert resp.json()["name"] == "new-config"

    def test_invalid_formula_returns_422(self, client, session):
        resp = client.post(
            "/scoring/configs",
            json={
                "name": "bad",
                "formula": "nonexistent_formula",
                "weights": {"embedding_similarity": 1.0},
            },
        )
        assert resp.status_code == 422

    def test_unknown_signal_key_returns_422(self, client, session):
        resp = client.post(
            "/scoring/configs",
            json={
                "name": "bad",
                "formula": "linear",
                "weights": {"nonexistent_signal": 1.0},
            },
        )
        assert resp.status_code == 422

    def test_invalid_evidence_weight_value_returns_422(self, client, session):
        resp = client.post(
            "/scoring/configs",
            json={
                "name": "bad",
                "formula": "linear",
                "weights": {"embedding_similarity": 1.0},
                "evidence_weights": {"IEA": 1.5},
            },
        )
        assert resp.status_code == 422

    def test_unknown_evidence_code_returns_422(self, client, session):
        resp = client.post(
            "/scoring/configs",
            json={
                "name": "bad",
                "formula": "linear",
                "weights": {"embedding_similarity": 1.0},
                "evidence_weights": {"BADCODE": 0.5},
            },
        )
        assert resp.status_code == 422

    def test_evidence_weighted_formula_accepted(self, client, session):
        cfg = _make_config("ew-config", formula="evidence_weighted")
        session.flush = MagicMock()

        def add_side_effect(obj):
            obj.id = cfg.id
            obj.created_at = cfg.created_at

        session.add.side_effect = add_side_effect

        resp = client.post(
            "/scoring/configs",
            json={
                "name": "ew-config",
                "formula": "evidence_weighted",
                "weights": {"embedding_similarity": 1.0},
            },
        )
        assert resp.status_code == 201


# ---------------------------------------------------------------------------
# GET /configs/{config_id}
# ---------------------------------------------------------------------------


class TestGetScoringConfig:
    def test_found(self, client, session):
        cfg = _make_config("found")
        session.get.return_value = cfg
        resp = client.get(f"/scoring/configs/{cfg.id}")
        assert resp.status_code == 200
        assert resp.json()["name"] == "found"

    def test_not_found(self, client, session):
        session.get.return_value = None
        resp = client.get(f"/scoring/configs/{uuid4()}")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# DELETE /configs/{config_id}
# ---------------------------------------------------------------------------


class TestDeleteScoringConfig:
    def test_delete_existing(self, client, session):
        cfg = _make_config()
        session.get.return_value = cfg
        resp = client.delete(f"/scoring/configs/{cfg.id}")
        assert resp.status_code == 204
        session.delete.assert_called_once_with(cfg)

    def test_delete_not_found(self, client, session):
        session.get.return_value = None
        resp = client.delete(f"/scoring/configs/{uuid4()}")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# POST /configs/presets
# ---------------------------------------------------------------------------


class TestCreatePresets:
    def test_creates_all_presets_when_none_exist(self, client, session):
        session.query.return_value.all.return_value = []
        resp = client.post("/scoring/configs/presets")
        assert resp.status_code == 201
        data = resp.json()
        assert "created" in data
        assert len(data["created"]) > 0

    def test_skips_existing_presets(self, client, session):
        from protea.services.scoring_service import PRESET_CONFIGS as _PRESET_CONFIGS

        all_names = [(p["name"],) for p in _PRESET_CONFIGS]
        session.query.return_value.all.return_value = all_names
        resp = client.post("/scoring/configs/presets")
        assert resp.status_code == 201
        assert resp.json()["created"] == []


# ---------------------------------------------------------------------------
# GET /prediction-sets/{set_id}/training-data.tsv
# ---------------------------------------------------------------------------


def _make_eval_set():
    es = MagicMock(spec=EvaluationSet)
    es.id = uuid4()
    es.old_annotation_set_id = uuid4()
    es.new_annotation_set_id = uuid4()
    # When None, scoring router takes the on-the-fly compute_evaluation_data
    # branch (which tests mock); a truthy MagicMock pulls the persisted
    # artifact path that needs a real UUID for pivot_ontology_snapshot_id.
    es.groundtruth_uri = None
    es.stats = None
    return es


def _make_annotation_set(snapshot_id=None):
    """An annotation set carrying the snapshot it was built against.

    The delta resolver takes each set's native snapshot from the set itself
    rather than from the caller, so a double that cannot answer
    ``session.get(AnnotationSet, ...)`` is refused by name.
    """
    a = MagicMock(spec=AnnotationSet)
    a.id = uuid4()
    a.ontology_snapshot_id = snapshot_id or uuid4()
    return a


def _make_pred_set():
    ps = MagicMock(spec=PredictionSet)
    ps.id = uuid4()
    ps.ontology_snapshot_id = uuid4()
    return ps


def _make_go_prediction(**kwargs):
    pred = MagicMock()
    pred.protein_accession = kwargs.get("protein_accession", "P12345")
    pred.distance = kwargs.get("distance", 0.1)
    pred.ref_protein_accession = kwargs.get("ref_protein_accession", "Q99999")
    pred.qualifier = kwargs.get("qualifier", "enables")
    pred.evidence_code = kwargs.get("evidence_code", "IDA")
    pred.identity_nw = kwargs.get("identity_nw", 0.9)
    pred.similarity_nw = kwargs.get("similarity_nw", 0.85)
    pred.alignment_score_nw = kwargs.get("alignment_score_nw", 450.0)
    pred.gaps_pct_nw = kwargs.get("gaps_pct_nw", 0.02)
    pred.alignment_length_nw = kwargs.get("alignment_length_nw", 300.0)
    pred.identity_sw = kwargs.get("identity_sw", 0.92)
    pred.similarity_sw = kwargs.get("similarity_sw", 0.88)
    pred.alignment_score_sw = kwargs.get("alignment_score_sw", 420.0)
    pred.gaps_pct_sw = kwargs.get("gaps_pct_sw", 0.01)
    pred.alignment_length_sw = kwargs.get("alignment_length_sw", 280.0)
    pred.length_query = kwargs.get("length_query", 350)
    pred.length_ref = kwargs.get("length_ref", 340)
    pred.query_taxonomy_id = kwargs.get("query_taxonomy_id", 9606)
    pred.ref_taxonomy_id = kwargs.get("ref_taxonomy_id", 10090)
    pred.taxonomic_lca = kwargs.get("taxonomic_lca", 314146)
    pred.taxonomic_distance = kwargs.get("taxonomic_distance", 4)
    pred.taxonomic_common_ancestors = kwargs.get("taxonomic_common_ancestors", 20)
    pred.taxonomic_relation = kwargs.get("taxonomic_relation", "sibling")
    pred.vote_count = kwargs.get("vote_count", 3)
    pred.k_position = kwargs.get("k_position", 1)
    pred.go_term_frequency = kwargs.get("go_term_frequency", 15)
    pred.ref_annotation_density = kwargs.get("ref_annotation_density", 8)
    pred.neighbor_distance_std = kwargs.get("neighbor_distance_std", 0.05)
    return pred


class TestTrainingDataEndpoint:
    def _url(self, set_id, eval_set_id, category="nk"):
        return (
            f"/scoring/prediction-sets/{set_id}/training-data.tsv"
            f"?evaluation_set_id={eval_set_id}&category={category}"
        )

    def test_prediction_set_not_found(self, client, session):
        session.get.return_value = None
        resp = client.get(self._url(uuid4(), uuid4()))
        assert resp.status_code == 404
        assert "PredictionSet" in resp.json()["detail"]

    def test_evaluation_set_not_found(self, client, session):
        ps = _make_pred_set()
        session.get.side_effect = lambda model, id_: ps if model is PredictionSet else None
        resp = client.get(self._url(ps.id, uuid4()))
        assert resp.status_code == 404
        assert "EvaluationSet" in resp.json()["detail"]

    def test_invalid_category_returns_422(self, client, session):
        resp = client.get(
            f"/scoring/prediction-sets/{uuid4()}/training-data.tsv"
            f"?evaluation_set_id={uuid4()}&category=invalid"
        )
        assert resp.status_code == 422

    @patch("protea.services._scoring_validation_helpers.compute_evaluation_data_for_sets")
    def test_streams_labeled_data_positive(self, mock_eval, session):
        """Prediction matching ground truth gets label=1."""
        ps = _make_pred_set()
        es = _make_eval_set()
        pred = _make_go_prediction(protein_accession="P12345")

        def get_side(model, id_):
            if model is PredictionSet:
                return ps
            if model is EvaluationSet:
                return es
            return None

        session.get.side_effect = get_side

        eval_data = MagicMock()
        eval_data.nk = {"P12345": {"GO:0003674"}}
        mock_eval.return_value = eval_data

        q_mock = MagicMock()
        session.query.return_value.join.return_value.filter.return_value = q_mock
        q_mock.yield_per.return_value = [(pred, "GO:0003674", "F")]

        app = FastAPI()
        app.state.session_factory = MagicMock()
        app.include_router(router)
        with (
            patch(
                "protea.api.routers.scoring.session_scope",
                side_effect=lambda _: _mock_scope(session),
            ),
            patch(
                "protea.services._scoring_streaming_helpers.session_scope",
                side_effect=lambda _: _mock_scope(session),
            ),
        ):
            with TestClient(app) as c:
                resp = c.get(self._url(ps.id, es.id, "nk"))

        assert resp.status_code == 200
        assert "text/tab-separated-values" in resp.headers["content-type"]
        lines = resp.text.strip().split("\n")
        assert len(lines) == 2
        header = lines[0].split("\t")
        assert "label" in header
        assert "vote_count" in header
        row = lines[1].split("\t")
        label_idx = header.index("label")
        assert row[label_idx] == "1"

    @patch("protea.services._scoring_validation_helpers.compute_evaluation_data_for_sets")
    def test_streams_labeled_data_negative(self, mock_eval, session):
        """Prediction NOT in ground truth gets label=0."""
        ps = _make_pred_set()
        es = _make_eval_set()
        pred = _make_go_prediction(protein_accession="P12345")

        def get_side(model, id_):
            if model is PredictionSet:
                return ps
            if model is EvaluationSet:
                return es
            return None

        session.get.side_effect = get_side

        eval_data = MagicMock()
        eval_data.nk = {"P99999": {"GO:0005575"}}  # different protein
        mock_eval.return_value = eval_data

        q_mock = MagicMock()
        session.query.return_value.join.return_value.filter.return_value = q_mock
        q_mock.yield_per.return_value = [(pred, "GO:0003674", "F")]

        app = FastAPI()
        app.state.session_factory = MagicMock()
        app.include_router(router)
        with (
            patch(
                "protea.api.routers.scoring.session_scope",
                side_effect=lambda _: _mock_scope(session),
            ),
            patch(
                "protea.services._scoring_streaming_helpers.session_scope",
                side_effect=lambda _: _mock_scope(session),
            ),
        ):
            with TestClient(app) as c:
                resp = c.get(self._url(ps.id, es.id, "nk"))

        lines = resp.text.strip().split("\n")
        header = lines[0].split("\t")
        row = lines[1].split("\t")
        label_idx = header.index("label")
        assert row[label_idx] == "0"

    @patch("protea.services._scoring_validation_helpers.compute_evaluation_data_for_sets")
    def test_all_columns_present(self, mock_eval, session):
        """Verify all 32 columns are in the TSV header."""
        ps = _make_pred_set()
        es = _make_eval_set()

        def get_side(model, id_):
            if model is PredictionSet:
                return ps
            if model is EvaluationSet:
                return es
            return None

        session.get.side_effect = get_side

        eval_data = MagicMock()
        eval_data.nk = {}
        mock_eval.return_value = eval_data

        q_mock = MagicMock()
        session.query.return_value.join.return_value.filter.return_value = q_mock
        q_mock.yield_per.return_value = []

        app = FastAPI()
        app.state.session_factory = MagicMock()
        app.include_router(router)
        with (
            patch(
                "protea.api.routers.scoring.session_scope",
                side_effect=lambda _: _mock_scope(session),
            ),
            patch(
                "protea.services._scoring_streaming_helpers.session_scope",
                side_effect=lambda _: _mock_scope(session),
            ),
        ):
            with TestClient(app) as c:
                resp = c.get(self._url(ps.id, es.id))

        header = resp.text.strip().split("\n")[0].split("\t")
        assert len(header) == 31
        assert header[0] == "protein_accession"
        assert header[3] == "label"
        assert header[-1] == "neighbor_distance_std"

    @patch("protea.services._scoring_validation_helpers.compute_evaluation_data_for_sets")
    def test_pk_category(self, mock_eval, session):
        """PK category uses eval_data.pk for ground truth."""
        ps = _make_pred_set()
        es = _make_eval_set()
        pred = _make_go_prediction(protein_accession="P12345")

        def get_side(model, id_):
            if model is PredictionSet:
                return ps
            if model is EvaluationSet:
                return es
            return None

        session.get.side_effect = get_side

        eval_data = MagicMock()
        eval_data.nk = {}
        eval_data.lk = {}
        eval_data.pk = {"P12345": {"GO:0003674"}}
        mock_eval.return_value = eval_data

        q_mock = MagicMock()
        session.query.return_value.join.return_value.filter.return_value = q_mock
        q_mock.yield_per.return_value = [(pred, "GO:0003674", "F")]

        app = FastAPI()
        app.state.session_factory = MagicMock()
        app.include_router(router)
        with (
            patch(
                "protea.api.routers.scoring.session_scope",
                side_effect=lambda _: _mock_scope(session),
            ),
            patch(
                "protea.services._scoring_streaming_helpers.session_scope",
                side_effect=lambda _: _mock_scope(session),
            ),
        ):
            with TestClient(app) as c:
                resp = c.get(self._url(ps.id, es.id, "pk"))

        lines = resp.text.strip().split("\n")
        header = lines[0].split("\t")
        row = lines[1].split("\t")
        label_idx = header.index("label")
        assert row[label_idx] == "1"

    @patch("protea.services._scoring_validation_helpers.compute_evaluation_data_for_sets")
    def test_none_features_render_as_empty(self, mock_eval, session):
        """None values are rendered as empty strings in the TSV."""
        ps = _make_pred_set()
        es = _make_eval_set()
        pred = _make_go_prediction(
            identity_nw=None,
            similarity_nw=None,
            vote_count=None,
            neighbor_distance_std=None,
        )

        def get_side(model, id_):
            if model is PredictionSet:
                return ps
            if model is EvaluationSet:
                return es
            return None

        session.get.side_effect = get_side

        eval_data = MagicMock()
        eval_data.nk = {}
        mock_eval.return_value = eval_data

        q_mock = MagicMock()
        session.query.return_value.join.return_value.filter.return_value = q_mock
        q_mock.yield_per.return_value = [(pred, "GO:0003674", "F")]

        app = FastAPI()
        app.state.session_factory = MagicMock()
        app.include_router(router)
        with (
            patch(
                "protea.api.routers.scoring.session_scope",
                side_effect=lambda _: _mock_scope(session),
            ),
            patch(
                "protea.services._scoring_streaming_helpers.session_scope",
                side_effect=lambda _: _mock_scope(session),
            ),
        ):
            with TestClient(app) as c:
                resp = c.get(self._url(ps.id, es.id))

        lines = resp.text.strip().split("\n")
        header = lines[0].split("\t")
        row = lines[1].split("\t")
        # identity_nw should be empty
        nw_idx = header.index("identity_nw")
        assert row[nw_idx] == ""
        # vote_count should be empty
        vc_idx = header.index("vote_count")
        assert row[vc_idx] == ""


# ---------------------------------------------------------------------------
# Reranker CRUD endpoints
# ---------------------------------------------------------------------------


def _make_reranker_model(**kwargs):
    m = MagicMock(spec=RerankerModel)
    m.id = kwargs.get("id", uuid4())
    m.name = kwargs.get("name", "test-reranker")
    m.prediction_set_id = kwargs.get("prediction_set_id", uuid4())
    m.evaluation_set_id = kwargs.get("evaluation_set_id", uuid4())
    m.category = kwargs.get("category", "nk")
    m.aspect = kwargs.get("aspect", None)
    m.model_data = kwargs.get("model_data", "lgb_model_string")
    m.metrics = kwargs.get("metrics", {"val_auc": 0.85})
    m.feature_importance = kwargs.get("feature_importance", {"distance": 100})
    m.feature_schema_sha = kwargs.get("feature_schema_sha", "ab12cd34ef56")
    m.producer_version = kwargs.get("producer_version", None)
    m.producer_git_sha = kwargs.get("producer_git_sha", None)
    m.external_source = kwargs.get("external_source", None)
    m.dataset_id = kwargs.get("dataset_id", None)
    m.spec_yaml = kwargs.get("spec_yaml", None)
    m.created_at = datetime(2026, 3, 18, tzinfo=UTC)
    return m


def _make_dataset(**kwargs):
    d = MagicMock(spec=Dataset)
    d.id = kwargs.get("id", uuid4())
    d.name = kwargs.get("name", "bench-v1-K5-v226-lineage")
    d.schema_sha = kwargs.get("schema_sha", "ds5chemafff0")
    d.manifest_sha = kwargs.get("manifest_sha", "f" * 64)
    return d


def _query_router(models, datasets):
    """Route ``session.query(RerankerModel|Dataset)`` to the right rows."""

    def _query(model):
        q = MagicMock()
        if model is Dataset:
            q.filter.return_value.all.return_value = datasets
        else:
            q.order_by.return_value.all.return_value = models
        return q

    return _query


class TestListRerankers:
    def test_empty_list(self, client, session):
        session.query.side_effect = _query_router([], [])
        resp = client.get("/scoring/rerankers")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_returns_rerankers(self, client, session):
        m = _make_reranker_model(name="my-model")
        session.query.side_effect = _query_router([m], [])
        resp = client.get("/scoring/rerankers")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["name"] == "my-model"
        assert "metrics" in data[0]

    def test_surfaces_provenance_and_dataset_shas(self, client, session):
        ds = _make_dataset()
        m = _make_reranker_model(
            name="champion",
            dataset_id=ds.id,
            feature_schema_sha="featschemaaa1",
            producer_version="0.9.2",
            producer_git_sha="deadbeefcafe",
            external_source="protea-reranker-lab@cec8ccd",
            spec_yaml="training:\n  cell: nk-bpo\n",
            metrics={
                "fmax": 0.7291,
                "__feature_selection__": {
                    "families_enabled": None,
                    "families_available": ["knn", "lineage"],
                    "drop_features": [],
                    "feature_count": 56,
                },
            },
        )
        session.query.side_effect = _query_router([m], [ds])
        resp = client.get("/scoring/rerankers")
        assert resp.status_code == 200
        row = resp.json()[0]
        assert row["feature_schema_sha"] == "featschemaaa1"
        assert row["producer_version"] == "0.9.2"
        assert row["producer_git_sha"] == "deadbeefcafe"
        assert row["external_source"] == "protea-reranker-lab@cec8ccd"
        assert row["dataset_id"] == str(ds.id)
        assert row["dataset_name"] == ds.name
        assert row["dataset_schema_sha"] == ds.schema_sha
        assert row["dataset_manifest_sha"] == ds.manifest_sha
        assert row["spec_yaml"] == "training:\n  cell: nk-bpo\n"
        assert row["feature_selection"]["feature_count"] == 56
        assert row["feature_selection"]["families_available"] == ["knn", "lineage"]


class TestGetReranker:
    def test_found(self, client, session):
        m = _make_reranker_model(name="found")
        session.get.return_value = m
        resp = client.get(f"/scoring/rerankers/{m.id}")
        assert resp.status_code == 200
        assert resp.json()["name"] == "found"

    def test_found_resolves_linked_dataset(self, client, session):
        ds = _make_dataset()
        m = _make_reranker_model(name="found", dataset_id=ds.id)

        def get_side(model, id_):
            if model is Dataset:
                return ds
            return m

        session.get.side_effect = get_side
        resp = client.get(f"/scoring/rerankers/{m.id}")
        assert resp.status_code == 200
        body = resp.json()
        assert body["dataset_schema_sha"] == ds.schema_sha
        assert body["dataset_manifest_sha"] == ds.manifest_sha

    def test_not_found(self, client, session):
        session.get.return_value = None
        resp = client.get(f"/scoring/rerankers/{uuid4()}")
        assert resp.status_code == 404


class TestDeleteReranker:
    def test_delete_existing(self, client, session):
        m = _make_reranker_model()
        session.get.return_value = m
        resp = client.delete(f"/scoring/rerankers/{m.id}")
        assert resp.status_code == 204
        session.delete.assert_called_once_with(m)

    def test_delete_not_found(self, client, session):
        session.get.return_value = None
        resp = client.delete(f"/scoring/rerankers/{uuid4()}")
        assert resp.status_code == 404
