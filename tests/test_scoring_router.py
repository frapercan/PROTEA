"""Unit tests for the scoring API router — no real DB required."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from protea.api.routers.scoring import router
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
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
    cfg.description = None
    cfg.created_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
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
    with patch("protea.api.routers.scoring.session_scope", side_effect=lambda _: _mock_scope(session)):
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

        resp = client.post("/scoring/configs", json={
            "name": "new-config",
            "formula": "linear",
            "weights": {"embedding_similarity": 1.0},
        })
        assert resp.status_code == 201
        assert resp.json()["name"] == "new-config"

    def test_invalid_formula_returns_422(self, client, session):
        resp = client.post("/scoring/configs", json={
            "name": "bad",
            "formula": "nonexistent_formula",
            "weights": {"embedding_similarity": 1.0},
        })
        assert resp.status_code == 422

    def test_unknown_signal_key_returns_422(self, client, session):
        resp = client.post("/scoring/configs", json={
            "name": "bad",
            "formula": "linear",
            "weights": {"nonexistent_signal": 1.0},
        })
        assert resp.status_code == 422

    def test_invalid_evidence_weight_value_returns_422(self, client, session):
        resp = client.post("/scoring/configs", json={
            "name": "bad",
            "formula": "linear",
            "weights": {"embedding_similarity": 1.0},
            "evidence_weights": {"IEA": 1.5},
        })
        assert resp.status_code == 422

    def test_unknown_evidence_code_returns_422(self, client, session):
        resp = client.post("/scoring/configs", json={
            "name": "bad",
            "formula": "linear",
            "weights": {"embedding_similarity": 1.0},
            "evidence_weights": {"BADCODE": 0.5},
        })
        assert resp.status_code == 422

    def test_evidence_weighted_formula_accepted(self, client, session):
        cfg = _make_config("ew-config", formula="evidence_weighted")
        session.flush = MagicMock()

        def add_side_effect(obj):
            obj.id = cfg.id
            obj.created_at = cfg.created_at

        session.add.side_effect = add_side_effect

        resp = client.post("/scoring/configs", json={
            "name": "ew-config",
            "formula": "evidence_weighted",
            "weights": {"embedding_similarity": 1.0},
        })
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
        from protea.api.routers.scoring import _PRESET_CONFIGS
        all_names = [(p["name"],) for p in _PRESET_CONFIGS]
        session.query.return_value.all.return_value = all_names
        resp = client.post("/scoring/configs/presets")
        assert resp.status_code == 201
        assert resp.json()["created"] == []


# ---------------------------------------------------------------------------
# GET /prediction-sets/{set_id}/score.tsv — 404 preflight checks
# ---------------------------------------------------------------------------

class TestScoredTSV:
    def test_prediction_set_not_found(self, client, session):
        session.get.return_value = None
        resp = client.get(
            f"/scoring/prediction-sets/{uuid4()}/score.tsv"
            f"?scoring_config_id={uuid4()}"
        )
        assert resp.status_code == 404

    def test_scoring_config_not_found(self, client, session):
        from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
        # First get (PredictionSet) found, second (ScoringConfig) not found
        session.get.side_effect = [MagicMock(), None]
        resp = client.get(
            f"/scoring/prediction-sets/{uuid4()}/score.tsv"
            f"?scoring_config_id={uuid4()}"
        )
        assert resp.status_code == 404

    @patch("protea.api.routers.scoring.compute_score", return_value=0.85)
    def test_streams_tsv_with_data(self, mock_score, session):
        """Full streaming path: header + data rows."""
        set_id = uuid4()
        config_id = uuid4()
        cfg = _make_config("stream", formula="linear")
        cfg.id = config_id
        pred_set = MagicMock()

        pred = MagicMock()
        pred.protein_accession = "P12345"
        pred.distance = 0.1
        pred.ref_protein_accession = "Q99999"
        pred.evidence_code = "IDA"
        pred.qualifier = "enables"
        pred.identity_nw = 0.9
        pred.identity_sw = 0.8
        pred.taxonomic_distance = 2

        def get_side(model, id_):
            from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
            from protea.infrastructure.orm.models.embedding.scoring_config import ScoringConfig
            if model is PredictionSet:
                return pred_set
            if model is ScoringConfig:
                return cfg
            return None

        session.get.side_effect = get_side
        q_mock = MagicMock()
        session.query.return_value.join.return_value.filter.return_value = q_mock
        q_mock.filter.return_value = q_mock
        q_mock.yield_per.return_value = [(pred, "GO:0003674")]

        app = FastAPI()
        factory = MagicMock()
        app.state.session_factory = factory
        app.include_router(router)
        with patch("protea.api.routers.scoring.session_scope", side_effect=lambda _: _mock_scope(session)):
            with TestClient(app) as c:
                resp = c.get(
                    f"/scoring/prediction-sets/{set_id}/score.tsv"
                    f"?scoring_config_id={config_id}"
                )
        assert resp.status_code == 200
        assert "text/tab-separated-values" in resp.headers["content-type"]
        lines = resp.text.strip().split("\n")
        assert len(lines) == 2
        assert lines[0].startswith("protein_accession")
        assert "P12345" in lines[1]
        assert "GO:0003674" in lines[1]

    @patch("protea.api.routers.scoring.compute_score", return_value=0.3)
    def test_min_score_filters_rows(self, mock_score, session):
        """Rows below min_score are excluded from the stream."""
        set_id = uuid4()
        config_id = uuid4()
        cfg = _make_config("filter")
        cfg.id = config_id

        pred = MagicMock()
        pred.protein_accession = "P00001"
        pred.distance = 0.5
        pred.ref_protein_accession = None
        pred.evidence_code = "IEA"
        pred.qualifier = None
        pred.identity_nw = None
        pred.identity_sw = None
        pred.taxonomic_distance = None

        def get_side(model, id_):
            from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
            from protea.infrastructure.orm.models.embedding.scoring_config import ScoringConfig
            if model is PredictionSet:
                return MagicMock()
            if model is ScoringConfig:
                return cfg
            return None

        session.get.side_effect = get_side
        q_mock = MagicMock()
        session.query.return_value.join.return_value.filter.return_value = q_mock
        q_mock.filter.return_value = q_mock
        q_mock.yield_per.return_value = [(pred, "GO:0005575")]

        app = FastAPI()
        factory = MagicMock()
        app.state.session_factory = factory
        app.include_router(router)
        with patch("protea.api.routers.scoring.session_scope", side_effect=lambda _: _mock_scope(session)):
            with TestClient(app) as c:
                resp = c.get(
                    f"/scoring/prediction-sets/{set_id}/score.tsv"
                    f"?scoring_config_id={config_id}&min_score=0.5"
                )
        assert resp.status_code == 200
        lines = resp.text.strip().split("\n")
        # Only header — score 0.3 < min_score 0.5
        assert len(lines) == 1
        assert lines[0].startswith("protein_accession")

    @patch("protea.api.routers.scoring.compute_score", return_value=0.9)
    def test_accession_filter(self, mock_score, session):
        """Accession query parameter is forwarded to the DB query."""
        set_id = uuid4()
        config_id = uuid4()
        cfg = _make_config("acc-filter")
        cfg.id = config_id

        pred = MagicMock()
        pred.protein_accession = "P99999"
        pred.distance = 0.05
        pred.ref_protein_accession = "Q11111"
        pred.evidence_code = "EXP"
        pred.qualifier = "enables"
        pred.identity_nw = 0.95
        pred.identity_sw = 0.92
        pred.taxonomic_distance = 0

        def get_side(model, id_):
            from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
            from protea.infrastructure.orm.models.embedding.scoring_config import ScoringConfig
            if model is PredictionSet:
                return MagicMock()
            if model is ScoringConfig:
                return cfg
            return None

        session.get.side_effect = get_side
        q_mock = MagicMock()
        session.query.return_value.join.return_value.filter.return_value = q_mock
        q_mock.filter.return_value = q_mock
        q_mock.yield_per.return_value = [(pred, "GO:0008150")]

        app = FastAPI()
        factory = MagicMock()
        app.state.session_factory = factory
        app.include_router(router)
        with patch("protea.api.routers.scoring.session_scope", side_effect=lambda _: _mock_scope(session)):
            with TestClient(app) as c:
                resp = c.get(
                    f"/scoring/prediction-sets/{set_id}/score.tsv"
                    f"?scoring_config_id={config_id}&accession=P99999"
                )
        assert resp.status_code == 200
        lines = resp.text.strip().split("\n")
        assert len(lines) == 2
        assert "P99999" in lines[1]


# ---------------------------------------------------------------------------
# GET /prediction-sets/{set_id}/metrics — 404 preflight checks
# ---------------------------------------------------------------------------

class TestMetricsEndpoint:
    def _url(self):
        return (
            f"/scoring/prediction-sets/{uuid4()}/metrics"
            f"?scoring_config_id={uuid4()}"
            f"&old_annotation_set_id={uuid4()}"
            f"&new_annotation_set_id={uuid4()}"
            f"&ontology_snapshot_id={uuid4()}"
        )

    def test_prediction_set_not_found(self, client, session):
        session.get.return_value = None
        resp = client.get(self._url())
        assert resp.status_code == 404

    def test_scoring_config_not_found(self, client, session):
        session.get.side_effect = [MagicMock(), None]
        resp = client.get(self._url())
        assert resp.status_code == 404

    def test_invalid_category_returns_422(self, client, session):
        resp = client.get(
            f"/scoring/prediction-sets/{uuid4()}/metrics"
            f"?scoring_config_id={uuid4()}"
            f"&old_annotation_set_id={uuid4()}"
            f"&new_annotation_set_id={uuid4()}"
            f"&ontology_snapshot_id={uuid4()}"
            f"&category=invalid"
        )
        assert resp.status_code == 422

    @patch("protea.api.routers.scoring.compute_cafa_metrics")
    @patch("protea.api.routers.scoring.compute_evaluation_data")
    @patch("protea.api.routers.scoring.compute_score", return_value=0.9)
    def test_returns_metrics_with_curve(self, mock_score, mock_eval, mock_metrics, client, session):
        set_id = uuid4()
        config_id = uuid4()
        cfg = _make_config("metrics-cfg")
        cfg.id = config_id
        pred_set = MagicMock()

        def get_side(model, id_):
            from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
            from protea.infrastructure.orm.models.embedding.scoring_config import ScoringConfig
            if model is PredictionSet:
                return pred_set
            if model is ScoringConfig:
                return cfg
            return None

        session.get.side_effect = get_side
        mock_eval.return_value = MagicMock()

        pred = MagicMock()
        pred.protein_accession = "P12345"
        pred.distance = 0.1
        pred.identity_nw = 0.9
        pred.identity_sw = 0.8
        pred.evidence_code = "IDA"
        pred.taxonomic_distance = 2

        session.query.return_value.join.return_value.filter.return_value.all.return_value = [
            (pred, "GO:0003674"),
        ]

        point = MagicMock()
        point.threshold = 0.5
        point.precision = 0.9
        point.recall = 0.8
        point.f1 = 0.85
        metrics_result = MagicMock()
        metrics_result.summary.return_value = {"fmax": 0.85, "auc_pr": 0.78}
        metrics_result.curve = [point]
        mock_metrics.return_value = metrics_result

        resp = client.get(
            f"/scoring/prediction-sets/{set_id}/metrics"
            f"?scoring_config_id={config_id}"
            f"&old_annotation_set_id={uuid4()}"
            f"&new_annotation_set_id={uuid4()}"
            f"&ontology_snapshot_id={uuid4()}"
            f"&category=nk"
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["prediction_set_id"] == str(set_id)
        assert data["scoring_config_id"] == str(config_id)
        assert data["scoring_config_name"] == "metrics-cfg"
        assert "fmax" in data
        assert "curve" in data
        assert len(data["curve"]) == 1
        assert data["curve"][0]["threshold"] == 0.5

    @patch("protea.api.routers.scoring.compute_cafa_metrics")
    @patch("protea.api.routers.scoring.compute_evaluation_data")
    @patch("protea.api.routers.scoring.compute_score", return_value=0.5)
    def test_lk_category(self, mock_score, mock_eval, mock_metrics, client, session):
        set_id = uuid4()
        config_id = uuid4()
        cfg = _make_config("lk-cfg")
        cfg.id = config_id

        def get_side(model, id_):
            from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
            from protea.infrastructure.orm.models.embedding.scoring_config import ScoringConfig
            if model is PredictionSet:
                return MagicMock()
            if model is ScoringConfig:
                return cfg
            return None

        session.get.side_effect = get_side
        mock_eval.return_value = MagicMock()
        session.query.return_value.join.return_value.filter.return_value.all.return_value = []

        metrics_result = MagicMock()
        metrics_result.summary.return_value = {"fmax": 0.0, "auc_pr": 0.0}
        metrics_result.curve = []
        mock_metrics.return_value = metrics_result

        resp = client.get(
            f"/scoring/prediction-sets/{set_id}/metrics"
            f"?scoring_config_id={config_id}"
            f"&old_annotation_set_id={uuid4()}"
            f"&new_annotation_set_id={uuid4()}"
            f"&ontology_snapshot_id={uuid4()}"
            f"&category=lk"
        )
        assert resp.status_code == 200
        mock_metrics.assert_called_once()
        call_kwargs = mock_metrics.call_args
        assert call_kwargs[1]["category"] == "lk" or call_kwargs[0][2] == "lk" if len(call_kwargs[0]) > 2 else call_kwargs[1].get("category") == "lk"


# ---------------------------------------------------------------------------
# GET /prediction-sets/{set_id}/training-data.tsv
# ---------------------------------------------------------------------------


def _make_eval_set():
    es = MagicMock(spec=EvaluationSet)
    es.id = uuid4()
    es.old_annotation_set_id = uuid4()
    es.new_annotation_set_id = uuid4()
    return es


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

    @patch("protea.api.routers.scoring.compute_evaluation_data")
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
        with patch("protea.api.routers.scoring.session_scope", side_effect=lambda _: _mock_scope(session)):
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

    @patch("protea.api.routers.scoring.compute_evaluation_data")
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
        with patch("protea.api.routers.scoring.session_scope", side_effect=lambda _: _mock_scope(session)):
            with TestClient(app) as c:
                resp = c.get(self._url(ps.id, es.id, "nk"))

        lines = resp.text.strip().split("\n")
        header = lines[0].split("\t")
        row = lines[1].split("\t")
        label_idx = header.index("label")
        assert row[label_idx] == "0"

    @patch("protea.api.routers.scoring.compute_evaluation_data")
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
        with patch("protea.api.routers.scoring.session_scope", side_effect=lambda _: _mock_scope(session)):
            with TestClient(app) as c:
                resp = c.get(self._url(ps.id, es.id))

        header = resp.text.strip().split("\n")[0].split("\t")
        assert len(header) == 31
        assert header[0] == "protein_accession"
        assert header[3] == "label"
        assert header[-1] == "neighbor_distance_std"

    @patch("protea.api.routers.scoring.compute_evaluation_data")
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
        with patch("protea.api.routers.scoring.session_scope", side_effect=lambda _: _mock_scope(session)):
            with TestClient(app) as c:
                resp = c.get(self._url(ps.id, es.id, "pk"))

        lines = resp.text.strip().split("\n")
        header = lines[0].split("\t")
        row = lines[1].split("\t")
        label_idx = header.index("label")
        assert row[label_idx] == "1"

    @patch("protea.api.routers.scoring.compute_evaluation_data")
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
        with patch("protea.api.routers.scoring.session_scope", side_effect=lambda _: _mock_scope(session)):
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
    m.created_at = datetime(2026, 3, 18, tzinfo=timezone.utc)
    return m


class TestListRerankers:
    def test_empty_list(self, client, session):
        session.query.return_value.order_by.return_value.all.return_value = []
        resp = client.get("/scoring/rerankers")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_returns_rerankers(self, client, session):
        m = _make_reranker_model(name="my-model")
        session.query.return_value.order_by.return_value.all.return_value = [m]
        resp = client.get("/scoring/rerankers")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["name"] == "my-model"
        assert "metrics" in data[0]


class TestGetReranker:
    def test_found(self, client, session):
        m = _make_reranker_model(name="found")
        session.get.return_value = m
        resp = client.get(f"/scoring/rerankers/{m.id}")
        assert resp.status_code == 200
        assert resp.json()["name"] == "found"

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


class TestTrainReranker:
    def test_prediction_set_not_found(self, client, session):
        session.get.return_value = None
        session.query.return_value.filter.return_value.first.return_value = None
        resp = client.post("/scoring/rerankers/train", json={
            "name": "test",
            "prediction_set_id": str(uuid4()),
            "evaluation_set_id": str(uuid4()),
        })
        assert resp.status_code == 404
        assert "PredictionSet" in resp.json()["detail"]

    def test_evaluation_set_not_found(self, client, session):
        ps = _make_pred_set()
        session.query.return_value.filter.return_value.first.return_value = None

        def get_side(model, id_):
            if model is PredictionSet:
                return ps
            return None

        session.get.side_effect = get_side
        resp = client.post("/scoring/rerankers/train", json={
            "name": "test",
            "prediction_set_id": str(ps.id),
            "evaluation_set_id": str(uuid4()),
        })
        assert resp.status_code == 404
        assert "EvaluationSet" in resp.json()["detail"]

    def test_duplicate_name_returns_409(self, client, session):
        ps = _make_pred_set()
        es = _make_eval_set()

        def get_side(model, id_):
            if model is PredictionSet:
                return ps
            if model is EvaluationSet:
                return es
            return None

        session.get.side_effect = get_side
        session.query.return_value.filter.return_value.first.return_value = _make_reranker_model()

        resp = client.post("/scoring/rerankers/train", json={
            "name": "existing-name",
            "prediction_set_id": str(ps.id),
            "evaluation_set_id": str(es.id),
        })
        assert resp.status_code == 409

    def test_empty_predictions_returns_422(self, client, session):
        ps = _make_pred_set()
        es = _make_eval_set()

        def get_side(model, id_):
            if model is PredictionSet:
                return ps
            if model is EvaluationSet:
                return es
            return None

        session.get.side_effect = get_side
        session.query.return_value.filter.return_value.first.return_value = None

        eval_data = MagicMock()
        eval_data.nk = {}

        with patch("protea.api.routers.scoring.compute_evaluation_data", return_value=eval_data):
            # Empty result set
            session.query.return_value.join.return_value.filter.return_value.all.return_value = []
            resp = client.post("/scoring/rerankers/train", json={
                "name": "empty-test",
                "prediction_set_id": str(ps.id),
                "evaluation_set_id": str(es.id),
            })
        assert resp.status_code == 422

    def test_invalid_category_returns_422(self, client, session):
        resp = client.post("/scoring/rerankers/train", json={
            "name": "test",
            "prediction_set_id": str(uuid4()),
            "evaluation_set_id": str(uuid4()),
            "category": "invalid",
        })
        assert resp.status_code == 422


class TestRerankedTSV:
    def test_prediction_set_not_found(self, client, session):
        session.get.return_value = None
        resp = client.get(
            f"/scoring/prediction-sets/{uuid4()}/rerank.tsv"
            f"?reranker_id={uuid4()}"
        )
        assert resp.status_code == 404
        assert "PredictionSet" in resp.json()["detail"]

    def test_reranker_not_found(self, client, session):
        ps = _make_pred_set()

        def get_side(model, id_):
            if model is PredictionSet:
                return ps
            return None

        session.get.side_effect = get_side
        resp = client.get(
            f"/scoring/prediction-sets/{ps.id}/rerank.tsv"
            f"?reranker_id={uuid4()}"
        )
        assert resp.status_code == 404
        assert "RerankerModel" in resp.json()["detail"]


class TestRerankerMetrics:
    def _url(self, set_id, reranker_id, eval_set_id, category="nk"):
        return (
            f"/scoring/prediction-sets/{set_id}/reranker-metrics"
            f"?reranker_id={reranker_id}"
            f"&evaluation_set_id={eval_set_id}"
            f"&category={category}"
        )

    def test_prediction_set_not_found(self, client, session):
        session.get.return_value = None
        resp = client.get(self._url(uuid4(), uuid4(), uuid4()))
        assert resp.status_code == 404
        assert "PredictionSet" in resp.json()["detail"]

    def test_reranker_not_found(self, client, session):
        ps = _make_pred_set()

        def get_side(model, id_):
            if model is PredictionSet:
                return ps
            return None

        session.get.side_effect = get_side
        resp = client.get(self._url(ps.id, uuid4(), uuid4()))
        assert resp.status_code == 404
        assert "RerankerModel" in resp.json()["detail"]

    def test_evaluation_set_not_found(self, client, session):
        ps = _make_pred_set()
        rm = _make_reranker_model()

        call_count = 0
        def get_side(model, id_):
            nonlocal call_count
            call_count += 1
            if model is PredictionSet:
                return ps
            if model is RerankerModel:
                return rm
            return None

        session.get.side_effect = get_side
        resp = client.get(self._url(ps.id, rm.id, uuid4()))
        assert resp.status_code == 404
        assert "EvaluationSet" in resp.json()["detail"]

    def test_invalid_category_returns_422(self, client, session):
        resp = client.get(
            f"/scoring/prediction-sets/{uuid4()}/reranker-metrics"
            f"?reranker_id={uuid4()}"
            f"&evaluation_set_id={uuid4()}"
            f"&category=invalid"
        )
        assert resp.status_code == 422

    @patch("protea.api.routers.scoring.compute_cafa_metrics")
    @patch("protea.api.routers.scoring.reranker_predict")
    @patch("protea.api.routers.scoring.model_from_string")
    @patch("protea.api.routers.scoring.compute_evaluation_data")
    def test_returns_metrics(self, mock_eval, mock_from_str, mock_predict, mock_metrics, client, session):
        ps = _make_pred_set()
        rm = _make_reranker_model(name="test-rr")
        es = _make_eval_set()

        def get_side(model, id_):
            if model is PredictionSet:
                return ps
            if model is RerankerModel:
                return rm
            if model is EvaluationSet:
                return es
            return None

        session.get.side_effect = get_side
        mock_eval.return_value = MagicMock()

        pred = _make_go_prediction()
        session.query.return_value.join.return_value.filter.return_value.yield_per.return_value = [
            (pred, "GO:0003674"),
        ]

        import numpy as np
        mock_from_str.return_value = MagicMock()
        mock_predict.return_value = np.array([0.85])

        point = MagicMock()
        point.threshold = 0.5
        point.precision = 0.9
        point.recall = 0.8
        point.f1 = 0.85
        metrics_result = MagicMock()
        metrics_result.summary.return_value = {
            "category": "nk",
            "fmax": 0.85,
            "threshold_at_fmax": 0.5,
            "auc_pr": 0.78,
            "n_ground_truth_proteins": 10,
            "n_predicted_proteins": 8,
            "n_predictions": 1,
        }
        metrics_result.curve = [point]
        mock_metrics.return_value = metrics_result

        resp = client.get(self._url(ps.id, rm.id, es.id))
        assert resp.status_code == 200
        data = resp.json()
        assert data["prediction_set_id"] == str(ps.id)
        assert data["reranker_id"] == str(rm.id)
        assert data["reranker_name"] == "test-rr"
        assert "fmax" in data
        assert "curve" in data
        assert len(data["curve"]) == 1

    @patch("protea.api.routers.scoring.compute_evaluation_data")
    def test_empty_predictions_returns_zero_metrics(self, mock_eval, client, session):
        ps = _make_pred_set()
        rm = _make_reranker_model()
        es = _make_eval_set()

        def get_side(model, id_):
            if model is PredictionSet:
                return ps
            if model is RerankerModel:
                return rm
            if model is EvaluationSet:
                return es
            return None

        session.get.side_effect = get_side
        mock_eval.return_value = MagicMock()
        session.query.return_value.join.return_value.filter.return_value.yield_per.return_value = []

        resp = client.get(self._url(ps.id, rm.id, es.id))
        assert resp.status_code == 200
        data = resp.json()
        assert data["fmax"] == 0.0
        assert data["n_predictions"] == 0
