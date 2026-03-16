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
        from unittest.mock import call
        from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
        # First get (PredictionSet) found, second (ScoringConfig) not found
        session.get.side_effect = [MagicMock(), None]
        resp = client.get(
            f"/scoring/prediction-sets/{uuid4()}/score.tsv"
            f"?scoring_config_id={uuid4()}"
        )
        assert resp.status_code == 404


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
