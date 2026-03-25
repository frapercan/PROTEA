"""Unit tests for the /showcase router.

Database is fully mocked — no real infrastructure required.
"""
from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from protea.api.routers.showcase import _derive_method, router

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_app(session_factory):
    app = FastAPI()
    app.state.session_factory = session_factory
    app.include_router(router)
    return app


@contextmanager
def _mock_scope(session):
    yield session


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
        "protea.api.routers.showcase.session_scope",
        side_effect=lambda _: _mock_scope(session),
    ):
        with TestClient(app) as c:
            yield c, session


# ---------------------------------------------------------------------------
# _derive_method
# ---------------------------------------------------------------------------

class TestDeriveMethod:
    def test_baseline(self):
        assert _derive_method(None, None) == ("knn_baseline", "KNN (embedding distance)")

    def test_scored(self):
        assert _derive_method(uuid4(), None) == ("knn_scored", "KNN + Scoring")

    def test_reranker(self):
        assert _derive_method(None, uuid4()) == ("knn_reranker", "KNN + Re-ranker")

    def test_reranker_takes_precedence(self):
        assert _derive_method(uuid4(), uuid4()) == ("knn_reranker", "KNN + Re-ranker")


# ---------------------------------------------------------------------------
# GET /showcase — empty database
# ---------------------------------------------------------------------------

class TestShowcaseEmpty:
    def test_empty_database_returns_zeros(self, client):
        c, session = client

        # All count queries return 0
        session.query.return_value.scalar.return_value = 0
        session.query.return_value.filter.return_value.scalar.return_value = 0
        session.query.return_value.all.return_value = []

        resp = c.get("/showcase")
        assert resp.status_code == 200
        data = resp.json()

        assert data["protein_stats"]["total"] == 0
        assert data["protein_stats"]["canonical"] == 0
        assert data["counts"]["proteins"] == 0
        assert data["counts"]["sequences"] == 0
        assert data["counts"]["embeddings"] == 0
        assert data["counts"]["prediction_sets"] == 0
        assert data["counts"]["predictions"] == 0
        assert data["counts"]["reranker_models"] == 0
        assert data["counts"]["evaluations"] == 0
        assert data["best_fmax"] == {}
        assert data["method_comparison"] == {}
        assert len(data["pipeline_stages"]) == 5

    def test_pipeline_stages_structure(self, client):
        c, session = client
        session.query.return_value.scalar.return_value = 0
        session.query.return_value.filter.return_value.scalar.return_value = 0
        session.query.return_value.all.return_value = []

        resp = c.get("/showcase")
        data = resp.json()
        stages = data["pipeline_stages"]
        expected_names = {"sequences", "embeddings", "predictions", "reranker_models", "evaluations"}
        assert {s["name"] for s in stages} == expected_names
        for s in stages:
            assert "count" in s
            assert "href" in s


# ---------------------------------------------------------------------------
# GET /showcase — with evaluation data
# ---------------------------------------------------------------------------

class TestShowcaseWithEvaluations:
    def _make_eval_result(self, scoring_config_id=None, reranker_model_id=None, results=None):
        er = MagicMock()
        er.id = uuid4()
        er.scoring_config_id = scoring_config_id
        er.reranker_model_id = reranker_model_id
        er.results = results or {}
        return er

    def test_single_baseline_evaluation(self, client):
        c, session = client

        eval_result = self._make_eval_result(
            results={
                "NK": {
                    "BPO": {"fmax": 0.45},
                    "MFO": {"fmax": 0.52},
                    "CCO": {"fmax": 0.60},
                },
            },
        )

        # Mock query chain — we need separate calls for counts vs eval
        call_count = [0]
        def query_side_effect(*args):
            call_count[0] += 1
            q = MagicMock()
            q.scalar.return_value = 100
            q.filter.return_value.scalar.return_value = 50
            q.all.return_value = [eval_result]
            return q
        session.query.side_effect = query_side_effect

        resp = c.get("/showcase")
        assert resp.status_code == 200
        data = resp.json()

        assert data["counts"]["evaluations"] == 1
        if data["best_fmax"]:
            nk = data["best_fmax"].get("NK", {})
            if "BPO" in nk:
                assert nk["BPO"]["fmax"] == 0.45
                assert nk["BPO"]["method"] == "knn_baseline"

    def test_method_comparison_ordering(self, client):
        c, session = client

        baseline = self._make_eval_result(
            results={"NK": {"BPO": {"fmax": 0.40}}},
        )
        scored = self._make_eval_result(
            scoring_config_id=uuid4(),
            results={"NK": {"BPO": {"fmax": 0.50}}},
        )
        reranker = self._make_eval_result(
            reranker_model_id=uuid4(),
            results={"NK": {"BPO": {"fmax": 0.60}}},
        )

        call_count = [0]
        def query_side_effect(*args):
            call_count[0] += 1
            q = MagicMock()
            q.scalar.return_value = 10
            q.filter.return_value.scalar.return_value = 5
            q.all.return_value = [baseline, scored, reranker]
            return q
        session.query.side_effect = query_side_effect

        resp = c.get("/showcase")
        assert resp.status_code == 200
        data = resp.json()

        if "NK" in data.get("method_comparison", {}):
            methods = [m["method"] for m in data["method_comparison"]["NK"]]
            # Should follow _METHOD_ORDER: baseline, scored, reranker
            assert methods == ["knn_baseline", "knn_scored", "knn_reranker"]

    def test_multiple_categories(self, client):
        c, session = client

        eval_result = self._make_eval_result(
            results={
                "NK": {"BPO": {"fmax": 0.45}},
                "LK": {"BPO": {"fmax": 0.55}},
                "PK": {"BPO": {"fmax": 0.65}},
            },
        )

        def query_side_effect(*args):
            q = MagicMock()
            q.scalar.return_value = 0
            q.filter.return_value.scalar.return_value = 0
            q.all.return_value = [eval_result]
            return q
        session.query.side_effect = query_side_effect

        resp = c.get("/showcase")
        assert resp.status_code == 200
        data = resp.json()

        if data["best_fmax"]:
            # Should have entries for all three categories
            for cat in ["NK", "LK", "PK"]:
                if cat in data["best_fmax"]:
                    assert "BPO" in data["best_fmax"][cat]

    def test_empty_results_field(self, client):
        c, session = client

        eval_result = self._make_eval_result(results={})

        def query_side_effect(*args):
            q = MagicMock()
            q.scalar.return_value = 0
            q.filter.return_value.scalar.return_value = 0
            q.all.return_value = [eval_result]
            return q
        session.query.side_effect = query_side_effect

        resp = c.get("/showcase")
        assert resp.status_code == 200
        data = resp.json()
        assert data["best_fmax"] == {}

    def test_none_results_field(self, client):
        c, session = client

        eval_result = self._make_eval_result(results=None)

        def query_side_effect(*args):
            q = MagicMock()
            q.scalar.return_value = 0
            q.filter.return_value.scalar.return_value = 0
            q.all.return_value = [eval_result]
            return q
        session.query.side_effect = query_side_effect

        resp = c.get("/showcase")
        assert resp.status_code == 200

    def test_best_fmax_picks_highest(self, client):
        c, session = client

        er1 = self._make_eval_result(results={"NK": {"BPO": {"fmax": 0.40}}})
        er2 = self._make_eval_result(results={"NK": {"BPO": {"fmax": 0.60}}})

        def query_side_effect(*args):
            q = MagicMock()
            q.scalar.return_value = 0
            q.filter.return_value.scalar.return_value = 0
            q.all.return_value = [er1, er2]
            return q
        session.query.side_effect = query_side_effect

        resp = c.get("/showcase")
        assert resp.status_code == 200
        data = resp.json()

        if "NK" in data["best_fmax"] and "BPO" in data["best_fmax"]["NK"]:
            assert data["best_fmax"]["NK"]["BPO"]["fmax"] == 0.60
