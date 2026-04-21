"""Unit tests for the /showcase router.

The router was rewritten 2026-04-10 to emit a single attributed "best" result
instead of the previous three-bucket ``method_comparison`` table.  These tests
cover the new shape: empty state, single-best selection, embedding attribution,
and pipeline-stage counts.

Database is fully mocked — no real infrastructure required.
"""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from protea.api.routers.showcase import _avg_fmax, _flatten_cells, router

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


def _make_eval(results, scoring_config_id=None, reranker_model_id=None):
    er = MagicMock()
    er.id = uuid4()
    er.evaluation_set_id = uuid4()
    er.scoring_config_id = scoring_config_id
    er.reranker_model_id = reranker_model_id
    er.results = results
    return er


def _make_cfg(model_name="esmc_300m", backend="esm3c"):
    cfg = MagicMock()
    cfg.id = uuid4()
    cfg.model_name = model_name
    cfg.model_backend = backend
    return cfg


def _install_mock(session, *, scalar_values, eval_rows):
    """Wire up ``session.scalar`` and ``session.execute`` mocks.

    ``scalar_values`` is a list consumed in order by each ``session.scalar()``
    call made by the router (one per count query, in the order they appear in
    ``get_showcase``).  ``eval_rows`` is the sequence yielded by the single
    ``session.execute(...)`` call for the join of ``EvaluationResult`` ×
    ``EmbeddingConfig``.
    """
    scalars = iter(scalar_values)
    session.scalar.side_effect = lambda *a, **kw: next(scalars)

    exec_result = MagicMock()
    exec_result.all.return_value = eval_rows
    session.execute.return_value = exec_result


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
# Pure helpers
# ---------------------------------------------------------------------------


class TestAvgFmax:
    def test_empty_results(self):
        assert _avg_fmax({}) is None

    def test_all_cells_populated(self):
        results = {
            "NK": {"BPO": {"fmax": 0.3}, "MFO": {"fmax": 0.5}, "CCO": {"fmax": 0.4}},
            "LK": {"BPO": {"fmax": 0.6}, "MFO": {"fmax": 0.7}, "CCO": {"fmax": 0.5}},
            "PK": {"BPO": {"fmax": 0.8}, "MFO": {"fmax": 0.9}, "CCO": {"fmax": 0.7}},
        }
        avg = _avg_fmax(results)
        assert avg is not None
        assert round(avg, 4) == round(sum([0.3, 0.5, 0.4, 0.6, 0.7, 0.5, 0.8, 0.9, 0.7]) / 9, 4)

    def test_missing_cells_are_ignored(self):
        # Only 2 out of 9 cells populated — should average those two
        results = {
            "NK": {"BPO": {"fmax": 0.4}},
            "LK": {"BPO": {"fmax": 0.6}},
        }
        assert _avg_fmax(results) == pytest.approx(0.5)

    def test_none_fmax_cells_are_ignored(self):
        results = {"NK": {"BPO": {"fmax": None}, "MFO": {"fmax": 0.42}}}
        assert _avg_fmax(results) == pytest.approx(0.42)


class TestFlattenCells:
    def test_flatten_skips_missing(self):
        results = {
            "NK": {"BPO": {"fmax": 0.4, "precision": 0.5, "recall": 0.3}},
            "LK": {"MFO": {"fmax": 0.6}},
        }
        flat = _flatten_cells(results)
        assert len(flat) == 2
        nk_bpo = next(c for c in flat if c["category"] == "NK" and c["aspect"] == "BPO")
        assert nk_bpo["fmax"] == 0.4
        assert nk_bpo["precision"] == 0.5
        assert nk_bpo["recall"] == 0.3

    def test_none_precision_recall_preserved(self):
        results = {"NK": {"BPO": {"fmax": 0.5}}}
        flat = _flatten_cells(results)
        assert flat[0]["precision"] is None
        assert flat[0]["recall"] is None


# ---------------------------------------------------------------------------
# GET /showcase — empty database
# ---------------------------------------------------------------------------


class TestShowcaseEmpty:
    def test_empty_database_returns_zero_counts_and_null_best(self, client):
        c, session = client
        # 7 scalar calls in get_showcase: total_proteins, canonical_proteins,
        # sequences, embeddings, prediction_sets, predictions, rerankers
        _install_mock(session, scalar_values=[0] * 7, eval_rows=[])

        resp = c.get("/showcase")
        assert resp.status_code == 200
        data = resp.json()

        assert data["best"] is None
        assert data["counts"]["proteins"] == 0
        assert data["counts"]["evaluations"] == 0
        assert len(data["pipeline_stages"]) == 5
        assert {s["name"] for s in data["pipeline_stages"]} == {
            "sequences",
            "embeddings",
            "predictions",
            "reranker_models",
            "evaluations",
        }

    def test_empty_state_still_reports_stage_hrefs(self, client):
        c, session = client
        _install_mock(session, scalar_values=[0] * 7, eval_rows=[])

        resp = c.get("/showcase")
        data = resp.json()
        for s in data["pipeline_stages"]:
            assert "href" in s
            assert s["count"] == 0


# ---------------------------------------------------------------------------
# GET /showcase — with evaluation data
# ---------------------------------------------------------------------------


class TestShowcaseBestSelection:
    def test_single_evaluation_becomes_the_best(self, client):
        c, session = client
        cfg = _make_cfg(model_name="esmc_300m", backend="esm3c")
        er = _make_eval(results={"NK": {"BPO": {"fmax": 0.5, "precision": 0.6, "recall": 0.4}}})

        _install_mock(session, scalar_values=[10, 5, 4, 8, 1, 100, 3], eval_rows=[(er, cfg)])

        resp = c.get("/showcase")
        assert resp.status_code == 200
        data = resp.json()

        assert data["counts"]["evaluations"] == 1
        assert data["best"] is not None
        assert data["best"]["avg_fmax"] == 0.5
        assert data["best"]["stage"] == "baseline"
        assert data["best"]["embedding"]["display_name"] == "ESMC-300M"
        assert data["best"]["embedding"]["family"] == "esmc"
        assert len(data["best"]["per_cell"]) == 1

    def test_best_picks_highest_avg_fmax(self, client):
        c, session = client
        cfg1 = _make_cfg("esmc_300m", "esm3c")
        cfg2 = _make_cfg("Rostlab/ProstT5", "t5")

        # er1 averages 0.40, er2 averages 0.60 → er2 must win
        er1 = _make_eval(results={"NK": {"BPO": {"fmax": 0.40}}})
        er2 = _make_eval(
            results={"NK": {"BPO": {"fmax": 0.60}}},
            reranker_model_id=uuid4(),
        )

        _install_mock(
            session,
            scalar_values=[0] * 7,
            eval_rows=[(er1, cfg1), (er2, cfg2)],
        )

        resp = c.get("/showcase")
        data = resp.json()
        assert data["best"]["avg_fmax"] == 0.6
        assert data["best"]["stage"] == "reranker"
        assert data["best"]["embedding"]["display_name"] == "ProstT5-XL"

    def test_eval_result_with_empty_results_blob_is_skipped(self, client):
        c, session = client
        cfg = _make_cfg()
        # er1 has no fmax values at all (empty dict) — must be skipped for best
        # er2 has one cell — becomes the best
        er1 = _make_eval(results={})
        er2 = _make_eval(results={"PK": {"MFO": {"fmax": 0.33}}})

        _install_mock(
            session,
            scalar_values=[0] * 7,
            eval_rows=[(er1, cfg), (er2, cfg)],
        )

        resp = c.get("/showcase")
        data = resp.json()
        # total_evaluations counts both rows (it's len(rows)), but best came from er2
        assert data["counts"]["evaluations"] == 2
        assert data["best"] is not None
        assert data["best"]["avg_fmax"] == 0.33

    def test_all_empty_results_leaves_best_null(self, client):
        c, session = client
        cfg = _make_cfg()
        er = _make_eval(results={})
        _install_mock(session, scalar_values=[0] * 7, eval_rows=[(er, cfg)])

        resp = c.get("/showcase")
        data = resp.json()
        assert data["counts"]["evaluations"] == 1
        assert data["best"] is None
