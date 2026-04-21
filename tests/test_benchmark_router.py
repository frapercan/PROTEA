"""Unit tests for the /benchmark router.

Covers the derived display metadata helper and the shapes of the two
endpoints (``/benchmark/embeddings`` and ``/benchmark/matrix``).  The
session is fully mocked.
"""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from protea.api.routers.benchmark import _describe_embedding, _stage_of, router

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_app(factory):
    app = FastAPI()
    app.state.session_factory = factory
    app.include_router(router)
    return app


@contextmanager
def _mock_scope(session):
    yield session


def _make_cfg(model_name="esmc_300m", backend="esm3c"):
    cfg = MagicMock()
    cfg.id = uuid4()
    cfg.model_name = model_name
    cfg.model_backend = backend
    cfg.description = None
    cfg.pooling = "mean"
    cfg.layer_agg = "mean"
    return cfg


def _make_eval(results, scoring_config_id=None, reranker_model_id=None):
    er = MagicMock()
    er.id = uuid4()
    er.evaluation_set_id = uuid4()
    er.scoring_config_id = scoring_config_id
    er.reranker_model_id = reranker_model_id
    er.results = results
    return er


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
        "protea.api.routers.benchmark.session_scope",
        side_effect=lambda _: _mock_scope(session),
    ):
        with TestClient(app) as c:
            yield c, session


# ---------------------------------------------------------------------------
# _describe_embedding
# ---------------------------------------------------------------------------


class TestDescribeEmbedding:
    def test_esmc_300m(self):
        meta = _describe_embedding("esmc_300m", "esm3c")
        assert meta["family"] == "esmc"
        assert meta["display_name"] == "ESMC-300M"
        assert meta["param_count"] == 300_000_000

    def test_esmc_600m(self):
        meta = _describe_embedding("esmc_600m", "esm3c")
        assert meta["display_name"] == "ESMC-600M"
        assert meta["param_count"] == 600_000_000

    def test_prost_before_prott5(self):
        # ProstT5 must match "prostt5" branch first, not "prot_t5"
        meta = _describe_embedding("Rostlab/ProstT5_fp16", "t5")
        assert meta["family"] == "prostt5"
        assert meta["display_name"] == "ProstT5-XL"

    def test_prott5_xl(self):
        meta = _describe_embedding("Rostlab/prot_t5_xl_uniref50", "t5")
        assert meta["family"] == "prot_t5"
        assert meta["display_name"] == "ProtT5-XL"
        assert meta["param_count"] == 3_000_000_000

    def test_ankh_base(self):
        meta = _describe_embedding("ElnaggarLab/ankh-base", "ankh")
        assert meta["family"] == "ankh"
        assert meta["display_name"] == "Ankh-base"

    def test_ankh_large(self):
        meta = _describe_embedding("ElnaggarLab/ankh-large", "ankh")
        assert meta["display_name"] == "Ankh-large"
        assert meta["param_count"] == 1_900_000_000

    def test_esm2_650m(self):
        meta = _describe_embedding("facebook/esm2_t33_650M_UR50D", "esm")
        assert meta["family"] == "esm2"
        assert meta["display_name"] == "ESM2-650M"

    def test_esm2_3b(self):
        meta = _describe_embedding("facebook/esm2_t36_3B_UR50D", "esm")
        assert meta["display_name"] == "ESM2-3B"
        assert meta["param_count"] == 3_000_000_000

    def test_unknown_falls_back_to_raw(self):
        meta = _describe_embedding("some-weird-model", "")
        assert meta["display_name"] == "some-weird-model"
        assert meta["param_count"] is None


# ---------------------------------------------------------------------------
# _stage_of
# ---------------------------------------------------------------------------


class TestStageOf:
    def test_baseline(self):
        er = _make_eval({})
        assert _stage_of(er) == "baseline"

    def test_alignment_weighted(self):
        er = _make_eval({}, scoring_config_id=uuid4())
        assert _stage_of(er) == "alignment_weighted"

    def test_reranker_takes_precedence(self):
        er = _make_eval({}, scoring_config_id=uuid4(), reranker_model_id=uuid4())
        assert _stage_of(er) == "reranker"


# ---------------------------------------------------------------------------
# GET /benchmark/embeddings
# ---------------------------------------------------------------------------


class TestListBenchmarkEmbeddings:
    def test_empty_database(self, client):
        c, session = client
        exec_result = MagicMock()
        exec_result.scalars.return_value.all.return_value = []
        session.execute.return_value = exec_result

        resp = c.get("/benchmark/embeddings")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 0
        assert data["embeddings"] == []

    def test_enriches_with_derived_metadata(self, client):
        c, session = client
        cfgs = [
            _make_cfg("esmc_300m", "esm3c"),
            _make_cfg("facebook/esm2_t33_650M_UR50D", "esm"),
            _make_cfg("ElnaggarLab/ankh-base", "ankh"),
        ]
        exec_result = MagicMock()
        exec_result.scalars.return_value.all.return_value = cfgs
        session.execute.return_value = exec_result

        resp = c.get("/benchmark/embeddings")
        data = resp.json()
        assert data["total"] == 3
        names = [e["display_name"] for e in data["embeddings"]]
        assert "ESMC-300M" in names
        assert "ESM2-650M" in names
        assert "Ankh-base" in names
        # Every enriched entry must carry a family tag
        for e in data["embeddings"]:
            assert e["family"]
            assert "param_count" in e
            assert "model_name" in e
            assert "model_backend" in e


# ---------------------------------------------------------------------------
# GET /benchmark/matrix
# ---------------------------------------------------------------------------


class TestBenchmarkMatrix:
    def test_empty_database(self, client):
        c, session = client
        exec_result = MagicMock()
        exec_result.all.return_value = []
        session.execute.return_value = exec_result

        resp = c.get("/benchmark/matrix")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 0
        assert data["rows"] == []
        assert data["evaluation_set_ids"] == []
        assert data["embedding_config_ids"] == []
        assert set(data["stages"]) == {"baseline", "alignment_weighted", "reranker"}

    def test_row_per_cell_and_deduplication(self, client):
        c, session = client
        embedding_id = uuid4()

        # Two eval results for the same (embedding, eval_set, stage, cat, asp):
        # the higher Fmax must win.
        er_low = _make_eval({"NK": {"BPO": {"fmax": 0.4}}})
        er_high = _make_eval({"NK": {"BPO": {"fmax": 0.55}}})
        # Pin the same eval_set_id so the dedup key collides
        eval_set_id = uuid4()
        er_low.evaluation_set_id = eval_set_id
        er_high.evaluation_set_id = eval_set_id

        exec_result = MagicMock()
        exec_result.all.return_value = [
            (er_low, embedding_id),
            (er_high, embedding_id),
        ]
        session.execute.return_value = exec_result

        resp = c.get("/benchmark/matrix")
        data = resp.json()
        assert data["total"] == 1
        row = data["rows"][0]
        assert row["fmax"] == 0.55
        assert row["category"] == "NK"
        assert row["aspect"] == "BPO"
        assert row["stage"] == "baseline"
        assert str(embedding_id) in data["embedding_config_ids"]
        assert str(eval_set_id) in data["evaluation_set_ids"]

    def test_stage_filter(self, client):
        c, session = client
        embedding_id = uuid4()

        er_baseline = _make_eval({"NK": {"BPO": {"fmax": 0.4}}})
        er_reranker = _make_eval(
            {"NK": {"BPO": {"fmax": 0.6}}},
            reranker_model_id=uuid4(),
        )

        exec_result = MagicMock()
        exec_result.all.return_value = [
            (er_baseline, embedding_id),
            (er_reranker, embedding_id),
        ]
        session.execute.return_value = exec_result

        resp = c.get("/benchmark/matrix?stage=reranker")
        data = resp.json()
        assert data["total"] == 1
        assert data["rows"][0]["stage"] == "reranker"
        assert data["rows"][0]["fmax"] == 0.6
        assert data["filters"]["stage"] == "reranker"

    def test_invalid_stage_filter_rejected(self, client):
        c, _ = client
        resp = c.get("/benchmark/matrix?stage=invalid")
        assert resp.status_code == 422

    def test_missing_fmax_cells_are_skipped(self, client):
        c, session = client
        er = _make_eval(
            {
                "NK": {"BPO": {"fmax": None}, "MFO": {"fmax": 0.4}},
                "LK": {"CCO": {}},
            }
        )
        exec_result = MagicMock()
        exec_result.all.return_value = [(er, uuid4())]
        session.execute.return_value = exec_result

        resp = c.get("/benchmark/matrix")
        data = resp.json()
        # Only NK/MFO has a real fmax → exactly one row
        assert data["total"] == 1
        assert data["rows"][0]["category"] == "NK"
        assert data["rows"][0]["aspect"] == "MFO"
