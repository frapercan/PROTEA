"""Unit tests for the /proteins/stats analytical endpoints.

Database is fully mocked. Verifies wiring, response shape, cache hit /
serve-stale-on-error behaviour, and a perf smoke that the cached path
returns in under 5s on the fixture data (the slow path is the DB query;
the cache hit must be near-instant).
"""

from __future__ import annotations

import time
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from protea.api.cache import invalidate as _cache_invalidate
from protea.api.routers.proteins_stats import (
    CACHE_KEY_DATASETS,
    CACHE_KEY_GO_DENSITY,
    CACHE_KEY_GO_MATRIX,
    CACHE_KEY_PIPELINE,
    CACHE_KEY_PLM,
    CACHE_KEY_SEQ_LEN,
    CACHE_KEY_TAXONOMY,
    prewarm_all,
    router,
)


@pytest.fixture(autouse=True)
def _reset_cache():
    _cache_invalidate()
    yield
    _cache_invalidate()


def _make_app(factory):
    app = FastAPI()
    app.state.session_factory = factory
    app.include_router(router)
    return app


@contextmanager
def _mock_scope(session):
    yield session


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
        "protea.api.routers.proteins_stats.session_scope",
        side_effect=lambda _: _mock_scope(session),
    ):
        with TestClient(app) as c:
            yield c, session


# ── Embeddings by PLM ────────────────────────────────────────────────────────


class TestEmbeddingsByPlm:
    def test_returns_per_plm_coverage(self, client):
        c, session = client
        # First .scalar() call = total proteins.
        total_q = MagicMock()
        total_q.filter.return_value = total_q
        total_q.scalar.return_value = 1000
        # Second branch = the GROUP BY.
        group_q = MagicMock()
        group_q.join.return_value = group_q
        group_q.group_by.return_value = group_q
        group_q.order_by.return_value = group_q
        group_q.all.return_value = [
            ("cfg-1", "esm2_t36_3B_UR50D", "ESM2 3B", "esm2", 3_000_000_000, 800),
            ("cfg-2", "prot_t5_xl_uniref50", "ProtT5", "t5", 1_500_000_000, 600),
        ]
        session.query.side_effect = [total_q, group_q]

        resp = c.get("/proteins/stats/embeddings-by-plm")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_proteins"] == 1000
        assert len(data["items"]) == 2
        assert data["items"][0]["coverage_pct"] == 80.0
        assert data["items"][1]["coverage_pct"] == 60.0

    def test_zero_total(self, client):
        c, session = client
        total_q = MagicMock()
        total_q.filter.return_value = total_q
        total_q.scalar.return_value = 0
        group_q = MagicMock()
        group_q.join.return_value = group_q
        group_q.group_by.return_value = group_q
        group_q.order_by.return_value = group_q
        group_q.all.return_value = []
        session.query.side_effect = [total_q, group_q]

        resp = c.get("/proteins/stats/embeddings-by-plm")
        assert resp.status_code == 200
        assert resp.json()["total_proteins"] == 0


# ── GO matrix ────────────────────────────────────────────────────────────────


class TestGoMatrix:
    def test_returns_grouped_counts(self, client):
        c, session = client
        q = MagicMock()
        session.query.return_value = q
        q.join.return_value = q
        q.group_by.return_value = q
        q.order_by.return_value = q
        q.all.return_value = [
            ("goa", "2024-01", "F", True, 1200),
            ("goa", "2024-01", "P", False, 900),
            ("quickgo", "2024-06", "C", True, 300),
        ]
        resp = c.get("/proteins/stats/go-annotations-matrix")
        assert resp.status_code == 200
        items = resp.json()["items"]
        assert len(items) == 3
        assert items[0]["source"] == "goa"
        assert items[0]["experimental"] is True

    def test_empty(self, client):
        c, session = client
        q = MagicMock()
        session.query.return_value = q
        q.join.return_value = q
        q.group_by.return_value = q
        q.order_by.return_value = q
        q.all.return_value = []
        resp = c.get("/proteins/stats/go-annotations-matrix")
        assert resp.status_code == 200
        assert resp.json()["items"] == []


# ── Taxonomy ─────────────────────────────────────────────────────────────────


class TestTaxonomyTop:
    def test_returns_organisms(self, client):
        c, session = client
        q = MagicMock()
        session.query.return_value = q
        q.filter.return_value = q
        q.group_by.return_value = q
        q.order_by.return_value = q
        q.limit.return_value = q
        q.all.return_value = [
            ("Homo sapiens", "9606", 20000),
            ("Mus musculus", "10090", 15000),
        ]
        resp = c.get("/proteins/stats/taxonomy-top-20")
        assert resp.status_code == 200
        items = resp.json()["items"]
        assert len(items) == 2
        assert items[0]["organism"] == "Homo sapiens"
        assert items[0]["protein_count"] == 20000


# ── Sequence length ──────────────────────────────────────────────────────────


class TestSequenceLength:
    def test_returns_percentiles_and_bins(self, client):
        c, session = client
        # count() call
        base_q = MagicMock()
        base_q.filter.return_value = base_q
        base_q.count.return_value = 1000
        # percentile_cont query (chained .filter)
        pct_q = MagicMock()
        pct_q.filter.return_value = pct_q
        pct_q.one.return_value = (50, 100.0, 350.0, 800.0, 1500.0, 4000)
        # histogram bucket query
        hist_q = MagicMock()
        hist_q.filter.return_value = hist_q
        hist_q.group_by.return_value = hist_q
        hist_q.order_by.return_value = hist_q
        hist_q.all.return_value = [(1, 100), (2, 200), (3, 700)]
        session.query.side_effect = [base_q, pct_q, hist_q]

        resp = c.get("/proteins/stats/sequence-length")
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 1000
        assert data["percentiles"]["p50"] == 350.0
        assert len(data["bins"]) == 3

    def test_empty_returns_zero(self, client):
        c, session = client
        base_q = MagicMock()
        base_q.filter.return_value = base_q
        base_q.count.return_value = 0
        session.query.return_value = base_q

        resp = c.get("/proteins/stats/sequence-length")
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 0
        assert data["bins"] == []


# ── GO density ───────────────────────────────────────────────────────────────


class TestGoDensity:
    def test_returns_density_per_aspect(self, client):
        c, _session = client
        # Subquery composition (sub.c.X + percentile_cont) is hostile to
        # MagicMock; patch the compute helper directly so we exercise the
        # route wiring + response shape without simulating SQLAlchemy
        # internals.
        payload = {
            "items": [
                {"aspect": "MFO", "protein_count": 500, "avg": 4.2, "p50": 3.0, "p90": 10.0, "max": 50},
                {"aspect": "BPO", "protein_count": 400, "avg": 6.1, "p50": 5.0, "p90": 15.0, "max": 80},
                {"aspect": "CCO", "protein_count": 300, "avg": 2.5, "p50": 2.0, "p90": 6.0, "max": 20},
            ]
        }
        with patch(
            "protea.api.routers.proteins_stats._compute_go_density",
            return_value=payload,
        ):
            resp = c.get("/proteins/stats/go-density")
        assert resp.status_code == 200
        items = resp.json()["items"]
        assert {item["aspect"] for item in items} == {"MFO", "BPO", "CCO"}


# ── Pipeline activity ────────────────────────────────────────────────────────


class TestPipelineActivity:
    def test_returns_daily_and_last_24h(self, client):
        c, session = client
        from datetime import UTC, datetime

        daily_q = MagicMock()
        daily_q.filter.return_value = daily_q
        daily_q.group_by.return_value = daily_q
        daily_q.order_by.return_value = daily_q
        daily_q.all.return_value = [
            ("compute_embeddings", datetime(2026, 5, 20, tzinfo=UTC), 5),
            ("predict_go_terms", datetime(2026, 5, 21, tzinfo=UTC), 3),
        ]
        last_24h_q = MagicMock()
        last_24h_q.filter.return_value = last_24h_q
        last_24h_q.group_by.return_value = last_24h_q
        last_24h_q.all.return_value = [("compute_embeddings", 2)]
        session.query.side_effect = [daily_q, last_24h_q]

        resp = c.get("/proteins/stats/pipeline-activity")
        assert resp.status_code == 200
        data = resp.json()
        assert data["window_days"] == 30
        assert len(data["items"]) == 2
        assert len(data["last_24h"]) == 1


# ── Dataset registry ─────────────────────────────────────────────────────────


class TestDatasetRegistry:
    def test_returns_per_k_pair(self, client):
        c, session = client
        q = MagicMock()
        session.query.return_value = q
        q.group_by.return_value = q
        q.order_by.return_value = q
        q.all.return_value = [
            (5, "v226-lineage", 8, 1_000_000, 50_000),
            (10, "v226-lineage", 8, 2_000_000, 100_000),
        ]
        resp = c.get("/proteins/stats/dataset-registry")
        assert resp.status_code == 200
        items = resp.json()["items"]
        assert items[0]["k"] == 5
        assert items[0]["n_train_rows"] == 1_000_000
        assert items[1]["k"] == 10
        assert items[1]["dataset_count"] == 8


# ── Cache behaviour ──────────────────────────────────────────────────────────


class TestCache:
    def test_serves_stale_on_error(self, client):
        """Reproduces the proteins:stats stale-serve test for one section."""
        from protea.api.cache import _store as cache_store

        c, session = client
        cache_store[CACHE_KEY_TAXONOMY] = (
            0.0,
            {"items": [{"organism": "stale", "taxonomy_id": "0", "protein_count": 1}]},
        )
        session.query.side_effect = RuntimeError("DB blip")
        resp = c.get("/proteins/stats/taxonomy-top-20")
        assert resp.status_code == 200
        assert resp.json()["items"][0]["organism"] == "stale"

    def test_cache_hit_returns_under_5s(self, client):
        """Perf smoke: a warm cache must serve in under 5s even when the DB
        producer would otherwise sleep for hours. Validates that the cache
        short-circuit fires before the producer is invoked.
        """
        import time as _t

        from protea.api.cache import _store as cache_store

        c, session_obj = client
        cache_store[CACHE_KEY_PLM] = (
            _t.monotonic() + 600.0,
            {"total_proteins": 0, "items": []},
        )
        session_obj.query = MagicMock(side_effect=AssertionError("producer must not run"))

        start = _t.monotonic()
        resp = c.get("/proteins/stats/embeddings-by-plm")
        elapsed = _t.monotonic() - start
        assert resp.status_code == 200
        assert elapsed < 5.0


# ── Prewarm ─────────────────────────────────────────────────────────────────


class TestPrewarmAll:
    def test_prewarm_populates_every_cache_key(self):
        """``prewarm_all`` must populate every key registered in ``_SECTIONS``.

        Mocks the compute helpers so the prewarm runs without a real DB and
        every cache slot ends up pointing at its mocked payload.
        """
        from protea.api import cache as _cache_mod

        with (
            patch("protea.api.routers.proteins_stats._compute_embeddings_by_plm", return_value={"k": "plm"}),
            patch("protea.api.routers.proteins_stats._compute_go_matrix", return_value={"k": "gom"}),
            patch("protea.api.routers.proteins_stats._compute_taxonomy_top", return_value={"k": "tax"}),
            patch("protea.api.routers.proteins_stats._compute_sequence_length", return_value={"k": "sl"}),
            patch("protea.api.routers.proteins_stats._compute_go_density", return_value={"k": "gd"}),
            patch("protea.api.routers.proteins_stats._compute_pipeline_activity", return_value={"k": "pa"}),
            patch("protea.api.routers.proteins_stats._compute_dataset_registry", return_value={"k": "ds"}),
        ):
            prewarm_all(MagicMock())

        for key in (
            CACHE_KEY_PLM,
            CACHE_KEY_GO_MATRIX,
            CACHE_KEY_TAXONOMY,
            CACHE_KEY_SEQ_LEN,
            CACHE_KEY_GO_DENSITY,
            CACHE_KEY_PIPELINE,
            CACHE_KEY_DATASETS,
        ):
            assert key in _cache_mod._store


# Silence pyflakes unused-import warnings for symbols only referenced in
# the body of test classes via patch() string literals.
_ = (time,)
