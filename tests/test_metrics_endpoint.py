"""Tests for the Prometheus /v1/metrics endpoint (T5.2).

These tests exercise the metric-registry construction in
``protea.infrastructure.telemetry`` and the router in
``protea.api.routers.metrics``. They do NOT spin up a Postgres engine
or Prometheus scraper; the SQLAlchemy session factory is stubbed and
``prometheus_client`` is hit for real (it is pure-Python and ships
with the main install).
"""

from __future__ import annotations

import sys
from typing import Any
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from protea.api.routers.metrics import router as metrics_router
from protea.infrastructure.telemetry import (
    MetricRegistry,
    build_metric_registry,
    refresh_db_pool_gauge,
    render_metrics,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_app(metrics: MetricRegistry | None, *, engine: Any | None = None) -> FastAPI:
    """Build a minimal FastAPI app wired with the metrics router.

    ``engine`` is exposed via a stub sessionmaker so the router can
    read ``factory.kw['bind']`` exactly like the real session factory
    in :mod:`protea.infrastructure.session`.
    """
    app = FastAPI()
    app.state.metrics = metrics
    factory = MagicMock()
    factory.kw = {"bind": engine} if engine is not None else {}
    app.state.session_factory = factory
    app.include_router(metrics_router)
    return app


# ---------------------------------------------------------------------------
# build_metric_registry
# ---------------------------------------------------------------------------


class TestBuildMetricRegistry:
    def test_returns_registry_with_baseline_metrics(self) -> None:
        metrics = build_metric_registry()
        assert metrics is not None
        # Render the payload and assert every baseline metric name shows
        # up at least once. This sidesteps the private _names_to_collectors
        # mapping (which is a prometheus_client implementation detail).
        body, _ = render_metrics(metrics)
        text = body.decode("utf-8")
        assert "protea_jobs_total" in text
        assert "protea_job_duration_seconds" in text
        assert "protea_embeddings_batch_seconds" in text
        assert "protea_predictions_batch_seconds" in text
        assert "protea_db_pool_in_use" in text
        assert "protea_http_requests_total" in text
        assert "protea_http_request_duration_seconds" in text
        assert "protea_http_requests_in_flight" in text

    def test_returns_none_when_prometheus_client_missing(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setitem(sys.modules, "prometheus_client", None)
        assert build_metric_registry() is None

    def test_registries_are_independent(self) -> None:
        """Two consecutive builds must use disjoint registries so test
        runs do not leak samples across cases."""
        a = build_metric_registry()
        b = build_metric_registry()
        assert a is not None and b is not None
        assert a.registry is not b.registry


# ---------------------------------------------------------------------------
# render_metrics
# ---------------------------------------------------------------------------


class TestRenderMetrics:
    def test_returns_bytes_and_prom_content_type(self) -> None:
        metrics = build_metric_registry()
        assert metrics is not None
        body, content_type = render_metrics(metrics)
        assert isinstance(body, bytes)
        # prometheus_client.CONTENT_TYPE_LATEST is the canonical value;
        # we just assert the prefix so version bumps do not break us.
        assert content_type.startswith("text/plain")

    def test_payload_lists_all_five_metric_names(self) -> None:
        metrics = build_metric_registry()
        assert metrics is not None
        body, _ = render_metrics(metrics)
        text = body.decode("utf-8")
        assert "protea_jobs_total" in text
        assert "protea_job_duration_seconds" in text
        assert "protea_embeddings_batch_seconds" in text
        assert "protea_predictions_batch_seconds" in text
        assert "protea_db_pool_in_use" in text

    def test_counter_and_histogram_observations_round_trip(self) -> None:
        metrics = build_metric_registry()
        assert metrics is not None
        metrics.jobs_total.labels(operation="ping", status="succeeded").inc()
        metrics.job_duration_seconds.labels(operation="ping", status="succeeded").observe(0.5)
        body, _ = render_metrics(metrics)
        text = body.decode("utf-8")
        # Counter sample line.
        assert 'protea_jobs_total{operation="ping",status="succeeded"} 1.0' in text
        # Histogram emits a _count line.
        assert "protea_job_duration_seconds_count" in text


# ---------------------------------------------------------------------------
# refresh_db_pool_gauge
# ---------------------------------------------------------------------------


class TestRefreshDbPoolGauge:
    def test_sets_gauge_from_pool_checkedout(self) -> None:
        metrics = build_metric_registry()
        assert metrics is not None
        engine = MagicMock()
        engine.pool.checkedout.return_value = 7
        refresh_db_pool_gauge(metrics, engine)
        # Read the gauge back via the registry's collect protocol.
        body, _ = render_metrics(metrics)
        assert "protea_db_pool_in_use 7.0" in body.decode("utf-8")

    def test_swallows_attribute_error(self) -> None:
        metrics = build_metric_registry()
        assert metrics is not None
        engine = object()  # No .pool attribute.
        # Should not raise.
        refresh_db_pool_gauge(metrics, engine)


# ---------------------------------------------------------------------------
# GET /metrics router
# ---------------------------------------------------------------------------


class TestMetricsEndpoint:
    def test_returns_prometheus_exposition_format(self) -> None:
        metrics = build_metric_registry()
        app = _make_app(metrics)
        client = TestClient(app)
        resp = client.get("/metrics")
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("text/plain")
        # Every baseline metric should appear in the payload even though
        # nothing has been observed yet.
        body = resp.text
        assert "protea_jobs_total" in body
        assert "protea_job_duration_seconds" in body
        assert "protea_embeddings_batch_seconds" in body
        assert "protea_predictions_batch_seconds" in body
        assert "protea_db_pool_in_use" in body

    def test_returns_503_when_registry_missing(self) -> None:
        app = _make_app(metrics=None)
        client = TestClient(app, raise_server_exceptions=False)
        resp = client.get("/metrics")
        assert resp.status_code == 503

    def test_refreshes_db_pool_gauge_on_scrape(self) -> None:
        metrics = build_metric_registry()
        assert metrics is not None
        engine = MagicMock()
        engine.pool.checkedout.return_value = 3
        app = _make_app(metrics, engine=engine)
        client = TestClient(app)
        resp = client.get("/metrics")
        assert resp.status_code == 200
        assert "protea_db_pool_in_use 3.0" in resp.text
        # The router pulled the value via the engine's pool.
        engine.pool.checkedout.assert_called_once()

    def test_works_without_session_factory(self) -> None:
        """The endpoint must still serve when ``session_factory`` is not
        configured (e.g. early boot, embedded tests). The pool gauge
        simply stays at zero in that case.
        """
        metrics = build_metric_registry()
        app = FastAPI()
        app.state.metrics = metrics
        # No session_factory at all.
        app.include_router(metrics_router)
        client = TestClient(app)
        resp = client.get("/metrics")
        assert resp.status_code == 200
