"""Smoke tests for HttpMetricsMiddleware.

Confirms the Prometheus HTTP series populate after a request hits the API,
and that the route label uses the FastAPI template (not the raw URL) to
keep cardinality bounded.
"""

from __future__ import annotations

from fastapi import FastAPI
from starlette.testclient import TestClient

from protea.api.middleware import HttpMetricsMiddleware
from protea.infrastructure.telemetry import build_metric_registry, render_metrics


def _app_with_metrics() -> FastAPI:
    app = FastAPI()
    app.state.metrics = build_metric_registry()
    app.add_middleware(HttpMetricsMiddleware)

    @app.get("/jobs/{job_id}")
    def get_job(job_id: str) -> dict[str, str]:
        return {"id": job_id}

    @app.get("/boom")
    def boom() -> None:
        raise RuntimeError("synthetic failure")

    return app


def test_metric_series_populate_after_one_request() -> None:
    app = _app_with_metrics()
    client = TestClient(app)
    assert app.state.metrics is not None

    client.get("/jobs/abc123")

    body, _ = render_metrics(app.state.metrics)
    text = body.decode("utf-8")
    assert 'protea_http_requests_total{method="GET",route="/jobs/{job_id}",status="200"}' in text
    assert 'protea_http_request_duration_seconds_count{method="GET",route="/jobs/{job_id}"}' in text


def test_route_template_not_raw_path() -> None:
    """Two distinct job_ids must collapse onto the same route label."""
    app = _app_with_metrics()
    client = TestClient(app)
    assert app.state.metrics is not None

    client.get("/jobs/abc")
    client.get("/jobs/def")
    client.get("/jobs/ghi")

    body, _ = render_metrics(app.state.metrics)
    text = body.decode("utf-8")
    # No raw IDs in the label set.
    for raw in ("abc", "def", "ghi"):
        assert f'route="/jobs/{raw}"' not in text
    # The templated form is present, and the counter sums to 3.
    assert 'protea_http_requests_total{method="GET",route="/jobs/{job_id}",status="200"} 3.0' in text


def test_unmatched_path_gets_sentinel_label() -> None:
    app = _app_with_metrics()
    client = TestClient(app)
    assert app.state.metrics is not None

    client.get("/this-route-does-not-exist")

    body, _ = render_metrics(app.state.metrics)
    text = body.decode("utf-8")
    assert 'route="__unmatched__"' in text


def test_in_flight_returns_to_zero_after_request() -> None:
    app = _app_with_metrics()
    client = TestClient(app)
    assert app.state.metrics is not None

    client.get("/jobs/abc")

    body, _ = render_metrics(app.state.metrics)
    text = body.decode("utf-8")
    assert "protea_http_requests_in_flight 0.0" in text


def test_metrics_skipped_when_registry_absent() -> None:
    app = FastAPI()
    app.add_middleware(HttpMetricsMiddleware)

    @app.get("/ping")
    def ping() -> dict[str, str]:
        return {"pong": "ok"}

    client = TestClient(app)
    response = client.get("/ping")
    assert response.status_code == 200
