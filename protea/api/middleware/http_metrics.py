"""Prometheus instrumentation middleware for the FastAPI surface.

Emits the three HTTP series the operability dashboards expect:

- ``protea_http_requests_total{method,route,status}`` — request count.
- ``protea_http_request_duration_seconds{method,route}`` — wall-clock latency.
- ``protea_http_requests_in_flight`` — concurrent requests gauge.

The ``route`` label is the FastAPI route template (e.g. ``/jobs/{job_id}``),
NOT the raw URL path, to keep cardinality bounded. Requests that don't match
any route (404 / 405) are labelled ``__unmatched__`` so the dashboards stay
readable.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.types import ASGIApp

logger = logging.getLogger(__name__)


class HttpMetricsMiddleware(BaseHTTPMiddleware):
    """Records per-request Prometheus counters + histograms + in-flight gauge.

    The metric registry is read lazily from ``app.state.metrics`` on each
    request so the middleware degrades to a no-op when the optional
    ``prometheus_client`` dependency is absent (mirrors the existing
    soft-fail in :mod:`protea.infrastructure.telemetry`).
    """

    def __init__(self, app: ASGIApp) -> None:
        super().__init__(app)

    async def dispatch(self, request: Request, call_next: Any) -> Any:
        metrics = getattr(request.app.state, "metrics", None)
        if metrics is None:
            return await call_next(request)

        gauge = getattr(metrics, "http_requests_in_flight", None)
        counter = getattr(metrics, "http_requests_total", None)
        histogram = getattr(metrics, "http_request_duration_seconds", None)
        if gauge is None or counter is None or histogram is None:
            return await call_next(request)

        gauge.inc()
        start = time.perf_counter()
        status_code = "500"
        try:
            response = await call_next(request)
            status_code = str(response.status_code)
            return response
        except Exception:
            status_code = "500"
            raise
        finally:
            gauge.dec()
            elapsed = time.perf_counter() - start
            route = self._route_template(request)
            method = request.method.upper()
            try:
                counter.labels(method=method, route=route, status=status_code).inc()
                histogram.labels(method=method, route=route).observe(elapsed)
            except Exception:
                logger.debug("http metrics emit failed", exc_info=True)

    @staticmethod
    def _route_template(request: Request) -> str:
        """Return the FastAPI route path (``/jobs/{job_id}``) or sentinel.

        The matched ``Route`` is attached to ``request.scope["route"]`` by
        the routing layer; falling back to ``__unmatched__`` keeps the
        Prometheus label cardinality bounded for 404 paths.
        """
        route = request.scope.get("route")
        path = getattr(route, "path", None)
        if path:
            return str(path)
        return "__unmatched__"
