# protea/api/routers/metrics.py
"""Prometheus scrape endpoint (T5.2).

Exposes ``GET /metrics`` returning the standard Prometheus text-based
exposition format. The collector registry is built once in
:func:`protea.api.app.create_app` and stashed on ``app.state.metrics``
so requests do not pay the registration cost on every scrape.

T5.2 scope is intentionally narrow: the endpoint is always served (so
Prometheus can be wired up at deploy time without flipping any feature
flag), and the five baseline metrics are registered up-front so they
appear in the output even before any sample has been observed. Call
sites that increment counters / observe histograms land in follow-up
slices.

The ``protea_db_pool_in_use`` gauge is refreshed on each scrape by
reading the SQLAlchemy pool's ``checkedout()`` count from the session
factory's bound engine. This keeps the gauge accurate without needing
event-listener wiring, at the cost of one cheap method call per
scrape (typically every 15s).
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request, Response

from protea.infrastructure.telemetry import (
    MetricRegistry,
    refresh_db_pool_gauge,
    render_metrics,
)

router = APIRouter(tags=["metrics"])


@router.get(
    "/metrics",
    response_class=Response,
    responses={
        200: {
            "content": {"text/plain": {}},
            "description": "Prometheus text exposition format (version 0.0.4).",
        },
        503: {"description": "prometheus_client not installed in this build."},
    },
)
def get_metrics(request: Request) -> Response:
    """Render the live Prometheus exposition payload.

    Returns 503 when the API was booted without the ``prometheus_client``
    dependency (a minimal worker image, for example). This keeps the
    endpoint shape stable for Prometheus scrapers, which retry on 5xx,
    instead of leaking an import error.
    """
    metrics: MetricRegistry | None = getattr(request.app.state, "metrics", None)
    if metrics is None:
        raise HTTPException(
            status_code=503,
            detail="prometheus_client not installed; /metrics is disabled.",
        )

    factory = getattr(request.app.state, "session_factory", None)
    bind = getattr(factory, "kw", {}).get("bind") if factory is not None else None
    if bind is not None:
        refresh_db_pool_gauge(metrics, bind)

    body, content_type = render_metrics(metrics)
    return Response(content=body, media_type=content_type)
