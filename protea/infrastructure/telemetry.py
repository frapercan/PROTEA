# protea/infrastructure/telemetry.py
"""OpenTelemetry SDK boot for PROTEA (T5.1a).

This module is the single entry point for tracing in PROTEA. It wires
a global :class:`opentelemetry.sdk.trace.TracerProvider` configured
from environment variables (per ADR D07) and instruments FastAPI.

The OTel SDK is treated as optional at import time: if the libraries
are not installed (e.g. minimal worker images), :func:`configure_telemetry`
logs a single warning and returns ``None`` instead of raising. This
keeps the existing developer workflow (``poetry install``) green until
the F-OPS stack rolls out and is the pattern recommended by the OTel
docs for SDK-optional applications.

T5.1a scope: SDK boot + env-driven exporter URL + FastAPI instrumentation.
T5.1b (next slice) adds SQLAlchemy + pika instrumentation and
``traceparent`` propagation across HTTP -> queue -> worker boundaries.

Environment variables consumed
------------------------------
``PROTEA_OTEL_ENABLED``
    Truthy values (``1``, ``true``, ``yes``, ``on``) enable tracing.
    Default ``false`` so opting in is explicit and the developer
    workflow never blocks on a running collector.
``PROTEA_OTEL_ENDPOINT``
    OTLP HTTP exporter endpoint (e.g. ``http://otel-collector:4318``).
    When unset and tracing is enabled, the OTLP HTTP exporter falls back
    to its own default (``http://localhost:4318``).
``PROTEA_OTEL_SERVICE_NAME``
    ``service.name`` resource attribute. Defaults to ``protea-api``.
    Workers set this to ``protea-worker-<queue>`` at boot.
``PROTEA_OTEL_SAMPLE_RATIO``
    ``ParentBased(TraceIdRatioBased(<ratio>))`` sampler ratio. ``0.0``
    disables sampling, ``1.0`` samples every trace. Default ``1.0``
    (sampling is expected to be tuned via the collector once F-OPS
    sets up budgets).
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any

_LOGGER = logging.getLogger(__name__)

_DEFAULT_SERVICE_NAME = "protea-api"
_DEFAULT_SAMPLE_RATIO = 1.0


@dataclass(frozen=True)
class SdkBundle:
    """Bag of OTel SDK classes used by :func:`_build_provider`.

    Wrapped in a Parameter Object so :func:`_build_provider`'s signature
    stays under the project-wide 6-arg ceiling (see ``check_smells.py``)
    while remaining testable with stand-ins.
    """

    TracerProvider: Any
    Resource: Any
    BatchSpanProcessor: Any
    OTLPSpanExporter: Any
    ParentBased: Any
    TraceIdRatioBased: Any


@dataclass(frozen=True)
class TelemetryConfig:
    """Resolved telemetry settings.

    Built by :func:`resolve_telemetry_config` from the environment so
    callers can introspect what the SDK boot will actually do without
    triggering the boot itself (handy for ``/health`` reporting and
    tests).
    """

    enabled: bool
    endpoint: str | None
    service_name: str
    sample_ratio: float


def _as_bool(value: str | None) -> bool:
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _as_float(value: str | None, default: float) -> float:
    if value is None or not value.strip():
        return default
    try:
        return float(value)
    except ValueError:
        _LOGGER.warning("invalid PROTEA_OTEL_SAMPLE_RATIO=%r, falling back to %s", value, default)
        return default


def resolve_telemetry_config(
    env: dict[str, str] | None = None,
    *,
    default_service_name: str = _DEFAULT_SERVICE_NAME,
) -> TelemetryConfig:
    """Resolve telemetry settings from the environment.

    ``env`` defaults to :data:`os.environ`. ``default_service_name`` lets
    workers override the default ``protea-api`` value at boot.
    """
    env_map = os.environ if env is None else env
    return TelemetryConfig(
        enabled=_as_bool(env_map.get("PROTEA_OTEL_ENABLED")),
        endpoint=(env_map.get("PROTEA_OTEL_ENDPOINT") or None),
        service_name=(env_map.get("PROTEA_OTEL_SERVICE_NAME") or default_service_name),
        sample_ratio=_as_float(env_map.get("PROTEA_OTEL_SAMPLE_RATIO"), _DEFAULT_SAMPLE_RATIO),
    )


def _load_otel_sdk() -> tuple[Any, SdkBundle] | None:
    """Import the OTel SDK lazily, returning ``None`` when unavailable.

    Returns the ``opentelemetry.trace`` module + an :class:`SdkBundle`
    so :func:`configure_telemetry` can stay agnostic of the import shape.
    """
    try:
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
            OTLPSpanExporter,
        )
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        from opentelemetry.sdk.trace.sampling import (
            ParentBased,
            TraceIdRatioBased,
        )
    except ImportError as exc:
        _LOGGER.warning(
            "telemetry enabled but opentelemetry SDK not installed (%s); "
            "tracing will be a no-op. Install the `telemetry` extra or run "
            "`poetry install --with telemetry`.",
            exc,
        )
        return None

    return trace, SdkBundle(
        TracerProvider=TracerProvider,
        Resource=Resource,
        BatchSpanProcessor=BatchSpanProcessor,
        OTLPSpanExporter=OTLPSpanExporter,
        ParentBased=ParentBased,
        TraceIdRatioBased=TraceIdRatioBased,
    )


def configure_telemetry(
    app: Any | None = None,
    *,
    config: TelemetryConfig | None = None,
    default_service_name: str = _DEFAULT_SERVICE_NAME,
) -> TelemetryConfig:
    """Boot the OTel SDK and (optionally) instrument a FastAPI app.

    Idempotent: a second call with an already-configured global
    provider is a no-op (logged at DEBUG). Returns the resolved
    :class:`TelemetryConfig` regardless of whether tracing was actually
    enabled, so callers can stash it on ``app.state`` and surface it via
    ``/health``.
    """
    resolved = config or resolve_telemetry_config(default_service_name=default_service_name)

    if not resolved.enabled:
        _LOGGER.debug("telemetry disabled (PROTEA_OTEL_ENABLED is not truthy)")
        return resolved

    sdk = _load_otel_sdk()
    if sdk is None:
        return resolved
    trace, bundle = sdk

    if _provider_already_installed(trace):
        _LOGGER.debug("telemetry provider already installed, skipping rebuild")
    else:
        provider = _build_provider(resolved, bundle)
        trace.set_tracer_provider(provider)
        _LOGGER.info(
            "telemetry boot: service=%s endpoint=%s sample_ratio=%s",
            resolved.service_name,
            resolved.endpoint or "<default>",
            resolved.sample_ratio,
        )

    if app is not None:
        _instrument_fastapi(app)

    return resolved


def _provider_already_installed(trace_module: Any) -> bool:
    """Return ``True`` when a non-default global :class:`TracerProvider`
    is already set.

    The OTel SDK ships with a ``ProxyTracerProvider`` until the first
    ``set_tracer_provider`` call; that sentinel is what we use to detect
    "fresh process" vs "second configure_telemetry call".
    """
    from opentelemetry.trace import ProxyTracerProvider

    current = trace_module.get_tracer_provider()
    return not isinstance(current, ProxyTracerProvider)


def _build_provider(config: TelemetryConfig, bundle: SdkBundle) -> Any:
    """Assemble the :class:`TracerProvider` graph.

    Kept separate from :func:`configure_telemetry` so tests can drive
    the wiring with fake classes without touching the global provider.
    """
    resource = bundle.Resource.create({"service.name": config.service_name})
    sampler = bundle.ParentBased(bundle.TraceIdRatioBased(config.sample_ratio))
    provider = bundle.TracerProvider(resource=resource, sampler=sampler)

    exporter_kwargs: dict[str, Any] = {}
    if config.endpoint:
        # OTLP HTTP exporter expects the full ``/v1/traces`` path; the
        # collector accepts the bare endpoint and appends it itself in
        # most deployments, but being explicit avoids surprises.
        exporter_kwargs["endpoint"] = config.endpoint.rstrip("/") + "/v1/traces"

    exporter = bundle.OTLPSpanExporter(**exporter_kwargs)
    provider.add_span_processor(bundle.BatchSpanProcessor(exporter))
    return provider


def _instrument_fastapi(app: Any) -> None:
    """Instrument a FastAPI app, swallowing the optional-import error.

    The FastAPI instrumentor lives in a separate package
    (``opentelemetry-instrumentation-fastapi``); when it is missing we
    keep the SDK boot but skip HTTP-server spans rather than failing
    the whole API boot.
    """
    try:
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    except ImportError as exc:
        _LOGGER.warning(
            "FastAPI instrumentor not installed (%s); HTTP spans disabled. "
            "Install `opentelemetry-instrumentation-fastapi` to enable.",
            exc,
        )
        return

    FastAPIInstrumentor.instrument_app(app)
    _LOGGER.debug("FastAPI instrumentation installed")


# ---------------------------------------------------------------------------
# T5.2: Prometheus metric registry
# ---------------------------------------------------------------------------
#
# A single :class:`prometheus_client.CollectorRegistry` instance owns the
# five process-level metrics the platform exports. We deliberately keep
# the registry isolated (i.e. NOT the default global one) so test runs
# can rebuild it between cases without leaking samples, and so the
# ``/v1/metrics`` router never accidentally surfaces metrics defined by
# third-party libraries that auto-register on import.
#
# Metric inventory (T5.2 AC):
#   * protea_jobs_total{operation,status} - Counter
#   * protea_job_duration_seconds         - Histogram
#   * protea_embeddings_batch_seconds     - Histogram
#   * protea_predictions_batch_seconds    - Histogram
#   * protea_db_pool_in_use               - Gauge
#
# Histogram buckets cover the realistic spread for PROTEA: jobs run from
# sub-second (cheap ping ops) to several hours (full embeddings batch).
# Embedding / prediction batches are scoped to a single chunk and live in
# the seconds-to-minutes range.

_JOB_DURATION_BUCKETS = (
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
    30.0,
    60.0,
    120.0,
    300.0,
    600.0,
    1800.0,
    3600.0,
)
_BATCH_DURATION_BUCKETS = (
    0.01,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
    30.0,
    60.0,
    120.0,
)


@dataclass(frozen=True)
class MetricRegistry:
    """Bag of the five Prometheus collectors exposed by ``/v1/metrics``.

    The registry is intentionally a Parameter Object (not a module-level
    set of globals) so tests can build a throwaway one and the API can
    stash a single instance on ``app.state.metrics``.
    """

    registry: Any  # prometheus_client.CollectorRegistry
    jobs_total: Any  # Counter
    job_duration_seconds: Any  # Histogram
    embeddings_batch_seconds: Any  # Histogram
    predictions_batch_seconds: Any  # Histogram
    db_pool_in_use: Any  # Gauge


def build_metric_registry() -> MetricRegistry | None:
    """Construct the five-metric registry, returning ``None`` if
    ``prometheus_client`` is not installed.

    Mirrors the soft-fail pattern of :func:`configure_telemetry`: a
    minimal worker image that does not pull the observability extras
    keeps booting; the ``/v1/metrics`` router degrades to a 503 instead
    of crashing the whole API.
    """
    try:
        from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram
    except ImportError as exc:
        _LOGGER.warning(
            "prometheus_client not installed (%s); /v1/metrics will return 503. "
            "Install the `telemetry` extra to enable Prometheus scraping.",
            exc,
        )
        return None

    registry = CollectorRegistry()
    return MetricRegistry(
        registry=registry,
        jobs_total=Counter(
            "protea_jobs_total",
            "Total number of jobs processed, labelled by operation and terminal status.",
            labelnames=("operation", "status"),
            registry=registry,
        ),
        job_duration_seconds=Histogram(
            "protea_job_duration_seconds",
            "End-to-end job duration in seconds (claim -> terminal status).",
            labelnames=("operation", "status"),
            buckets=_JOB_DURATION_BUCKETS,
            registry=registry,
        ),
        embeddings_batch_seconds=Histogram(
            "protea_embeddings_batch_seconds",
            "Wall-clock duration of a single embeddings batch in seconds.",
            labelnames=("backend",),
            buckets=_BATCH_DURATION_BUCKETS,
            registry=registry,
        ),
        predictions_batch_seconds=Histogram(
            "protea_predictions_batch_seconds",
            "Wall-clock duration of a single predictions batch in seconds.",
            labelnames=("runner",),
            buckets=_BATCH_DURATION_BUCKETS,
            registry=registry,
        ),
        db_pool_in_use=Gauge(
            "protea_db_pool_in_use",
            "Connections currently checked out of the SQLAlchemy pool.",
            registry=registry,
        ),
    )


def refresh_db_pool_gauge(metrics: MetricRegistry, engine: Any) -> None:
    """Read the SQLAlchemy pool's checked-out count and set the gauge.

    Called from the ``/v1/metrics`` handler on every scrape so the
    gauge reflects the live pool state without requiring event-listener
    plumbing. ``engine`` is expected to be a SQLAlchemy ``Engine``; we
    duck-type via ``engine.pool.checkedout()`` so tests can pass a
    minimal stub. Any AttributeError is swallowed (the pool may not
    expose the method on some dialects).
    """
    try:
        in_use = engine.pool.checkedout()
    except AttributeError:
        return
    metrics.db_pool_in_use.set(in_use)


def render_metrics(metrics: MetricRegistry) -> tuple[bytes, str]:
    """Generate the Prometheus exposition payload for ``metrics``.

    Returns the body bytes + the canonical ``Content-Type`` header value
    so the router can return a ``Response`` without re-importing
    ``prometheus_client`` itself.
    """
    from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

    return generate_latest(metrics.registry), CONTENT_TYPE_LATEST
