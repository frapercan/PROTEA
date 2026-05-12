Observability: OpenTelemetry SDK
================================

PROTEA ships an optional distributed tracing layer built on the
`OpenTelemetry <https://opentelemetry.io/>`_ SDK (T5.1a, ADR-D7).
``protea/infrastructure/telemetry.py`` is the single entry point: it
reads four ``PROTEA_OTEL_*`` environment variables, boots a global
:class:`~opentelemetry.sdk.trace.TracerProvider`, and instruments the
FastAPI application. When the OTel SDK packages are absent or tracing is
disabled, the module degrades silently to a no-op rather than crashing
the API boot.

T5.1a scope (current): SDK boot, OTLP/HTTP exporter, FastAPI
auto-instrumentation.

T5.1b (next): SQLAlchemy and ``pika`` instrumentation plus
``traceparent`` propagation across the HTTP-to-queue-to-worker boundary.

.. contents:: On this page
   :local:
   :depth: 2

Environment variables
---------------------

All four variables default to a safe "opt-in" state so a plain
``poetry install`` starts PROTEA without requiring a running collector.

.. list-table::
   :header-rows: 1
   :widths: 36 12 52

   * - Variable
     - Default
     - Description
   * - ``PROTEA_OTEL_ENABLED``
     - ``false``
     - Truthy values ``1``, ``true``, ``yes``, ``on`` enable tracing.
       Any other value (including unset) keeps tracing disabled.
   * - ``PROTEA_OTEL_ENDPOINT``
     - (none)
     - OTLP HTTP exporter endpoint, e.g. ``http://otel-collector:4318``.
       When unset, the OTel SDK falls back to its compiled default
       (``http://localhost:4318``). The module appends ``/v1/traces`` to
       whatever value is supplied so the bare collector root is the
       expected input.
   * - ``PROTEA_OTEL_SERVICE_NAME``
     - ``protea-api``
     - ``service.name`` resource attribute sent with every span.
       Worker processes override this at boot (e.g.
       ``protea-worker-embeddings``).
   * - ``PROTEA_OTEL_SAMPLE_RATIO``
     - ``1.0``
     - ``ParentBased(TraceIdRatioBased(<ratio>))`` sampler.
       ``1.0`` records every trace, ``0.0`` discards all traces.
       Production tuning is expected to happen at the collector level
       once F-OPS SLO budgets are set; this variable provides a
       per-process escape hatch.

Installing the optional dependencies
-------------------------------------

The OTel SDK and FastAPI instrumentor live in the optional
``telemetry`` poetry group and are not installed by a plain
``poetry install``:

.. code-block:: bash

   # Install the base dependencies plus the telemetry extras.
   poetry install --with telemetry

The group pins the following packages:

.. code-block:: text

   opentelemetry-api                        >=1.27.0,<2.0.0
   opentelemetry-sdk                        >=1.27.0,<2.0.0
   opentelemetry-exporter-otlp-proto-http   >=1.27.0,<2.0.0
   opentelemetry-instrumentation-fastapi    >=0.48b0,<1.0.0

If the SDK is not installed but ``PROTEA_OTEL_ENABLED=1`` is set, the
API boots normally and logs a single ``WARNING``:

.. code-block:: text

   telemetry enabled but opentelemetry SDK not installed (...);
   tracing will be a no-op. Install the `telemetry` extra or run
   `poetry install --with telemetry`.

Quick-start: enabling tracing locally
--------------------------------------

1. Install telemetry dependencies (one-off):

   .. code-block:: bash

      poetry install --with telemetry

2. Start a local OTLP collector (Jaeger all-in-one is the simplest
   option):

   .. code-block:: bash

      docker run --rm -p 4318:4318 -p 16686:16686 \
          jaegertracing/all-in-one:latest

3. Set the required variables and start the API:

   .. code-block:: bash

      export PROTEA_OTEL_ENABLED=1
      export PROTEA_OTEL_ENDPOINT=http://localhost:4318
      export PROTEA_OTEL_SERVICE_NAME=protea-api
      poetry run uvicorn protea.api.app:create_app \
          --factory --host 0.0.0.0 --port 8000

4. Send any request to the API and open the Jaeger UI at
   ``http://localhost:16686`` to inspect the trace.

To enable tracing in the docker-compose stack, add the variables to the
``api`` service in ``docker-compose.yml`` (or a ``docker-compose.override.yml``
that is not committed):

.. code-block:: yaml

   services:
     api:
       environment:
         PROTEA_OTEL_ENABLED: "1"
         PROTEA_OTEL_ENDPOINT: "http://otel-collector:4318"
         PROTEA_OTEL_SERVICE_NAME: "protea-api"

FastAPI auto-instrumentation behaviour
---------------------------------------

When both the ``telemetry`` group is installed and
``PROTEA_OTEL_ENABLED=1``, ``configure_telemetry(app)`` calls
``FastAPIInstrumentor.instrument_app(app)`` before middlewares are
registered. This means every incoming HTTP request receives a root span
with the route template as the operation name (e.g.
``GET /jobs/{job_id}``). The span includes HTTP status, method, and
route attributes following the OTel HTTP semantic conventions.

``configure_telemetry`` is idempotent: a second call on an already-booted
process is a no-op (logged at ``DEBUG``). This matters during test runs
that reinitialise the FastAPI app.

Soft-degrade fallback
----------------------

The module is designed to never block the API or worker boot:

- If ``PROTEA_OTEL_ENABLED`` is falsy (or unset), ``configure_telemetry``
  returns immediately with the resolved :class:`~protea.infrastructure.telemetry.TelemetryConfig`
  and touches nothing.
- If the OTel SDK packages are missing, a single ``WARNING`` is emitted
  and the function returns the config without raising.
- If ``opentelemetry-instrumentation-fastapi`` is missing but the core
  SDK is present, the SDK boots successfully and only HTTP-server spans
  are disabled (a second ``WARNING`` is emitted).

The resolved config is stored on ``app.state.telemetry`` so it can be
surfaced through ``/health`` in a future iteration.

Diagnosis
----------

**Verify the current telemetry config at runtime**

``resolve_telemetry_config()`` can be called without booting anything:

.. code-block:: bash

   python3 - <<'EOF'
   import os
   os.environ.setdefault("PROTEA_OTEL_ENABLED", "0")
   from protea.infrastructure.telemetry import resolve_telemetry_config
   cfg = resolve_telemetry_config()
   print(cfg)
   EOF

**Check whether the provider was installed**

.. code-block:: python

   from opentelemetry import trace
   from opentelemetry.trace import ProxyTracerProvider

   provider = trace.get_tracer_provider()
   print("OTel active:", not isinstance(provider, ProxyTracerProvider))

A ``ProxyTracerProvider`` means no SDK boot happened in this process.

**Inspect startup logs for the boot confirmation line**

When tracing boots successfully, the API logs an ``INFO`` line:

.. code-block:: text

   telemetry boot: service=protea-api endpoint=http://otel-collector:4318 sample_ratio=1.0

Absence of this line alongside a ``DEBUG`` line
``telemetry disabled (PROTEA_OTEL_ENABLED is not truthy)`` confirms the
feature is off.

**Check the OTLP endpoint is reachable**

.. code-block:: bash

   # Replace with the value of PROTEA_OTEL_ENDPOINT.
   curl -v http://otel-collector:4318/v1/traces \
       -H "Content-Type: application/json" \
       -d '{}' 2>&1 | grep -E "^< HTTP|Connection refused|Could not resolve"

A ``405 Method Not Allowed`` response confirms the endpoint is reachable
(the collector rejects empty JSON but the TCP connection succeeded).
``Connection refused`` or ``Could not resolve host`` points to a
misconfigured ``PROTEA_OTEL_ENDPOINT`` or a collector that is not running.

**No spans appearing in the collector**

1. Confirm the ``BatchSpanProcessor`` queue is not full. Under sustained
   load the batch queue can back up; this appears as dropped-span log
   lines from the OTel SDK internals (level ``WARNING``, prefix
   ``Failed to export``).

2. Check the sample ratio. A ``PROTEA_OTEL_SAMPLE_RATIO=0.0`` silently
   discards every trace:

   .. code-block:: bash

      echo ${PROTEA_OTEL_SAMPLE_RATIO:-"(unset, default 1.0)"}

3. Confirm the exporter endpoint includes the ``/v1/traces`` suffix.
   The module appends it automatically, so the variable should be set
   to the bare collector root (e.g. ``http://otel-collector:4318``), not
   ``http://otel-collector:4318/v1/traces``.

Operational notes
------------------

**Worker service names**

Each worker process should set ``PROTEA_OTEL_SERVICE_NAME`` at startup
so traces distinguish API spans from queue-worker spans:

.. code-block:: bash

   PROTEA_OTEL_SERVICE_NAME=protea-worker-embeddings \
       python3 -m protea.workers.run_worker embeddings

**Production sample ratio**

``PROTEA_OTEL_SAMPLE_RATIO=1.0`` (the default) records every trace. For
high-throughput production deployments, lower the ratio at the worker
level or configure tail sampling at the collector instead of head
sampling here:

.. code-block:: bash

   # Record 10% of traces from this process.
   export PROTEA_OTEL_SAMPLE_RATIO=0.1

The ``ParentBased`` wrapper ensures that child spans respect the
sampling decision made by the parent so partial traces do not appear in
the collector.

**T5.1b extension**

The next sub-slice (T5.1b) adds:

- ``opentelemetry-instrumentation-sqlalchemy`` wrapping the session factory.
- ``opentelemetry-instrumentation-pika`` wrapping the AMQP publisher and
  consumer.
- ``traceparent`` header injection and extraction so a single API request
  that dispatches a queue message appears as one end-to-end trace in the
  collector.

This runbook will be extended in-place when T5.1b merges. No
configuration changes are required on the operator side; the same four
``PROTEA_OTEL_*`` variables control the whole stack.

See also
---------

- :doc:`/adr/D07-observability-stack` for the design rationale behind
  the OTel stack choice (versus ELK/Jaeger-native SDK).
- ``protea/infrastructure/telemetry.py`` for the full module docstring
  and API surface.
- ``tests/test_telemetry.py`` for unit-level examples of
  ``resolve_telemetry_config`` and ``configure_telemetry`` usage.
