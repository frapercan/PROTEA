ADR-D7: Observability stack
============================

:Status: Pending
:Date: 2026-05-05
:Phase: F-OPS
:Gate: opens at F-OPS entry

Context
-------
PROTEA currently relies on per-process log files and ad-hoc
``print``/logger statements. Multi-target deployment (cloud, HPC,
airgap) and external adopters need distributed tracing, metrics with
SLOs, and structured log aggregation.

Decision (recommended)
----------------------
Single canonical stack:

- **Tracing**: OpenTelemetry (OTLP exporter) instrumenting FastAPI,
  SQLAlchemy, ``pika``. ``traceparent`` propagated HTTP -> queue -> worker.
- **Metrics**: Prometheus client, ``/metrics`` exposed.
- **Dashboards**: Grafana with dashboards committed in ``deploy/grafana/``.
- **Logs**: structured JSON via ``python-json-logger``, shipped to Loki
  via promtail or vector.

Consequences
------------
- A single prediction is visible end-to-end as one OTel trace.
- Three SLOs documented in ``docs/SLOs.md``.
- Alert rules committed; runbook per alert (see F7).

Resolution
----------
Pending; gate opens with F-OPS (T5.1-T5.4).
