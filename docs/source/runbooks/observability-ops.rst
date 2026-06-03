Observability Operator Runbook
==============================

This runbook covers the full PROTEA observability stack from an
operator's perspective: Prometheus metrics, Loki log aggregation,
Grafana dashboards and alerting, and the OpenTelemetry tracing layer.
It is a quick-reference companion to the component-specific runbooks
(:doc:`prometheus`, :doc:`loki`, :doc:`observability`).

All four signals (metrics, logs, traces, alerts) ship from the same
monitoring compose project:

.. code-block:: bash

   docker compose -f docker-compose.monitoring.yml up -d

.. contents:: On this page
   :local:
   :depth: 2


Architecture overview
~~~~~~~~~~~~~~~~~~~~~

The PROTEA process stack (uvicorn API + workers + Next.js) on the host
emits three observable signals:

- ``/metrics`` on port 8000 (Prometheus text format) scraped by
  ``prometheus:9090``.
- ``logs/*.log`` files tailed by the ``promtail`` sidecar and shipped to
  ``loki:3100``.
- OTLP HTTP spans (when ``PROTEA_OTEL_ENABLED=true``) sent to an OTel
  collector (not yet deployed in the standard monitoring stack).

Grafana at port 3001 queries both datasources:

- datasource ``protea-prometheus`` (uid ``protea-prometheus``) for metric panels.
- datasource ``protea-loki`` (uid ``protea-loki``) for log panels and alerts.

Prometheus scrapes the host API on port 8000, RabbitMQ on 15692 (via
container name ``protea-rabbitmq-1``), and the Postgres exporter sidecar
on port 9187. Workers do not expose their own ``/metrics`` port; all
worker counters surface through the shared API registry.

Loki receives log lines via a ``promtail`` sidecar that tails
``logs/*.log`` files written by the process stack. The docker-driver
plugin path (documented in :doc:`loki`) applies when the application
itself runs in containers.

Tracing via the OpenTelemetry SDK is optional and disabled by default
(``PROTEA_OTEL_ENABLED=false``). See the `Tracing`_ section below.


Prometheus
~~~~~~~~~~

**Scrape config:** ``deploy/prometheus/prometheus.yml``

**Scrape interval:** 15 s

Jobs defined in the scrape config:

.. list-table::
   :header-rows: 1
   :widths: 20 30 50

   * - Job
     - Target
     - Series powered
   * - ``protea-api``
     - ``host.docker.internal:8000/metrics``
     - ``protea_*`` (HTTP latency, job counters, embedding/prediction
       histograms, DB pool gauge)
   * - ``rabbitmq``
     - ``protea-rabbitmq-1:15692/metrics``
     - ``rabbitmq_queue_messages_ready``,
       ``rabbitmq_queue_messages_unacknowledged``,
       ``rabbitmq_queue_consumers``, publish/deliver rate counters
   * - ``postgres``
     - ``protea-postgres-exporter:9187/metrics``
     - ``pg_stat_activity_count``,
       ``pg_stat_database_xact_commit``,
       ``pg_stat_database_deadlocks``,
       ``pg_stat_activity_max_tx_duration``
   * - ``prometheus``
     - ``localhost:9090``
     - Self-scrape: ``up{}``, server health

.. rubric:: Useful PromQL queries

**API p99 latency (last 5 min)**

.. code-block:: text

   histogram_quantile(
     0.99,
     rate(http_request_duration_seconds_bucket{job="protea-api"}[5m])
   )

**Worker queue depth (ready messages, all queues)**

.. code-block:: text

   sum by (queue) (rabbitmq_queue_messages_ready)

**Active Postgres sessions**

.. code-block:: text

   pg_stat_activity_count{datname="protea"}

**PROTEA DB connection pool utilisation**

.. code-block:: text

   protea_db_pool_checked_out / protea_db_pool_size

**GPU utilisation** (if ``nvidia_smi_gpu_utilization_ratio`` is
exported via a node-exporter or custom script): no scrape job is
currently defined for GPU metrics; add a ``dcgm-exporter`` or
``nvidia_gpu_exporter`` target to ``prometheus.yml`` when GPU
monitoring is required.

Hot-reload the scrape config without restarting Prometheus:

.. code-block:: bash

   curl -X POST http://localhost:9090/-/reload


Loki
~~~~

**Loki API:** ``http://localhost:3100``

**Promtail config:** ``deploy/promtail/promtail.yml``

**Log files tailed:** ``logs/*.log`` under the deploy working directory

Labels injected per line:

- ``compose_project=protea`` (static)
- ``service_name=<log-file-stem>`` (e.g., ``api``, ``worker-jobs``)

.. rubric:: Top LogQL queries for operators

**Worker errors in the last hour**

.. code-block:: text

   {compose_project="protea", service_name=~"worker-.*"}
   | json
   | level="error"
   | __error__=""

**API 5xx responses in the last 15 minutes**

.. code-block:: text

   {compose_project="protea", service_name="api"}
   | json
   | level="error"
   | line_format "{{.message}}"

**Stuck job filter (no progress on a known job_id)**

Replace ``<JOB_ID>`` with the UUID visible in the UI:

.. code-block:: text

   {compose_project="protea"}
   | json
   | job_id="<JOB_ID>"

Verify log ingestion is live:

.. code-block:: bash

   curl -sf http://localhost:3100/ready && echo "loki ready"

   # Force a log line and check it arrives.
   curl -sf http://localhost:8000/health > /dev/null
   # Query the last line from Loki (URL-encode the query manually or use a tool):
   curl -sG http://localhost:3100/loki/api/v1/query_range \
       -d "query=%7Bcompose_project%3D%22protea%22%7D&limit=1" \
       | python3 -m json.tool | head -20


Grafana
~~~~~~~

**Local URL:** ``http://localhost:3001``

**Ngrok-exposed URL:** ``https://protea.ngrok.app`` (tunnels to the
Next.js frontend on port 3000; Grafana is accessible at its own port
3001 on the host but is not reverse-proxied through the ngrok tunnel).

Dashboard JSON files live in ``deploy/grafana/dashboards/``.

.. rubric:: Dashboards by audience

**Developer dashboards** (day-to-day pipeline work):

.. list-table::
   :header-rows: 1
   :widths: 35 35 30

   * - Dashboard
     - JSON file
     - Primary signal
   * - PROTEA / API latency
     - ``api-latency.json``
     - ``protea_*`` HTTP percentiles
   * - PROTEA / Worker throughput
     - ``worker-throughput.json``
     - Job counters per operation
   * - PROTEA / Embeddings pipeline
     - ``embeddings-pipeline.json``
     - Batch throughput + error rate
   * - PROTEA / Logs (Loki)
     - ``logs.json``
     - Structured log stream

**Operator dashboards** (infrastructure health):

.. list-table::
   :header-rows: 1
   :widths: 35 35 30

   * - Dashboard
     - JSON file
     - Primary signal
   * - PROTEA / DB connections
     - ``db-connections.json``
     - ``pg_*`` pool + activity
   * - PROTEA / Queue depth
     - ``queue-depth.json``
     - ``rabbitmq_queue_messages_ready``
   * - Agent-farm heartbeats
     - ``agent-farm-heartbeats.json``
     - Per-task error bursts (FARM-UI.7)

The ``visitors.json`` dashboard tracks public endpoint traffic via
nginx access log metrics (only active when an nginx proxy is in front
of the stack).

.. rubric:: Top 5 bookmarks

Quick links to check first when something goes wrong:

1. ``http://localhost:3001/d/api-latency`` (API request latency, p50/p95/p99).
2. ``http://localhost:3001/d/queue-depth`` (RabbitMQ queue depths across all workers).
3. ``http://localhost:3001/d/db-connections`` (Postgres connection pool and active sessions).
4. ``http://localhost:3001/d/worker-throughput`` (job success/failure counters per operation).
5. ``http://localhost:3001/explore`` (free-form Loki LogQL and Prometheus PromQL queries).

If Grafana shows "No data" on metric panels, confirm Prometheus is
running (``curl -sf http://localhost:9090/-/ready``) and that the
``protea-api`` scrape target is ``up``
(``curl -sf 'http://localhost:9090/api/v1/targets'``).


Alerting
~~~~~~~~

**Alert rules file:** ``deploy/grafana/provisioning/alerting/rules.yml``

**Contact points file:**
``deploy/grafana/provisioning/alerting/contact-points.yml``

**Routing policy:**
``deploy/grafana/provisioning/alerting/policies.yml``

Currently one alert rule is provisioned (FARM-UI.7):

.. list-table::
   :header-rows: 1
   :widths: 30 20 50

   * - Alert
     - Severity
     - Condition
   * - agent-farm error burst
     - warning
     - 3 or more ``level=error`` agent-farm heartbeats in a 5-minute
       window for the same ``task_id``

The alert routes via the ``agent-farm`` Grafana folder to the Slack
contact point keyed on ``SLACK_WEBHOOK_URL``. Repeat interval is 30
minutes (one immediate fire, then at most one follow-up per half hour).

**Setup (one command):**

.. code-block:: bash

   export SLACK_WEBHOOK_URL='https://hooks.slack.com/services/...'
   docker compose -f docker-compose.monitoring.yml up -d

When ``SLACK_WEBHOOK_URL`` is unset, Grafana boots cleanly and the
contact point delivers to an empty URL (no 4xx storm).

Acknowledging an alert in Grafana: open the alert rule in
``Alerting > Alert rules > agent-farm``, click ``Silence`` and set a
duration. Silences do not stop evaluation; they suppress Slack
notifications.

**Planned alerts (not yet provisioned):**

The following conditions are currently detected only by log inspection
or manual Prometheus queries. Adding Grafana alert rules for them is
tracked under F-OPS:

- ``worker-queue-stuck``: ``rabbitmq_queue_messages_ready > N`` for
  more than 10 minutes with no consumer activity.
- ``postgres-down``: ``pg_up == 0`` for more than 30 seconds.
- ``ngrok-tunnel-down``: the deploy-keeper supervisor already polls
  for this and escalates to a janitor agent; a Grafana alert is
  additive redundancy.


Tracing
~~~~~~~

Distributed tracing via the OpenTelemetry SDK is implemented but
disabled by default. The four ``PROTEA_OTEL_*`` environment variables
control it; see :doc:`observability` for the full variable reference
and quick-start.

Current scope (T5.1a): SDK boot, OTLP/HTTP exporter, FastAPI
auto-instrumentation. Each HTTP request to the API generates a root
span with the route template as the operation name.

**No dedicated OTel collector is deployed.** To enable tracing locally,
point ``PROTEA_OTEL_ENDPOINT`` at a Jaeger all-in-one container (see
:doc:`observability` Quick-start). A production OTel collector is
planned under ADR-D7; until it lands, set ``PROTEA_OTEL_ENABLED=false``
(the default) in production.

Upcoming (T5.1b): SQLAlchemy + pika instrumentation and
``traceparent`` propagation across the HTTP-to-queue-to-worker boundary.


See also
~~~~~~~~

- :doc:`prometheus` (detailed Prometheus bring-up and troubleshooting).
- :doc:`loki` (detailed Loki / promtail bring-up and troubleshooting).
- :doc:`observability` (OpenTelemetry SDK reference: environment
  variables, soft-degrade behaviour, quick-start).
- :doc:`/adr/D07-observability-stack` (design rationale for the stack
  choices: Prometheus over Datadog, Loki over ELK, OTel over custom).
- ``deploy/grafana/dashboards/`` (Grafana dashboard JSON source).
- ``deploy/grafana/provisioning/alerting/`` (alert rules and contact
  points, Slack integration, FARM-UI.7).
- ``deploy/prometheus/prometheus.yml`` (committed scrape config).
