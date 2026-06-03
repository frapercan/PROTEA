Observability: Prometheus metrics
==================================

PROTEA exposes a Prometheus scrape endpoint on the API and ships the
metric dashboards (api-latency, worker-throughput, embeddings-pipeline,
db-connections) in the Grafana monitoring stack. The missing piece for a
long time was the scraper itself: the Grafana ``protea-prometheus``
datasource pointed at ``host.docker.internal:9090`` but no Prometheus
server ran there, so every metric panel showed "No data". This runbook
covers bringing that Prometheus server up.

Prometheus runs as a container inside
``docker-compose.monitoring.yml`` alongside Grafana and Loki. The scrape
configuration is committed at ``deploy/prometheus/prometheus.yml``.

.. contents:: On this page
   :local:
   :depth: 2


How the pipeline fits together
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

   PROTEA API (host process, :8000/metrics, protea_* series)
      |
      v
   prometheus:9090   (monitoring compose, scrapes via host.docker.internal)
      |
      v
   Grafana           (PROTEA Prometheus datasource, uid protea-prometheus)
      |
      v
   dashboards        (api-latency, worker-throughput, embeddings-pipeline,
                      db-connections, queue-depth)

Two exporters bridge the gap for the queue-depth and db-connections
dashboards:

- **RabbitMQ** (``rabbitmq_*`` series): the ``rabbitmq_prometheus`` plugin
  built into ``rabbitmq:3-management`` exposes metrics on port 15692.
  ``docker/rabbitmq/enabled_plugins`` activates the plugin explicitly so the
  image never needs a custom entrypoint.
- **Postgres** (``pg_*`` series): ``prometheuscommunity/postgres-exporter``
  runs as a sidecar in ``docker-compose.monitoring.yml`` and connects to
  host-published Postgres on port 5432.

The application code does not need to know anything about Prometheus.
The API already serves the Prometheus text exposition format at
``/metrics`` (and the canonical alias ``/v1/metrics``) on port 8000; the
collector registry is built once at API startup in
``protea.api.app.create_app`` and rendered on demand by
``protea/api/routers/metrics.py``.


Starting Prometheus
~~~~~~~~~~~~~~~~~~~

Prometheus runs inside ``docker-compose.monitoring.yml``. Bring up the
full monitoring stack from the repo root:

.. code-block:: bash

   docker compose -f docker-compose.monitoring.yml up -d
   curl -sf http://localhost:9090/-/ready && echo "prometheus ready"

The container publishes 9090 on the host because the Grafana datasource
reaches it through ``host.docker.internal:9090`` (the same host-gateway
convention used for Postgres). Grafana also reaches the same container by
service name (``http://prometheus:9090``) on the ``protea_monitoring``
bridge network if you prefer to edit the datasource ``url`` to use the
in-network name.


Scrape targets
~~~~~~~~~~~~~~

The committed scrape config defines these jobs:

``protea-api``
   Scrapes ``host.docker.internal:8000/metrics``. This is the host-run
   PROTEA API; the application stack is not part of the monitoring
   compose project, so the target uses ``host.docker.internal`` (mapped
   to the docker host gateway via ``extra_hosts``). This job feeds every
   ``protea_*`` series: HTTP request latency, job counters, embedding and
   prediction batch histograms, and the DB pool gauge.

``prometheus``
   Self-scrape so the ``up{}`` series and the server's own health are
   visible.

``rabbitmq``
   Scrapes ``host.docker.internal:15692/metrics`` via the
   ``rabbitmq_prometheus`` plugin. Port 15692 is published on the host by
   the ``rabbitmq`` service in ``docker-compose.yml`` and
   ``docker-compose.bundle.yml``. This job powers the queue-depth dashboard
   (``rabbitmq_queue_messages_ready``,
   ``rabbitmq_queue_messages_unacknowledged``, ``rabbitmq_queue_consumers``,
   and the publish/deliver rate counters).

``postgres``
   Scrapes ``host.docker.internal:9187/metrics`` from the
   ``prometheuscommunity/postgres-exporter`` sidecar in
   ``docker-compose.monitoring.yml``. This job powers the server-side panels
   of the db-connections dashboard (``pg_stat_activity_count``,
   ``pg_stat_database_xact_commit/rollback``,
   ``pg_stat_activity_max_tx_duration``,
   ``pg_stat_database_deadlocks``).

Only the API process serves ``/metrics``. The queue workers do not open
their own metrics port, so worker-emitted counters surface through the
shared API registry rather than a per-worker scrape target. There is no
separate worker scrape job by design.


Verifying metrics reach Prometheus
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Confirm the API is up and serving metrics on the host:

   .. code-block:: bash

      curl -sf http://localhost:8000/metrics | head -20

   The output should be Prometheus text exposition lines such as
   ``# HELP protea_jobs_total ...``. A connection refused here means the
   PROTEA application stack is not running; Prometheus has nothing to
   scrape until it is.

2. Check the scrape target health from Prometheus:

   .. code-block:: bash

      curl -sf 'http://localhost:9090/api/v1/targets' \
          | grep -o '"health":"[a-z]*"'

   The ``protea-api`` target should report ``"health":"up"``. ``down``
   with a ``connection refused`` last-error means Prometheus cannot reach
   the host API; see Troubleshooting.

3. Open Grafana at http://localhost:3001 and confirm the
   ``PROTEA / API latency`` (and other metric) dashboards populate
   within a couple of scrape intervals (15s each).


Reloading the scrape config without a restart
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The container enables the lifecycle API (the ``web.enable-lifecycle``
flag in the compose ``command``), so an edited
``deploy/prometheus/prometheus.yml`` can be hot-reloaded:

.. code-block:: bash

   curl -X POST http://localhost:9090/-/reload

Validate the file before reloading if ``promtool`` is available:

.. code-block:: bash

   promtool check config deploy/prometheus/prometheus.yml


Troubleshooting
~~~~~~~~~~~~~~~

**Dashboards still show "No data" with Prometheus running**

First confirm the application stack is up (step 1 above). Prometheus
only scrapes; it emits no ``protea_*`` series of its own. With no API
running, every ``protea-api`` panel is empty even though Prometheus is
healthy.

**protea-api target is down with "connection refused"**

Prometheus reaches the host API through ``host.docker.internal``. On
this host the mapping is provided by the ``extra_hosts:
host.docker.internal:host-gateway`` entry on the prometheus service. If
the target is down, confirm the API listens on ``0.0.0.0:8000`` (not
``127.0.0.1``) so the docker bridge gateway can reach it, and that no
host firewall blocks the bridge subnet.

**Datasource error in Grafana ("bad gateway" / "no such host")**

The ``protea-prometheus`` datasource url is
``http://host.docker.internal:9090``. Grafana resolves that via its own
``extra_hosts`` host-gateway entry. If you removed the published 9090
port or renamed the container, point the datasource at
``http://prometheus:9090`` (the in-network service name) instead and
restart Grafana.

**queue-depth panels still empty after bring-up**

Confirm the RabbitMQ container is running and port 15692 is reachable
from the host:

.. code-block:: bash

   curl -sf http://localhost:15692/metrics | head -5

If this times out, the container may not have the plugin enabled. Check
that ``docker/rabbitmq/enabled_plugins`` was mounted into the container
and that the file contains ``rabbitmq_prometheus``. A container restart
is required after mounting the file for the first time.

**db-connections "server-side" panels (pg_* series) empty**

Confirm the postgres-exporter sidecar is running:

.. code-block:: bash

   curl -sf http://localhost:9187/metrics | head -5

If it is not running, bring the monitoring stack up again:

.. code-block:: bash

   docker compose -f docker-compose.monitoring.yml up -d postgres-exporter

If the exporter starts but immediately exits, check its logs for a
connection-refused error. The ``DATA_SOURCE_NAME`` defaults to
``postgresql://protea:protea@host.docker.internal:5432/protea?sslmode=disable``.
Override via ``POSTGRES_EXPORTER_DATA_SOURCE_NAME`` in the environment if
the dev Postgres instance uses different credentials.


Operational notes
~~~~~~~~~~~~~~~~~~

**Retention**

The compose ``command`` sets the TSDB retention to 15 days (the
``storage.tsdb.retention.time`` flag). The TSDB lives on the
``prometheus_data`` named volume. Raise the flag value for
deployments that need more history, or front Prometheus with a remote
write target (Thanos, Mimir, Cortex) for long-term storage; that
migration is out of scope here.

**Auth**

Prometheus ships with no authentication. Keep port 9090 off the public
internet. A reverse proxy with basic auth is the simplest hardening
step, mirroring the Loki guidance.


See also
~~~~~~~~~

- :doc:`/adr/D07-observability-stack` for the rationale behind the
  observability stack choice.
- :doc:`loki` for the log-aggregation side of the same monitoring stack.
- :doc:`observability` for the OpenTelemetry tracing side of the stack.
- ``deploy/prometheus/prometheus.yml`` for the committed scrape config.
- ``deploy/grafana/provisioning/datasources/prometheus.yml`` for the
  Grafana datasource (uid ``protea-prometheus``).
- ``protea/api/routers/metrics.py`` and
  ``protea/infrastructure/telemetry.py`` for the metrics endpoint and
  the collector registry.
