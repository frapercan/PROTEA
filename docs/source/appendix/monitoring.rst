Monitoring
==========

PROTEA ships an optional monitoring stack that captures lightweight,
privacy-respecting visitor analytics from the live frontend. The stack
is opt-in: it lives in a separate ``docker-compose.monitoring.yml``
file so the production deploy does not pay for a Grafana container
that nobody is reading.

.. contents:: Contents
   :local:
   :depth: 2


What gets measured
------------------

The :class:`~protea.api.middleware.visitor_counter.VisitorCounterMiddleware`
records one row in the ``visitor_event`` table per **user-visible**
request. It deliberately ignores polling traffic, asset requests, health
probes and metrics scrapes; see ``_should_record`` for the full filter
list.

Recorded fields
~~~~~~~~~~~~~~~

Each row stores:

- ``ts``: UTC timestamp of the request
- ``visitor_hash``: 16-byte hex digest derived from the client IP
  (see "Privacy design" below)
- ``method``: HTTP method (mostly ``GET``, sometimes ``POST``)
- ``path``: request path with the ASGI root prefix stripped
- ``status_code``: HTTP response status
- ``duration_ms``: wall-clock latency, populated by the middleware
- ``user_agent_short``: browser family only (no full UA string)

Privacy design
~~~~~~~~~~~~~~

The middleware never persists IP addresses, cookies or full
user agents. The visitor identifier is::

    visitor_hash = sha256(daily_salt || client_ip)[:16]

where ``daily_salt`` is a 32-byte random value held only in process
memory and rotated on every UTC calendar day. When the day rolls over
the previous salt is discarded, so cross-day correlation becomes
cryptographically infeasible (the same rotating-salt approach used by
Plausible and Fathom).

Day-bounded uniqueness is enough to compute "unique visitors per day",
"page views per day" and "top paths" without any storage of personal
data.


Bringing the stack up
---------------------

Grafana and Loki live in their own Compose file so that bringing them
up does not disturb the application stack:

.. code-block:: bash

   docker compose -f docker-compose.monitoring.yml up -d
   open http://localhost:3001    # admin / admin on first login

The Grafana container reaches Postgres through the host gateway
(``host.docker.internal:5432``), which works as long as the application
stack publishes its port (the default in ``docker-compose.yml``).
``protea-postgres-1`` does this, but a deployment using the
``docker-compose.prod.yml`` profile may keep Postgres on an internal
network. In that case extend the monitoring stack with a shared
network instead of relying on the host gateway.

The compose file also provisions a Loki container on the same
``protea_monitoring`` bridge network. Grafana reaches it at
``http://loki:3100``; the host port 3100 is published so the
``loki-docker-driver`` plugin (which runs in the docker daemon, not in
any compose project) can push from the application containers. See
:doc:`/runbooks/loki` for the plugin install and the per-service
``logging:`` block that opts api / worker containers into shipping
their structured JSON log lines.

Stopping is symmetric:

.. code-block:: bash

   docker compose -f docker-compose.monitoring.yml down


Provisioning
------------

Grafana auto-loads its datasource and dashboards from the
``deploy/grafana/`` directory at startup; nothing needs to be clicked
in the UI to see the visitor dashboard.

::

    deploy/grafana/
    ├── dashboards/
    │   ├── visitors.json                 # exported dashboard JSON
    │   └── logs.json                     # Loki-backed log stream + rates
    └── provisioning/
        ├── dashboards/dashboards.yml      # registers the dashboards/ folder
        └── datasources/
            ├── postgres.yml               # registers the protea Postgres source
            ├── prometheus.yml             # registers the protea Prometheus source
            └── loki.yml                   # registers the protea Loki source

Editing a panel in the UI is fine for exploration but is not
persisted. To make a change permanent: edit the panel, **Dashboard
Settings → JSON Model**, copy the JSON back into
``deploy/grafana/dashboards/visitors.json`` and commit.


The visitor dashboard
---------------------

``PROTEA: Visitor analytics`` ships with seven panels keyed off
``visitor_event``:

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Panel
     - Question it answers
   * - Unique visitors today
     - How many distinct browsers reached the platform since 00:00 UTC?
   * - Page views today
     - How many recorded requests since 00:00 UTC?
   * - Visitor-days in range
     - In the dashboard's date range, how many ``(date, visitor)``
       combinations occurred?
   * - Unique visitors per day
     - Daily distinct ``visitor_hash`` count, useful as a long-running
       traffic trend.
   * - Page views per day
     - Daily total recorded requests; ratio to "unique visitors per day"
       gives an average pages-per-visit estimate.
   * - Top paths
     - Most-visited paths in the dashboard's date range.
   * - Status codes over time
     - Sanity check for spikes of 4xx / 5xx that visitor traffic surfaces.

All panels run plain SQL against ``visitor_event``; tweak them in the
JSON or fork the dashboard to add your own.


Adding new metrics
------------------

The visitor table is intentionally narrow. Adding new metrics that
fit the same shape is a Postgres-only change:

1. Edit the panel SQL (or add a new panel) directly in the dashboard
   JSON under ``deploy/grafana/dashboards/visitors.json``.
2. Restart Grafana so the provisioning picks up the change:

   .. code-block:: bash

      docker compose -f docker-compose.monitoring.yml restart grafana

3. Commit the dashboard JSON to the PROTEA repo.

For metrics that go beyond visitor traffic (queue depth, prediction
throughput, embedding GPU memory) the canonical stack is OpenTelemetry
for traces, Prometheus for metrics, and Loki for logs, all surfaced in
the same Grafana instance (ADR-D7, see
:doc:`/adr/D07-observability-stack`). The Loki side is set up in
:doc:`/runbooks/loki`; the OpenTelemetry side in :doc:`/runbooks/observability`.
Ad-hoc panels against ``visitor_event`` remain the simplest way to
surface a SQL-based metric.
