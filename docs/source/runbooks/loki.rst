Observability: Loki log aggregation
====================================

PROTEA emits structured JSON log lines through
``protea.infrastructure.logging.configure_logging`` and ships them to
Loki via the ``loki-docker-driver`` plugin (T5.4, ADR-D7). This runbook
covers the one-time host setup that turns container stdout into a
searchable log stream inside the Grafana monitoring stack.

Loki itself is a container that runs alongside Grafana; see
``docker-compose.monitoring.yml`` for the service definition. The
plugin is what gets installed on the host's docker daemon and what each
application service opts into via a ``logging:`` block.

.. note::

   The Loki HTTP API push endpoint is exposed at
   ``/loki/api/<api-version>/push`` (the current Loki release exposes
   API version 1). Throughout this page the placeholder
   ``${LOKI_PUSH_URL}`` stands in for the full URL, for example
   ``http://localhost:3100/loki/api/1/push`` after substituting the
   actual API version into the path. Resolve the placeholder against
   the running Loki container's ``/loki/api/<version>/`` route before
   copy-pasting any compose or shell snippet.

.. contents:: On this page
   :local:
   :depth: 2


How the pipeline fits together
-------------------------------

PROTEA runs as a process-based stack (uvicorn API, worker processes,
Next.js frontend) that writes plaintext log files into ``logs/`` under
the deploy working directory.  A ``promtail`` sidecar container in
``docker-compose.monitoring.yml`` tails those files and ships each line
to Loki.  This replaces the earlier docker-driver approach, which only
works when the application itself runs as a Docker container.

::

   uvicorn / workers / next.js
      |  write log lines to ./logs/*.log
      v
   promtail container  (tails ./logs/*.log, bind-mounted read-only)
      |  labels: compose_project=protea, service_name=<stem>
      v
   loki:3100          (monitoring compose, protea_monitoring network)
      |
      v
   Grafana            (PROTEA Loki datasource, "PROTEA / Logs" dashboard)

The docker-driver plugin path documented further below remains valid for
deployments where the application runs inside containers; it is not the
active path on the current process-based dev stack.

The application code does not need to know anything about Loki. As long
as ``configure_logging(json=True)`` ran at process startup (the default
for the API and every worker) the lines on stdout are already in the
schema the dashboard expects.


Starting the Loki container
----------------------------

Loki runs inside ``docker-compose.monitoring.yml``. Bring up the full
monitoring stack from the repo root:

.. code-block:: bash

   docker compose -f docker-compose.monitoring.yml up -d
   curl -sf http://localhost:3100/ready && echo "loki ready"

The container exposes 3100 on the host so the docker driver (which runs
in the host's daemon namespace, not in this compose project) can push
to it. Grafana reaches the same Loki container by service name on the
``protea_monitoring`` bridge network.


Promtail: tailing process-stack log files
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``promtail`` service defined in ``docker-compose.monitoring.yml``
handles log shipping for the process-based PROTEA stack.  No additional
host setup is required; promtail starts automatically as part of
``docker compose -f docker-compose.monitoring.yml up -d``.

**What files are tailed**

Promtail watches ``/var/log/protea/*.log`` inside the container, which
corresponds to ``./logs/*.log`` relative to the monitoring compose working
directory (the deploy root).  The current process-stack writes:

- ``logs/api.log`` (uvicorn FastAPI process)
- ``logs/worker-jobs.log``
- ``logs/worker-embeddings.log``
- ``logs/worker-embeddings-batch.log``
- ``logs/worker-embeddings-write.log``
- ``logs/worker-predictions.log``
- ``logs/worker-predictions-batch.log``
- ``logs/worker-predictions-write.log``
- ``logs/worker-training.log``
- ``logs/worker-reaper.log``
- ``logs/worker-ping.log``
- ``logs/frontend.log`` (Next.js dev server)

Any additional ``*.log`` file dropped into ``logs/`` is picked up
automatically on the next tail cycle; no promtail restart is needed.

**Labels injected**

``compose_project=protea`` is a static label applied to every line.
``service_name`` is derived from the log file stem, for example
``api.log`` produces ``service_name=api`` and ``worker-jobs.log``
produces ``service_name=worker-jobs``.  These two labels are exactly
what the ``PROTEA / Logs (Loki)`` Grafana dashboard filters on.

**Configuration file**

``deploy/promtail/promtail.yml`` is mounted read-only into the container
at ``/etc/promtail/promtail.yml``.  The scrape job uses a
``pipeline_stages`` regex to extract ``service_name`` from the ``filename``
label that promtail sets from the glob pattern, then promotes it to a
Loki stream label via the ``labels:`` stage.

**Applying changes**

Re-run the monitoring compose to pick up any config edit:

.. code-block:: bash

   docker compose -f docker-compose.monitoring.yml up -d promtail

Promtail stores its tail positions in ``/tmp/promtail-positions.yaml``
inside the container.  That file is ephemeral and lost on container
restart; promtail re-tails from the last-seen position on a best-effort
basis using inode matching when the position file is missing.


Installing the docker driver plugin
------------------------------------

The plugin is a one-off host install. Run on every host that runs
PROTEA containers:

.. code-block:: bash

   docker plugin install grafana/loki-docker-driver:3.3.2 \
       --alias loki --grant-all-permissions
   docker plugin ls   # verify "loki" appears, status ENABLED

Upgrading the plugin later requires disabling it first:

.. code-block:: bash

   docker plugin disable loki --force
   docker plugin upgrade loki grafana/loki-docker-driver:<new-version> \
       --grant-all-permissions
   docker plugin enable loki


Opting an application service into Loki
----------------------------------------

Add a ``logging:`` block to any service in ``docker-compose.yml`` that
should ship logs. The minimum useful set is the API and all workers:

.. code-block:: yaml

   services:
     api:
       # ... existing config ...
       logging:
         driver: loki
         options:
           loki-url: "${LOKI_PUSH_URL}"
           loki-retries: "5"
           loki-batch-size: "400"
           mode: non-blocking
           max-buffer-size: 4m
           loki-pipeline-stages: |
             - json:
                 expressions:
                   level: level
                   logger: logger
             - labels:
                 level:

     worker-jobs:
       # ... existing config ...
       logging:
         driver: loki
         options:
           loki-url: "${LOKI_PUSH_URL}"
           loki-retries: "5"
           mode: non-blocking
           max-buffer-size: 4m
           loki-pipeline-stages: |
             - json:
                 expressions:
                   level: level
             - labels:
                 level:

Restart the affected services for the new logging driver to take effect
(``docker compose up -d`` is enough, the driver is applied on container
re-creation). Use ``mode: non-blocking`` so a paused Loki cannot stall
the application container's stdout pipe.

The ``loki-pipeline-stages`` block parses the JSON line that
``JSONFormatter`` produces and promotes the ``level`` field to a Loki
label so panels can filter on ``{level="ERROR"}`` without a full text
match.


Verifying logs reach Loki
--------------------------

1. Generate a log line. The API logs on every request:

   .. code-block:: bash

      curl -sf http://localhost:8000/health > /dev/null

2. Query Loki directly:

   .. code-block:: bash

      # ${LOKI_QUERY_URL} stands in for
      # http://localhost:3100/loki/api/<api-version>/query_range
      curl -sG "${LOKI_QUERY_URL}" \
          --data-urlencode 'query={compose_project="protea"}' \
          --data-urlencode 'limit=1' | head -200

   The response should contain a ``data.result`` array with at least one
   stream. An empty array means the driver is not pushing; jump to
   "Troubleshooting" below.

3. Open the ``PROTEA / Logs (Loki)`` dashboard at
   http://localhost:3001 and confirm that the log stream panel
   populates within the last few seconds.


Troubleshooting
---------------

**Driver not installed**

.. code-block:: bash

   docker plugin ls

If ``loki`` is missing or DISABLED the application containers will fail
to start with ``Error response from daemon: error looking up logging
plugin loki: plugin "loki" not found``. Reinstall per the install
section above.

**Loki not reachable from the driver**

The plugin talks to ``http://localhost:3100`` on the host. If Loki is
behind a host firewall or running on a different machine, override
``loki-url`` to a reachable address. From inside the application
container the address is irrelevant; the driver runs in the host
daemon, not the container.

.. code-block:: bash

   curl -sf http://localhost:3100/ready

A ``404`` means the URL is wrong; ``ready`` is the only public health
endpoint. ``Connection refused`` points to a stopped or unhealthy Loki
container.

**No logs for one specific service**

The driver applies per service. Check that the ``logging:`` block is
present and the container has been re-created since it was added:

.. code-block:: bash

   docker inspect <container> --format '{{.HostConfig.LogConfig.Type}}'

The output must read ``loki``. ``json-file`` means the driver was never
applied to this container.

**Log lines reach Loki but the dashboard shows "No data"**

The dashboard filters on ``compose_project="protea"``. The
loki-docker-driver injects this label automatically when the
application stack is started through ``docker compose``. If the API or
workers were started ``docker run`` directly, the label is missing.
Either restart them through compose or edit the dashboard expression to
match the labels your driver injects (``docker inspect`` on a log
stream shows them).

**Driver buffer pressure during a Loki outage**

``mode: non-blocking`` drops the oldest log lines when ``max-buffer-size``
is exceeded rather than blocking the application's stdout pipe. The
trade-off is intentional: PROTEA's API and workers must never stall on
the telemetry stack. If lost lines during a Loki outage are
unacceptable for a given deployment, switch the affected service to
``mode: blocking`` and accept that a wedged Loki will wedge the
application.


Operational notes
------------------

**Retention**

``deploy/loki/loki-config.yml`` sets ``retention_period: 168h`` (7 days)
as a sane default. Override at the loki container level (env var or a
local mounted config override) for deployments that need more history.

**Filesystem storage**

The default config uses Loki's filesystem object store backed by the
``loki_data`` named volume. This is fine for local-host and small
single-tenant deployments; for cloud production deployments the
recommended path is to swap the store for S3 (or compatible) and run
Loki in microservices mode. That migration is out of scope for T5.4.

**Auth**

``auth_enabled: false`` means anyone with network access to port 3100
can push or read. Keep the port off the public internet. A reverse
proxy with basic auth is the simplest hardening step.


**agent-farm heartbeat alerting (FARM-UI.7)**

The same Loki / Grafana stack also carries the agent-farm tmux session's
heartbeat events (``state/tasks.sqlite`` events table, mirrored to Loki
by the sidecar in ``scripts/tail-events-to-loki.py``). Each heartbeat
ships as a JSON line with labels ``compose_project="agent-farm"``,
``agent_name``, ``task_id`` and field ``level`` (info, warn, error).
Three artefacts live under ``deploy/``:
``deploy/grafana/dashboards/agent-farm-heartbeats.json`` is the
Grafana 11.3 dashboard with a live heartbeat stream panel, per-agent
error timeseries, and a per-task error table that links into
``/en/farm/<task_id>/``.
``deploy/grafana/provisioning/alerting/rules.yml`` is the
unified-alerting rule (3 or more error-level heartbeats per task in a
5-minute window, evaluated every minute, in the ``agent-farm`` folder).
``deploy/grafana/provisioning/alerting/contact-points.yml`` and
``policies.yml`` provision the Slack contact point keyed on
``${SLACK_WEBHOOK_URL}`` plus a notification policy that routes any
alert with label ``service=agent-farm`` to that point. The policy sets
``repeat_interval: 30m`` so a sustained burst produces one Slack
message immediately then at most one follow-up per half hour.

Operator setup is one line:

.. code-block:: bash

   export SLACK_WEBHOOK_URL='https://hooks.slack.com/services/...'
   docker compose -f docker-compose.monitoring.yml up -d

When the variable is unset, Grafana boots cleanly; the contact point
delivers to an empty URL (no 4xx storm) until the operator drops the
secret in. Test plumbing without involving Slack by pointing
``SLACK_WEBHOOK_URL`` at ``https://httpbin.org/post`` and inspecting the
response, or at a local ``nc -l 9999`` listener.


See also
---------

- :doc:`/adr/D07-observability-stack` for the rationale behind picking
  Loki over the ELK stack.
- :doc:`observability` for the OpenTelemetry side of the stack (traces).
- ``deploy/grafana/dashboards/logs.json`` for the source of truth of
  the Loki dashboard.
- ``deploy/grafana/dashboards/agent-farm-heartbeats.json`` for the
  agent-farm heartbeat dashboard (FARM-UI.7).
- ``deploy/grafana/provisioning/alerting/`` for the Slack alert rule
  and contact-point provisioning (FARM-UI.7).
- ``deploy/loki/loki-config.yml`` for the Loki single-binary configuration.
