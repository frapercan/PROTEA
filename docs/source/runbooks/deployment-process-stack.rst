Process-Based Stack Deployment Guide
=====================================

This runbook covers the production deployment model used on the
development host: a process-based stack (uvicorn + workers + Next.js
standalone + ngrok) supervised by deploy-keeper, with Docker used only
for the infrastructure layer (Postgres, RabbitMQ, MinIO, Grafana, Loki).
It complements the container-centric modes documented in
:doc:`deployment` (docker-compose dev, Swarm, Helm).

.. contents:: On this page
   :local:
   :depth: 2


Architecture overview
~~~~~~~~~~~~~~~~~~~~~

**Supervisor layer** (bash, zero LLM cost):

- ``deploy-keeper-supervisor.sh`` runs in tmux, polling ``origin/develop`` every 5 min.
- On a new SHA or ngrok outage it calls ``scripts/deploy.sh``, which updates
  the ``protea-deploy`` worktree and calls ``manage.sh start``.
- Before each tick it checks ``stack-owner.json``; when ``owner=export`` it defers.

**Application layer** (process-based, started by ``manage.sh start``):

- ``alembic upgrade head`` (runs before any process starts)
- uvicorn API on port 8000
- worker-ping (``protea.ping`` queue)
- worker-jobs (``protea.jobs`` queue)
- worker-training (``protea.training`` queue)
- worker-embeddings-coord / worker-embeddings-batch x N / worker-embeddings-write
- worker-predictions-coord / worker-predictions-batch x N / worker-predictions-write
- worker-evaluations (``protea.evaluations`` queue)
- worker-reaper (``reaper`` queue)
- Next.js standalone on port 3000 (``node .next/standalone/server.js``)

**Tunnel:** ``scripts/expose.sh`` runs ngrok, mapping port 3000 to
``https://protea.ngrok.app``.

**Infrastructure containers** (Docker only):

- ``docker compose -f docker-compose.yml`` for Postgres, RabbitMQ, MinIO.
- ``docker compose -f docker-compose.monitoring.yml`` for Grafana (:3001),
  Loki (:3100), Prometheus (:9090).

The Next.js frontend is built in production mode (``npm run build``) at
each deploy. ``manage.sh`` copies ``apps/web/.next/static`` and
``apps/web/public/`` into the standalone tree before launching
``node server.js``, so the ``/api-proxy`` reverse-proxy rewrite in
``apps/web/next.config.ts`` routes browser API calls to ``localhost:8000``
without a second tunnel.


Secrets and the ``.env`` file
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The canonical secret store lives at ``~/.secrets/protea.env``
(``chmod 600``, outside any git repository). Both
``~/Thesis2/repositories/PROTEA/.env`` and
``~/Thesis2/worktrees/protea-deploy/.env`` are symlinks to that file.
Editing ``~/.secrets/protea.env`` propagates atomically to both trees.

``manage.sh start`` now sources its own env (step ``[0] Environment``):
it loads ``$ROOT/.env`` then ``$ROOT/.env.local`` with ``set -a`` so a
naive ``bash scripts/manage.sh start`` no longer aborts the API on a
missing ``PROTEA_JWT_SECRET`` (``AUTHN_REQUIRED`` defaults to ``true``,
which makes the JWT secret a required variable). Sourcing the bundle
manually beforehand is still safe and remains a no-op when both files are
absent (CI, fresh clones):

.. code-block:: bash

   bash scripts/manage.sh start              # self-sources .env + .env.local
   # equivalent explicit form (still valid):
   set -a && source ~/.secrets/protea.env && set +a
   bash scripts/manage.sh start

Key variables required at runtime:

.. list-table::
   :header-rows: 1
   :widths: 36 64

   * - Variable
     - Purpose
   * - ``PROTEA_DB_URL``
     - SQLAlchemy connection URL (``postgresql+psycopg://...``).
   * - ``PROTEA_AMQP_URL``
     - AMQP URL for RabbitMQ (``amqp://...``).
   * - ``JWT_SECRET``
     - Required when ``AUTHN_REQUIRED=true`` (the default).
   * - ``PROTEA_STORAGE_BACKEND``
     - ``local`` or ``minio`` (default ``local``).
   * - ``PROTEA_ANC2VEC_PATH``
     - Absolute path to ``anc2vec_2020-10.npz``. Falls back to
       ``artifacts/anc2vec/anc2vec_2020-10.npz`` relative to the
       working directory. Required by ``export_research_dataset``;
       absence causes ``FileNotFoundError`` ~33 min into an export job.
   * - ``PROTEA_OTEL_ENABLED``
     - Set to ``true`` to enable distributed tracing (default ``false``).

For MinIO storage add ``PROTEA_MINIO_ENDPOINT``, ``PROTEA_MINIO_BUCKET``,
``PROTEA_MINIO_ACCESS_KEY``, ``PROTEA_MINIO_SECRET_KEY``.


Bootstrap on a fresh machine
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

One-shot bring-up (powered-off box)
"""""""""""""""""""""""""""""""""""

Once the deploy worktree and secrets exist (the one-time setup in steps 2
and 3 below), every later power-on is a single command:

.. code-block:: bash

   bash scripts/cold-boot.sh            # full: infra + deploy.sh + ngrok
   bash scripts/cold-boot.sh --fast     # nothing changed: skips deps + build
   bash scripts/cold-boot.sh --no-ngrok # local only, no public tunnel

``cold-boot.sh`` starts the infra containers, delegates the whole app
bring-up to ``scripts/deploy.sh`` (sync ``origin/develop`` + ``poetry
install`` + GPU torch flip + frontend/Sphinx build + ``manage.sh start`` +
health wait), then launches the ngrok tunnels detached. ``--fast`` forwards
``--no-deps --no-build`` to ``deploy.sh`` for a warm reboot where neither
dependencies nor the frontend changed since the last boot; the on-disk
``.venv`` (with its GPU torch) persists across reboots, so the heavy
reinstall is unnecessary then. Any extra argument is passed straight to
``deploy.sh`` (for example a git ref to deploy instead of ``origin/develop``).

The manual breakdown below documents each step ``cold-boot.sh`` automates;
run it directly only for first-time host setup or to debug one stage.

**Step 1: start the infrastructure containers**

.. code-block:: bash

   # From the PROTEA repo root or the protea-deploy worktree.
   docker compose -f docker-compose.yml up -d postgres rabbitmq minio

Verify readiness:

.. code-block:: bash

   pg_isready -h localhost -p 5432 -U protea -d protea
   curl -sf http://localhost:15672  # RabbitMQ management UI

**Step 2: create the deploy worktree (once per host)**

.. code-block:: bash

   git -C ~/Thesis2/repositories/PROTEA fetch origin
   git -C ~/Thesis2/repositories/PROTEA worktree add \
       ~/Thesis2/worktrees/protea-deploy \
       -b feat/deploy-tooling \
       origin/develop

**Step 3: symlink secrets**

.. code-block:: bash

   ln -sf ~/.secrets/protea.env \
       ~/Thesis2/worktrees/protea-deploy/.env

**Step 4: install Python + Node dependencies**

.. code-block:: bash

   cd ~/Thesis2/worktrees/protea-deploy
   poetry install
   cd apps/web && npm ci && cd ../..

**Step 5: start the PROTEA stack**

.. code-block:: bash

   cd ~/Thesis2/worktrees/protea-deploy
   set -a && source ~/.secrets/protea.env && set +a
   bash scripts/manage.sh start

``manage.sh start`` runs ``alembic upgrade head`` automatically before
launching any process. On the very first boot ``init_db.py`` is not
required: Alembic creates all tables from the migration history.

**Step 6: open the ngrok tunnel**

.. code-block:: bash

   cd ~/Thesis2/worktrees/protea-deploy
   bash scripts/expose.sh

The tunnel binds ``https://protea.ngrok.app`` to port 3000.

**Step 7: start the monitoring stack**

.. code-block:: bash

   docker compose -f docker-compose.monitoring.yml up -d

Grafana is then available at ``http://localhost:3001``.


manage.sh reference
~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   bash scripts/manage.sh start [N]
   bash scripts/manage.sh stop
   bash scripts/manage.sh status
   bash scripts/manage.sh logs [name]
   bash scripts/manage.sh scale <queue> [N]
   bash scripts/manage.sh health
   bash scripts/manage.sh watch

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Command
     - Description
   * - ``start [N]``
     - Stop any survivors, run ``alembic upgrade head``, start the full
       process set. ``N`` sets the number of ``embeddings.batch`` and
       ``predictions.batch`` workers (default 1).
   * - ``stop``
     - Send SIGTERM to all tracked processes (via PID files in
       ``logs/pids/``). API and frontend receive SIGKILL (safe; no
       in-flight jobs). Workers receive SIGTERM so long-running jobs
       (e.g. ``run_cafa_evaluation``) finish their current page commit
       before exiting.
   * - ``status``
     - Print a table of all tracked workers (name, PID, RSS in MB,
       running/dead).
   * - ``logs [name]``
     - Without ``name``: interactive picker. With ``name``: tail that
       process log directly (e.g. ``bash scripts/manage.sh logs api``).
   * - ``scale <queue> [N]``
     - Add ``N`` extra workers to an existing queue without restarting
       anything. Example: ``bash scripts/manage.sh scale protea.predictions.batch 2``.
   * - ``health``
     - One-shot probe + heal. Sources env, restarts the API if
       ``/health`` is down, and restarts any tracked worker whose PID is
       dead by replaying its recorded ``logs/pids/<name>.cmd``. Idempotent
       (live workers are left alone, never duplicated). Safe to run from
       cron. Always exits 0.
   * - ``watch``
     - Supervised self-heal loop: runs ``health`` every
       ``WATCH_INTERVAL`` seconds (default 30) until killed, logging to
       ``logs/watchdog.log``. Refuses to start a second watchdog while one
       is already live (``logs/pids/watchdog.pid``). Run under ``nohup`` /
       tmux / a systemd user unit for a persistent watchdog.

**Self-healing the stack:**

The stack self-recovers from process death without a full bounce:

.. code-block:: bash

   # background watchdog (probes API + workers every 30 s)
   nohup bash scripts/manage.sh watch >/dev/null 2>&1 &

   # or a one-shot heal from cron / a manual check
   bash scripts/manage.sh health

The watchdog only restarts the API (via its ``/health`` probe) and
*dead* tracked workers. It never touches a worker whose PID is still
alive, so an in-flight export or prediction job is never interrupted.
The frontend is intentionally left to the deploy-keeper / ``deploy.sh``
rebuild path because restarting it needs a fresh standalone build, not a
bare re-exec.

**Hardened behaviours in the current version:**

- ``start`` self-sources ``.env`` + ``.env.local`` at step [0] so a
  naive caller never crashes the API on a missing ``PROTEA_JWT_SECRET``.
- ``_start_bg`` records each worker's launch argv in
  ``logs/pids/<name>.cmd`` so ``health`` / ``watch`` can replay the exact
  invocation when a worker dies.

- The API readiness check at step [3] of ``start`` waits up to 120 s
  (was 3 s before PR #470) before declaring failure and exiting.
- The standalone asset copy (``cp -r .next/static`` and
  ``cp -r public/``) verifies the ``STANDALONE_DIR`` exists before
  attempting the copy; a missing directory triggers a fallback to
  ``next start`` with a logged warning.
- An untracked worker process (one started by a previous ``scale``
  call outside the PID registry) is left running by ``stop`` to avoid
  interrupting long jobs.

PID files live in ``logs/pids/<name>.pid``. Log files live in
``logs/<name>.log``. Both directories are created on first ``start``.


Deploy-keeper supervisor
~~~~~~~~~~~~~~~~~~~~~~~~~

The deploy-keeper supervisor is a pure-bash process (zero LLM cost)
that keeps ``https://protea.ngrok.app`` serving the HEAD of
``origin/develop``. It polls the remote every 5 minutes and
re-deploys when the SHA advances or when the ngrok tunnel goes down.

**Architecture:**

- ``agent-farm/scripts/services/deploy-keeper-supervisor.sh`` is the
  outer loop. It calls ``deploy-keeper-tick.sh`` on every poll
  interval and on configured triggers.
- ``deploy-keeper-tick.sh`` is one atomic tick: check the stack-owner
  lock, verify prereqs (Docker, Postgres, RabbitMQ), call
  ``scripts/deploy.sh`` to update the worktree and restart the stack,
  verify the ngrok tunnel is live.
- On a non-prereq tick failure the supervisor runs a quick-retry
  ladder (a few pure-bash ticks). Only after the ladder is exhausted
  does it escalate to a janitor subagent via
  ``scripts/spawn-subagent.sh``.
- Prereq failures (e.g. Docker daemon not responding) are logged and
  the supervisor backs off; the operator is the recovery path for
  daemon-level issues.

The supervisor runs inside a dedicated tmux session. To start it:

.. code-block:: bash

   cd ~/Thesis2/agent-farm
   TASK_ID=deploy-keeper bash scripts/services/deploy-keeper-supervisor.sh

To stop it, kill the tmux pane or run
``bash agent-farm/scripts/kill.sh deploy-keeper``.

**Triggers** (fire an immediate tick without waiting for the poll
interval):

- ``new_commit_on:origin/develop``: git fetch detects a new SHA.
- ``ngrok_tunnel_down``: a probe to ``https://protea.ngrok.app`` fails.
- ``manual``: write a marker file via
  ``bash agent-farm/scripts/services/deploy-keeper-trigger.sh``.


Stack-owner lock
~~~~~~~~~~~~~~~~

The stack-owner lock prevents deploy-keeper from restarting the stack
while a long export pipeline (``export_research_dataset`` jobs) is
in flight. Without the lock, a commit landing on ``origin/develop``
mid-export would trigger a blind ``manage.sh start``, killing all
workers and corrupting the in-flight run.

**Lock file:** ``~/Thesis2/agent-farm/state/stack-owner.json``

**Helper:** ``~/Thesis2/agent-farm/scripts/lib/stack_owner.sh``

JSON shape:

.. code-block:: json

   {
     "owner": "export",
     "task_id": "farm-exp-13",
     "acquired_at": "2026-05-20T14:00:00+00:00",
     "reason": "FARM-EXP.13 24-cell sweep"
   }

Valid owner values: ``free`` (nobody holds the lock), ``deploy``
(deploy-keeper holds the lock for its own tick), ``export`` (a long
export pipeline holds the lock; deploy-keeper defers).

**Shell API:**

.. code-block:: bash

   source ~/Thesis2/agent-farm/scripts/lib/stack_owner.sh

   stack_owner_current                            # prints: free | deploy | export
   stack_owner_status                             # prints full JSON record

   stack_owner_acquire export farm-exp-13 "FARM-EXP.13 sweep"
   # ... export pipeline runs ...
   stack_owner_release farm-exp-13

**CLI form:**

.. code-block:: bash

   bash ~/Thesis2/agent-farm/scripts/lib/stack_owner.sh acquire export farm-exp-13 "reason"
   bash ~/Thesis2/agent-farm/scripts/lib/stack_owner.sh current
   bash ~/Thesis2/agent-farm/scripts/lib/stack_owner.sh release farm-exp-13

Exit codes: 0 success, 2 contention (different owner holds), 3 release
mismatch, 4 flock timeout (>5 s; the holder is stuck or has crashed).

Acquire is idempotent: re-acquiring the same ``owner+task_id`` pair is
a no-op and returns 0. Release refuses if the caller's ``task_id`` does
not match the current holder; a stale supervisor cannot trample a live
export.

When ``owner=export`` the deploy-keeper tick exits 0 ("noop") and
sleeps the full poll interval. It logs a heartbeat line noting the
export task ID. The lock is advisory: an operator can force a redeploy
by setting ``owner=free`` manually, but this risks an interrupted
export.

FARM-FEAT.13 (in flight) adds a conductor-side wrapper that
automatically acquires the lock before dispatching a multi-cell export
sweep and releases it when all cells reach ``SUCCEEDED``.


Ngrok tunnel
~~~~~~~~~~~~~

The public demo endpoint ``https://protea.ngrok.app`` is a static ngrok
domain that tunnels to the Next.js frontend on port 3000. API calls
from the browser go through the Next.js reverse proxy
(``/api-proxy/*`` rewrites to ``http://localhost:8000`` in
``apps/web/next.config.ts``), so only one tunnel is required.

Start the tunnel:

.. code-block:: bash

   cd ~/Thesis2/worktrees/protea-deploy
   bash scripts/expose.sh

``expose.sh`` validates the local stack before opening the tunnel. If
the stack is not running, start it first via ``manage.sh start``.

Run the tunnel in the background for unattended operation:

.. code-block:: bash

   nohup bash scripts/expose.sh >> logs/expose.log 2>&1 &
   echo $! > logs/pids/expose.pid

Verify the tunnel is live:

.. code-block:: bash

   curl -sf https://protea.ngrok.app -o /dev/null && echo "Tunnel OK"

After any ``manage.sh stop / start`` cycle, confirm the tunnel process
is still alive: ``pgrep -fa ngrok``. The ngrok process is not tracked
in the ``logs/pids/`` registry; it must be restarted manually if it
dies during a stack restart.

For full ngrok recovery steps (re-authentication, re-create deploy slot)
see :doc:`ngrok-deploy-recovery`.


Postgres backup and recovery
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Dump location:** ``~/Thesis2/backups/protea-*.dump`` (pg_custom format)

**Dump command (manual):**

.. code-block:: bash

   pg_dump -Fc -h localhost -U protea protea \
       > ~/Thesis2/backups/protea-$(date +%Y%m%d-%H%M%S).dump

**Recovery procedure** (takes approximately 28 minutes on the current
dataset size):

.. code-block:: bash

   # 1. Stop the PROTEA stack so no writes land during restore.
   cd ~/Thesis2/worktrees/protea-deploy
   bash scripts/manage.sh stop

   # 2. Drop and re-create the target database.
   psql -h localhost -U postgres -c "DROP DATABASE IF EXISTS protea;"
   psql -h localhost -U postgres -c "CREATE DATABASE protea OWNER protea;"
   psql -h localhost -U protea -d protea -c "CREATE EXTENSION IF NOT EXISTS vector;"

   # 3. Restore from the latest dump.
   pg_restore -d protea -h localhost -U protea \
       ~/Thesis2/backups/protea-latest.dump

   # 4. Bring the schema to HEAD (idempotent if already at head).
   set -a && source ~/.secrets/protea.env && set +a
   poetry run alembic upgrade head

   # 5. Restart the stack.
   bash scripts/manage.sh start

The ``vector`` extension must be enabled before the restore; otherwise
``pg_restore`` fails on the ``halfvec`` column type used by
``SequenceEmbedding``.

For the full disaster-recovery drill (volume wipe scenario, Docker
volume re-creation) see :doc:`disaster-recovery`.


See also
~~~~~~~~

- :doc:`deployment` (container-based deployment modes: docker-compose,
  Swarm, Helm, sharing the same environment variables).
- :doc:`ngrok-deploy-recovery` (ngrok tunnel and deploy-slot recovery).
- :doc:`disaster-recovery` (postgres dump/restore drill and real
  recovery path).
- :doc:`secrets-management` (full secret resolution order and
  sops + age encrypted ``secrets/secrets.prod.enc.yaml``).
- :doc:`observability-ops` (Prometheus, Loki, Grafana, alerting
  operator reference).
- ``scripts/manage.sh`` (authoritative inline comments for each
  start / stop / scale behaviour).
- ``agent-farm/scripts/lib/stack_owner.sh`` (stack-owner lock
  implementation and CLI reference).
- ``agent-farm/scripts/services/deploy-keeper-supervisor.sh`` (outer
  supervisor loop and trigger subsystem).
