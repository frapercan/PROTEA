Deployment Guide
================

PROTEA supports four deployment modes. Choose the mode that matches your
infrastructure; all modes run the same service set (postgres, rabbitmq,
api, workers, frontend) with the same environment variables.

.. contents:: On this page
   :local:
   :depth: 2

Deployment modes at a glance
------------------------------

.. list-table::
   :header-rows: 1
   :widths: 20 30 50

   * - Mode
     - When to use
     - Entry point
   * - **docker-compose dev**
     - Local development, single host
     - ``bash scripts/deploy.sh`` (see :ref:`deploy-compose-dev`)
   * - **docker-compose bundle**
     - Smoke-test from pre-built images, laptop or CI
     - ``docker compose -f docker-compose.bundle.yml up`` (see :ref:`deploy-bundle`)
   * - **Docker Swarm**
     - Multi-node production cluster, no Kubernetes (T-OPS.4)
     - ``docker stack deploy -c deploy/swarm/stack.yml protea`` (see :ref:`deploy-swarm`)
   * - **Helm / Kubernetes**
     - Kubernetes cluster (T-OPS.3, CI pending)
     - ``helm install protea deploy/helm/protea/`` (see :ref:`deploy-helm`)

Telemetry variables apply across all modes; see
:doc:`observability` for the ``PROTEA_OTEL_*`` variable reference.

Prerequisites (all modes)
--------------------------

- Docker 24+ installed on every node that runs containers.
- Python 3.11+ and Poetry (dev mode only).
- Access to ``ghcr.io/frapercan/protea`` (bundle, Swarm, Helm modes
  pull pre-built images; dev mode builds locally).
- A valid ``.env`` or environment override for non-default secrets (see
  per-mode sections below).

.. _deploy-compose-dev:

Mode 1: docker-compose dev stack
----------------------------------

The canonical local development workflow. Images are built from the
local source tree; a dedicated deploy slot is kept at
``~/Thesis2/worktrees/protea-deploy`` so the developer's working tree
can move freely without disturbing a running stack.

**Setup (once)**

Create the deploy slot if it does not exist:

.. code-block:: bash

   git worktree add ~/Thesis2/worktrees/protea-deploy \
     -b feat/deploy-tooling origin/develop

**Deploy**

.. code-block:: bash

   # Update the slot to origin/develop, build frontend, start stack.
   bash scripts/deploy.sh

   # Deploy a specific branch or SHA.
   bash scripts/deploy.sh my-feature-branch

   # Skip the frontend build (faster for back-end iteration).
   bash scripts/deploy.sh --no-build

   # Deploy from a local folder snapshot (skips git).
   bash scripts/deploy.sh --from /path/to/snapshot

**Status and stop**

.. code-block:: bash

   bash scripts/deploy.sh --status
   bash scripts/deploy.sh --stop

``--status`` prints the active branch/SHA, API health, and frontend
health. The API is available at ``http://localhost:8000``; the frontend
at ``http://localhost:3000``.

**GPU detection**

``deploy.sh`` auto-detects the presence of NVIDIA drivers via
``nvidia-smi``. When a GPU is available, it swaps the PyTorch wheel to
the ``cu128`` build post-install. Override with
``PROTEA_DEPLOY_GPU=1|0|auto`` (default ``auto``). Older CUDA wheels are
still reachable via ``CUDA_VARIANT=cu121`` (or ``cu118``) when invoking
``scripts/install_gpu_torch.sh`` directly.

**Key environment variables** (override via shell or ``.env`` in the
deploy slot):

.. list-table::
   :header-rows: 1
   :widths: 36 64

   * - Variable
     - Purpose
   * - ``PROTEA_DEPLOY_PATH``
     - Target worktree path (default ``~/Thesis2/worktrees/protea-deploy``).
   * - ``PROTEA_DEPLOY_REF``
     - Default git ref when none is passed as argument (default ``origin/develop``).
   * - ``PROTEA_DEPLOY_GPU``
     - GPU wheel selection: ``auto`` (default), ``1``, or ``0``.
   * - ``PROTEA_PUBLIC_API_URL``
     - ``NEXT_PUBLIC_API_URL`` injected into ``apps/web/.env.local``
       if the file is absent (default ``/api-proxy``).

For telemetry configuration in this mode see :doc:`observability`.

.. _deploy-bundle:

Mode 2: docker-compose bundle (T-OPS.2)
-----------------------------------------

A single, self-contained compose file that pulls pre-built images from
``ghcr.io`` without requiring a local build context or any plugin
repository to be cloned. Designed for smoke-testing the production
service wiring on a laptop or in a CI job.

Source: ``docker-compose.bundle.yml``

**Quick start**

.. code-block:: bash

   cp .env.bundle.example .env.bundle   # adjust credentials if needed
   docker compose -f docker-compose.bundle.yml --env-file .env.bundle up -d

   # Verify the API is healthy.
   curl -s http://localhost:8000/health   # {"status":"ok"}

   # Tear down (preserves the postgres volume by default).
   docker compose -f docker-compose.bundle.yml down

   # Full tear down including volumes.
   docker compose -f docker-compose.bundle.yml down -v

**Image tag**

Override the image tag via the ``PROTEA_IMAGE_TAG`` variable in
``.env.bundle``:

.. code-block:: bash

   PROTEA_IMAGE_TAG=v1.2.0

The default tag is ``latest``.

**Key environment variables** (``.env.bundle`` / shell overrides):

.. list-table::
   :header-rows: 1
   :widths: 36 64

   * - Variable
     - Purpose
   * - ``PROTEA_IMAGE_TAG``
     - Image tag for the ``ghcr.io/frapercan/protea`` image (default ``latest``).
   * - ``PROTEA_FRONTEND_TAG``
     - Image tag for the frontend image (default ``latest``).
   * - ``PROTEA_ALLOWED_ORIGINS``
     - CORS allowed origins (default ``http://localhost:3000``).
   * - ``PROTEA_API_PORT``
     - Host port for the API (default ``8000``).
   * - ``PROTEA_WEB_PORT``
     - Host port for the frontend (default ``3000``).
   * - ``POSTGRES_PORT``
     - Host port for postgres (default ``5432``).
   * - ``RABBITMQ_PORT`` / ``RABBITMQ_MGMT_PORT``
     - AMQP and management ports (default ``5672`` / ``15672``).

**Differences versus the dev stack**

The bundle uses pre-built images (no ``build:`` keys), a trimmed worker
set (``worker-jobs``, ``worker-ping``, ``worker-embeddings``) sufficient
for smoke runs, and an explicit named network (``protea-bundle``) so
external monitoring stacks can attach. The full worker set lives in
``docker-compose.yml``.

For telemetry configuration in this mode see :doc:`observability`.

.. _deploy-swarm:

Mode 3: Docker Swarm stack (T-OPS.4)
---------------------------------------

The production deployment target for operators running a Docker Swarm
cluster. Source: ``deploy/swarm/stack.yml``.

Full annotated documentation is in ``deploy/swarm/README.md``. This
section summarises the key operational steps.

**Prerequisites**

- Docker 24+ on every node; Swarm initialised:

  .. code-block:: bash

     docker swarm init          # on the manager
     docker swarm join ...      # on each worker node

- Every node able to pull from ``ghcr.io``. Authenticate once:

  .. code-block:: bash

     docker login ghcr.io

- A node labelled for the GPU embedding worker (optional):

  .. code-block:: bash

     docker node update --label-add gpu=true <node-id>

**Create Swarm secrets (once per cluster)**

The stack reads credentials from Docker Swarm secrets, not from plaintext
environment variables. The canonical source of those credentials is the
sops + age encrypted ``secrets/secrets.prod.enc.yaml`` in the repo;
see :doc:`secrets-management` for how to obtain a private key, decrypt
the file, and pipe values into ``docker secret create``. Create the six
secrets before the first deploy:

.. code-block:: bash

   printf 'change-me-pg' | docker secret create protea_postgres_password -
   printf 'change-me-rmq' | docker secret create protea_rabbitmq_password -
   printf 'change-me-minio' | docker secret create protea_minio_password -
   printf 'postgresql+psycopg://protea:change-me-pg@postgres:5432/protea' \
       | docker secret create protea_db_url -
   printf 'amqp://protea:change-me-rmq@rabbitmq:5672/' \
       | docker secret create protea_amqp_url -

**Validate the stack file**

.. code-block:: bash

   docker stack config -c deploy/swarm/stack.yml > /dev/null

A zero exit code confirms the file is well-formed.

**Deploy**

.. code-block:: bash

   export PROTEA_IMAGE_TAG=$(git describe --tags --always)
   export PROTEA_FRONTEND_TAG=$PROTEA_IMAGE_TAG
   export PROTEA_ALLOWED_ORIGINS=https://protea.example.org

   docker stack deploy \
     --with-registry-auth \
     -c deploy/swarm/stack.yml \
     protea

``--with-registry-auth`` propagates the manager's ``ghcr.io``
credentials to worker nodes. The ``api`` and ``frontend`` services use
``start-first`` rolling updates with automatic rollback on failure. The
``migrate`` service runs ``alembic upgrade head`` on every deploy and
exits 0 if the schema is already current.

**Status, logs, scaling**

.. code-block:: bash

   docker stack services protea
   docker service logs -f protea_api
   docker service scale protea_worker-embeddings=4

**Tear down**

.. code-block:: bash

   docker stack rm protea

   # Remove persistent volumes only when wiping the cluster.
   docker volume rm protea_postgres_data protea_minio_data

**Service placement**

Stateful services (``postgres``, ``rabbitmq``, ``minio``) are pinned to
the manager node so their volumes stay co-located with a known host.
Operators running a multi-manager cluster should replace the
``node.role == manager`` constraint with a custom label such as
``node.labels.protea_data == true``. Worker processes are constrained to
worker nodes (``node.role == worker``) to leave the manager headroom for
the API.

For telemetry configuration in this mode pass ``PROTEA_OTEL_*`` variables
via additional Swarm secrets or environment overrides in the stack file.
See :doc:`observability` for variable definitions.

.. _deploy-helm:

Mode 4: Helm chart on Kubernetes (T-OPS.3)
--------------------------------------------

A Helm 3 chart for installing the full PROTEA stack on any
Kubernetes 1.24+ cluster. Source: ``deploy/helm/protea/``.

.. note::

   T-OPS.3 CI gates are pending. The chart has landed in the repository
   (PR #326, merged to develop) and can be installed manually; full
   automated promotion to production is blocked until CI passes.

**Prerequisites**

- ``helm`` 3.x installed.
- ``kubectl`` configured for the target cluster.
- Access to ``ghcr.io/frapercan/protea`` from the cluster nodes.

**Install**

.. code-block:: bash

   # Install with default values (all services enabled, internal postgres/rabbitmq).
   helm install protea deploy/helm/protea/

   # Override values inline.
   helm install protea deploy/helm/protea/ \
     --set image.tag=v1.2.0 \
     --set api.replicaCount=2 \
     --set workers.embeddingsBatch.gpu.enabled=true

   # Override via a custom values file.
   helm install protea deploy/helm/protea/ -f my-values.yaml

**Upgrade and rollback**

.. code-block:: bash

   helm upgrade protea deploy/helm/protea/ --set image.tag=v1.3.0
   helm rollback protea

**Key chart knobs** (``deploy/helm/protea/values.yaml``):

.. list-table::
   :header-rows: 1
   :widths: 36 64

   * - Value path
     - Purpose
   * - ``image.tag``
     - Image tag for all PROTEA-owned containers (default ``latest``).
   * - ``database.internal``
     - When ``true`` (default) the chart deploys postgres. Set to ``false``
       and provide ``database.externalUrl`` to use an external database.
   * - ``amqp.internal``
     - Same pattern as ``database.internal`` for RabbitMQ.
   * - ``objectStore.enabled``
     - Deploys MinIO when ``true`` (default ``false``).
   * - ``api.replicaCount``
     - Number of API pod replicas (default ``1``).
   * - ``api.ingress.enabled``
     - Enables an Ingress resource (default ``false``).
   * - ``workers.<name>.replicaCount``
     - Per-worker replica count.
   * - ``workers.embeddingsBatch.gpu.enabled``
     - Requests ``nvidia.com/gpu: 1`` on the batch embedding pod
       (default ``false``).
   * - ``workers.embeddingsBatch.runtimeClassName``
     - Runtime class for GPU workloads (e.g. ``nvidia``).

The chart deploys a pre-install migration Job (``alembic upgrade head``)
that completes before the API and workers start, mirroring the ``migrate``
service in compose.

For telemetry, pass ``PROTEA_OTEL_*`` variables via ``api.extraEnv`` in
your values override:

.. code-block:: yaml

   api:
     extraEnv:
       - name: PROTEA_OTEL_ENABLED
         value: "true"
       - name: PROTEA_OTEL_ENDPOINT
         value: "http://otel-collector:4318"

See :doc:`observability` for the full variable reference.

Common operations
------------------

Environment variables common to all modes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following variables are recognised by the API and worker processes
across all deployment modes. Credential variables accept either a direct
value (e.g. ``PROTEA_DB_URL``) or a file-path variant (e.g.
``PROTEA_DB_URL_FILE``) for secret-store integration.

.. list-table::
   :header-rows: 1
   :widths: 36 12 52

   * - Variable
     - Default
     - Description
   * - ``PROTEA_DB_URL``
     - (required)
     - SQLAlchemy connection URL for postgres.
       Format: ``postgresql+psycopg://user:pass@host:5432/db``.
   * - ``PROTEA_AMQP_URL``
     - (required)
     - AMQP connection URL for RabbitMQ.
       Format: ``amqp://user:pass@host:5672/``.
   * - ``PROTEA_ALLOWED_ORIGINS``
     - (none)
     - Comma-separated list of allowed CORS origins.
   * - ``PROTEA_STORAGE_BACKEND``
     - ``local``
     - Storage backend: ``local`` (filesystem) or ``minio``.
   * - ``PROTEA_ANC2VEC_PATH``
     - (none)
     - Absolute path to the Anc2Vec npz artefact
       (``anc2vec_2020-10.npz``). When unset, the API/worker process
       falls back to the repo-relative
       ``artifacts/anc2vec/anc2vec_2020-10.npz`` (gitignored, so absent
       on fresh deploy worktrees). On a deploy without the file in
       either location the process logs the resolution chain and
       raises ``FileNotFoundError``. See
       :doc:`secrets-management` for the resolution order.
   * - ``PROTEA_OTEL_ENABLED``
     - ``false``
     - Enable OpenTelemetry distributed tracing. See :doc:`observability`.
   * - ``PROTEA_OTEL_ENDPOINT``
     - (none)
     - OTLP HTTP exporter endpoint. See :doc:`observability`.
   * - ``PROTEA_OTEL_SERVICE_NAME``
     - ``protea-api``
     - OTel service name. See :doc:`observability`.
   * - ``PROTEA_OTEL_SAMPLE_RATIO``
     - ``1.0``
     - OTel head-sampling ratio. See :doc:`observability`.

Health checks
^^^^^^^^^^^^^

The API exposes ``GET /health`` which returns ``{"status": "ok"}`` when
the process is running. All deployment modes configure a health check
against this endpoint:

.. code-block:: bash

   curl -s http://localhost:8000/health

Schema migrations
^^^^^^^^^^^^^^^^^

Every deployment mode runs ``alembic upgrade head`` before starting the
API and workers. In compose modes the ``migrate`` service handles this; in
Swarm the ``migrate`` task runs on every ``docker stack deploy``; in Helm
the chart deploys a pre-install Job. Never start the API against an
un-migrated schema.

To run migrations manually:

.. code-block:: bash

   # dev mode (inside deploy slot)
   poetry run alembic upgrade head

   # bundle / Swarm / Helm (via a one-off container)
   docker run --rm \
     -e PROTEA_DB_URL=postgresql+psycopg://... \
     ghcr.io/frapercan/protea:latest \
     alembic upgrade head

The documentation ships with the revision it documents
------------------------------------------------------

**Every deploy publishes the documentation of the revision it deploys.** This
is a step, not a habit, because as a habit it failed: on 2026-11-10 this
instance served code revision ``9f09e2c`` while ``/sphinx/`` served a build
from October 7th, five weeks and eight architecture decision records behind,
and nothing anywhere said so.

.. code-block:: bash

   bash scripts/deploy-docs.sh            # build this tree's revision and swap it in
   bash scripts/deploy-docs.sh --check    # what is served right now, building nothing

Run it after the tree moves to the deployed revision and before or after the
restart; it does not need the API, and it refuses rather than guessing when the
tree carries uncommitted changes under ``docs/`` or ``protea/``.

Three properties worth knowing, each one the reason a line of that script
exists:

* **It never leaves the served build empty.** It builds elsewhere, checks the
  result (``index.html``, ``adr/index.html``, a page floor), swaps, and puts
  the previous build back if the swap does not answer a request. ``sphinx-build``
  writes into its output directory, and the output directory is what FastAPI
  mounted at ``/sphinx``, so a failed in-place build publishes a broken site:
  the same outage shape as a failed ``next build``, which is what
  ``scripts/deploy-frontend.sh`` exists for.
* **It does not use the platform's virtualenv.** Sphinx is not installed there
  and must not be: that environment holds a hand-pinned torch. The build needs
  far less anyway, because ``docs/source/conf.py`` mocks torch, numpy,
  transformers and eighteen more for autodoc. The script assembles its own
  environment from ``docs/requirements.txt``, four light runtime packages and
  the ``protea-contracts`` sibling.
* **The build carries the revision it was made from**, stamped at
  ``docs/build/html/_static/protea-revision.txt``, and ``GET /health`` reports
  it as ``docs_revision`` with ``docs_match_code`` beside it. A skipped build is
  therefore visible from outside instead of silent. ``docs_match_code`` is
  ``None``, never ``False``, when the served build carries no stamp, which is
  every build made before this script.

Workers start only on the declared revision
-------------------------------------------

**A worker unit refuses to start on a revision nobody declared.** Each
``protea-worker@`` unit in ``deploy/systemd/`` runs
``deploy/systemd/protea-guard-revision.sh`` as an ``ExecStartPre``. The guard
only reads. When it refuses it exits 1, the unit fails and the worker stays
down, because a silent wrong consumer is worse than no consumer. Its journal
line starts with ``protea-guard-revision: REFUSING`` and says why.

It takes its inputs from the unit's environment:

.. list-table::
   :header-rows: 1
   :widths: 28 26 46

   * - Variable
     - Default
     - What it names
   * - ``DEPLOY``
     - (required)
     - The tree the worker will load. It must equal the unit's
       ``WorkingDirectory``, and it must be a clone: a worktree, whose
       ``.git`` is a file, is refused.
   * - ``AGENT_FARM_ROOT``
     - (required)
     - The agent-farm clone that holds the declaration.
   * - ``PROTEA_DECL_PATH``
     - ``plans/DECLARED-REVISION.txt``
     - The declaration, as a path inside that clone, read from its
       ``origin/main`` and never from a checkout. The name and the default are
       the ones agent-farm's ``protea-node-sync.sh`` reads.
   * - ``PROTEA_GUARD_UNDECLARED``
     - (unset)
     - ``1`` declares the host outside the revision protocol, and its workers
       start without any check.

It refuses when either path is unset or is not a clone, when the unit does not
run in ``DEPLOY``, when the declaration cannot be read from ``origin/main``,
when the declaration carries no 40 character ``coordinator`` sha, and when the
tree's ``HEAD`` is behind, ahead of or diverged from that sha, or has
uncommitted changes.

**A declaration that cannot be read is a refusal.** It used to let the worker
start, read as "nothing declared". With the path handed in by the unit, a typo
there, a declaration not merged yet and a clone without ``origin/main`` would
all read the same way, so the only way to run undeclared is
``PROTEA_GUARD_UNDECLARED=1``, set on purpose.

The guard never fetches. A declaration merged on the remote stays invisible
until somebody fetches the agent-farm clone, so after moving one, compare what
the guard will read with the tree, with ``AGENT_FARM_ROOT``, ``DEPLOY`` and
``PROTEA_DECL_PATH`` set as in the unit:

.. code-block:: bash

   git -C "$AGENT_FARM_ROOT" fetch origin
   git -C "$AGENT_FARM_ROOT" show "origin/main:${PROTEA_DECL_PATH:-plans/DECLARED-REVISION.txt}" \
     | grep -E '^(coordinator|schema-applied) '
   git -C "$DEPLOY" rev-parse HEAD

Move a declaration only when the server it describes has no job ``QUEUED`` or
``RUNNING`` and no prediction set being written.

The development instance
^^^^^^^^^^^^^^^^^^^^^^^^

The development instance runs commits of the integration branch, which the
production declaration never names: that file tracks the tip of ``develop``.
So it has a declaration of its own in agent-farm,
``plans/DECLARED-REVISION-DEV.txt``, with the same two lines as the production
one:

.. code-block:: text

   coordinator <sha>      the commit the dev workers run
   schema-applied <sha>   the commit whose migrations the dev database holds

Until that file is on ``origin/main`` of the agent-farm clone the units name,
every dev worker is refused.

Every dev worker unit runs in its clone and hands that file to the guard
beside the two paths:

.. code-block:: text

   WorkingDirectory=<the dev clone>
   Environment=DEPLOY=<the dev clone>
   Environment=AGENT_FARM_ROOT=<the agent-farm clone>
   Environment=PROTEA_DECL_PATH=plans/DECLARED-REVISION-DEV.txt
   ExecStartPre=<the dev clone>/deploy/systemd/protea-guard-revision.sh

Three rules come with it:

* **The dev tree is a clone of its own**, never a worktree of a clone somebody
  works in, because the guard refuses a worktree.
* **The dev declaration moves only between bootstrap phases**, with no job
  ``QUEUED`` or ``RUNNING`` on the dev server.
* **The guard reads only the coordinator line.** ``schema-applied`` is the
  server's word that the migrations of that commit are applied, and
  agent-farm's ``protea-node-sync.sh`` will not move a node across a migration
  without it.

A dev unit that forgets ``PROTEA_DECL_PATH`` reads the production declaration.
It is refused, naming that file, whenever the dev tree is at a commit other
than the one production declares. When the dev tree sits exactly at that
commit, the worker starts, although the dev declaration may name another one.
The guard cannot tell which instance a unit belongs to, so every dev unit sets
the variable.

See also
---------

- ``deploy/swarm/README.md`` for Swarm-specific prerequisites and secret rotation.
- ``deploy/helm/protea/values.yaml`` for the full Helm value reference.
- ``scripts/deploy.sh`` (in-file comments) for dev-stack behaviour details.
- ``deploy/systemd/protea-guard-revision.sh`` (in-file comments) for why each
  of the revision guard's refusals exists.
- :doc:`observability` for telemetry environment variables (``PROTEA_OTEL_*``)
  that apply across all deployment modes.
- :doc:`ngrok-deploy-recovery` if the public demo endpoint becomes unreachable.
- :doc:`disaster-recovery` for the postgres dump and restore procedure
  (drill and real recovery paths).
