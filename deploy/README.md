# PROTEA deployment assets

This directory holds the per-platform deployment artefacts for the
PROTEA stack (api, workers, postgres, rabbitmq, optional minio, and the
Next.js frontend). Every mode runs the same service set with the same
environment variables; pick the entry point that matches your
infrastructure.

The canonical, narrative deployment guide is
[`docs/source/runbooks/deployment.rst`](../docs/source/runbooks/deployment.rst).
This README is the operator-facing quick reference for the files under
`deploy/`; it complements (does not replace) the runbook.

## Directory layout

| Path | Contents | Mode |
| - | - | - |
| `deploy/swarm/`             | `stack.yml` + README for `docker stack deploy` | Docker Swarm |
| `deploy/helm/protea/`       | Helm chart (Chart.yaml, values.yaml, templates) | Kubernetes |
| `deploy/slurm/`             | `*.sbatch` scripts, `env.sh`, `submit_all.sh` | HPC / SLURM |
| `deploy/grafana/`           | Provisioned dashboards + alert rules (mounted by `docker-compose.monitoring.yml`) | All |
| `deploy/loki/`              | `loki-config.yml` (mounted by `docker-compose.monitoring.yml`) | All |

The compose files referenced below live at the repo root:

| File | Purpose |
| - | - |
| `docker-compose.yml`            | Default dev stack, builds from local source |
| `docker-compose.prod.yml`       | Production overrides, pulls `ghcr.io/frapercan/protea:*` images |
| `docker-compose.bundle.yml`     | Self-contained smoke-test stack (pre-built images, trimmed worker set) |
| `docker-compose.monitoring.yml` | Grafana + Loki sidecar (binds `deploy/grafana/` and `deploy/loki/`) |
| `docker-compose.e2e.yml`        | Ephemeral stack used by the e2e test suite |

## When to use which mode

| Mode | Best for | Entry point |
| - | - | - |
| Compose (dev)         | Local development, single host, fastest iteration   | `docker compose up -d` |
| Compose (bundle)      | Smoke test from pre-built images, laptop or CI      | `docker compose -f docker-compose.bundle.yml --env-file .env.bundle up -d` |
| Compose + monitoring  | Local dev with Grafana dashboards and Loki logs     | `docker compose -f docker-compose.yml -f docker-compose.monitoring.yml up -d` |
| Docker Swarm          | Multi-host production cluster without Kubernetes    | `docker stack deploy -c deploy/swarm/stack.yml protea` |
| Helm / Kubernetes     | Existing K8s cluster, GitOps-style rollouts         | `helm install protea deploy/helm/protea/` |
| SLURM                 | HPC batch site, worker fleet on a scheduler         | `sbatch deploy/slurm/<worker>.sbatch` |

A single-host operator who just wants the stack running locally should
follow the [Compose section](#compose-single-host). Multi-host operators
should choose between Swarm and Kubernetes per their existing tooling.
HPC operators with no docker daemon on compute nodes should use SLURM.

## Compose (single host)

The default `docker-compose.yml` builds images from the repo and runs
the full worker fleet plus api, postgres, rabbitmq, optional minio
(profile `storage`), and the frontend. This is the canonical local
development workflow.

### First-run walkthrough

1. Bring up the stack (builds images on first run): `docker compose up -d`.
2. Wait for the api health endpoint and verify it returns `{"status":"ok"}`: `curl -s http://localhost:8000/health`.
3. Open the frontend at `http://localhost:3000`.
4. Tail logs from the worker fleet: `docker compose logs -f worker-jobs worker-embeddings`.

Migrations run automatically via the `migrate` service before the api
and workers boot (`alembic upgrade head`). Re-running `docker compose
up -d` after a code change rebuilds only the images that changed.

### With monitoring (Grafana + Loki)

The monitoring stack lives in a separate compose file so dev sessions
can opt in cheaply. Bring up both stacks together with
`docker compose -f docker-compose.yml -f docker-compose.monitoring.yml up -d`.
Grafana then listens on `http://localhost:3001` (login `admin / admin`
on first start). Dashboards under `deploy/grafana/dashboards/` are
auto-provisioned; Loki listens on `http://localhost:3100` (push
endpoint for the docker driver).

Shipping container logs to Loki requires the `loki-docker-driver`
plugin and a `logging:` block on the api / worker services. See
[`docs/source/runbooks/loki.rst`](../docs/source/runbooks/loki.rst)
for the step-by-step procedure.

### Bundle stack (pre-built images)

`docker-compose.bundle.yml` pulls `ghcr.io/frapercan/protea*` images
and ships a trimmed worker set sufficient for smoke runs.

1. Copy the example env file: `cp .env.bundle.example .env.bundle` (tweak credentials as needed).
2. Start the stack: `docker compose -f docker-compose.bundle.yml --env-file .env.bundle up -d`.
3. Verify the api: `curl -s http://localhost:8000/health` should return `{"status":"ok"}`.

To pin an image tag instead of `latest`, set
`PROTEA_IMAGE_TAG=v1.2.0` in the shell or `.env.bundle` before the
`docker compose ... up -d` invocation. Tear down with
`docker compose -f docker-compose.bundle.yml down` (keeps volumes); add
`-v` to also wipe `postgres_data` / `minio_data`.

### Key environment variables (compose)

| Variable | Default | Purpose |
| - | - | - |
| `PROTEA_DB_URL`            | (set in compose)            | SQLAlchemy URL for postgres |
| `PROTEA_AMQP_URL`          | (set in compose)            | AMQP URL for RabbitMQ |
| `PROTEA_ADMIN_TOKEN`       | `protea-admin` (bundle)     | Bearer token for admin endpoints |
| `PROTEA_ALLOWED_ORIGINS`   | `http://localhost:3000`     | CORS allow list |
| `PROTEA_IMAGE_TAG`         | `latest`                    | Image tag for bundle / prod overrides |
| `PROTEA_FRONTEND_TAG`      | `latest`                    | Image tag for the frontend bundle |
| `PROTEA_API_PORT`          | `8000`                      | Host port for the API |
| `PROTEA_WEB_PORT`          | `3000`                      | Host port for the frontend |
| `POSTGRES_PORT`            | `5432`                      | Host port for postgres |
| `RABBITMQ_PORT`            | `5672`                      | AMQP port |
| `RABBITMQ_MGMT_PORT`       | `15672`                     | RabbitMQ management UI port |
| `SLACK_WEBHOOK_URL`        | (unset)                     | Slack URL for Grafana alerts (FARM-UI.7) |

The full variable list is in
[`docs/source/runbooks/deployment.rst`](../docs/source/runbooks/deployment.rst);
telemetry knobs (`PROTEA_OTEL_*`) are documented in
[`docs/source/runbooks/observability.rst`](../docs/source/runbooks/observability.rst).

## Docker Swarm (multi-host)

`deploy/swarm/stack.yml` is the production deploy target for operators
running PROTEA on a Docker Swarm cluster. It mirrors the compose
service set but uses overlay networks, Swarm secrets, explicit
`deploy.replicas` and rolling updates with rollback, and `ghcr.io`
pre-built images (no `build:` keys).

### Worked example

1. Initialise the swarm once: `docker swarm init` on the manager,
   then `docker swarm join --token <T> <M>:2377` on each worker node.
2. Authenticate every node against the image registry: `docker login ghcr.io`.
3. (Optional) Label a node for the GPU embedding worker:
   `docker node update --label-add gpu=true <node-id>`.
4. Create the six external secrets once per cluster. Pipe each value
   into `docker secret create`:
   - `printf 'change-me-pg'    | docker secret create protea_postgres_password -`
   - `printf 'change-me-rmq'   | docker secret create protea_rabbitmq_password -`
   - `printf 'change-me-minio' | docker secret create protea_minio_password -`
   - `printf 'change-me-admin' | docker secret create protea_admin_token -`
   - `printf 'postgresql+psycopg://protea:change-me-pg@postgres:5432/protea' | docker secret create protea_db_url -`
   - `printf 'amqp://protea:change-me-rmq@rabbitmq:5672/' | docker secret create protea_amqp_url -`
5. Validate the stack file (zero exit code means well-formed):
   `docker stack config -c deploy/swarm/stack.yml > /dev/null`.
6. Export tag and CORS overrides, then deploy:
   - `export PROTEA_IMAGE_TAG=$(git describe --tags --always)`
   - `export PROTEA_FRONTEND_TAG=$PROTEA_IMAGE_TAG`
   - `export PROTEA_ALLOWED_ORIGINS=https://protea.example.org`
   - `docker stack deploy --with-registry-auth -c deploy/swarm/stack.yml protea`
7. Verify: `docker stack services protea` and `docker service logs -f protea_api`.

The `--with-registry-auth` flag propagates the manager's `ghcr.io`
credentials to worker nodes. Stateful services (`postgres`, `rabbitmq`,
`minio`) are pinned to manager nodes via `node.role == manager`;
multi-manager clusters should swap that for a custom label such as
`node.labels.protea_data == true`.

### Day 2 operations

- Scale a worker: `docker service scale protea_worker-embeddings=4`.
- Tear down (volumes persist): `docker stack rm protea`.
- Wipe volumes too (full cluster reset): `docker volume rm protea_postgres_data protea_minio_data`.

Secret rotation is performed by creating a new versioned secret
(for example `protea_db_url_v2`), updating `stack.yml` to reference
it, and re-running `docker stack deploy`. See
[`deploy/swarm/README.md`](swarm/README.md) for the full annotated
walkthrough.

## Helm / Kubernetes

`deploy/helm/protea/` is a Helm 3 chart that installs the full PROTEA
stack (postgres, rabbitmq, optional minio, migrations Job, api, the
worker family, frontend) on any Kubernetes 1.24+ cluster. The chart
mirrors the compose service set 1:1 so the same knobs apply.

### Worked example

1. Prerequisite: `helm` 3.x and `kubectl` configured for the target
   cluster. Verify with `helm version` and `kubectl cluster-info`.
2. Install with defaults (internal postgres + rabbitmq, all workers on):
   `helm install protea deploy/helm/protea/ --namespace protea --create-namespace`.
3. Wait for the api Deployment to roll out:
   `kubectl -n protea rollout status deploy/protea-api`.
4. Port-forward and verify:
   - `kubectl -n protea port-forward svc/protea-api 8000:8000 &`
   - `curl -s http://localhost:8000/health` returns `{"status":"ok"}`.

### Override via values file

For real clusters, write a `my-values.yaml` instead of long inline
overrides. A sample shape:

```yaml
image:
  tag: v1.2.0
api:
  replicaCount: 2
  ingress:
    enabled: true
    className: nginx
    hosts:
      - host: protea.example.org
        paths:
          - path: /
            pathType: Prefix
workers:
  embeddingsBatch:
    gpu:
      enabled: true
      count: 1
    runtimeClassName: nvidia
    nodeSelector:
      nvidia.com/gpu.present: "true"
database:
  internal: false
  externalUrl: "postgresql+psycopg://protea:secret@pg.example.org:5432/protea"
amqp:
  internal: false
  externalUrl: "amqp://protea:secret@rabbitmq.example.org:5672/"
```

Install or upgrade with the override:

- `helm install protea deploy/helm/protea/ -n protea -f my-values.yaml`.
- `helm upgrade protea deploy/helm/protea/ -n protea -f my-values.yaml --set image.tag=v1.3.0`.
- `helm rollback protea` reverts to the previous chart revision.

The full value reference (every knob with defaults and resource sizing)
is in [`deploy/helm/protea/values.yaml`](helm/protea/values.yaml). Key
shortcuts:

| Value path | Purpose |
| - | - |
| `image.tag`                              | Image tag for all PROTEA-owned containers |
| `database.internal` / `database.externalUrl` | Toggle the chart-deployed postgres or point at an external one |
| `amqp.internal` / `amqp.externalUrl`     | Same pattern for RabbitMQ |
| `objectStore.enabled`                    | Opt in to chart-deployed MinIO |
| `api.replicaCount`                       | API pod replicas |
| `api.ingress.enabled`                    | Provision an Ingress resource |
| `workers.<name>.replicaCount`            | Per-worker replica count |
| `workers.embeddingsBatch.gpu.enabled`    | Requests `nvidia.com/gpu` on the batch embedding pod |
| `workers.embeddingsBatch.runtimeClassName` | Runtime class for GPU workloads (for example `nvidia`) |

The migrations Job runs as a `pre-install` / `pre-upgrade` hook and
completes before the api and workers start, mirroring the compose
`migrate` service.

## SLURM (HPC batch)

`deploy/slurm/` ships `sbatch` templates for the PROTEA worker fleet on
sites that have no docker daemon on compute nodes. Each template
matches one of the compose worker services 1:1.

| File | Queue | Partition | GPU |
| - | - | - | - |
| `worker_jobs.sbatch`              | `protea.jobs`              | cpu | no  |
| `worker_embeddings.sbatch`        | `protea.embeddings`        | cpu | no  |
| `worker_embeddings_batch.sbatch`  | `protea.embeddings.batch`  | gpu | yes |
| `worker_embeddings_write.sbatch`  | `protea.embeddings.write`  | cpu | no  |
| `worker_predictions_batch.sbatch` | `protea.predictions.batch` | gpu | yes |
| `worker_predictions_write.sbatch` | `protea.predictions.write` | cpu | no  |
| `worker_reaper.sbatch`            | (DB poll, no queue)        | cpu | no  |
| `example.sbatch`                  | env-var smoke test         | cpu | no  |

### Prerequisites

1. A shared filesystem visible from every compute node holding a PROTEA
   checkout and a python venv with `poetry install --no-root` plus the
   root package installed (so `import protea` resolves).
2. Postgres and RabbitMQ reachable from compute nodes. The head node
   typically hosts both; opening 5432 and 5672 to the compute subnet is
   enough.
3. A `logs/` directory in the PROTEA checkout (the sbatch headers write
   stdout / stderr there). Create it with `mkdir -p logs`.

### Worked example

1. Set the four required env vars in the submitting shell:
   - `export PROTEA_REPO_DIR=/lustre/protea/PROTEA`
   - `export PROTEA_VENV=/lustre/protea/.venv`
   - `export PROTEA_DB_URL=postgresql+psycopg://protea:protea@head:5432/protea`
   - `export PROTEA_AMQP_URL=amqp://guest:guest@head:5672/`
2. Smoke test the env wiring on a cpu node:
   `sbatch --export=ALL deploy/slurm/example.sbatch`, then read the
   output at `logs/protea-env-check-*.out`.
3. Submit one of every worker via the convenience script:
   `deploy/slurm/submit_all.sh --partition-gpu gpu_short`.
4. Or submit a single GPU embeddings batch worker:
   `sbatch --export=ALL deploy/slurm/worker_embeddings_batch.sbatch`.

To run more than one worker on a queue, submit the same sbatch file N
times: each submission becomes its own SLURM job id and attaches to the
queue independently. RabbitMQ load-balances messages across consumers.
The full template reference is in
[`deploy/slurm/README.md`](slurm/README.md).

### Tuning

The `#SBATCH --time`, `#SBATCH --mem`, and `#SBATCH --cpus-per-task`
headers default to match the compose resource limits in
`docker-compose.yml` plus a small headroom for cgroup overhead. Tweak
per site policy. GPU sbatch files request `#SBATCH --gres=gpu:1`; only
bump to `gpu:2` if the configured PLM backend is GPU parallel (the
bundled backends use a single device per worker).

## Cross references

- [`docs/source/runbooks/deployment.rst`](../docs/source/runbooks/deployment.rst): canonical narrative deployment guide for all five modes.
- [`docs/source/runbooks/observability.rst`](../docs/source/runbooks/observability.rst): `PROTEA_OTEL_*` telemetry variables that apply across modes.
- [`docs/source/runbooks/secrets-management.rst`](../docs/source/runbooks/secrets-management.rst): sops + age workflow for the encrypted production secrets file.
- [`docs/source/runbooks/loki.rst`](../docs/source/runbooks/loki.rst): wiring the `loki-docker-driver` so api / worker containers ship logs to Loki.
- [`docs/source/runbooks/disaster-recovery.rst`](../docs/source/runbooks/disaster-recovery.rst): postgres dump / restore drill and real-recovery path.
- [`docs/source/runbooks/ngrok-deploy-recovery.rst`](../docs/source/runbooks/ngrok-deploy-recovery.rst): recovery procedure for the public demo endpoint.
- [`docs/source/runbooks/dlq-triage.rst`](../docs/source/runbooks/dlq-triage.rst): dead-letter queue triage for stuck jobs.
- [`docs/source/runbooks/embedding-worker-oom.rst`](../docs/source/runbooks/embedding-worker-oom.rst): playbook for the GPU embedding worker hitting OOM.
- [`docs/source/runbooks/stale-job-reaper.rst`](../docs/source/runbooks/stale-job-reaper.rst): reaper behaviour and force-fail policy.
- [`deploy/swarm/README.md`](swarm/README.md): Swarm-specific prerequisites, secret rotation, scaling.
- [`deploy/slurm/README.md`](slurm/README.md): SLURM template reference and submission patterns.
- [`deploy/helm/protea/values.yaml`](helm/protea/values.yaml): full Helm value reference with defaults.

## Notes on health checks and migrations

The api exposes `GET /health` returning `{"status": "ok"}`. Every
deployment mode wires a probe against this endpoint (compose
`healthcheck`, Swarm `healthcheck`, Helm readiness and liveness probes,
SLURM operator-driven `curl`).

Migrations (`alembic upgrade head`) run before the api and workers
boot in every mode:

- Compose: the `migrate` service runs once and exits 0 if up to date.
- Swarm: the `migrate` task re-runs on every `docker stack deploy`.
- Helm: a `pre-install` / `pre-upgrade` Job (`templates/migrations-job.yaml`).
- SLURM: run `poetry run alembic upgrade head` from the head node before
  launching workers, or submit a one-off sbatch wrapping the same
  command.

Never start the api against an un-migrated schema; the
[`disaster-recovery.rst`](../docs/source/runbooks/disaster-recovery.rst)
runbook documents the recovery path if you do.
