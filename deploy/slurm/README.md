# PROTEA SLURM templates (deployment mode B)

These submission scripts run the PROTEA worker fleet on an HPC cluster
that exposes the SLURM scheduler. They mirror the docker-compose worker
set one to one, so a site can pick mode A (docker-compose), mode B
(SLURM, this directory), or mode C (airgap bundle) per D25.

## Layout

| File                                | Queue                         | Partition | GPU |
|-------------------------------------|-------------------------------|-----------|-----|
| `worker_jobs.sbatch`                | `protea.jobs`                 | cpu       | no  |
| `worker_embeddings.sbatch`          | `protea.embeddings`           | cpu       | no  |
| `worker_embeddings_batch.sbatch`    | `protea.embeddings.batch`     | gpu       | yes |
| `worker_embeddings_write.sbatch`    | `protea.embeddings.write`     | cpu       | no  |
| `worker_predictions_batch.sbatch`   | `protea.predictions.batch`    | gpu       | yes |
| `worker_predictions_write.sbatch`   | `protea.predictions.write`    | cpu       | no  |
| `worker_reaper.sbatch`              | (DB poll, no queue)           | cpu       | no  |
| `example.sbatch`                    | env-var smoke test            | cpu       | no  |
| `submit_all.sh`                     | submit one of each worker     | -         | -   |
| `env.sh`                            | shared sourced env block      | -         | -   |

## Prerequisites

1. A shared filesystem visible from every compute node holding a PROTEA
   checkout and a python venv with `poetry install --no-root` plus the
   root package installed (so `import protea` resolves).
2. Postgres and RabbitMQ reachable from compute nodes. The head node
   typically hosts both; opening 5432 and 5672 to the compute subnet is
   enough.
3. Logs directory at `${PROTEA_REPO_DIR}/logs`. The sbatch headers
   write stdout / stderr there; create it before submitting:
   `mkdir -p logs`.

## Required env vars

Set these in the submitting shell or pass via `sbatch --export`:

- `PROTEA_REPO_DIR` absolute path to the PROTEA checkout
- `PROTEA_VENV`     absolute path to the venv containing PROTEA
- `PROTEA_DB_URL`   SQLAlchemy URL of Postgres
- `PROTEA_AMQP_URL` AMQP URL of RabbitMQ

Optional: `PROTEA_STORAGE_BACKEND` (`local` or `minio`),
`PROTEA_MINIO_*`, `PROTEA_OTLP_ENDPOINT`, `PROTEA_LOG_FORMAT`. See
`env.sh` for the full list and defaults.

## Submitting

Worked example, single GPU embeddings batch worker:

```bash
sbatch \
  --export=ALL,PROTEA_REPO_DIR=/lustre/protea/PROTEA,\
PROTEA_VENV=/lustre/protea/.venv,\
PROTEA_DB_URL=postgresql+psycopg://protea:protea@head:5432/protea,\
PROTEA_AMQP_URL=amqp://guest:guest@head:5672/ \
  deploy/slurm/worker_embeddings_batch.sbatch
```

Or use the convenience submitter (one of each worker):

```bash
export PROTEA_REPO_DIR=/lustre/protea/PROTEA
export PROTEA_VENV=/lustre/protea/.venv
export PROTEA_DB_URL=postgresql+psycopg://protea:protea@head:5432/protea
export PROTEA_AMQP_URL=amqp://guest:guest@head:5672/
deploy/slurm/submit_all.sh --partition-gpu gpu_short
```

To validate the env wiring before launching real GPU jobs:

```bash
sbatch --export=ALL,PROTEA_REPO_DIR=...,PROTEA_VENV=... \
       deploy/slurm/example.sbatch
cat logs/protea-env-check-*.out
```

## Scaling

SLURM has no "replicas: N" knob equivalent to docker-compose `deploy:
replicas`. To run more than one worker against a queue, submit the same
sbatch file N times: every submission becomes its own job id and
attaches to the queue independently. RabbitMQ load balances messages
across consumers.

## Tuning

Tweak the `#SBATCH --time` / `--mem` / `--cpus-per-task` headers per
site policy. The defaults match the docker-compose resource limits
(`docker-compose.yml`) plus a small headroom for cgroup overhead.

GPU sbatch files request `--gres=gpu:1`; bump to `gpu:2` only if the
backend you have configured in `protea/backends/` is GPU-parallel.
PROTEA's bundled PLM backends use a single device per worker.
