# Docker bundles for PROTEA

PROTEA ships three compose files at the repo root:

| File | Role |
|---|---|
| `docker-compose.yml` | Dev stack. Builds local Dockerfile, full worker set. |
| `docker-compose.prod.yml` | Prod override. Swaps `build:` for `ghcr.io` images and grants GPU to the embeddings worker. Use with `-f docker-compose.yml -f docker-compose.prod.yml`. |
| `docker-compose.bundle.yml` | Self-contained smoke bundle. Pulls pre-built images, trimmed worker set, single `-f` invocation. |
| `docker-compose.monitoring.yml` | Grafana sidecar. Pairs with any of the above. |

This README covers the bundle. The dev and prod stacks are documented
in the top-level `README.md` and `scripts/deploy.sh`.

## Why the bundle exists

The bundle is what you want when you need to demo or smoke-test PROTEA
without cloning the repo onto the target host. Everything comes from
`ghcr.io/frapercan/protea*`, so `docker compose up` is the only step
between a fresh machine and a live API.

It is deliberately a trimmed worker set (one worker per queue family)
so it fits on a 16 GB laptop. Production deployments should keep using
`docker-compose.yml + docker-compose.prod.yml`, which carries the full
embeddings.batch / embeddings.write / predictions.batch / predictions.write
/ reaper split.

## Prerequisites

- Docker 24+ with the Compose plugin (`docker compose version`).
- About 6 GB free RAM and 4 GB free disk.
- Outbound HTTPS to `ghcr.io` for the image pulls.
- `curl` + `jq` if you want to run the smoke recipe below.

## Quickstart

```bash
# 1. From the repo root, copy the sample env (optional, defaults work).
cp .env.bundle.example .env.bundle

# 2. Bring the stack up.
docker compose -f docker-compose.bundle.yml --env-file .env.bundle up -d

# 3. Wait for the API health check to flip green.
docker compose -f docker-compose.bundle.yml ps

# 4. Hit /health.
curl -s http://localhost:8000/health
# {"status":"ok"}

# 5. Hit /health/ready (verifies DB + RabbitMQ).
curl -s http://localhost:8000/health/ready
# {"status":"ready"}

# 6. Submit a ping job (smoke test the full enqueue / worker / DB path).
JOB_ID=$(curl -s -X POST http://localhost:8000/jobs \
  -H 'content-type: application/json' \
  -d '{"operation":"ping","queue_name":"protea.ping","payload":{"smoke":true}}' \
  | jq -r '.id')
echo "queued $JOB_ID"

# 7. Poll until the job reaches a terminal state.
curl -s "http://localhost:8000/jobs/$JOB_ID" | jq '{status,result,error_code}'
# {"status":"succeeded","result":{"echo":"pong"},"error_code":null}

# 8. Tear down (use -v to also drop the postgres volume).
docker compose -f docker-compose.bundle.yml down -v
```

The frontend is reachable at `http://localhost:3000` once `api`
is healthy. RabbitMQ management UI is at `http://localhost:15672`
(guest / guest by default).

## Service map

| Service | Image | Port | Notes |
|---|---|---|---|
| `postgres` | `pgvector/pgvector:pg16` | 5432 | pgvector extension enabled by `docker/init.sql`. |
| `rabbitmq` | `rabbitmq:3-management` | 5672, 15672 | Management plugin on 15672. |
| `migrate` | `ghcr.io/frapercan/protea` | (one-shot) | Runs `alembic upgrade head` then exits 0. |
| `api` | `ghcr.io/frapercan/protea` | 8000 | FastAPI, includes `/health` and `/health/ready`. |
| `worker-jobs` | `ghcr.io/frapercan/protea` | (n/a) | Consumes `protea.jobs`. |
| `worker-ping` | `ghcr.io/frapercan/protea` | (n/a) | Consumes `protea.ping` (smoke). |
| `worker-embeddings` | `ghcr.io/frapercan/protea` | (n/a) | Consumes `protea.embeddings` (CPU only). |
| `web` | `ghcr.io/frapercan/protea-frontend` | 3000 | Next.js frontend. |

The bundle uses storage backend `local`, so artefacts land inside the
container filesystem. For MinIO-backed runs, set `PROTEA_STORAGE_BACKEND=minio`
plus the `PROTEA_MINIO_*` env vars and attach the minio service from
`docker-compose.yml` via `--profile storage`.

## Tuning

Override the image tag for a pinned release:

```bash
PROTEA_IMAGE_TAG=v1.2.3 PROTEA_FRONTEND_TAG=v1.2.3 \
  docker compose -f docker-compose.bundle.yml up -d
```

Override host ports if 8000 or 3000 are taken:

```bash
PROTEA_API_PORT=8081 PROTEA_WEB_PORT=3001 \
  docker compose -f docker-compose.bundle.yml up -d
```

## Validating the compose syntax

`docker compose config` resolves the file (no images pulled, no
containers created). Useful in CI before pushing a change to the
bundle:

```bash
docker compose -f docker-compose.bundle.yml config >/dev/null
```

## Companion monitoring stack

Bring up Grafana alongside the bundle:

```bash
docker compose -f docker-compose.bundle.yml -f docker-compose.monitoring.yml up -d
```

Grafana lands on `http://localhost:3001` (admin / admin).

## Troubleshooting

- `api` stuck in `unhealthy` for >2 minutes: `docker compose logs api`.
  Most often the migrate step failed and the container retries `/health`
  forever; check `docker compose logs migrate` first.
- `api` exits immediately with `exec: "uvicorn": executable file not
  found in $PATH`: the `:latest` ghcr image predates PR #278 (uvicorn
  pulled into the main dependency group). Pin a tag built after the fix
  (e.g. `PROTEA_IMAGE_TAG=develop` once the develop image is republished),
  or rebuild locally via `docker compose -f docker-compose.yml build api`
  and use that file instead of the bundle.
- Image pull denied: `docker login ghcr.io` if the image visibility
  was flipped to private during a release.
- Port already in use: override the `*_PORT` env vars (see `.env.bundle.example`).
