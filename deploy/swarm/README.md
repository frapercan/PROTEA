# PROTEA Docker Swarm deployment

`stack.yml` is the production deploy target for operators running PROTEA
on a Docker Swarm cluster. It mirrors the service set in
`docker-compose.yml` + `docker-compose.prod.yml` (postgres, rabbitmq,
minio, migrate, api, the worker family, frontend) but adapted for Swarm:

- Pre-built images from `ghcr.io/frapercan/protea*` (no `build:` keys).
- Overlay networks (`protea-data`, `protea-edge`) for cross-host traffic.
- Swarm secrets for every credential and connection URL.
- Explicit `deploy.replicas`, `deploy.placement`, `deploy.update_config`,
  `deploy.restart_policy` per service.
- GPU embedding worker uses `generic_resources` and a
  `node.labels.gpu == true` constraint.

This sits next to (not in place of) the local `docker-compose.yml`
workflow. Use compose for development, Swarm for production-shaped
clusters, and `deploy/helm/` (if present) for Kubernetes.

## Prerequisites

- Docker 24+ on every node, Swarm initialised:
  `docker swarm init` on the manager, `docker swarm join ...` on workers.
- Manager nodes able to pull from `ghcr.io/frapercan/protea` and
  `ghcr.io/frapercan/protea-frontend`. Use
  `docker login ghcr.io` and seed the registry credentials onto each
  node (or use a CI deploy key) before the first deploy.
- A node labelled for the GPU worker if you want
  `worker-embeddings-batch` to schedule:
  `docker node update --label-add gpu=true <node-id>`.

## Create the required secrets

The stack expects six external Swarm secrets. Create them once per
cluster before the first `docker stack deploy`:

```bash
printf 'change-me-pg' | docker secret create protea_postgres_password -
printf 'change-me-rmq' | docker secret create protea_rabbitmq_password -
printf 'change-me-minio' | docker secret create protea_minio_password -
printf 'change-me-admin' | docker secret create protea_admin_token -
printf 'postgresql+psycopg://protea:change-me-pg@postgres:5432/protea' \
  | docker secret create protea_db_url -
printf 'amqp://protea:change-me-rmq@rabbitmq:5672/' \
  | docker secret create protea_amqp_url -
```

Rotation is performed by creating a new versioned secret
(`protea_db_url_v2`), updating `stack.yml` to point at it, and running
`docker stack deploy` again. Swarm performs a rolling update.

## Validate before deploying

```bash
docker stack config -c deploy/swarm/stack.yml > /dev/null
```

A zero exit code means the stack is well-formed. This is the
acceptance check for T-OPS.4 and the gate that CI runs.

## Deploy / update

```bash
export PROTEA_IMAGE_TAG=$(git describe --tags --always)
export PROTEA_FRONTEND_TAG=$PROTEA_IMAGE_TAG
export PROTEA_ALLOWED_ORIGINS=https://protea.example.org

docker stack deploy \
  --with-registry-auth \
  -c deploy/swarm/stack.yml \
  protea
```

`--with-registry-auth` is required so worker nodes inherit the manager's
ghcr.io credentials.

The `api` and `frontend` services use `start-first` rolling updates with
rollback on failure, so a bad image tag will not take the public
endpoints offline; the stateful services (`postgres`, `rabbitmq`,
`minio`) use `replicas: 1` and stay pinned to manager nodes via
`node.role == manager`. Operators running multi-manager clusters should
swap this for a custom node label such as `protea_data=true`.

## Status, logs, scaling

```bash
docker stack services protea
docker service logs -f protea_api
docker service scale protea_worker-embeddings=4
```

`migrate` is intentionally `replicas: 1` and re-runs on every deploy; it
exits 0 if the schema is already current.

## Tear down

```bash
docker stack rm protea
```

Volumes (`postgres_data`, `minio_data`) are *not* removed by
`stack rm`. Remove them explicitly only when wiping the cluster:

```bash
docker volume rm protea_postgres_data protea_minio_data
```

## Differences vs compose

| Aspect              | docker-compose.yml          | deploy/swarm/stack.yml          |
|---------------------|-----------------------------|---------------------------------|
| Image source        | local build                 | ghcr.io pre-built               |
| Secrets             | plaintext env               | Swarm secrets + `*_FILE` env    |
| Networks            | implicit default            | explicit overlays               |
| Scaling             | one container per service   | `deploy.replicas`               |
| Service ordering    | `depends_on.condition`      | health probes + restart policy  |
| GPU access          | nvidia container runtime    | `generic_resources` + node label|
| Update strategy     | `docker compose up`         | rolling update with rollback    |
