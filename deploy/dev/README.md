# Desktop development stack

This directory defines a development stack for the desktop. It runs BESIDE a
production worker on the same machine, and it must never collide with it.
The two layers that keep them apart are:

- Its own infrastructure: a Postgres, a RabbitMQ and a MinIO of its own, on
  `127.0.0.1` ports in the 26xxx range, with their own credentials, database,
  vhost, bucket and data directory.
- Its own processes: units named `protea-dev-*`, a separate clone and venv,
  and a slice with its own memory ceiling.

Nothing here touches production's units, environment files, trees or
containers.

## What runs

| Unit | What | Listens on | Memory cap |
| - | - | - | - |
| `protea-dev-postgres` (Quadlet) | Postgres 18 with pgvector | `127.0.0.1:26432` | 4G |
| `protea-dev-rabbitmq` (Quadlet) | RabbitMQ 4.3.5 | `127.0.0.1:26672` (AMQP), `127.0.0.1:26673` (management) | 2G |
| `protea-dev-minio` (Quadlet) | MinIO | `127.0.0.1:26900` (API), `127.0.0.1:26901` (console) | 1G |
| `protea-dev-wait-infra.service` | waits until the three answer | | |
| `protea-dev-worker@<queue>.service` | one queue worker per instance | | 4G each |
| `protea-dev-api.service` | the API | `127.0.0.1:26800` | 1G |
| `protea-dev.slice` | holds all of the above | | 9G in total, CPUWeight 50 |
| `protea-dev.target` | groups them | | |

The images are the ones `docker-compose.yml` pins, by the same digests.

Files:

- `quadlet/`: the three containers for rootless podman (Quadlet) and the
  broker's `rabbitmq.conf`.
- `systemd/`: the slice, the target, the readiness gate, the worker template
  and the API unit.
- `protea-dev-clock-gate.sh`: the clock gate every dev process runs before it
  starts (below).

The readiness gate is `deploy/systemd/protea-wait-infra.sh`, the same script
production uses, told the dev ports through `PROTEA_WAIT_ENDPOINTS`.

## Rules that keep dev and production apart

- Every dev unit is named `protea-dev-*`. `protea-node-sync` picks the
  production worker units on this machine by their template name and stops
  and restarts them around every sync of the production tree. Never give a
  dev unit that name, and never start a dev worker through the production
  template.
- Never run `scripts/manage.sh`, `scripts/deploy.sh` or `scripts/cold-boot.sh`
  on this machine. They find workers by command-line pattern
  (`pkill -f "scripts/worker[.]py"`) and would stop the production worker
  together with dev's.
- Never run `deploy/systemd/install.sh` or `deploy/systemd/uninstall.sh` here.
  They manage the laptop's units, and `uninstall.sh` removes every
  `protea*.service` in `~/.config/systemd/user`, which on this machine
  includes the dev units and the production worker units.
- Never `podman system prune`: with `-a` it deletes every unused image on
  this machine, other projects' images included.
- Never reuse a production number: not 5432, 5672 or 9000, not the vhost
  `/`, not the bucket `protea`. A dev process that lost a setting must find
  nothing rather than production. Artifact URIs embed the bucket name
  (`s3://<bucket>/<key>`), so a dev bucket called `protea` would make dev
  URIs read as production ones.
- The GPU is hidden from dev workers: the worker template sets
  `CUDA_VISIBLE_DEVICES` to an empty value. The production GPU worker shares
  the single card, and two models on one 12 GB card is an out-of-memory for
  whichever loads second. Do not enable `protea.embeddings.batch`,
  `protea.methods` or the residue-encoding queues for dev until the author
  decides who owns the card. `protea.methods` launches containers that
  request the device directly, so the empty variable does not cover it.

## Prerequisites

1. `/srv/protea-dev` is a mount point: the dev disk, or an image file mounted
   there while the disk is pending (which disk backs it is still to be
   decided). Every dev unit asserts it (`AssertPathIsMountPoint`), so with the
   volume missing dev stays down instead of writing hundreds of GB onto the
   root filesystem that production lives on. Create the data directories:

   ```bash
   mkdir -p /srv/protea-dev/{postgres,rabbitmq,minio,cache,deploy}
   ```

2. A dev clone at `/srv/protea-dev/deploy/PROTEA`, checked out detached at the
   dev revision. It is a separate clone, not a worktree of another tree, and
   it must hold no `.env` and no `protea/config/system.yaml`: settings read
   both, and anything the environment file leaves out would silently come
   from them. The units run two scripts from this clone
   (`deploy/dev/protea-dev-clock-gate.sh` and
   `deploy/systemd/protea-wait-infra.sh`), so its revision must contain both.
3. A venv at `/srv/protea-dev/venv` with the clone's locked dependencies. The
   units call it directly and never through `poetry run`.
4. Rootless podman 5.x (Quadlet), and lingering enabled for the user, so that
   user units run without a login session.

## Secrets

Secrets live in files under `~/.secrets` with mode 0600, or in podman's
secret store. They enter through stdin or an environment file, and never
through a command line, which every local user can read in `/proc`. Nothing
secret is committed here.

Generate them once. The parentheses keep `umask 077` inside a subshell, so
the install step below does not inherit it:

```bash
(
  umask 077
  mkdir -p ~/.secrets/protea-dev
  for name in postgres rabbitmq minio jwt; do
    openssl rand -hex 24 | tr -d '\n' > ~/.secrets/protea-dev/"$name"
  done
  podman secret create protea-dev-postgres-password - < ~/.secrets/protea-dev/postgres
  podman secret create protea-dev-rabbitmq-password - < ~/.secrets/protea-dev/rabbitmq
  podman secret create protea-dev-minio-root-password - < ~/.secrets/protea-dev/minio
)
```

The trailing newline is stripped on purpose: a podman secret keeps it, and the
password inside the container would no longer match the one in the URLs.

## The environment file

The dev units read `~/.secrets/protea-dev.env` (mode 0600, plain `KEY=VALUE`
lines, never committed). A missing file fails the unit rather than starting a
process on settings defaults. Values in the file override the units'
`Environment=` lines, which is why some names must stay out of it (below).

Write the secret half from the files above, then append the rest. `printf`
is a shell builtin, so the values never become the arguments of another
process; `cat` only ever sees a path:

```bash
(
umask 077
{
  printf 'PROTEA_DB_URL=postgresql+psycopg://protea_dev:%s@127.0.0.1:26432/protea_dev\n' "$(cat ~/.secrets/protea-dev/postgres)"
  printf 'PROTEA_AMQP_URL=amqp://protea_dev:%s@127.0.0.1:26672/protea-dev\n' "$(cat ~/.secrets/protea-dev/rabbitmq)"
  printf 'PROTEA_MINIO_SECRET_KEY=%s\n' "$(cat ~/.secrets/protea-dev/minio)"
  printf 'PROTEA_JWT_SECRET=%s\n' "$(cat ~/.secrets/protea-dev/jwt)"
} > ~/.secrets/protea-dev.env
cat >> ~/.secrets/protea-dev.env <<'EOF'
PROTEA_ENVIRONMENT=dev
PROTEA_STORAGE_BACKEND=minio
PROTEA_MINIO_ENDPOINT=127.0.0.1:26900
PROTEA_MINIO_BUCKET=protea-dev
PROTEA_MINIO_ACCESS_KEY=protea-dev-root
PROTEA_MINIO_SECURE=false
PROTEA_ALLOWED_ORIGINS='["http://127.0.0.1:26800"]'
PROTEA_SKIP_TAXONOMY_WARMUP=1
HF_HOME=/srv/protea-dev/cache/huggingface
HF_HUB_OFFLINE=1
PROTEA_REF_CACHE_DIR=/srv/protea-dev/cache/ref_cache
PROTEA_PRED_CACHE_DIR=/srv/protea-dev/cache/pred_cache
PROTEA_ALIGN_CACHE_DIR=/srv/protea-dev/cache/align_cache
PROTEA_PCA_ARTIFACTS_DIR=/srv/protea-dev/cache/pca
PROTEA_EXPORT_RESUME_DIR=/srv/protea-dev/cache/export_resume
EOF
)
```

What each line is for:

- The URLs name the dev ports, the database `protea_dev` and the vhost
  `protea-dev`. The hex passwords need no escaping inside a URL.
- MinIO: the dev bucket (created on first use), with the MinIO root user as
  the access key.
- `PROTEA_JWT_SECRET`: authentication stays on in dev, and the API refuses to
  start without it.
- `PROTEA_ALLOWED_ORIGINS` is a JSON list. The single quotes keep its inner
  double quotes, both for systemd and for a shell that sources the file.
- `PROTEA_SKIP_TAXONOMY_WARMUP=1`: without it a worker downloads today's
  taxonomy at every start.
- `HF_HOME` and `HF_HUB_OFFLINE=1`: models come from the dev cache, never
  from a download at run time.
- The cache variables keep every cache on the dev volume.

Never set these in the file:

- `PROTEA_ALLOW_DB_RESET`: it enables `POST /admin/reset-db`.
- `PROTEA_IA_FEATURE_PATH`, `PROTEA_INTERPRO_GO_PRED_PATH`,
  `PROTEA_ANC2VEC_PATH`, `PROTEA_CLASSIFIER_*`, `PROTEA_LEARNED_ENCODER_*`
  and `PROTEA_TWO_TOWER_*`: each reads an artefact by path from outside the
  dev universe.
- `PROTEA_EXPORT_MINIJOBS`: it switches dataset exports to another pipeline.
- `DATABASE_URL` and `AMQP_URL`: legacy aliases that settings promote when a
  `PROTEA_*` name is missing, so a stray one can point dev at another server.

Also keep these out of the file, because the file would override the units:

- `CUDA_VISIBLE_DEVICES`: any value there hands the card to every dev worker.
- `PROTEA_AUTHN_REQUIRED`: `false` makes every local process an admin of the
  dev API.

## Install

Copy the sources into place. Copying starts nothing:

```bash
mkdir -p ~/.config/containers/systemd/protea-dev
install -m 0644 /srv/protea-dev/deploy/PROTEA/deploy/dev/quadlet/* ~/.config/containers/systemd/protea-dev/
install -m 0644 /srv/protea-dev/deploy/PROTEA/deploy/dev/systemd/* ~/.config/systemd/user/
```

`rabbitmq.conf` goes with the Quadlet files on purpose: the broker reads it
from beside its unit, never from a checkout that can move under it. It reads
it as its own user inside the container, which is why the mode is explicit:
a copy made under `umask 077` would be unreadable to it.

Check them without side effects:

```bash
QUADLET_UNIT_DIRS=~/.config/containers/systemd/protea-dev /usr/libexec/podman/quadlet -dryrun -user
systemd-analyze verify --user ~/.config/systemd/user/protea-dev-api.service
```

Load them and choose the workers. `daemon-reload` runs the Quadlet generator
and `enable` only adds the units to `protea-dev.target`; neither starts
anything:

```bash
systemctl --user daemon-reload
systemctl --user enable protea-dev-wait-infra.service protea-dev-api.service \
    protea-dev-worker@protea.ping.service protea-dev-worker@protea.jobs.service \
    protea-dev-worker@reaper.service
```

Enabling `protea-dev.target` itself would bring dev up at every boot, which
is a separate decision.

## First start

Every step below starts something, so each one waits for the author's word.

1. The infrastructure alone (the gate pulls the three containers in; the
   first start also pulls the images):

   ```bash
   systemctl --user start protea-dev-wait-infra.service
   podman exec protea-dev-rabbitmq rabbitmqctl list_vhosts   # shows protea-dev
   ```

2. The schema:

   ```bash
   ( set -a; . ~/.secrets/protea-dev.env; set +a
     cd /srv/protea-dev/deploy/PROTEA && /srv/protea-dev/venv/bin/alembic upgrade head )
   ```

3. The first operator API key. `scripts/bootstrap_operator_key.py` reads the
   database URL from the environment only, prints the key once on stdout and
   stores only its hash. It refuses while an unrevoked operator or admin key
   exists:

   ```bash
   ( set -a; . ~/.secrets/protea-dev.env; set +a
     umask 077
     cd /srv/protea-dev/deploy/PROTEA
     /srv/protea-dev/venv/bin/python scripts/bootstrap_operator_key.py --name dev-operator \
         > ~/.secrets/protea-dev/operator-key )
   ```

4. The rest:

   ```bash
   systemctl --user start protea-dev.target
   ```

5. A ping through the whole stack. The key goes in a header read from the
   file, never on the command line:

   ```bash
   curl -sS -H @<(printf 'X-Api-Key: %s\n' "$(cat ~/.secrets/protea-dev/operator-key)") \
       -H 'Content-Type: application/json' \
       -d '{"operation": "ping", "queue_name": "protea.ping"}' \
       http://127.0.0.1:26800/v1/jobs
   ```

## The clock gate

This machine dual-boots, and after a Windows session Linux comes up with the
clock hours off until chronyd steps it. Job leases last 120 s and the reaper
acts on expired leases, so a step of hours can reap healthy jobs and stamps
events in the future. `protea-dev-clock-gate.sh` runs as `ExecStartPre` of
every dev worker and of the API:

- It waits until `timedatectl` reports a synchronised clock, for up to
  `PROTEA_DEV_CLOCK_WAIT` seconds (default 120, capped at 600, 0 disables).
- It never refuses: when the budget runs out the unit starts anyway, says so,
  and does not wait again for that unit on this boot.
- A waiting unit shows as `activating`. The units' `TimeoutStartSec` (660)
  stays above the cap so that systemd never kills the wait.
- Its last line carries `clock=ok`, `clock=unsynced` or `clock=unchecked`:

  ```bash
  journalctl --user -u 'protea-dev-*' | grep 'clock='
  ```

`After=time-sync.target` would not do this: a user manager has no
`time-sync.target`, so the ordering would order against nothing.

## Not wired yet

- The revision guard (`deploy/systemd/protea-guard-revision.sh`) reads only
  the production declaration, so it would refuse every dev start. The dev
  units take it once it reads the declaration path from the unit.
- The in-code environment check, where a process with
  `PROTEA_ENVIRONMENT=dev` refuses any vhost or database that is not dev's.
- A taxonomy path that never downloads.
- A unit for the GPU queues, after the decision on the card.

## Remove

Stopping waits for the author's word as starting does:

```bash
systemctl --user stop protea-dev.target
systemctl --user disable protea-dev-wait-infra.service protea-dev-api.service \
    protea-dev-worker@protea.ping.service protea-dev-worker@protea.jobs.service \
    protea-dev-worker@reaper.service
rm -r ~/.config/containers/systemd/protea-dev
rm ~/.config/systemd/user/protea-dev.slice ~/.config/systemd/user/protea-dev.target \
   ~/.config/systemd/user/protea-dev-wait-infra.service \
   ~/.config/systemd/user/protea-dev-api.service \
   ~/.config/systemd/user/protea-dev-worker@.service
systemctl --user daemon-reload
```

The data under `/srv/protea-dev`, the podman secrets
(`podman secret rm protea-dev-...`) and the files under `~/.secrets` stay
until removed by hand.
