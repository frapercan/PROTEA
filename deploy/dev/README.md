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
| `protea-dev-postgres` (Quadlet) | Postgres 18 with pgvector | `127.0.0.1:26432` | 3G |
| `protea-dev-rabbitmq` (Quadlet) | RabbitMQ 4.3.5 | `127.0.0.1:26672` (AMQP), `127.0.0.1:26673` (management) | 1G |
| `protea-dev-minio` (Quadlet) | MinIO | `127.0.0.1:26900` (API), `127.0.0.1:26901` (console) | 768M |
| `protea-dev-wait-infra.service` | waits until the three answer | | |
| `protea-dev-worker@<queue>.service` | one queue worker per instance | | 2G each; 8G and no swap for `protea.training` and `protea.evaluations` (drop-ins, 2026-11-18); 44G, no swap and the aspect searches in series for `protea.predictions.batch` (drop-in, 2026-11-19; 12G before) |
| `protea-dev-api.service` | the API | `127.0.0.1:26800` | 768M |
| `protea-dev-web` (Quadlet, with `protea-dev-web.build`) | the web front (Next.js) | `127.0.0.1:26300` | 768M |
| `protea-dev.slice` | holds all of the above | | 50G in total (20G until 2026-11-19, 9G until 2026-11-18), CPUWeight 50 |
| `protea-dev.target` | groups them | | |

The images are the ones `docker-compose.yml` pins, by the same digests.

Files:

- `quadlet/`: the three infrastructure containers and the web front for rootless
  podman (Quadlet), the web front's image build, and the broker's
  `rabbitmq.conf`.
- `web/Containerfile`: the web front's image (below, "The web front").
- `systemd/`: the slice, the target, the readiness gate, the worker template
  and the API unit.
- `protea-dev-clock-gate.sh`: the clock gate every dev process runs before it
  starts (below).
- `protea-dev-disk-gate.sh`: the disk gate every dev unit runs before it
  starts ("Disk", below).

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
  decides who owns the card. Hidden is not refused: with no card in sight
  `compute_embeddings` runs on the processors with only a warning event.
  And `protea.methods` launches containers that request the device
  directly, so the empty variable does not cover it.

## Prerequisites

1. The data root `~/protea-dev`, on the system disk. The author decided on
   2026-11-15 to work with the disk each machine has: no new disk and no
   copies. Every dev unit asserts that the directory exists, and runs the disk
   gate first (below, "Disk"):

   ```bash
   mkdir -p ~/protea-dev/{postgres,rabbitmq,minio,cache,deploy}
   ```

   The Postgres and RabbitMQ images change the owner of their data directory
   from inside the container, which rootless podman allows only on directories
   this user owns, as these are.

2. A dev clone at `~/protea-dev/deploy/PROTEA`, checked out detached at the
   dev revision. It is a separate clone, not a worktree of another tree, and
   it must hold no `.env` and no `protea/config/system.yaml`: settings read
   both, and anything the environment file leaves out would silently come
   from them. The units run two scripts from this clone
   (`deploy/dev/protea-dev-clock-gate.sh` and
   `deploy/systemd/protea-wait-infra.sh`), so its revision must contain both.
3. A venv at `~/protea-dev/venv` with the clone's locked dependencies. The
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
HF_HUB_OFFLINE=1
EOF
# Paths are absolute in the file (systemd does not expand $HOME there), so
# they are written from the shell, which does.
{
  printf 'HF_HOME=%s\n' "$HOME/protea-dev/cache/huggingface"
  printf 'PROTEA_REF_CACHE_DIR=%s\n' "$HOME/protea-dev/cache/ref_cache"
  printf 'PROTEA_PRED_CACHE_DIR=%s\n' "$HOME/protea-dev/cache/pred_cache"
  printf 'PROTEA_ALIGN_CACHE_DIR=%s\n' "$HOME/protea-dev/cache/align_cache"
  printf 'PROTEA_PCA_ARTIFACTS_DIR=%s\n' "$HOME/protea-dev/cache/pca"
  printf 'PROTEA_EXPORT_RESUME_DIR=%s\n' "$HOME/protea-dev/cache/export_resume"
  printf 'PROTEA_TAXONOMY_DB_PATH=%s\n' "$HOME/protea-dev/cache/taxonomy/taxa.sqlite"
} >> ~/.secrets/protea-dev.env
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
- `PROTEA_SKIP_TAXONOMY_WARMUP=1`: without it the workers of
  `protea.jobs`, `protea.predictions.batch` and `protea.training` open the
  NCBI taxonomy before consuming anything, and download today's when the
  file is missing (see "Not wired yet").
- `HF_HOME` and `HF_HUB_OFFLINE=1`: models come from the dev cache, never
  from a download at run time.
- The cache variables keep every cache under `~/protea-dev/cache`.
- `PROTEA_TAXONOMY_DB_PATH`: the declared taxonomy file. While it does not
  exist, a taxonomy feature refuses instead of downloading today's NCBI
  taxonomy into `~/.etetoolkit`, the file production on this machine reads.
  The universe builds the taxonomy of each cut (phase 2).
- `PROTEA_ENVIRONMENT=dev`: workers and the API refuse to start (exit 78)
  against a database other than `protea_dev` or a vhost other than
  `protea-dev`.

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
install -m 0644 ~/protea-dev/deploy/PROTEA/deploy/dev/quadlet/* ~/.config/containers/systemd/protea-dev/
find ~/protea-dev/deploy/PROTEA/deploy/dev/systemd -maxdepth 1 -type f -exec install -m 0644 {} ~/.config/systemd/user/ \;
for d in ~/protea-dev/deploy/PROTEA/deploy/dev/systemd/*.service.d; do
  install -d ~/.config/systemd/user/"$(basename "$d")" && install -m 0644 "$d"/*.conf ~/.config/systemd/user/"$(basename "$d")"/
done
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

Every step below starts something. The author allowed it on 2026-11-15, each
step announced to him before it runs.

1. The infrastructure alone (the gate pulls the three containers in; the
   first start also pulls the images):

   ```bash
   systemctl --user start protea-dev-wait-infra.service
   podman exec protea-dev-rabbitmq rabbitmqctl list_vhosts   # shows protea-dev
   ```

2. The schema:

   ```bash
   ( set -a; . ~/.secrets/protea-dev.env; set +a
     cd ~/protea-dev/deploy/PROTEA && ~/protea-dev/venv/bin/alembic upgrade head )
   ```

3. The first operator API key. `scripts/bootstrap_operator_key.py` reads the
   database URL from the environment only, prints the key once on stdout and
   stores only its hash. It refuses while an unrevoked operator or admin key
   exists:

   ```bash
   ( set -a; . ~/.secrets/protea-dev.env; set +a
     umask 077
     cd ~/protea-dev/deploy/PROTEA
     ~/protea-dev/venv/bin/python scripts/bootstrap_operator_key.py --name dev-operator \
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

## Disk

Development shares the system disk with production on this machine, and
nothing is backed up: the author decided on 2026-11-15 to work with the disk
each machine has, with no copies. If the dev data is lost, the universe is
rebuilt from public sources by the same operations that built it.

`protea-dev-disk-gate.sh` runs first in every dev unit. It refuses to start,
with exit status 78, when the file system that holds `~/protea-dev` has less
than `PROTEA_DEV_MIN_FREE_GB` free (default 100), or when the directory is
missing. The units also carry `StartLimitBurst=3` within 15 minutes, so a
refusal does not loop. Its last line carries `disk=ok`:

```bash
journalctl --user -u 'protea-dev-*' | grep -E 'disk=ok|refused'
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

- The revision guard (`deploy/systemd/protea-guard-revision.sh`). It takes
  its declaration from `PROTEA_DECL_PATH`, but the dev units do not run it
  yet: dev runs on one machine, from a clone checked out at the dev revision.
  It joins them with the cluster pilots, together with a dev declaration in
  agent-farm.
- A unit for the GPU queues, after the decision on the card.
- CPU priority for production. `CPUWeight=50` on `protea-dev.slice` only
  weighs against the other children of `protea.slice`, and it has none.
  `protea.slice` itself competes with `app.slice`, where the production
  worker runs, at the default weight of 100 each, so when both are busy dev
  gets as large a share as everything in `app.slice` together, production
  included. Production wins only with a weight at that level, on
  `protea.slice` or on a dev slice that is not nested under it, which is the
  author's decision.

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

The data under `~/protea-dev`, the podman secrets
(`podman secret rm protea-dev-...`) and the files under `~/.secrets` stay
until removed by hand.

## The web front

`protea-dev-web` serves `apps/web` of the dev clone on `127.0.0.1:26300`, built
by `protea-dev-web.build` from `web/Containerfile`: the production Dockerfile with
the variables Next.js bakes in at build time, on the same Node major, pinned by
digest. The browser talks to the API through the app's own proxy
(`/api-proxy`, rewritten to `127.0.0.1:26800`), so the API keeps its loopback
binding and its allowed origins. The container runs on the host's network so its
server side reaches the API on the loopback too, and it binds to loopback for the
same reason the API does: the API's reads need no credential.

The image is rebuilt whenever the unit starts. Podman's layer cache makes that
free when `apps/web` did not change, and a few minutes when it did, so a move of
the clone to another revision is followed by `systemctl --user restart
protea-dev-web`. Install it with the other Quadlet files:

```bash
cp deploy/dev/quadlet/protea-dev-web.build deploy/dev/quadlet/protea-dev-web.container \
  ~/.config/containers/systemd/protea-dev/
systemctl --user daemon-reload
systemctl --user start protea-dev-web
```

