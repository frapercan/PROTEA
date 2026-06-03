Operational Runbook
===================

Practical guide for operating PROTEA: starting the system, diagnosing
problems, and maintaining infrastructure.

.. contents:: Contents
   :local:
   :depth: 2


Day-to-day operations
---------------------

Starting and stopping
~~~~~~~~~~~~~~~~~~~~~

The dev stack is split in two: the **infrastructure** (PostgreSQL, RabbitMQ,
MinIO) runs in ``docker compose``, and the **application** (API, workers,
frontend) runs bare-metal under ``manage.sh`` so code changes hot-reload
without container rebuilds.

.. code-block:: bash

   # Prerequisite: bring up the infrastructure once (idempotent — leaves
   # running containers alone)
   docker compose --profile storage up -d postgres rabbitmq minio

   # Start the application stack (API + workers + frontend)
   bash scripts/manage.sh start

   # Start with 3 batch workers per GPU pipeline
   bash scripts/manage.sh start 3

   # Check what is running
   bash scripts/manage.sh status

   # Stop everything
   bash scripts/manage.sh stop

Checking that everything works
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   # Liveness: is the API process alive?
   curl http://127.0.0.1:8000/health
   # -> {"status": "ok"}

   # Readiness: can it connect to DB and RabbitMQ?
   curl http://127.0.0.1:8000/health/ready
   # -> {"status": "ready"}  or  503 if something is down

If ``/health/ready`` returns 503, check that Docker containers are running
and that the URLs in ``protea/config/system.yaml`` are correct.

Scaling workers
~~~~~~~~~~~~~~~

Batch workers are stateless; they can be added on the fly:

.. code-block:: bash

   bash scripts/manage.sh scale protea.predictions.batch 2
   bash scripts/manage.sh scale protea.embeddings.batch 3

Scaling is linear for batch queues.

.. warning::

   The ``protea.embeddings`` queue must have **exactly one** consumer.
   The coordinator serialises GPU access; multiple coordinators step on
   each other and cause ``RetryLaterError`` storms.

Remote access
~~~~~~~~~~~~~

For demos or access from outside the local network:

.. code-block:: bash

   bash scripts/expose.sh

Opens an ngrok tunnel to the frontend (port 3000) with a static domain
(``protea.ngrok.app``).  API calls are proxied through Next.js rewrites,
so only one tunnel is needed.  Requires ngrok installed and authenticated.
Closes with Ctrl+C.


Troubleshooting
---------------

Jobs stuck in RUNNING
~~~~~~~~~~~~~~~~~~~~~

A job in ``RUNNING`` that is not progressing usually means the worker died.

**Automatic detection**: the ``worker-reaper`` process checks every 60 s
and marks as ``FAILED`` (error code ``JobTimeout``) any job that has been
in ``RUNNING`` for more than 6 hours (21 600 s).

**Manual intervention**:

.. code-block:: bash

   # Check job status and events
   curl -s http://127.0.0.1:8000/jobs/<job-id> | python -m json.tool
   curl -s http://127.0.0.1:8000/jobs/<job-id>/events | python -m json.tool

   # Cancel (also cancels child sub-jobs)
   curl -s -X POST http://127.0.0.1:8000/jobs/<job-id>/cancel

   # Delete a terminal job
   curl -s -X DELETE http://127.0.0.1:8000/jobs/<job-id>

To re-run, create a new job with the same operation and payload.
There is no "retry" button; jobs are immutable once finished.

Batch failures
~~~~~~~~~~~~~~

Batches (``compute_embeddings_batch``, ``predict_go_terms_batch``) do not
have their own row in ``job``.  To diagnose:

1. **Parent job events.** Failures are recorded as ``child.failed``:

   .. code-block:: bash

      curl -s http://127.0.0.1:8000/jobs/<parent-id>/events?limit=50 | python -m json.tool

2. **Worker logs.** Each worker writes structured JSON:

   .. code-block:: bash

      bash scripts/manage.sh logs embeddings-batch

      # Filter errors only with jq
      cat logs/worker-embeddings-batch-1.log | jq 'select(.level == "ERROR")'

      # Search for a specific job
      cat logs/worker-jobs.log | jq 'select(.message | contains("<job-id>"))'

3. **Dead letter queue.** Permanently failed messages:

   .. code-block:: bash

      # Check how many dead messages there are
      rabbitmqctl list_queues name messages | grep dead-letter

   Also accessible from the RabbitMQ UI: http://localhost:15672
   (guest/guest) -> Queues -> ``protea.dead-letter`` -> Get Message(s).

   To republish a corrected message, use "Move" in the UI.

CUDA out of memory
~~~~~~~~~~~~~~~~~~

When a batch worker runs out of GPU memory:

1. The worker automatically calls ``torch.cuda.empty_cache()`` and
   requeues the message for retry.
2. If it keeps failing, reduce ``batch_size`` in the job payload.
3. Check that no other process is using the GPU:

   .. code-block:: bash

      nvidia-smi

4. If another embedding job is using the GPU, the coordinator detects
   contention via ``RetryLaterError`` and waits with exponential backoff
   (up to 10 minutes between retries).


Maintenance
-----------

Database
~~~~~~~~

.. code-block:: bash

   # Total DB size
   psql postgresql://protea:protea@localhost:5432/protea \
     -c "SELECT pg_size_pretty(pg_database_size('protea'));"

   # Top 10 tables by size
   psql postgresql://protea:protea@localhost:5432/protea \
     -c "SELECT relname, pg_size_pretty(pg_total_relation_size(oid))
         FROM pg_class WHERE relkind='r'
         ORDER BY pg_total_relation_size(oid) DESC LIMIT 10;"

   # Clean up jobs and events older than 30 days
   psql postgresql://protea:protea@localhost:5432/protea \
     -c "DELETE FROM job_event WHERE ts < now() - interval '30 days';"
   psql postgresql://protea:protea@localhost:5432/protea \
     -c "DELETE FROM job WHERE finished_at < now() - interval '30 days'
         AND status IN ('succeeded', 'failed', 'cancelled');"

   # Full reset (destructive: deletes EVERYTHING)
   curl -s -X POST http://127.0.0.1:8000/admin/reset-db

Dead letter queue
~~~~~~~~~~~~~~~~~

Messages in ``protea.dead-letter`` accumulate and are not purged
automatically.  Review periodically:

.. code-block:: bash

   # Purge the DLQ when messages are no longer needed
   rabbitmqctl purge_queue protea.dead-letter

Logs
~~~~

Logs grow without limit.  To truncate without restarting workers:

.. code-block:: bash

   for f in logs/*.log; do : > "$f"; done


Backups
-------

PROTEA's persistent state lives in four places. A complete backup covers
all four; partial backups are useful for targeted recovery (rollback the
DB without replaying MinIO uploads, for example).

What to back up
~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 18 22 60

   * - Source
     - Tool
     - Notes
   * - Postgres ``protea`` DB
     - ``pg_dump -Fc -Z6``
     - Custom format with compression. Includes schema, data, sequences.
       Restore in any order with ``pg_restore --jobs``.
   * - MinIO bucket ``protea``
     - ``mc mirror`` to a host directory
     - Object-level copy of every Dataset, RerankerModel, EvaluationResult
       artefact. Not deduplicated (expect ~bucket-size on disk).
   * - ``protea-reranker-lab/{datasets,runs,experiments}``
     - ``tar -czf``
     - Pulled datasets, training runs, spec catalog. Re-pullable from
       PROTEA but rebuilds boosters from scratch.
   * - ``thesis/`` LaTeX manuscript
     - ``git bundle --all``
     - Local-only git repo; bundle is a single-file portable archive.

Full backup procedure
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   STAMP=$(date +%Y-%m-%d)
   BACKUP=~/Thesis2/backups

   # 1. Postgres dump (custom format, compressed)
   docker exec protea-postgres-1 pg_dump -U protea -d protea \\
     -Fc -Z6 -f "/tmp/protea-${STAMP}.dump"
   docker cp "protea-postgres-1:/tmp/protea-${STAMP}.dump" \\
     "${BACKUP}/protea-${STAMP}.dump"
   docker exec protea-postgres-1 rm "/tmp/protea-${STAMP}.dump"

   # 2. MinIO mirror via a temporary mc container with bind mount
   mkdir -p "${BACKUP}/minio-${STAMP}"
   docker run --rm --network host \\
     -v "${BACKUP}/minio-${STAMP}:/backup" \\
     --entrypoint sh minio/mc:latest -c "
       mc alias set local http://localhost:9000 minioadmin minioadmin &&
       mc mirror --quiet local/protea /backup
     "

   # 3. Lab data (datasets + training runs + spec catalog)
   tar -czf "${BACKUP}/lab-${STAMP}.tar.gz" \\
     -C ~/Thesis2/repositories/protea-reranker-lab \\
     datasets runs experiments

   # 4. Thesis git bundle
   git -C ~/Thesis2/thesis bundle create \\
     "${BACKUP}/thesis-${STAMP}.bundle" --all

Verifying a backup
~~~~~~~~~~~~~~~~~~

Quick (shallow) integrity checks before relying on a backup:

.. code-block:: bash

   # 1. pg_restore can parse the TOC (table of contents)
   docker cp "${BACKUP}/protea-${STAMP}.dump" \\
     protea-postgres-1:/tmp/verify.dump
   docker exec protea-postgres-1 pg_restore --list /tmp/verify.dump | head
   docker exec protea-postgres-1 pg_restore --list /tmp/verify.dump |
     grep -c "TABLE DATA"   # should match expected table count
   docker exec protea-postgres-1 rm /tmp/verify.dump

   # 2. MinIO mirror file count matches bucket
   find "${BACKUP}/minio-${STAMP}/" -type f | wc -l
   docker run --rm --network host --entrypoint sh minio/mc:latest -c "
     mc alias set local http://localhost:9000 minioadmin minioadmin &&
     mc ls --recursive local/protea | wc -l
   "

   # 3. Lab tarball lists files without extracting
   tar -tzf "${BACKUP}/lab-${STAMP}.tar.gz" | head

   # 4. Thesis bundle integrity
   git bundle verify "${BACKUP}/thesis-${STAMP}.bundle"

Deep verification (slow, optional) restores into a temporary database
and compares row counts table-by-table against the live DB.


Disaster recovery
-----------------

Restoring after a database loss
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When the Postgres volume is lost or corrupted but the dump survives:

.. code-block:: bash

   # 1. Stop the application stack so no client touches the DB
   bash scripts/manage.sh stop

   # 2. Drop and recreate the database (run each command separately;
   #    DROP DATABASE cannot run inside a transaction block)
   docker exec protea-postgres-1 psql -U protea -d postgres \\
     -c "DROP DATABASE IF EXISTS protea;"
   docker exec protea-postgres-1 psql -U protea -d postgres \\
     -c "CREATE DATABASE protea OWNER protea;"

   # 3. Copy the dump into the container and restore in parallel
   docker cp ~/Thesis2/backups/protea-2026-05-10.dump \\
     protea-postgres-1:/tmp/restore.dump
   docker exec protea-postgres-1 pg_restore -U protea -d protea \\
     --jobs=4 --no-owner /tmp/restore.dump
   docker exec protea-postgres-1 rm /tmp/restore.dump

   # 4. Verify table count and a few row counts
   docker exec protea-postgres-1 psql -U protea -d protea \\
     -c "SELECT count(*) FROM information_schema.tables
         WHERE table_schema='public';"
   docker exec protea-postgres-1 psql -U protea -d protea \\
     -c "SELECT count(*) FROM job;"

   # 5. Restart the stack
   bash scripts/manage.sh start

Restoring the MinIO bucket
~~~~~~~~~~~~~~~~~~~~~~~~~~

When the ``protea_minio_data`` volume is lost but the local mirror survives:

.. code-block:: bash

   # mc create + mirror back from the host directory
   docker run --rm --network host \\
     -v ~/Thesis2/backups/minio-2026-05-10:/backup:ro \\
     --entrypoint sh minio/mc:latest -c "
       mc alias set local http://localhost:9000 minioadmin minioadmin &&
       mc mb local/protea --ignore-existing &&
       mc mirror --quiet /backup local/protea
     "

Full from-zero rebuild
~~~~~~~~~~~~~~~~~~~~~~

When the entire Docker Desktop VM is reset (recoverable because all
state lives in named volumes that get recreated empty):

.. code-block:: bash

   # 1. Stop everything
   bash scripts/manage.sh stop
   pkill -f "ngrok http" || true

   # 2. Stop Docker Desktop and wipe the VM disk
   docker desktop stop
   rm ~/.docker/desktop/vms/0/data/Docker.raw
   docker desktop start

   # 3. Wait for the daemon to come up
   while ! docker ps >/dev/null 2>&1; do sleep 5; done

   # 4. Bring the infra back up (creates fresh empty volumes)
   docker compose --profile storage up -d postgres rabbitmq minio
   # Wait for "healthy"
   until docker compose --profile storage ps |
         grep -E "(postgres|rabbitmq|minio).*healthy" |
         wc -l | grep -q 3; do sleep 5; done

   # 5. Restore Postgres + MinIO from backups (sections above)

   # 6. Restart the application stack
   bash scripts/manage.sh start

   # 7. Re-expose via ngrok if needed
   bash scripts/expose.sh


Docker Desktop disk reclamation
-------------------------------

Docker Desktop on Linux holds container/image state inside a sparse
disk image at ``~/.docker/desktop/vms/0/data/Docker.raw``. When you
remove containers, images or volumes the freed space stays *inside*
the VM. ``docker system df`` reports the new low usage but ``df -h``
on the host still shows the old footprint.

What works
~~~~~~~~~~

Reset the VM disk completely (recoverable via the from-zero rebuild
procedure above):

.. code-block:: bash

   docker desktop stop
   rm ~/.docker/desktop/vms/0/data/Docker.raw
   docker desktop start

What does not work reliably
~~~~~~~~~~~~~~~~~~~~~~~~~~~

* ``fstrim`` from a privileged container does run, but the discard
  commands rarely propagate to the host raw file (in-place hole
  punching depends on hypervisor support that Docker Desktop does not
  guarantee on Linux).
* ``qemu-img convert`` to compact requires roughly the actual data
  size in temporary disk space; impractical when the VM is already
  consuming most of the drive.

Recommendation: take backups, then nuke ``Docker.raw`` and restore.
The procedure above takes about 15-30 minutes for a 60 GB DB and a
40 GB bucket.
