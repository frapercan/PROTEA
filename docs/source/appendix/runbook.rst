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

.. code-block:: bash

   # Prerequisite: PostgreSQL and RabbitMQ must be running
   docker start pgvectorsql rabbitmq

   # Start everything (API + workers + frontend)
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

Batch workers are stateless — they can be added on the fly:

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
There is no "retry" button — jobs are immutable once finished.

Batch failures
~~~~~~~~~~~~~~

Batches (``compute_embeddings_batch``, ``predict_go_terms_batch``) do not
have their own row in ``jobs``.  To diagnose:

1. **Parent job events** — failures are recorded as ``child.failed``:

   .. code-block:: bash

      curl -s http://127.0.0.1:8000/jobs/<parent-id>/events?limit=50 | python -m json.tool

2. **Worker logs** — each worker writes structured JSON:

   .. code-block:: bash

      bash scripts/manage.sh logs embeddings-batch

      # Filter errors only with jq
      cat logs/worker-embeddings-batch-1.log | jq 'select(.level == "ERROR")'

      # Search for a specific job
      cat logs/worker-jobs.log | jq 'select(.message | contains("<job-id>"))'

3. **Dead letter queue** — permanently failed messages:

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
     -c "DELETE FROM job_events WHERE ts < now() - interval '30 days';"
   psql postgresql://protea:protea@localhost:5432/protea \
     -c "DELETE FROM jobs WHERE finished_at < now() - interval '30 days'
         AND status IN ('succeeded', 'failed', 'cancelled');"

   # Full reset (destructive — deletes EVERYTHING)
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
