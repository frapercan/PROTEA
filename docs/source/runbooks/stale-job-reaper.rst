Stale Job Reaper
================

``StaleJobReaper`` (``protea/workers/stale_job_reaper.py``) runs on a
60-second timer and transitions stuck RUNNING jobs to FAILED. It applies
two thresholds:

- ``timeout_seconds`` (default 86400s / 24 h): a job must have been running
  longer than this before the reaper considers it a candidate.
- ``stall_seconds`` (default 1800s / 30 min): even if the wall-clock threshold
  is exceeded, the reaper skips jobs that have emitted a ``JobEvent`` row
  within the last stall window (coordinator jobs legitimately run for hours
  while child batch messages make forward progress).

Both thresholds are configurable via tuning knobs
``PROTEA_TUNING__WORKER__REAPER_MAIN_TIMEOUT_SECONDS`` and
``PROTEA_TUNING__WORKER__REAPER_STALL_SECONDS``.

Symptoms
--------

- ``GET /jobs?status=running`` returns jobs whose ``started_at`` is many hours
  in the past and whose ``finished_at`` is ``null``.
- Workers appear idle in ``bash scripts/manage.sh status`` (no CPU / GPU
  activity) yet the DB still shows a RUNNING job.
- ``logs/worker-reaper.log`` emits repeated ``Reaper cycle failed`` errors,
  meaning the reaper itself has crashed.
- A job transitions to FAILED with ``error_code = "JobTimeout"`` unexpectedly
  fast, indicating thresholds may be set too aggressively.

Diagnosis
---------

1. **Identify stuck jobs:**

   .. code-block:: sql

      -- Jobs RUNNING for more than 1 hour
      SELECT id, operation, status, started_at,
             EXTRACT(EPOCH FROM (now() - started_at))/3600 AS hours_running
      FROM job
      WHERE status = 'running'
        AND started_at < now() - interval '1 hour'
      ORDER BY started_at;

2. **Check the most recent JobEvent for each candidate:**

   .. code-block:: sql

      -- Last activity timestamp per stuck job
      SELECT j.id, j.operation, j.started_at,
             max(je.ts) AS last_event_ts,
             EXTRACT(EPOCH FROM (now() - max(je.ts)))/60 AS minutes_silent
      FROM job j
      LEFT JOIN job_event je ON je.job_id = j.id
      WHERE j.status = 'running'
        AND j.started_at < now() - interval '1 hour'
      GROUP BY j.id, j.operation, j.started_at
      ORDER BY last_event_ts;

3. **Inspect reaper logs:**

   .. code-block:: bash

      bash scripts/manage.sh logs reaper
      # or tail directly:
      tail -100 logs/worker-reaper.log

4. **Confirm the reaper process is alive:**

   .. code-block:: bash

      bash scripts/manage.sh status
      # Look for: worker-reaper  running

5. **Check current tuning values** (defaults shown if no override):

   .. code-block:: bash

      poetry run python -c "
      from protea.config.tuning import get_tuning
      w = get_tuning().worker
      print('timeout:', w.reaper_main_timeout_seconds, 's')
      print('stall:  ', w.reaper_stall_seconds, 's')
      "

Fix
---

1. **If the reaper worker is dead, restart it:**

   .. code-block:: bash

      bash scripts/manage.sh stop
      bash scripts/manage.sh start
      # The reaper is started automatically as 'worker-reaper'.

   Or restart only the reaper without cycling the whole stack:

   .. code-block:: bash

      # Stop existing reaper if any
      pkill -f "worker.py --queue reaper" || true
      # Start a new one
      poetry run python scripts/worker.py --queue reaper \
          >> logs/worker-reaper.log 2>&1 &

2. **If the reaper is running but has not yet fired** (job is under the
   ``timeout_seconds`` threshold): wait, or manually force-fail the job
   if you are certain it is stuck:

   .. code-block:: sql

      UPDATE job
      SET status      = 'failed',
          finished_at  = now(),
          error_code   = 'ManualTimeout',
          error_message = 'Manually marked FAILED by operator'
      WHERE id = '<job-uuid>'
        AND status = 'running';

      INSERT INTO job_event (job_id, ts, level, event, message, fields)
      VALUES (
          '<job-uuid>',
          now(),
          'error',
          'job.timeout',
          'Manually marked FAILED by operator',
          '{}'::jsonb
      );

3. **If the thresholds are too aggressive** for your workload, increase them
   via environment variables before restarting the reaper:

   .. code-block:: bash

      export PROTEA_TUNING__WORKER__REAPER_MAIN_TIMEOUT_SECONDS=172800  # 48h
      export PROTEA_TUNING__WORKER__REAPER_STALL_SECONDS=3600           # 1h
      poetry run python scripts/worker.py --queue reaper >> logs/worker-reaper.log 2>&1 &

4. **Verify the fix:**

   .. code-block:: sql

      -- Confirm no jobs remain stuck
      SELECT count(*) FROM job
      WHERE status = 'running'
        AND started_at < now() - interval '1 hour';

   .. code-block:: bash

      # Watch reaper log for "Reaped N stale job(s)." or "Reaper skipped live job."
      tail -f logs/worker-reaper.log

Prevention
----------

- Keep the reaper worker in the process table at all times. The
  ``manage.sh start`` command launches it as ``worker-reaper``.
- Monitor the ``job`` table for long-running RUNNING rows via a cron or
  alerting query; the presence of RUNNING jobs older than 2x
  ``timeout_seconds`` indicates the reaper has crashed.
- Coordinator operations (``compute_embeddings``, ``predict_go_terms``)
  emit child ``JobEvent`` rows regularly. If a coordinator goes silent,
  inspect the child batch queue (``protea.embeddings.batch``,
  ``protea.predictions.batch``) for depth or OOM events before concluding
  that the coordinator itself is stuck.
- Related source: ``protea/workers/stale_job_reaper.py``,
  ``protea/config/tuning.py`` (``WorkerTuning``).
