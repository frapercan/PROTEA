Measuring Performance
======================

This page describes how to profile PROTEA operations. It covers two
lightweight tools (scalene, pyinstrument) and the structured event log
that PROTEA writes to its database.

.. rubric:: PROTEA's built-in timing: JobEvent

Every ``Operation.execute`` call emits structured events via the ``emit``
callback. Timing information is available from the DB without any extra
tooling:

.. code-block:: sql

   SELECT event, created_at,
          payload->>'elapsed_s' AS elapsed_s
   FROM   job_events
   WHERE  job_id = '<your-job-uuid>'
   ORDER  BY created_at;

The ``export_research_dataset`` operation emits events with the
``export_research_dataset.*`` prefix (e.g.,
``export_research_dataset.knn_done``,
``export_research_dataset.alignment_done``) so each sub-step can be
timed from the event log alone.

.. rubric:: Profiling a single job run

PROTEA has no single-job CLI runner. Jobs are dispatched to a queue and a
worker consumes them, so to profile one operation end-to-end you enqueue
exactly one job and run the worker for its queue under a profiler.

**1. Dispatch one job.** ``POST /jobs`` with the operation name, its queue,
and the operation-specific payload. This is the only supported dispatch path;
do not poke other endpoints by hand. For example, to profile an
``export_research_dataset`` run:

.. code-block:: json

   {
     "operation": "export_research_dataset",
     "queue_name": "protea.training",
     "payload": {"...": "operation-specific; see architecture/operations"}
   }

The response is ``{"id": "<job-uuid>", "status": "queued"}``.

**2. Profile the worker that consumes it.** ``scalene`` is the bundled
profiler (line-level CPU + GPU + memory). Point it at ``scripts/worker.py``
on the same queue; the worker claims the one queued job, runs it, then idles:

.. parsed-literal::

   poetry run scalene `--cpu` `--gpu` `--memory` \\
       scripts/worker.py `--queue` protea.training

Once the job reaches ``SUCCEEDED`` (poll ``GET /jobs/<job-uuid>`` or read the
JobEvent log above), stop the worker with ``Ctrl+C``. It logs
``Worker stopped.`` and exits cleanly, so scalene writes its HTML report to
the current directory. Because the worker is a continuous consumer, the
profile also captures the idle wait on the AMQP socket between messages; that
time sits inside the consumer's blocking read and is easy to discount.

The PERF.1 slice will publish pre-computed flamegraphs from the FARM-EXP.13
run under ``docs/perf/`` once that slice lands.

.. rubric:: cProfile + pstats (function-level, standard library)

For a function-level view without any extra dependency, wrap the same worker
with the standard-library ``cProfile`` and inspect the dump with ``pstats``:

.. parsed-literal::

   poetry run python -m cProfile -o /tmp/protea.prof \\
       scripts/worker.py `--queue` protea.training
   poetry run python -m pstats /tmp/protea.prof

The ``cProfile`` output file is written when the interpreter exits, which the
clean ``Ctrl+C`` shutdown above triggers.

.. rubric:: Known gap: no single-job CLI

The queue-plus-worker recipe profiles the whole worker process, not one job
in isolation, so it also samples the consumer's idle wait. A thin CLI that
runs a single job by id and exits would be cleaner, especially for
call-stack profilers that report only on process exit. PROTEA once shipped
``scripts/run_one_job.py`` for exactly this, but it was removed (commit
``80ed10e``) once it drifted out of step with the queue-driven worker. The
internal entry point still exists as ``BaseWorker.handle_job(job_id)`` in
``protea/workers/base_worker.py`` (it claims a QUEUED job, runs its operation,
and records the terminal transition), but nothing exposes it on the command
line today. Until a supported wrapper lands, profile the continuous worker as
above, or read per-step timings straight from the JobEvent log at the top of
this page, which needs no profiler at all.

.. rubric:: Interpreting hot paths

Based on FARM-EXP.13 measurements, the typical cost breakdown for a
single ``export_research_dataset`` cell is:

- GPU embedding pass: 70-90% of wall clock (PLM-dependent)
- Pairwise alignment: 5-20% (cold cache); under 1% (warm cache, PR #421)
- KNN search: 3-8%
- DB queries + parquet IO: under 2%

If alignment dominates even on a warm cache, verify that
``PROTEA_PAIR_FEATURE_WORKERS`` is set and that
``PROTEA_ALIGN_CACHE_DIR`` points to a writable directory.

.. rubric:: Forward reference: PERF.1 flamegraphs

The upcoming PERF.1 slice will publish scalene HTML reports for each of
the 24 FARM-EXP.13 cells under ``docs/perf/``. This page will be
updated with direct links once that slice ships.

.. rubric:: Cross-reference

Thesis Ch. 5.6 summarises the profiling methodology and reproduces the
top-line measurements used to motivate the process-pool + cache design
in PR #421.
