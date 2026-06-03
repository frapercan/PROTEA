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

.. rubric:: scalene (line-level CPU + GPU + memory)

scalene is the recommended profiler for PROTEA workers. It samples both
CPU and GPU time per line without requiring code changes.

To profile the export operation module, run scalene with the
`--cpu`, `--gpu`, and `--memory` flags pointing at the operation module:

.. parsed-literal::

   poetry run scalene `--cpu` `--gpu` `--memory` \\
       protea/core/operations/export_research_dataset.py

Or to profile a specific worker invocation using the `--cpu` and `--memory`
flags:

.. parsed-literal::

   poetry run scalene `--cpu` `--memory` \\
       scripts/run_one_job.py <job_uuid>

Output is an HTML report in the current directory. The PERF.1 slice will
publish pre-computed flamegraphs from the FARM-EXP.13 run under
``docs/perf/`` once that slice lands.

.. rubric:: pyinstrument (call-stack sampling)

pyinstrument is faster to set up for a quick call-stack snapshot:

.. parsed-literal::

   poetry run pyinstrument scripts/run_one_job.py <job_uuid>

It groups time by call stack rather than by line, which makes it easier
to identify which function family (alignment vs KNN vs DB IO) dominates.

.. rubric:: cProfile + snakeviz (function-level)

For function-level profiling without installing extra tools:

.. parsed-literal::

   poetry run python -m cProfile -o /tmp/protea.prof \\
       scripts/run_one_job.py <job_uuid>
   snakeviz /tmp/protea.prof

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
