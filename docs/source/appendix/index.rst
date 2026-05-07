Appendix
========

Operational material that complements the architecture and reference
sections: how to install PROTEA, how to configure it, recipes for common
tasks, the script to reproduce the thesis results, and the on-call runbook.

:doc:`stack`
   The eight repositories that make up the PROTEA stack: the platform,
   the contracts package, the inference layer, source / runner / backend
   plugins, the LightGBM lab, and the cafaeval fork. Read this first when
   you need to jump between repositories.

:doc:`installation_and_quickstart`
   Bring up the full stack from a fresh checkout: dependencies, ``manage.sh``,
   the eleven process roles, and a ten-minute end-to-end smoke test.

:doc:`configuration`
   ``protea/config/system.yaml`` reference, environment-variable overrides,
   and the per-environment knobs that change behaviour without code edits.

:doc:`howto_guides`
   Task-oriented recipes — load an ontology, ingest GOA, upload a FASTA query
   set, compute embeddings, predict GO terms, scale workers — with the exact
   ``curl`` and ``manage.sh`` commands. Read this when you have one specific
   thing to accomplish.

:doc:`reproduction_guide`
   The full, ordered procedure that regenerates every figure and table in the
   :doc:`/results` chapter from a clean database. Read this when you want to
   reproduce the thesis evaluation end-to-end.

:doc:`runbook`
   On-call procedures: diagnosing stuck jobs, draining queues, recovering
   after a worker crash, restoring from backup.

.. toctree::
   :maxdepth: 2
   :hidden:

   stack
   installation_and_quickstart
   configuration
   howto_guides
   reproduction_guide
   runbook
