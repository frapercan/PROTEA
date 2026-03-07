Job Lifecycle
=============

States
------

Every job follows a linear state machine:

.. code-block:: text

   QUEUED  ──▶  RUNNING  ──▶  SUCCEEDED
                   │
                   └──▶  FAILED
   QUEUED  ──▶  CANCELLED   (via API, before execution)

Allowed transitions:

.. list-table::
   :header-rows: 1

   * - From
     - To
     - Trigger
   * - ``QUEUED``
     - ``RUNNING``
     - Worker claims the job (session 1)
   * - ``RUNNING``
     - ``SUCCEEDED``
     - Operation completes without exception
   * - ``RUNNING``
     - ``FAILED``
     - Operation raises an exception
   * - ``QUEUED``
     - ``CANCELLED``
     - ``POST /jobs/{id}/cancel``

The two-session pattern
-----------------------

``BaseWorker.handle_job(job_id)`` uses **two independent database sessions** by design:

**Session 1 — Claim**
   Loads the job, checks it is in QUEUED status, transitions it to RUNNING, writes a
   ``job.started`` event, and commits. After this commit, any monitoring tool or frontend
   can see the job is running. The session is then closed.

**Session 2 — Execute**
   Loads the job again (fresh session), resolves the operation from the registry, and
   calls ``operation.execute(session, payload, emit=emit)``. On success, writes
   ``job.succeeded`` and commits. On exception, writes ``job.failed`` with the error class
   and message, commits, and re-raises.

This pattern ensures consistency even if the process crashes mid-execution: the DB always
reflects the last committed state, and no session is left open across a long-running operation.

.. admonition:: Why two sessions?
   :class: note

   A single long-lived session would hold a transaction open for the entire duration of the
   operation (potentially minutes). This blocks table-level vacuuming and causes lock
   contention. More importantly, a crash in the execute phase leaves the claim phase committed
   (RUNNING is visible) while the result is not — which is the correct observable state.

Event log
---------

Every state transition and significant progress event is recorded as a ``JobEvent`` row.
The ``emit`` callback available to every operation writes a ``JobEvent`` with:

- ``event`` — a dot-separated name (e.g. ``insert_proteins.page_done``)
- ``message`` — optional human-readable description
- ``fields`` — arbitrary ``JSONB`` payload (counts, URLs, timing)
- ``level`` — ``info`` | ``warning`` | ``error``
- ``ts`` — server-side timestamp

The frontend polls ``GET /jobs/{id}/events`` to display this timeline in real time.

Cancellation
------------

``POST /jobs/{id}/cancel`` transitions QUEUED or RUNNING jobs to CANCELLED. If the job is
already in a terminal state (SUCCEEDED, FAILED, CANCELLED) the endpoint is a no-op.

.. note::
   Cancellation of a RUNNING job is a soft cancel — it marks the DB row as CANCELLED but
   does not interrupt the worker process. The worker will still complete the operation and
   attempt to write SUCCEEDED/FAILED, but the CANCELLED status is already committed and
   takes precedence in the frontend view.
