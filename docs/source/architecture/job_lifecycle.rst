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

**Session 1. Claim.**
   Loads the job, checks it is in QUEUED status, transitions it to RUNNING, writes a
   ``job.started`` event, and commits. After this commit, any monitoring tool or frontend
   can see the job is running. The session is then closed.

**Session 2. Execute.**
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
   (RUNNING is visible) while the result is not, which is the correct observable state.

Parent-child job hierarchy
--------------------------

Coordinator operations (``compute_embeddings``, ``predict_go_terms``) split work across
many parallel workers using a **parent-child pattern**:

.. code-block:: text

   Job (parent, RUNNING)
   ├── publishes N batch messages to RabbitMQ (ephemeral, no DB row)
   └── returns OperationResult(deferred=True)
                │
                ▼ each batch worker
        processes one batch, publishes to write queue
                │
                ▼ write worker
        inserts results, increments parent.progress_current
        if progress_current == progress_total → marks parent SUCCEEDED

The parent job stays in ``RUNNING`` state until the **last write worker** atomically
increments the progress counter and detects completion. The ``Job`` model includes:

- ``parent_job_id``: FK to the coordinator job (``NULL`` for top-level jobs)
- ``progress_current``: batches completed so far
- ``progress_total``: total batches dispatched

.. admonition:: Snapshotted context in batch payloads
   :class: note

   Coordinators that dispatch to ephemeral batch queues
   (``protea.embeddings.batch``, ``protea.predictions.batch``) serialise
   the full :class:`ProteaPayload` into the AMQP body; there is no DB
   row the worker can read from. When a coordinator needs to propagate
   state resolved against a row that lives in PostgreSQL, it **snapshots
   the relevant columns into the batch payload at dispatch time**.

   Example: when ``predict_go_terms`` receives a ``reranker_model_id``
   it loads the ``RerankerModel`` row, validates that ``artifact_uri``
   and ``feature_schema_sha`` are both populated, and copies them into
   every ``PredictGOTermsBatchPayload`` as
   ``reranker_artifact_uri`` / ``reranker_feature_schema_sha``. The
   batch worker therefore never re-queries the row, which keeps the
   batch path free of read dependencies on tables that could be updated
   mid-run.

Deferred execution pattern
---------------------------

An operation can return ``OperationResult(deferred=True)`` to signal that the job
should **not** be transitioned to SUCCEEDED immediately:

.. code-block:: python

   return OperationResult(
       deferred=True,
       result={"batches": n_batches},
   )

``BaseWorker`` detects this flag and skips the final SUCCEEDED transition. Responsibility
for closing the job passes to the child workers through the progress tracking mechanism.

This is used by all coordinator operations to allow the parent job to remain RUNNING
while batch workers process their messages in parallel.

RetryLaterError: deferring busy operations
------------------------------------------

When a resource is unavailable (e.g., GPU already in use by another embedding job),
an operation can raise ``RetryLaterError``:

.. code-block:: python

   raise RetryLaterError("GPU busy", delay_seconds=60)

``BaseWorker`` catches this exception and:

1. Resets the job status back to ``QUEUED``
2. Writes a ``job.retry_later`` event with the reason and delay
3. Re-publishes the job UUID to its queue after ``delay_seconds``

This prevents multiple GPU-intensive jobs from running simultaneously without
manual intervention.

Event log
---------

Every state transition and significant progress event is recorded as a ``JobEvent`` row.
The ``emit`` callback available to every operation writes a ``JobEvent`` with:

- ``event``: a dot-separated name (e.g. ``insert_proteins.page_done``)
- ``message``: optional human-readable description
- ``fields``: arbitrary ``JSONB`` payload (counts, URLs, timing)
- ``level``: ``info`` | ``warning`` | ``error``
- ``ts``: server-side timestamp

The frontend polls ``GET /jobs/{id}/events`` to display this timeline in real time.

Progress tracking
-----------------

Operations can report progress by including ``_progress_current`` and ``_progress_total``
in any ``emit`` call fields dict, or by returning them in ``OperationResult``.
``BaseWorker`` writes these values back to ``Job.progress_current`` and ``Job.progress_total``
after each update, allowing the frontend to display a live progress bar.

For distributed pipelines, the write workers use an atomic SQL update to increment
the parent's ``progress_current`` and conditionally close the job:

.. code-block:: sql

   UPDATE job SET progress_current = progress_current + 1
   WHERE id = :parent_id;

   UPDATE job SET status = 'succeeded', finished_at = now()
   WHERE id = :parent_id AND progress_current >= progress_total;

Cancellation
------------

``POST /jobs/{id}/cancel`` transitions QUEUED or RUNNING jobs to CANCELLED. If the job is
already in a terminal state (SUCCEEDED, FAILED, CANCELLED) the endpoint is a no-op.
Any queued child jobs (status = QUEUED) are also cancelled atomically.

.. note::
   Cancellation of a RUNNING job is a soft cancel: it marks the DB row as CANCELLED but
   does not interrupt the worker process. The worker will still complete the operation and
   attempt to write SUCCEEDED/FAILED, but the CANCELLED status is already committed and
   takes precedence in the frontend view.

.. seealso::

   - :doc:`operations`: the operations that workers actually run.
   - :doc:`/reference/workers`: class-level docs for ``BaseWorker``,
     ``QueueConsumer``, ``OperationConsumer``, and ``StaleJobReaper``.
   - :doc:`/adr/002-two-session-worker-pattern`: why two sessions, not one.
   - :doc:`/adr/003-queue-consumer-vs-operation-consumer`: when each
     consumer type applies.
   - :doc:`/adr/004-dead-letter-queue-and-retry-strategy`: how
     ``RetryLaterError`` and the DLQ interact.
