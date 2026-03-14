Workers
=======

The worker layer bridges the message queue and the domain layer. Workers
are long-running Python processes — one per RabbitMQ queue — that consume
messages and delegate execution to registered operations. They are
transport-agnostic with respect to domain logic: operations are resolved
by name from the ``OperationRegistry`` and receive only a database session
and an ``emit`` callback.

Base worker
-----------

``BaseWorker`` is the core execution engine. It implements the two-session
pattern that decouples job claiming from job execution:

**Session 1 — Claim**
   Loads the job, asserts it is in ``QUEUED`` status, transitions it to
   ``RUNNING``, writes a ``job.started`` event, and commits. After this
   commit the job is visible as running to any monitoring tool or frontend.
   The session is then closed.

**Session 2 — Execute**
   Opens a fresh session, resolves the operation from the registry, and
   calls ``operation.execute(session, payload, emit=emit)``. On success,
   transitions the job to ``SUCCEEDED`` (or marks it as deferred if the
   operation returns ``OperationResult(deferred=True)``). On exception,
   transitions to ``FAILED``, stores the error class name and message,
   and re-raises.

The two-session design ensures durability: a crash in the execute phase
leaves the claim committed (``RUNNING`` is visible) while the result is
not — which is the correct observable state. No session is held open across
a long-running GPU inference call.

Three exceptional flows are handled explicitly:

- **RetryLaterError**: the job is reset to ``QUEUED`` and the consumer
  re-publishes it after ``delay_seconds``. Used by the embedding coordinator
  when the GPU is already occupied.
- **Parent cancellation**: if a child job's parent was cancelled between
  claim and execute, the child transitions to ``CANCELLED`` without running.
- **Corrupt execute session**: if the execute session fails to commit (e.g.
  the DB connection drops mid-operation), a fallback session marks the job
  ``FAILED`` so it is never permanently stuck in ``RUNNING``.

.. automodule:: protea.workers.base_worker
   :members:
   :undoc-members:
   :show-inheritance:

Worker entry points
-------------------

Workers are started by ``scripts/worker.py`` via ``scripts/manage.sh``.
Each process is bound to a single RabbitMQ queue and registers all
operations at startup, making every worker capable of executing any
operation routed to its queue.

.. code-block:: bash

   # Start the full stack (all workers + API + frontend)
   bash scripts/manage.sh start [N]

   # Start a single worker manually (for debugging)
   poetry run python scripts/worker.py protea.jobs

   # Run a single queued job by UUID (bypasses RabbitMQ entirely)
   poetry run python scripts/run_one_job.py <job-id>

The ``run_one_job.py`` script loads the job from the database, executes it
through ``BaseWorker``, and exits. No RabbitMQ connection is required. This
is the recommended way to debug a failing job without re-queuing it.

QueueConsumer vs OperationConsumer
-----------------------------------

Two consumer patterns exist in ``protea/infrastructure/queue/consumer.py``,
selected by the queue configuration in ``scripts/worker.py``:

**QueueConsumer**
   Reads a job UUID from the queue and delegates to
   ``BaseWorker.handle_job()``. Creates full ``Job`` rows with status
   transitions and a ``JobEvent`` audit log. Used for queues where
   observability and traceability matter:

   - ``protea.ping`` — smoke test
   - ``protea.jobs`` — all coordinator operations
   - ``protea.embeddings`` — serialised embedding coordinator

**OperationConsumer**
   Reads a raw serialised operation payload from the queue and executes it
   directly, without a ``Job`` row. Used for high-throughput batch queues
   where creating thousands of child rows per pipeline run would cause
   significant write contention and table bloat. Progress is tracked
   exclusively through atomic increments to the parent job's
   ``progress_current`` counter:

   - ``protea.embeddings.batch`` — GPU inference per batch
   - ``protea.embeddings.write`` — bulk pgvector insert
   - ``protea.predictions.batch`` — KNN search + GO transfer
   - ``protea.predictions.write`` — bulk GOPrediction insert
