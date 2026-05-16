ADR-009: Pre-dispatch cancellation nack in QueueConsumer
=========================================================

:Status: Accepted
:Date: 2026-05-16
:PR: #373 (fix(queue): nack on cancellation and RetryLaterError)

Context
-------

``QueueConsumer`` pre-acks every delivery before handing it to the
worker, to protect long-running jobs from RabbitMQ's
``consumer_timeout`` (messages acked after the timeout trigger
a channel close and a reconnect storm). The pre-ack pattern means that
once a message is consumed, the only way to "return" it to the queue
is a republish (used by ``RetryLaterError``).

Before PR #373, a ``Job`` that was cancelled through the API/UI while
already queued in RabbitMQ would still be delivered to a worker.  The
worker's ``_claim_job`` guard detected ``CANCELLED`` status and exited
cleanly, but only after taking a prefetch slot for the full round-trip.
With ``prefetch_count=1`` on the prediction queue, a queue of orphaned
cancellation messages could stall all legitimate work.

Decision
--------

Check ``Job.status == CANCELLED`` inside ``QueueConsumer._on_message``,
*before* pre-acking and before entering the worker. If cancelled, call
``channel.basic_nack(requeue=False)`` to discard the message
immediately.

The check is a point-in-time DB query; it accepts two failure modes:

1. A race where the job is cancelled after the check passes: the worker
   still terminates at ``_claim_job``, as before.
2. A transient DB error: log a warning and proceed with dispatch (fail
   open) so a DB blip does not orphan legitimate jobs.

Consequences
------------

- Cancelled messages are drained from the queue without blocking a
  prefetch slot.
- Prefetch=1 queues (predictions) can no longer deadlock on a backlog
  of cancellations.
- The DB gains one extra ``SELECT`` per delivery on the job-backed
  queues (``protea.ping``, ``protea.jobs``, ``protea.embeddings``).
  These queues are low-throughput; the overhead is negligible.
- ``OperationConsumer`` is unaffected (no ``Job`` row to cancel).
