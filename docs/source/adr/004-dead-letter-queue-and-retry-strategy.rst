ADR-004: Dead letter queue and retries
======================================

:Date: 2026-03-18
:Author: frapercan

The problem
-----------

Two related messaging problems:

1. **Lost messages**: when a message failed permanently (invalid JSON,
   unknown operation), it was discarded with ``basic_nack``.  The payload
   disappeared and there was no way to do post-mortem.

2. **Aggressive retries**: transient failures (broker down, GPU busy)
   were retried immediately, amplifying load on the service that was
   already struggling.

What we do
----------

**Dead letter queue** — all queues are declared with
``x-dead-letter-exchange: protea.dlx``.  Rejected messages
(``nack`` without ``requeue``) end up in ``protea.dead-letter``, a durable
queue where they can be inspected, fixed, and republished.

**Publisher retries** — exponential backoff: 5 attempts with delays of
1, 2, 4, 8, 16s (capped at 30s).  If the connection is broken, it is
discarded and a new one is created.

**Worker retries** — operations can raise
``RetryLaterError("GPU busy", delay_seconds=60)``.  The worker calculates
adaptive backoff based on how many previous retries have occurred:
``delay = min(base * 2^retries, 600s)``.  The job goes back to ``QUEUED``
and is republished after the wait.

Trade-offs
----------

- The DLQ grows if nobody inspects it — it must be monitored (see runbook).
- Adaptive backoff makes one DB query per retry to count previous
  ``job.retry_later`` events.  Negligible cost.

Rejected
--------

- **TTL + delay queue in RabbitMQ**: more complex to set up and debug than
  an application-level ``sleep()``.
- **Celery retries**: PROTEA does not use Celery; reimplementing its
  countdown over raw pika adds no value.
