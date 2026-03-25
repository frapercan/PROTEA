ADR-005: Reusable RabbitMQ connections
======================================

:Date: 2026-03-18
:Author: frapercan

The problem
-----------

When a coordinator (``compute_embeddings``) dispatches 500 batches, the
publisher opened and closed a TCP connection for each ``publish_operation()``
call.  This caused:

- 500 TCP+AMQP handshakes in a burst.
- ``EMFILE`` (too many open files) errors on the worker.
- Broker-side resource exhaustion (each connection costs RabbitMQ memory).

What we do
----------

Each thread keeps **a single connection** stored in ``threading.local()``.
``_get_connection()`` returns the existing connection if it is open, or
creates a new one.  If a publish fails, ``_close_cached_connection()``
discards the broken connection so the next attempt reconnects.

Result: from O(messages) connections down to O(threads) — in practice,
1-4 connections total.

Trade-offs
----------

- ``pika.BlockingConnection`` is not thread-safe, which is why
  ``threading.local()`` isolation is mandatory.
- Connections are never proactively closed — they live until the thread
  dies or a publish fails.  If RabbitMQ restarts, the first publish after
  restart always fails once (and reconnects automatically).

Rejected
--------

- **Connection pool** (``pika_pool``): external dependency for something
  ``threading.local()`` solves in 15 lines.
- **Global connection with a lock**: serialises all publishes, creating a
  bottleneck when dispatching hundreds of messages.
- **``aio-pika`` async**: workers are synchronous; adding an event loop
  just for the publisher is disproportionate.
