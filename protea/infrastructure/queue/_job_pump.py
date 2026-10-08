"""Run a long job off the AMQP IO thread, and ack it only once it is done.

These two things travel together and cannot be separated, which is why they
live in one module.

**Why the job leaves the IO thread.** ``handle_job`` runs for an hour and never
yields. On the IO thread it starves pika's heartbeat sender, and the broker
closes the connection two heartbeat intervals in. Measured on 2026-10-08, in
the middle of the GOA campaign: 75 ``missed heartbeats from client, timeout:
600s`` in the broker's log and 58 ``StreamLostError`` + reconnect cycles in the
worker's, one per long job, for days. The comment that used to stand in
``_dispatch_job`` blamed RabbitMQ's ``consumer_timeout`` instead. That was the
wrong timer, and raising it changed nothing.

**Why the ack waits.** Acking on START frees the prefetch window while the
worker is still busy, so the broker hands that worker the next message and the
message sits undelivered for an hour. With a second node on the same queue the
second node is starved: it is not that it loses a race, it is that the work was
already assigned to somebody who cannot begin it.

**Why neither works alone.** The pump keeps the connection alive so the late
ack can succeed at all; holding the ack keeps the prefetch window occupied so
the pump cannot dispatch a SECOND job re-entrantly into the same callback.

**Safe to thread.** The worker builds every Session from a plain
``sessionmaker`` inside the call, and the only AMQP it does is ``publish_job`` /
``publish_operation``, which open their own connection from a URL rather than
touching this channel. The two threads never share a pika object.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from uuid import UUID

from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic

logger = logging.getLogger(__name__)

#: How long each pump call blocks before checking on the job thread.
_PUMP_SECONDS = 5.0


def run_while_pumping(channel: BlockingChannel, run: Callable[[], None], job_id: UUID) -> None:
    """Run ``run`` on a worker thread while this one answers heartbeats.

    The exception is re-raised HERE, on the caller's thread, so the branches
    around the call see it exactly as they would from a direct call.
    """
    box: dict[str, BaseException] = {}

    def _run() -> None:
        try:
            run()
        except BaseException as exc:  # noqa: BLE001 - handed to the caller
            box["exc"] = exc

    thread = threading.Thread(target=_run, name=f"job-{job_id}", daemon=True)
    thread.start()
    while thread.is_alive():
        try:
            channel.connection.process_data_events(time_limit=_PUMP_SECONDS)
        except Exception as exc:  # noqa: BLE001
            # The link died anyway. Let the job finish and let the ack fail
            # loudly rather than abandoning work already underway.
            logger.warning("AMQP pump stopped; job continues. job_id=%s error=%s", job_id, exc)
            thread.join()
            break
    thread.join()
    if "exc" in box:
        raise box["exc"]


def ack_when_done(channel: BlockingChannel, method: Basic.Deliver, job_id: UUID) -> None:
    """Ack the delivery, last and on every path.

    Until this runs the broker counts the worker as busy, which is the point.
    A failure here is logged and swallowed: the delivery stays unacked, the
    broker redelivers it, and that cannot double-execute because
    ``BaseWorker._claim_job`` is a conditional UPDATE on ``status='queued'``
    and the second arrival loses it.
    """
    try:
        channel.basic_ack(delivery_tag=method.delivery_tag)
        logger.info("Job acked. job_id=%s", job_id)
    except Exception as exc:  # noqa: BLE001 - never lose the worker
        logger.error("Ack failed. job_id=%s error=%s", job_id, exc)
