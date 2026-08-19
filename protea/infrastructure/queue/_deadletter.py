"""The dead-letter exchange and queue every consumer declares.

Split out of ``consumer.py`` because that file sits exactly on its 800-line
budget, and the stop-flag fix needed room. This block is the most self
contained thing in it: two names and one idempotent declaration, read by both
consumers and by nothing else.
"""

from __future__ import annotations

from pika.adapters.blocking_connection import BlockingChannel

DLX_NAME = "protea.dlx"
DLQ_NAME = "protea.dead-letter"


def setup_dead_letter(channel: BlockingChannel) -> None:
    """Declare the dead-letter exchange and queue (idempotent)."""
    channel.exchange_declare(exchange=DLX_NAME, exchange_type="fanout", durable=True)
    channel.queue_declare(queue=DLQ_NAME, durable=True)
    channel.queue_bind(queue=DLQ_NAME, exchange=DLX_NAME)
