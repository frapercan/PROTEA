"""Bounded retries for an operation message that failed for a passing reason.

Phase 3 of ADR-D60, the executor rules. Before it, an operation that raised
``RetryLaterError`` was nacked with ``requeue=True`` and redelivered at once,
with no count and no wait: a card that stays busy turns that into a loop that
burns a worker and floods the log, and nothing ever ends it. A lost connection
to the database, the broker or the object store was worse the other way round:
it counted as a failure and dead-lettered a message that would have run a
minute later, which is exactly what the desktop's wireless link produces.

Both are now the same thing, a transient failure. The message is republished
after a wait, with its attempt counted in the ``x-protea-retry`` header, and
dead-lettered with a ``child.retry_exhausted`` event once the count reaches
``queue.transient_max_retries``. The wait is the operation's own
``delay_seconds`` for ``RetryLaterError`` and an exponential backoff for a lost
connection, both capped at ``queue.transient_max_delay``. It is spent with the
connection's heartbeat-safe sleep, as the CUDA out-of-memory path already does;
that path keeps its own header and cap (``x-oom-retry``), which were bounded
already.

A graceful stop never spends the count: a stopping consumer requeues what it
holds without touching the header.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any, NamedTuple

import pika

from protea.core.contracts.operation import RetryLaterError

logger = logging.getLogger(__name__)

#: The header that counts a message's transient retries.
RETRY_HEADER = "x-protea-retry"
#: How far down an exception's ``__cause__`` / ``__context__`` chain to look.
_CHAIN_DEPTH = 6


class Delivery(NamedTuple):
    """One delivery as the retry needs it: where it came from and its bytes."""

    channel: Any
    method: Any
    body: bytes
    queue_name: str


def _connection_types() -> tuple[type[BaseException], ...]:
    """The exception types that mean "a connection was lost", imported lazily."""
    found: list[type[BaseException]] = [ConnectionError, TimeoutError]
    try:
        import sqlalchemy.exc as sa_exc

        # OperationalError is what the driver raises for a lost or refused
        # connection, an administrator shutdown, a deadlock or a full disk: all
        # of them pass, or end the count. InterfaceError and DisconnectionError
        # are the pool's view of a dead connection.
        found += [sa_exc.OperationalError, sa_exc.InterfaceError, sa_exc.DisconnectionError]
    except ImportError:  # pragma: no cover - the platform always has it
        pass
    import pika.exceptions as pika_exc

    found += [pika_exc.AMQPConnectionError, pika_exc.AMQPChannelError, pika_exc.StreamLostError]
    try:
        import urllib3.exceptions as http_exc

        found += [
            http_exc.NewConnectionError,
            http_exc.MaxRetryError,
            http_exc.ProtocolError,
            http_exc.TimeoutError,
        ]
    except ImportError:  # pragma: no cover - MinIO brings it
        pass
    return tuple(found)


def is_connection_error(exc: BaseException) -> bool:
    """Whether ``exc``, or what it wraps, is a lost connection rather than a defect."""
    kinds = _connection_types()
    seen: BaseException | None = exc
    for _ in range(_CHAIN_DEPTH):
        if seen is None:
            return False
        if isinstance(seen, kinds):
            return True
        seen = seen.__cause__ or seen.__context__
    return False


def retry_count(headers: dict[str, Any] | None) -> int:
    """How many transient retries a message has spent, from its headers."""
    try:
        return max(0, int((headers or {}).get(RETRY_HEADER, 0)))
    except (TypeError, ValueError):
        return 0


def transient_delay(exc: BaseException, attempt: int, tuning: Any) -> int:
    """The wait before the next attempt, in seconds, capped by the tuning."""
    if isinstance(exc, RetryLaterError):
        wanted = int(exc.delay_seconds)
    else:
        wanted = int(tuning.transient_base_delay) * (2**attempt)
    return max(0, min(wanted, int(tuning.transient_max_delay)))


def _republish_after(delivery: Delivery, headers: dict[str, Any], delay: int) -> None:
    """Wait with heartbeats, republish persistently with ``headers``, ack the original."""
    delivery.channel.connection.sleep(delay)
    delivery.channel.basic_publish(
        exchange="",
        routing_key=delivery.queue_name,
        body=delivery.body,
        properties=pika.BasicProperties(
            delivery_mode=pika.DeliveryMode.Persistent, headers=headers
        ),
    )
    delivery.channel.basic_ack(delivery_tag=delivery.method.delivery_tag)


def retry_or_dead_letter(
    delivery: Delivery,
    decoded: Any,
    exc: BaseException,
    kind: str,
    emit_parent: Callable[..., None],
) -> None:
    """Republish after a wait with the count raised, or dead-letter at the cap.

    ``kind`` names the reason in the events (``retry_later`` or ``connection``).
    A republish that fails dead-letters too, rather than leaving the message
    unacknowledged.
    """
    from protea.config.tuning import get_tuning

    tuning = get_tuning().queue
    attempt = retry_count(decoded.headers)
    fields = {
        "operation": decoded.operation_name,
        "kind": kind,
        "attempt": attempt + 1,
        "max_retries": tuning.transient_max_retries,
        "error": str(exc)[:500],
    }
    if attempt < tuning.transient_max_retries:
        delay = transient_delay(exc, attempt, tuning)
        emit_parent(
            decoded.parent_job_id,
            "child.retry",
            f"{kind} on {decoded.operation_name}; retry {attempt + 1}/"
            f"{tuning.transient_max_retries} after {delay}s",
            {**fields, "delay_seconds": delay},
            level="warning",
        )
        try:
            _republish_after(delivery, {**decoded.headers, RETRY_HEADER: attempt + 1}, delay)
            return
        except Exception as republish_exc:
            logger.error(
                "Republishing a retry failed; dead-lettering. operation=%s error=%s",
                decoded.operation_name,
                republish_exc,
            )
    emit_parent(
        decoded.parent_job_id,
        "child.retry_exhausted",
        f"{kind} on {decoded.operation_name} after {attempt} retries; message dead-lettered",
        fields,
        level="error",
    )
    delivery.channel.basic_nack(delivery_tag=delivery.method.delivery_tag, requeue=False)
