"""DLQ (Dead-Letter Queue) management helpers.

Provides peek-summary, replay, and purge logic for the ``protea.dead-letter``
queue without requiring the RabbitMQ management plugin. All three operations
use pika's ``basic_get`` over a short-lived blocking connection.

Summary:
    Peeks up to ``max_peek`` messages, groups by operation + source queue,
    then re-queues all of them (queue depth unchanged).

Replay:
    Drains matching messages, re-publishes to the original queue (or a
    caller-supplied override), and acks them from the DLQ. Non-matching
    messages are nacked with requeue=True.

Purge:
    Drains matching messages and acks them without republishing. Non-matching
    messages are nacked with requeue=True.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Any, NamedTuple, TypedDict

import pika
from pika.adapters.blocking_connection import BlockingChannel

from protea.config.tuning import get_tuning

logger = logging.getLogger(__name__)

_DLQ_NAME = "protea.dead-letter"


class _DlqGroup(TypedDict):
    operation: str
    first_death_queue: str
    age_bucket: str
    count: int


class _DecodedDlqMsg(NamedTuple):
    operation: str
    first_death_queue: str
    age_bucket: str
    delivery_tag: int
    body: bytes
    headers: dict[str, Any]


def _connect(amqp_url: str) -> tuple[pika.BlockingConnection, BlockingChannel]:
    params = pika.URLParameters(amqp_url)
    params.heartbeat = get_tuning().queue.amqp_heartbeat
    conn = pika.BlockingConnection(params)
    ch = conn.channel()
    ch.queue_declare(queue=_DLQ_NAME, durable=True, passive=True)
    return conn, ch


def _decode_body(body: bytes) -> dict[str, Any]:
    try:
        return json.loads(body.decode("utf-8"))
    except Exception:
        return {}


def _extract_operation(body_dict: dict[str, Any], headers: dict[str, Any]) -> str:
    op = body_dict.get("operation")
    if op:
        return str(op)
    return headers.get("x-protea-operation", "job_dispatch")


def _extract_first_death_queue(headers: dict[str, Any]) -> str:
    x_death = headers.get("x-death")
    if isinstance(x_death, list) and x_death:
        entry = x_death[0]
        if isinstance(entry, dict):
            q = entry.get("queue", "")
            if q:
                return str(q)
    return "unknown"


def _age_bucket(headers: dict[str, Any]) -> str:
    x_death = headers.get("x-death")
    if not (isinstance(x_death, list) and x_death):
        return "unknown"
    entry = x_death[0]
    if not isinstance(entry, dict):
        return "unknown"
    ts_raw = entry.get("time")
    if ts_raw is None:
        return "unknown"
    try:
        if hasattr(ts_raw, "timestamp"):
            ts = ts_raw
        else:
            ts = datetime.fromtimestamp(int(ts_raw), tz=UTC)
        age_days = (datetime.now(tz=UTC) - ts).days
        if age_days < 1:
            return "<1d"
        if age_days < 7:
            return f"{age_days}d"
        if age_days < 30:
            return f"{age_days // 7}w"
        return f"{age_days // 30}mo"
    except Exception:
        return "unknown"


def _peek_messages(
    ch: BlockingChannel,
    total: int,
    limit: int,
) -> list[_DecodedDlqMsg]:
    """basic_get up to ``limit`` messages from the DLQ without acking them."""
    msgs: list[_DecodedDlqMsg] = []
    for _ in range(min(limit, total)):
        method, properties, body = ch.basic_get(queue=_DLQ_NAME, auto_ack=False)
        if method is None:
            break
        headers = dict(properties.headers or {})
        body_dict = _decode_body(body)
        msgs.append(
            _DecodedDlqMsg(
                operation=_extract_operation(body_dict, headers),
                first_death_queue=_extract_first_death_queue(headers),
                age_bucket=_age_bucket(headers),
                delivery_tag=method.delivery_tag,
                body=body,
                headers=headers,
            )
        )
    return msgs


def _matches_filter(
    op: str,
    fdq: str,
    operation_filter: str | None,
    queue_filter: str | None,
) -> bool:
    if operation_filter is not None and op != operation_filter:
        return False
    if queue_filter is not None and fdq != queue_filter:
        return False
    return True


def _publish_to_origin(
    ch: BlockingChannel,
    msg: _DecodedDlqMsg,
    target_queue: str | None,
) -> None:
    dest = target_queue or (
        msg.first_death_queue if msg.first_death_queue != "unknown" else _DLQ_NAME
    )
    ch.basic_publish(
        exchange="",
        routing_key=dest,
        body=msg.body,
        properties=pika.BasicProperties(
            delivery_mode=pika.DeliveryMode.Persistent,
            headers={k: v for k, v in msg.headers.items() if not k.startswith("x-death")},
        ),
    )
    ch.basic_ack(delivery_tag=msg.delivery_tag)


def dlq_summary(amqp_url: str, max_peek: int = 500) -> dict[str, Any]:
    """Group up to ``max_peek`` DLQ messages by operation/queue/age.

    All peeked messages are re-queued; the DLQ depth is unchanged.
    """
    conn, ch = _connect(amqp_url)
    try:
        q_ok = ch.queue_declare(queue=_DLQ_NAME, durable=True, passive=True)
        total_in_queue = q_ok.method.message_count
        msgs = _peek_messages(ch, total_in_queue, max_peek)

        group_counts: dict[tuple[str, str, str], int] = {}
        for m in msgs:
            key = (m.operation, m.first_death_queue, m.age_bucket)
            group_counts[key] = group_counts.get(key, 0) + 1

        for m in msgs:
            ch.basic_nack(delivery_tag=m.delivery_tag, requeue=True)

        raw_groups: list[_DlqGroup] = [
            _DlqGroup(operation=k[0], first_death_queue=k[1], age_bucket=k[2], count=v)
            for k, v in group_counts.items()
        ]
        sorted_groups = sorted(raw_groups, key=lambda x: x["count"], reverse=True)
        return {
            "total_peeked": len(msgs),
            "queue_message_count": total_in_queue,
            "groups": sorted_groups,
        }
    finally:
        try:
            conn.close()
        except Exception:
            pass


def dlq_replay(
    amqp_url: str,
    operation_filter: str | None = None,
    queue_filter: str | None = None,
    target_queue: str | None = None,
    dry_run: bool = False,
    max_messages: int = 1000,
) -> dict[str, Any]:
    """Re-enqueue matching DLQ messages.

    ``dry_run=True`` reports ``would_replay`` without moving messages.
    """
    conn, ch = _connect(amqp_url)
    try:
        q_ok = ch.queue_declare(queue=_DLQ_NAME, durable=True, passive=True)
        msgs = _peek_messages(ch, q_ok.method.message_count, max_messages)

        replayed = 0
        skipped = 0
        would_replay = 0
        for m in msgs:
            if _matches_filter(m.operation, m.first_death_queue, operation_filter, queue_filter):
                would_replay += 1
                if dry_run:
                    ch.basic_nack(delivery_tag=m.delivery_tag, requeue=True)
                else:
                    _publish_to_origin(ch, m, target_queue)
                    replayed += 1
            else:
                skipped += 1
                ch.basic_nack(delivery_tag=m.delivery_tag, requeue=True)

        return {
            "replayed": 0 if dry_run else replayed,
            "would_replay": would_replay,
            "skipped": skipped,
            "dry_run": dry_run,
        }
    finally:
        try:
            conn.close()
        except Exception:
            pass


def dlq_purge(
    amqp_url: str,
    operation_filter: str | None = None,
    queue_filter: str | None = None,
    dry_run: bool = False,
    max_messages: int = 10000,
) -> dict[str, Any]:
    """Discard matching DLQ messages.

    ``dry_run=True`` reports ``would_purge`` without removing messages.
    """
    conn, ch = _connect(amqp_url)
    try:
        q_ok = ch.queue_declare(queue=_DLQ_NAME, durable=True, passive=True)
        msgs = _peek_messages(ch, q_ok.method.message_count, max_messages)

        purged = 0
        skipped = 0
        would_purge = 0
        for m in msgs:
            if _matches_filter(m.operation, m.first_death_queue, operation_filter, queue_filter):
                would_purge += 1
                if dry_run:
                    ch.basic_nack(delivery_tag=m.delivery_tag, requeue=True)
                else:
                    ch.basic_ack(delivery_tag=m.delivery_tag)
                    purged += 1
            else:
                skipped += 1
                ch.basic_nack(delivery_tag=m.delivery_tag, requeue=True)

        return {
            "purged": 0 if dry_run else purged,
            "would_purge": would_purge,
            "skipped": skipped,
            "dry_run": dry_run,
        }
    finally:
        try:
            conn.close()
        except Exception:
            pass
