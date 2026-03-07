from __future__ import annotations

import json
from uuid import UUID

import pika


def publish_job(amqp_url: str, queue_name: str, job_id: UUID) -> None:
    """
    Publish a job dispatch message to a RabbitMQ queue.

    Opens a connection, publishes a single persistent message of the form
    ``{"job_id": "<uuid>"}``, then closes the connection.

    Intended to be called immediately after a Job row is created so that
    a QueueConsumer can pick it up and call BaseWorker.handle_job().
    """
    connection = pika.BlockingConnection(pika.URLParameters(amqp_url))
    try:
        channel = connection.channel()
        channel.queue_declare(queue=queue_name, durable=True)
        channel.basic_publish(
            exchange="",
            routing_key=queue_name,
            body=json.dumps({"job_id": str(job_id)}).encode("utf-8"),
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
            ),
        )
    finally:
        if connection.is_open:
            connection.close()
