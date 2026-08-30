# scripts/worker.py
"""
Continuous queue worker. Consumes jobs from RabbitMQ and executes them.

Usage:
    poetry run python scripts/worker.py
    poetry run python scripts/worker.py --queue protea.jobs
"""

from __future__ import annotations

import argparse
import logging
import os
import time
from pathlib import Path

from protea.config.tuning import WorkerTuning
from protea.core.operation_catalog import build_operation_registry
from protea.infrastructure.queue.consumer import (
    ConsumerOptions,
    OperationConsumer,
    QueueConsumer,
)
from protea.infrastructure.session import build_session_factory
from protea.infrastructure.settings import load_settings
from protea.workers.base_worker import BaseWorker, WorkerConfig
from protea.workers.stale_job_reaper import StaleJobReaper, StaleJobReaperConfig


def _build_reaper_config(worker_settings: WorkerTuning) -> StaleJobReaperConfig:
    """Build a StaleJobReaperConfig from WorkerTuning values.

    Extracted as a standalone helper so that tests can exercise the exact
    mapping without requiring a running AMQP connection or database.
    """
    return StaleJobReaperConfig(
        timeout_seconds=worker_settings.reaper_main_timeout_seconds,
        stall_seconds=worker_settings.reaper_stall_seconds,
        max_lease_requeues=worker_settings.max_lease_requeues,
        event_grace_seconds=worker_settings.reaper_event_grace_seconds,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="PROTEA queue worker")
    parser.add_argument("--queue", default="protea.jobs", help="Queue name to consume")
    parser.add_argument("--requeue-on-failure", action="store_true")
    parser.add_argument(
        "--log-format",
        choices=["json", "text"],
        default="json",
        help="Log output format (default: json)",
    )
    args = parser.parse_args()

    from protea.infrastructure.logging import configure_logging
    from protea.infrastructure.telemetry import configure_telemetry

    configure_logging(json=(args.log_format == "json"))
    # Suppress pika's verbose connection lifecycle messages
    logging.getLogger("pika").setLevel(logging.WARNING)

    # T5.1b: boot the OTel SDK before building the session factory so
    # the SQLAlchemy instrumentor in ``build_engine`` sees an active
    # provider. ``default_service_name`` derives from the queue so
    # workers show up as distinct resources (``protea-worker-<queue>``)
    # in the OTel UI.
    configure_telemetry(
        default_service_name=f"protea-worker-{args.queue}",
    )

    project_root = Path(__file__).resolve().parents[1]
    settings = load_settings(project_root)

    factory = build_session_factory(settings.db_url)

    registry = build_operation_registry()

    # Queues that carry ephemeral operation messages (no DB Job row per message)
    # use OperationConsumer.  All other queues use the standard QueueConsumer.
    _OPERATION_QUEUES = {
        "protea.embeddings.batch",
        "protea.embeddings.write",
        "protea.predictions.batch",
        "protea.predictions.write",
        # Export minijob pipeline sub-queues (PROTEA_EXPORT_MINIJOBS=1).
        # No DB Job row per message; parent Job progress tracked by coordinator.
        "protea.training.knn-batch",
        "protea.training.features",
        "protea.training.write",
    }

    # Special mode: stale job reaper (no queue, just periodic DB check).
    if args.queue == "reaper":
        # 24h hard timeout + 30min stall window by default. Earlier value
        # (6h) killed predict_go_terms coords that waited in the batch FIFO
        # behind other coords; with only one predictions.batch worker the
        # last ones in a 23-job batch routinely sat past 6h even though
        # work was progressing upstream.
        # Both numbers configurable via WorkerTuning (PROTEA_TUNING__WORKER__
        # REAPER_MAIN_TIMEOUT_SECONDS and ..._STALL_SECONDS).
        from protea.config.tuning import get_tuning

        worker_settings = get_tuning().worker
        # F-OPS-JOBS.1: pass the AMQP URL so lease-expired jobs can be
        # re-enqueued onto their source queue instead of marked FAILED.
        reaper = StaleJobReaper(
            factory,
            amqp_url=settings.amqp_url,
            config=_build_reaper_config(worker_settings),
        )
        logging.info(
            "Stale job reaper started. timeout=%ds stall=%ds interval=60s",
            worker_settings.reaper_main_timeout_seconds,
            worker_settings.reaper_stall_seconds,
        )
        reaper.run(interval_seconds=60)
        return

    options = ConsumerOptions(requeue_on_failure=args.requeue_on_failure)
    if args.queue in _OPERATION_QUEUES:
        consumer: QueueConsumer | OperationConsumer = OperationConsumer(
            amqp_url=settings.amqp_url,
            queue_name=args.queue,
            registry=registry,
            session_factory=factory,
            options=options,
        )
    else:
        worker = BaseWorker(
            factory, registry, WorkerConfig(worker_name="queue-worker"), amqp_url=settings.amqp_url
        )
        consumer = QueueConsumer(
            amqp_url=settings.amqp_url,
            queue_name=args.queue,
            worker=worker,
            options=options,
        )

    # Pre-warm taxonomy DB for prediction workers that may need it.
    #
    # PROTEA_SKIP_TAXONOMY_WARMUP exists for the deploy smoke. The warm-up
    # downloads about 100 MB and parses 2.9 million nodes before the worker
    # consumes anything, which is right in production and fatal in CI: the
    # smoke posts a ping to protea.jobs and waits 60 seconds for it to
    # succeed, and this is the queue whose worker is still parsing taxonomy.
    # Ping needs no taxonomy, so the smoke skips the warm-up rather than
    # waiting for it. Raising the timeout instead would only make the failure
    # intermittent, since the download time is not ours to bound.
    skip_warmup = os.environ.get("PROTEA_SKIP_TAXONOMY_WARMUP", "").strip().lower() in (
        "1",
        "true",
        "yes",
    )
    if skip_warmup:
        logging.info("Taxonomy DB warmup skipped: PROTEA_SKIP_TAXONOMY_WARMUP is set")
    elif args.queue in ("protea.predictions.batch", "protea.jobs", "protea.training"):
        try:
            from protea.core.feature_engineering import warmup_taxonomy_db

            warmup_taxonomy_db()
        except Exception as exc:
            logging.warning("Taxonomy DB warmup skipped: %s", exc)

    # The revision goes on the startup line beside the queue, so grep over the
    # logs answers "what was this process actually running" with no database
    # round trip. Its absence is why a tree that moved under two live arms on
    # 2026-08-30 went unnoticed until the labels were read back out of the
    # database hours later.
    from protea.core.code_revision import code_revision, tree_revision_now

    stamped, tree = code_revision(), tree_revision_now()
    logging.info(
        "Worker started. queue=%s revision=%s%s",
        args.queue,
        stamped,
        "" if stamped == tree else f" TREE_HAS_MOVED_TO={tree}",
    )
    while True:
        try:
            consumer.run()
        except KeyboardInterrupt:
            logging.info("Worker stopped.")
            break
        except Exception as exc:
            logging.error("Consumer crashed: %s — reconnecting in 5s", exc)
            time.sleep(5)
            continue
        # run() returns without raising both when the connection drops and
        # when a signal stopped it deliberately. Reconnecting is right in the
        # first case and wrong in the second: SIGTERM would stop the consumer,
        # fall through to the loop, and start a fresh one with the old code
        # still loaded, so a deploy could not restart the worker at all.
        if consumer.stopped:
            logging.info("Worker stopped.")
            break


if __name__ == "__main__":
    main()
