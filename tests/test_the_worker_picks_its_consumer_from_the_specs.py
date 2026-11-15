"""``scripts/worker.py`` picks OperationConsumer or QueueConsumer from the operation specs.

Until ADR-D60's operation descriptor, the worker carried the queues that get an
``OperationConsumer`` as a hard-coded set, so a new stage needed an edit in a
script that nothing tied to the stage. The set is now derived from the specs:
a queue whose operations are message stages or writes takes an
``OperationConsumer``. Deriving it must not move a single queue, so the old set
is pinned here exactly as ``scripts/worker.py`` wrote it.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from protea.core import queues
from protea.core.operation_catalog import build_operation_registry
from protea.core.operation_spec import message_queues
from protea.infrastructure.settings import Settings

#: ``_OPERATION_QUEUES`` as ``scripts/worker.py`` hard-coded it before the specs.
HARD_CODED_UNTIL_THE_SPECS: frozenset[str] = frozenset(
    {
        "protea.embeddings.batch",
        "protea.embeddings.write",
        "protea.embeddings.upload",
        "protea.predictions.batch",
        "protea.predictions.write",
        "protea.training.knn-batch",
        "protea.training.features",
        "protea.training.write",
    }
)


def test_the_derived_set_is_the_hard_coded_one() -> None:
    specs = build_operation_registry().specs()
    assert message_queues(specs.values()) == HARD_CODED_UNTIL_THE_SPECS


class _Recorded:
    """A consumer that records which class the worker built, then stops at once."""

    stopped = True
    built: list[tuple[str, str]] = []

    def __init__(self, *_args: object, **kwargs: object) -> None:
        self.built.append((type(self).__name__, str(kwargs.get("queue_name"))))

    def run(self) -> None:
        return None


class OperationConsumer(_Recorded):
    pass


class QueueConsumer(_Recorded):
    pass


@pytest.fixture
def start_worker(monkeypatch: pytest.MonkeyPatch):
    """Run ``scripts/worker.py`` with the real catalog and nothing behind it."""
    import scripts.worker as worker_script

    monkeypatch.setattr("protea.infrastructure.logging.configure_logging", lambda **_kw: None)
    monkeypatch.setattr("protea.infrastructure.telemetry.configure_telemetry", lambda **_kw: None)
    settings = Settings(
        db_url="postgresql+psycopg://protea:pw@127.0.0.1:1/protea",
        amqp_url="amqp://protea:pw@127.0.0.1:1/",
        artifacts_dir=Path("unused"),
    )
    monkeypatch.setattr(worker_script, "load_settings", lambda _root: settings)
    monkeypatch.setattr(worker_script, "exit_on_environment_mismatch", lambda _settings: None)
    monkeypatch.setattr(worker_script, "build_session_factory", lambda _url: MagicMock())
    monkeypatch.setattr(worker_script, "BaseWorker", MagicMock())
    monkeypatch.setattr(worker_script, "QueueConsumer", QueueConsumer)
    monkeypatch.setattr(worker_script, "OperationConsumer", OperationConsumer)
    monkeypatch.setenv("PROTEA_SKIP_TAXONOMY_WARMUP", "1")
    _Recorded.built = []
    pika_logger = logging.getLogger("pika")
    level = pika_logger.level

    def _start(queue: str) -> tuple[str, str]:
        monkeypatch.setattr(sys, "argv", ["worker.py", "--queue", queue])
        worker_script.main()
        return _Recorded.built[-1]

    yield _start
    pika_logger.setLevel(level)


def test_every_work_queue_gets_the_consumer_it_got_before(start_worker) -> None:
    operation_queues = set()
    for queue in sorted(queues.WORK_QUEUES):
        consumer, consumed = start_worker(queue)
        assert consumed == queue
        if consumer == "OperationConsumer":
            operation_queues.add(queue)
    assert operation_queues == HARD_CODED_UNTIL_THE_SPECS


def test_a_queue_no_spec_names_still_gets_a_queue_consumer(start_worker) -> None:
    assert start_worker("protea.not-declared") == ("QueueConsumer", "protea.not-declared")
