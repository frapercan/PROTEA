"""``job.started`` says where the job runs: the machine, the interpreter, the revision.

ADR-D55's receipt records, for every job, where it ran, and its checklist
refuses a heavy step on the server. Until this, the start event carried the
worker's name alone, so two machines serving one queue left their jobs
indistinguishable, and a worker replaced between two jobs left no trace.
"""

from __future__ import annotations

import platform
from unittest.mock import MagicMock
from uuid import uuid4

from protea.core.code_revision import code_revision
from protea.core.provenance import where_it_runs
from protea.infrastructure.orm.models.job import JobEvent
from protea.workers.base_worker import BaseWorker, WorkerConfig


def test_it_answers_without_shelling_out() -> None:
    where = where_it_runs()
    assert where["python"] == platform.python_version()
    assert where["code_revision"] == code_revision()
    assert where["host"]


def test_the_start_event_carries_it() -> None:
    session = MagicMock()
    claimed = MagicMock()
    claimed.rowcount = 1
    session.execute.return_value = claimed
    worker = BaseWorker(
        MagicMock(return_value=session), MagicMock(), WorkerConfig(worker_name="node")
    )

    assert worker._claim_job(uuid4()) is True

    started = [
        call.args[0]
        for call in session.add.call_args_list
        if isinstance(call.args[0], JobEvent) and call.args[0].event == "job.started"
    ]
    assert len(started) == 1
    assert started[0].fields == {"worker": "node", **where_it_runs()}
