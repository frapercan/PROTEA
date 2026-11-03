"""Only a cancellation stops a coordinator's child; a FAILED coordinator does not discard its work.

The failure this pins: a deferred coordinator holds no lease while its children run, so the
reaper can requeue it and then mark it FAILED while the queue still holds its batches. A child
that stopped on FAILED acknowledged every one of those batches and wrote nothing, with nothing
in the dead-letter queue. Here a FAILED parent warns and the child runs; a CANCELLED one stops.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from typing import Any

import pytest

from protea.core.operations import store_residue_codes as src
from protea.core.operations._parent_guard import parent_cancelled
from protea.infrastructure.orm.models.job import Job, JobStatus


class Events:
    def __init__(self) -> None:
        self.seen: list[tuple[str, str]] = []

    def __call__(self, event: str, message: Any, fields: dict[str, Any], level: str) -> None:
        self.seen.append((event, level))


class OneJob:
    """A session that holds one parent job."""

    def __init__(self, status: JobStatus | None) -> None:
        self.parent_id = uuid.uuid4()
        self.parent = (
            None if status is None else SimpleNamespace(status=status, error_code="LeaseExpired")
        )
        self.executed: list[Any] = []

    def get(self, model: type, key: uuid.UUID) -> Any:
        assert model is Job and key == self.parent_id
        return self.parent

    def execute(self, statement: Any, rows: Any = None) -> None:
        self.executed.append(rows)


@pytest.mark.parametrize(
    ("status", "stops", "events"),
    [
        (JobStatus.CANCELLED, True, [("encode.skipped", "warning")]),
        (JobStatus.FAILED, False, [("encode.parent_failed", "warning")]),
        (JobStatus.RUNNING, False, []),
        (JobStatus.SUCCEEDED, False, []),
        (None, False, []),
    ],
)
def test_only_a_cancellation_stops_a_child(
    status: JobStatus | None, stops: bool, events: list[tuple[str, str]]
) -> None:
    session, seen = OneJob(status), Events()
    assert parent_cancelled(session, str(session.parent_id), seen, "encode") is stops  # type: ignore[arg-type]
    assert seen.seen == events


def test_a_child_without_a_parent_runs() -> None:
    seen = Events()
    assert parent_cancelled(OneJob(None), None, seen, "encode") is False  # type: ignore[arg-type]
    assert seen.seen == []


def test_the_server_still_indexes_a_shard_whose_coordinator_the_reaper_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = OneJob(JobStatus.FAILED)
    shard = SimpleNamespace()
    monkeypatch.setattr(src, "_artifact_store", lambda: SimpleNamespace(get=lambda key: b"blob"))
    monkeypatch.setattr(src, "unpack_shard", lambda blob, sha: shard)
    monkeypatch.setattr(src, "check_shard", lambda got, p: None)
    advanced: list[uuid.UUID] = []
    monkeypatch.setattr(
        src, "update_parent_progress", lambda s, parent, emit, **k: advanced.append(parent)
    )
    payload = {
        "parent_job_id": str(session.parent_id),
        "target_embedding_config_id": str(uuid.uuid4()),
        "shard_key": "residue-codes/t/1-2-0123456789abcdef.bin",
        "shard_sha256": "0" * 64,
        "sequences": [{"sequence_id": 1, "position": 0, "residue_count": 5, "entry_count": 40}],
    }
    seen = Events()
    result = src.StoreResidueCodesOperation().execute(session, payload, emit=seen)  # type: ignore[arg-type]
    assert result.result.get("skipped") is not True
    (rows,) = session.executed
    assert [r["sequence_id"] for r in rows] == [1]
    assert advanced == [session.parent_id]
    assert ("store_residue_codes.parent_failed", "warning") in seen.seen


def test_the_server_skips_a_shard_whose_coordinator_a_person_cancelled() -> None:
    session = OneJob(JobStatus.CANCELLED)
    payload = {
        "parent_job_id": str(session.parent_id),
        "target_embedding_config_id": str(uuid.uuid4()),
        "shard_key": "residue-codes/t/1-2-0123456789abcdef.bin",
        "shard_sha256": "0" * 64,
        "sequences": [{"sequence_id": 1, "position": 0, "residue_count": 5, "entry_count": 40}],
    }
    result = src.StoreResidueCodesOperation().execute(session, payload, emit=Events())  # type: ignore[arg-type]
    assert result.result == {"skipped": True}
    assert session.executed == []
