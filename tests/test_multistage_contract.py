"""Tests for the multistage pipeline contract (F-MULTISTAGE-COHERENCE.1).

Exercises the four public abstractions introduced in
``protea/core/contracts/multistage.py`` using stub operations so no real
DB, queue, or artifact-store infrastructure is required.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from protea.core.contracts.multistage import (
    Coordinator,
    MultiStagePayload,
    PipelineStage,
    StageArtifactStore,
    publish_next_stage,
)
from protea.core.contracts.operation import EmitFn, OperationResult
from protea.infrastructure.storage.local import LocalFsArtifactStore

_noop_emit = lambda *_: None  # noqa: E731


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def tmpstore(tmp_path: Path) -> LocalFsArtifactStore:
    return LocalFsArtifactStore(root=tmp_path / "store")


@pytest.fixture()
def stage_store(tmpstore: LocalFsArtifactStore) -> StageArtifactStore:
    coord_id = uuid4()
    return StageArtifactStore(store=tmpstore, pipeline="test_pipe", coordinator_job_id=coord_id)


# ---------------------------------------------------------------------------
# MultiStagePayload
# ---------------------------------------------------------------------------


class TestMultiStagePayload:
    def test_requires_coordinator_job_id(self):
        with pytest.raises(ValueError):
            MultiStagePayload.model_validate({})

    def test_valid_payload(self):
        p = MultiStagePayload.model_validate({"coordinator_job_id": str(uuid4())})
        assert isinstance(p.coordinator_job_id, str)

    def test_immutable(self):
        p = MultiStagePayload.model_validate({"coordinator_job_id": str(uuid4())})
        with pytest.raises((TypeError, AttributeError, ValidationError)):
            p.coordinator_job_id = "other"  # type: ignore[misc]

    def test_subclass_inherits_field(self):
        class MyPayload(MultiStagePayload, frozen=True):
            items: list[int]

        p = MyPayload.model_validate({"coordinator_job_id": str(uuid4()), "items": [1, 2, 3]})
        assert p.items == [1, 2, 3]
        assert isinstance(p.coordinator_job_id, str)


# ---------------------------------------------------------------------------
# StageArtifactStore
# ---------------------------------------------------------------------------


class TestStageArtifactStore:
    def test_write_and_read_bytes(self, stage_store: StageArtifactStore):
        data = b"hello world"
        uri = stage_store.write_intermediate("batch", "chunk_0.bin", data)
        assert uri.startswith("file://")
        read_back = stage_store.read_intermediate(uri)
        assert read_back == data

    def test_write_and_read_json_object(self, stage_store: StageArtifactStore):
        obj = {"foo": [1, 2, 3], "bar": "baz"}
        uri = stage_store.write_intermediate("write", "result.json", obj)
        read_back = json.loads(stage_store.read_intermediate(uri))
        assert read_back == obj

    def test_exists_intermediate_false_before_write(self, stage_store: StageArtifactStore):
        assert stage_store.exists_intermediate("batch", "missing.bin") is False

    def test_exists_intermediate_true_after_write(self, stage_store: StageArtifactStore):
        stage_store.write_intermediate("batch", "present.bin", b"data")
        assert stage_store.exists_intermediate("batch", "present.bin") is True

    def test_key_encodes_pipeline_and_coord_id(
        self, tmpstore: LocalFsArtifactStore, stage_store: StageArtifactStore
    ):
        stage_store.write_intermediate("stage1", "k.txt", b"x")
        uri = stage_store.uri_for("stage1", "k.txt")
        assert "temp/test_pipe/" in uri
        assert "stage1/k.txt" in uri

    def test_uri_for_does_not_upload(
        self, stage_store: StageArtifactStore, tmpstore: LocalFsArtifactStore
    ):
        uri = stage_store.uri_for("stage1", "ghost.bin")
        assert "ghost.bin" in uri
        assert stage_store.exists_intermediate("stage1", "ghost.bin") is False

    def test_two_coordinators_do_not_collide(self, tmpstore: LocalFsArtifactStore):
        coord_a = StageArtifactStore(tmpstore, "pipe", uuid4())
        coord_b = StageArtifactStore(tmpstore, "pipe", uuid4())
        coord_a.write_intermediate("s", "k", b"A")
        coord_b.write_intermediate("s", "k", b"B")
        uri_a = coord_a.uri_for("s", "k")
        uri_b = coord_b.uri_for("s", "k")
        assert uri_a != uri_b
        assert coord_a.read_intermediate(uri_a) == b"A"
        assert coord_b.read_intermediate(uri_b) == b"B"

    def test_uri_to_key_roundtrip_s3(self):
        mock_s3 = MagicMock()
        mock_s3.url.return_value = "s3://mybucket/temp/pipe/abc123/stage/key.bin"
        mock_s3.put.return_value = "s3://mybucket/temp/pipe/abc123/stage/key.bin"
        mock_s3.get.return_value = b"data"
        s = StageArtifactStore(mock_s3, "pipe", "abc123")
        uri = "s3://mybucket/temp/pipe/abc123/stage/key.bin"
        key = s._uri_to_key(uri)
        assert key == "temp/pipe/abc123/stage/key.bin"

    def test_uri_to_key_unsupported_scheme(self, stage_store: StageArtifactStore):
        with pytest.raises(ValueError, match="Unsupported URI"):
            stage_store._uri_to_key("http://example.com/foo")


# ---------------------------------------------------------------------------
# publish_next_stage
# ---------------------------------------------------------------------------


class TestPublishNextStage:
    def test_inline_small_payload(self):
        payload = {"coordinator_job_id": str(uuid4()), "items": [1, 2]}
        queue, body = publish_next_stage("my.queue", payload)
        assert queue == "my.queue"
        assert body == payload
        assert "_artifact_uri" not in body

    def test_overflow_to_artifact_store(
        self, stage_store: StageArtifactStore
    ):
        big_payload = {"coordinator_job_id": str(uuid4()), "data": "x" * 100_000}
        queue, body = publish_next_stage(
            "my.queue",
            big_payload,
            artifact_store=stage_store,
            size_threshold_kb=1.0,
        )
        assert queue == "my.queue"
        assert "_artifact_uri" in body
        raw = stage_store.read_intermediate(body["_artifact_uri"])
        assert json.loads(raw) == big_payload

    def test_large_payload_inlined_without_store(self):
        big_payload = {"coordinator_job_id": str(uuid4()), "data": "x" * 100_000}
        queue, body = publish_next_stage(
            "my.queue",
            big_payload,
            artifact_store=None,
            size_threshold_kb=1.0,
        )
        assert body == big_payload

    def test_accepts_pydantic_payload(self):
        p = MultiStagePayload.model_validate({"coordinator_job_id": str(uuid4())})
        queue, body = publish_next_stage("q", p)
        assert body["coordinator_job_id"] == p.coordinator_job_id


# ---------------------------------------------------------------------------
# PipelineStage
# ---------------------------------------------------------------------------


class _StubStage(PipelineStage):
    name = "stub_stage"
    description = "stub"
    stage_name = "stub"

    def __init__(self, result_dict: dict[str, Any] | None = None):
        self._result_dict = result_dict or {}

    def _execute_stage(
        self, session: Any, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        return OperationResult(result=self._result_dict)


class TestPipelineStage:
    def test_execute_emits_start_and_done(self):
        events: list[str] = []

        def capture(event, *_):
            events.append(event)

        stage = _StubStage({"ok": True})
        result = stage.execute(None, {}, emit=capture)
        assert "stub.start" in events
        assert "stub.done" in events
        assert result.result == {"ok": True}

    def test_artifact_store_returns_scoped_store(self, tmpstore: LocalFsArtifactStore):
        stage = _StubStage()
        coord_id = uuid4()
        store = stage._artifact_store(tmpstore, coord_id)
        assert isinstance(store, StageArtifactStore)
        assert store._pipeline == "stub"
        assert store._coord_id == str(coord_id)

    def test_summarize_payload_default_empty(self):
        assert _StubStage().summarize_payload({}) == ""


# ---------------------------------------------------------------------------
# Coordinator
# ---------------------------------------------------------------------------


class _StubCoordinator(Coordinator):
    name = "stub_coordinator"
    description = "stub"

    def __init__(self, parts: list[tuple[str, dict[str, Any]]]):
        self._parts = parts

    def partition(
        self,
        session: Any,
        payload: dict[str, Any],
        *,
        parent_job_id: UUID,
        emit: EmitFn,
    ) -> list[tuple[str, dict[str, Any]]]:
        return self._parts


class TestCoordinator:
    def _make_payload(self, n: int) -> list[tuple[str, dict[str, Any]]]:
        return [("q", {"coordinator_job_id": str(uuid4()), "idx": i}) for i in range(n)]

    def test_dispatch_returns_deferred(self):
        parts = self._make_payload(3)
        coord = _StubCoordinator(parts)
        result = coord.dispatch_partition_with_progress(
            session=None,
            payload={},
            parent_job_id=uuid4(),
            emit=_noop_emit,
        )
        assert result.deferred is True
        assert result.progress_current == 0
        assert result.progress_total == 3
        assert len(result.publish_operations) == 3

    def test_dispatch_result_contains_batch_count(self):
        coord = _StubCoordinator(self._make_payload(5))
        result = coord.dispatch_partition_with_progress(
            session=None,
            payload={},
            parent_job_id=uuid4(),
            emit=_noop_emit,
        )
        assert result.result["batches"] == 5

    def test_dispatch_merges_extra_result(self):
        coord = _StubCoordinator(self._make_payload(2))
        result = coord.dispatch_partition_with_progress(
            session=None,
            payload={},
            parent_job_id=uuid4(),
            emit=_noop_emit,
            extra_result={"sequences": 42},
        )
        assert result.result["sequences"] == 42
        assert result.result["batches"] == 2

    def test_dispatch_emits_dispatching_event(self):
        events: list[str] = []
        coord = _StubCoordinator(self._make_payload(4))
        coord.dispatch_partition_with_progress(
            session=None,
            payload={},
            parent_job_id=uuid4(),
            emit=lambda ev, *_: events.append(ev),
        )
        assert "stub_coordinator.dispatching" in events

    def test_execute_uses_job_id_from_payload(self):
        job_id = uuid4()
        called_with: list[UUID] = []

        class _CapturingCoord(Coordinator):
            name = "cap"
            description = "cap"

            def partition(self, session, payload, *, parent_job_id, emit):
                called_with.append(parent_job_id)
                return []

        coord = _CapturingCoord()
        coord.execute(None, {"_job_id": str(job_id)}, emit=_noop_emit)
        assert called_with[0] == job_id

    def test_summarize_payload_default_empty(self):
        assert _StubCoordinator([]).summarize_payload({}) == ""

    def test_zero_parts_returns_empty_operations(self):
        coord = _StubCoordinator([])
        result = coord.dispatch_partition_with_progress(
            session=None, payload={}, parent_job_id=uuid4(), emit=_noop_emit
        )
        assert result.progress_total == 0
        assert result.publish_operations == []
        assert result.deferred is True
