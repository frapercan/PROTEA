"""Smoke tests for F-EXPORT-MINIJOB.1 scaffolding.

Verifies that:
1. All four stub operations register without import errors.
2. Each handler is invoked when a message lands on its queue (unit-level).
3. The env gate helper honours the PROTEA_EXPORT_MINIJOBS variable.
4. The operations appear in the live OperationRegistry.
5. The three minijob queues are listed in the OperationConsumer set
   inside scripts/worker.py.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from protea.core.operation_catalog import build_operation_registry
from protea.core.operations.export_minijob import (
    EXPORT_MINIJOBS_ENV,
    ExportCoordinatorOperation,
    ExportCoordinatorPayload,
    ExportFeaturesBatchOperation,
    ExportFeaturesBatchPayload,
    ExportKnnBatchOperation,
    ExportKnnBatchPayload,
    ExportWriteOperation,
    ExportWritePayload,
    _minijobs_enabled,
)

_NOOP_EMIT = lambda *_: None  # noqa: E731


# ---------------------------------------------------------------------------
# Env gate
# ---------------------------------------------------------------------------


class TestMinijobsEnabledGate:
    def test_disabled_by_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv(EXPORT_MINIJOBS_ENV, raising=False)
        assert _minijobs_enabled() is False

    def test_disabled_when_zero(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(EXPORT_MINIJOBS_ENV, "0")
        assert _minijobs_enabled() is False

    def test_disabled_when_false_str(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(EXPORT_MINIJOBS_ENV, "false")
        assert _minijobs_enabled() is False

    def test_enabled_when_one(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(EXPORT_MINIJOBS_ENV, "1")
        assert _minijobs_enabled() is True

    def test_enabled_when_truthy_str(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(EXPORT_MINIJOBS_ENV, "true")
        assert _minijobs_enabled() is True


# ---------------------------------------------------------------------------
# Payload validation
# ---------------------------------------------------------------------------


class TestPayloadValidation:
    def test_coordinator_payload_defaults(self) -> None:
        p = ExportCoordinatorPayload.model_validate({})
        assert p.cell_spec == {}

    def test_coordinator_payload_with_cell_spec(self) -> None:
        p = ExportCoordinatorPayload.model_validate({"cell_spec": {"output_name": "test-ds"}})
        assert p.cell_spec["output_name"] == "test-ds"

    def test_knn_batch_payload_defaults(self) -> None:
        p = ExportKnnBatchPayload.model_validate({})
        assert p.coordinator_job_id == ""
        assert p.pair_id == ""

    def test_features_batch_payload_defaults(self) -> None:
        p = ExportFeaturesBatchPayload.model_validate({})
        assert p.knn_artifact_uri == ""

    def test_write_payload_defaults(self) -> None:
        p = ExportWritePayload.model_validate({})
        assert p.output_name == ""


# ---------------------------------------------------------------------------
# Operation execution (stub handlers return SUCCEEDED noop)
# ---------------------------------------------------------------------------


def _mock_session() -> Any:
    return MagicMock()


class TestExportCoordinatorOperation:
    def test_name(self) -> None:
        assert ExportCoordinatorOperation.name == "export_coordinator"

    def test_execute_returns_noop(self) -> None:
        op = ExportCoordinatorOperation()
        result = op.execute(_mock_session(), {}, emit=_NOOP_EMIT)
        assert result.result == {"status": "stub_noop"}
        assert result.deferred is False

    def test_execute_emits_event(self) -> None:
        emitted: list[tuple] = []
        op = ExportCoordinatorOperation()
        op.execute(_mock_session(), {}, emit=lambda *a: emitted.append(a))
        assert any(e[0] == "export_coordinator.noop" for e in emitted)

    def test_summarize_payload_with_output_name(self) -> None:
        op = ExportCoordinatorOperation()
        summary = op.summarize_payload({"cell_spec": {"output_name": "my-ds"}})
        assert "my-ds" in summary

    def test_summarize_payload_empty(self) -> None:
        op = ExportCoordinatorOperation()
        summary = op.summarize_payload({})
        assert summary == "stub"


class TestExportKnnBatchOperation:
    def test_name(self) -> None:
        assert ExportKnnBatchOperation.name == "export_knn_batch"

    def test_execute_returns_noop(self) -> None:
        op = ExportKnnBatchOperation()
        result = op.execute(_mock_session(), {}, emit=_NOOP_EMIT)
        assert result.result == {"status": "stub_noop"}

    def test_execute_emits_event(self) -> None:
        emitted: list[tuple] = []
        op = ExportKnnBatchOperation()
        op.execute(_mock_session(), {}, emit=lambda *a: emitted.append(a))
        assert any(e[0] == "export_knn_batch.noop" for e in emitted)

    def test_summarize_payload_with_pair_id(self) -> None:
        op = ExportKnnBatchOperation()
        summary = op.summarize_payload({"pair_id": "2019-2020"})
        assert "2019-2020" in summary


class TestExportFeaturesBatchOperation:
    def test_name(self) -> None:
        assert ExportFeaturesBatchOperation.name == "export_features_batch"

    def test_execute_returns_noop(self) -> None:
        op = ExportFeaturesBatchOperation()
        result = op.execute(_mock_session(), {}, emit=_NOOP_EMIT)
        assert result.result == {"status": "stub_noop"}

    def test_execute_emits_event(self) -> None:
        emitted: list[tuple] = []
        op = ExportFeaturesBatchOperation()
        op.execute(_mock_session(), {}, emit=lambda *a: emitted.append(a))
        assert any(e[0] == "export_features_batch.noop" for e in emitted)


class TestExportWriteOperation:
    def test_name(self) -> None:
        assert ExportWriteOperation.name == "export_write"

    def test_execute_returns_noop(self) -> None:
        op = ExportWriteOperation()
        result = op.execute(_mock_session(), {}, emit=_NOOP_EMIT)
        assert result.result == {"status": "stub_noop"}

    def test_execute_emits_event(self) -> None:
        emitted: list[tuple] = []
        op = ExportWriteOperation()
        op.execute(_mock_session(), {}, emit=lambda *a: emitted.append(a))
        assert any(e[0] == "export_write.noop" for e in emitted)

    def test_summarize_payload_with_output_name(self) -> None:
        op = ExportWriteOperation()
        summary = op.summarize_payload({"output_name": "bench-v1-K5-v226"})
        assert "bench-v1-K5-v226" in summary


# ---------------------------------------------------------------------------
# OperationRegistry registration
# ---------------------------------------------------------------------------


class TestRegistration:
    def test_all_four_registered(self) -> None:
        registry = build_operation_registry()
        for name in (
            "export_coordinator",
            "export_knn_batch",
            "export_features_batch",
            "export_write",
        ):
            op = registry.get(name)
            assert op is not None, f"operation {name!r} not registered"

    def test_existing_operations_unaffected(self) -> None:
        registry = build_operation_registry()
        for name in ("export_research_dataset", "compute_embeddings", "ping"):
            assert registry.get(name) is not None


# ---------------------------------------------------------------------------
# OperationConsumer queue set in scripts/worker.py
# ---------------------------------------------------------------------------


class TestWorkerQueueSet:
    """The three minijob queues must be in _OPERATION_QUEUES inside worker.py."""

    def test_minijob_queues_in_operation_queues(self) -> None:
        import ast
        import pathlib

        worker_src = (
            pathlib.Path(__file__).resolve().parents[1] / "scripts" / "worker.py"
        ).read_text()
        tree = ast.parse(worker_src)
        # Find the _OPERATION_QUEUES set literal in the source
        queues: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id == "_OPERATION_QUEUES":
                        if isinstance(node.value, ast.Set):
                            for elt in node.value.elts:
                                if isinstance(elt, ast.Constant):
                                    queues.add(elt.value)
        assert "protea.training.knn-batch" in queues
        assert "protea.training.features" in queues
        assert "protea.training.write" in queues
