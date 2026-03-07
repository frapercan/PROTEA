"""
Unit tests for core contracts and simple operations.
No DB or network required.
"""
from __future__ import annotations

import pytest

from protea.core.contracts.operation import OperationResult
from protea.core.contracts.registry import OperationRegistry
from protea.core.operations.ping import PingOperation

# ---------------------------------------------------------------------------
# OperationRegistry
# ---------------------------------------------------------------------------

class TestOperationRegistry:
    def test_register_and_get(self):
        reg = OperationRegistry()
        op = PingOperation()
        reg.register(op)
        assert reg.get("ping") is op

    def test_duplicate_register_raises(self):
        reg = OperationRegistry()
        reg.register(PingOperation())
        with pytest.raises(ValueError, match="already registered"):
            reg.register(PingOperation())

    def test_get_unknown_raises(self):
        reg = OperationRegistry()
        with pytest.raises(KeyError, match="Unknown operation"):
            reg.get("does_not_exist")


# ---------------------------------------------------------------------------
# PingOperation
# ---------------------------------------------------------------------------

class TestPingOperation:
    def setup_method(self):
        self.op = PingOperation()

    def test_name(self):
        assert self.op.name == "ping"

    def test_execute_returns_operation_result(self):
        events = []

        def emit(event, message, fields, level):
            events.append(event)

        result = self.op.execute(session=None, payload={}, emit=emit)
        assert isinstance(result, OperationResult)
        assert result.result == {"ok": True}
        assert "ping.start" in events
        assert "ping.done" in events

    def test_execute_emits_info_level(self):
        levels = []

        def emit(event, message, fields, level):
            levels.append(level)

        self.op.execute(session=None, payload={}, emit=emit)
        assert all(lv == "info" for lv in levels)
