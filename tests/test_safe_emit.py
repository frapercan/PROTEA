"""Tests for make_safe_emit (T0.2 of master plan v3)."""

from __future__ import annotations

import logging
from typing import Any

import pytest

from protea.core.contracts.operation import make_safe_emit


class TestMakeSafeEmit:
    """make_safe_emit wraps an EmitFn so failures don't crash the operation."""

    def test_passthrough_on_success(self) -> None:
        calls: list[tuple] = []

        def raw(event, message, fields, level):
            calls.append((event, message, fields, level))

        safe = make_safe_emit(raw)
        safe("step.start", "doing work", {"k": 1}, "info")

        assert calls == [("step.start", "doing work", {"k": 1}, "info")]

    def test_default_arguments_normalised(self) -> None:
        calls: list[tuple] = []

        def raw(event, message, fields, level):
            calls.append((event, message, fields, level))

        safe = make_safe_emit(raw)
        safe("step.tick")

        # Defaults: message=None, fields={} (not None), level="info".
        assert calls == [("step.tick", None, {}, "info")]

    def test_swallows_exceptions(self, caplog: pytest.LogCaptureFixture) -> None:
        def raw(*_args: Any, **_kwargs: Any) -> None:
            raise RuntimeError("DB lost connection")

        safe = make_safe_emit(raw)

        with caplog.at_level(logging.ERROR, logger="protea.emit"):
            safe("step.boom", "oops", {"x": 2}, "warning")

        assert any(
            "emit failed" in record.message and record.levelno == logging.ERROR
            for record in caplog.records
        )
        # Find the record and confirm exc_info captured.
        emit_records = [r for r in caplog.records if "emit failed" in r.message]
        assert emit_records, "expected at least one error log from safe_emit"
        assert emit_records[0].exc_info is not None

    def test_does_not_propagate_arbitrary_exceptions(self) -> None:
        class CustomError(Exception):
            pass

        def raw(*_args: Any, **_kwargs: Any) -> None:
            raise CustomError("anything")

        safe = make_safe_emit(raw)
        # Must not raise.
        safe("any.event")

    def test_can_be_called_repeatedly_after_failure(self) -> None:
        attempts: list[bool] = []

        def raw(*_args: Any, **_kwargs: Any) -> None:
            attempts.append(True)
            if len(attempts) == 1:
                raise RuntimeError("first attempt fails")

        safe = make_safe_emit(raw)
        safe("first")
        safe("second")
        safe("third")

        assert len(attempts) == 3
