"""Tests for protea/infrastructure/logging.py"""
from __future__ import annotations

import json
import logging

from protea.infrastructure.logging import JSONFormatter, configure_logging


class TestJSONFormatter:
    """Tests for the JSONFormatter class."""

    def _make_record(self, msg="hello", level=logging.INFO, name="test.logger", **kwargs):
        record = logging.LogRecord(
            name=name,
            level=level,
            pathname="test.py",
            lineno=1,
            msg=msg,
            args=(),
            exc_info=kwargs.pop("exc_info", None),
        )
        for k, v in kwargs.items():
            setattr(record, k, v)
        return record

    def test_formats_valid_json_with_expected_keys(self):
        formatter = JSONFormatter()
        record = self._make_record("test message")
        output = formatter.format(record)
        data = json.loads(output)

        assert "timestamp" in data
        assert data["level"] == "INFO"
        assert data["message"] == "test message"
        assert data["logger"] == "test.logger"

    def test_timestamp_is_utc_iso_format(self):
        formatter = JSONFormatter()
        record = self._make_record()
        data = json.loads(formatter.format(record))
        # UTC ISO timestamps end with +00:00
        assert "+00:00" in data["timestamp"]

    def test_includes_exc_info_when_present(self):
        formatter = JSONFormatter()
        try:
            raise ValueError("boom")
        except ValueError:
            import sys
            exc_info = sys.exc_info()

        record = self._make_record("error occurred", exc_info=exc_info)
        data = json.loads(formatter.format(record))

        assert "exception" in data
        assert "ValueError" in data["exception"]
        assert "boom" in data["exception"]

    def test_exc_info_absent_when_no_exception(self):
        formatter = JSONFormatter()
        record = self._make_record("all good")
        data = json.loads(formatter.format(record))
        assert "exception" not in data

    def test_includes_extra_fields(self):
        formatter = JSONFormatter()
        record = self._make_record("with extras", queue="protea.jobs", batch_size=100)
        data = json.loads(formatter.format(record))

        assert data["queue"] == "protea.jobs"
        assert data["batch_size"] == 100

    def test_builtin_attrs_excluded_from_extras(self):
        formatter = JSONFormatter()
        record = self._make_record("check builtins")
        data = json.loads(formatter.format(record))

        # Standard LogRecord attributes should not appear as top-level keys
        for attr in ("args", "exc_info", "exc_text", "lineno", "pathname", "thread"):
            assert attr not in data

    def test_stack_info_included_when_present(self):
        formatter = JSONFormatter()
        record = self._make_record("with stack")
        record.stack_info = "Stack trace here"
        data = json.loads(formatter.format(record))
        assert data["stack_info"] == "Stack trace here"

    def test_non_serializable_extra_uses_default_str(self):
        formatter = JSONFormatter()
        record = self._make_record("non-serializable", obj=object())
        # Should not raise — json.dumps(default=str) handles it
        output = formatter.format(record)
        data = json.loads(output)
        assert "obj" in data


class TestConfigureLogging:
    """Tests for the configure_logging function."""

    def setup_method(self):
        """Save root logger state before each test."""
        self._root = logging.getLogger()
        self._original_handlers = list(self._root.handlers)
        self._original_level = self._root.level

    def teardown_method(self):
        """Restore root logger state after each test."""
        self._root.handlers = self._original_handlers
        self._root.setLevel(self._original_level)

    def test_json_true_sets_json_formatter(self):
        configure_logging(json=True, level="WARNING")
        root = logging.getLogger()
        assert len(root.handlers) == 1
        assert isinstance(root.handlers[0].formatter, JSONFormatter)

    def test_json_false_uses_standard_formatter(self):
        configure_logging(json=False, level="INFO")
        root = logging.getLogger()
        assert len(root.handlers) == 1
        formatter = root.handlers[0].formatter
        assert not isinstance(formatter, JSONFormatter)
        assert isinstance(formatter, logging.Formatter)

    def test_respects_level_parameter(self):
        configure_logging(json=True, level="DEBUG")
        assert logging.getLogger().level == logging.DEBUG

        configure_logging(json=True, level="ERROR")
        assert logging.getLogger().level == logging.ERROR

    def test_level_is_case_insensitive(self):
        configure_logging(json=True, level="warning")
        assert logging.getLogger().level == logging.WARNING

    def test_clears_existing_handlers(self):
        root = logging.getLogger()
        root.addHandler(logging.StreamHandler())
        root.addHandler(logging.StreamHandler())
        assert len(root.handlers) >= 2

        configure_logging(json=True)
        assert len(root.handlers) == 1

    def test_invalid_level_falls_back_to_info(self):
        configure_logging(json=True, level="NONEXISTENT")
        assert logging.getLogger().level == logging.INFO

    def test_handler_is_stream_handler(self):
        configure_logging(json=True)
        root = logging.getLogger()
        assert isinstance(root.handlers[0], logging.StreamHandler)
