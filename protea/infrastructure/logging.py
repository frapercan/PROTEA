# protea/infrastructure/logging.py
"""Structured logging configuration for PROTEA.

Provides a JSON formatter using only the Python standard library and a
``configure_logging()`` helper that workers and the API can call at startup.
"""
from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Any


class JSONFormatter(logging.Formatter):
    """Formats log records as single-line JSON objects.

    Each line contains at least ``timestamp``, ``level``, ``logger``, and
    ``message``.  Any *extra* fields attached to the record are merged into
    the top-level JSON object, making it easy to add structured context
    (e.g. ``logger.info("started", extra={"queue": "protea.jobs"})``).
    """

    # Keys that belong to the standard LogRecord and should not be forwarded
    # as extra fields.
    _BUILTIN_ATTRS: frozenset[str] = frozenset(
        {
            "args",
            "created",
            "exc_info",
            "exc_text",
            "filename",
            "funcName",
            "levelname",
            "levelno",
            "lineno",
            "message",
            "module",
            "msecs",
            "msg",
            "name",
            "pathname",
            "process",
            "processName",
            "relativeCreated",
            "stack_info",
            "taskName",
            "thread",
            "threadName",
        }
    )

    def format(self, record: logging.LogRecord) -> str:  # noqa: D401
        log_entry: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Merge any extra fields the caller attached to the record.
        for key, value in record.__dict__.items():
            if key not in self._BUILTIN_ATTRS:
                log_entry[key] = value

        # Append exception info when present.
        if record.exc_info and record.exc_info[1] is not None:
            log_entry["exception"] = self.formatException(record.exc_info)

        if record.stack_info:
            log_entry["stack_info"] = record.stack_info

        return json.dumps(log_entry, default=str)


_TEXT_FORMAT = "%(asctime)s %(levelname)s %(message)s"


def configure_logging(*, json: bool = True, level: str = "INFO") -> None:
    """Configure the root logger for the process.

    Parameters
    ----------
    json:
        When *True* (the default), use :class:`JSONFormatter` so that every
        log line is a valid JSON object.  When *False*, fall back to the
        plain-text format used during local development.
    level:
        Root log level name (e.g. ``"INFO"``, ``"DEBUG"``).
    """
    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Remove any handlers that may have been added by earlier basicConfig
    # calls or library imports so we start fresh.
    root.handlers.clear()

    handler = logging.StreamHandler()
    if json:
        handler.setFormatter(JSONFormatter())
    else:
        handler.setFormatter(logging.Formatter(_TEXT_FORMAT))

    root.addHandler(handler)
