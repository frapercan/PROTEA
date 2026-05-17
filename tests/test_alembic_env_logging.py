"""Regression test for the alembic env logging side effect.

PR #396 (T1.6 schema_sha v2 migration) added an integration test that
calls ``alembic.command.upgrade`` directly. The default alembic env.py
boilerplate invokes ``logging.config.fileConfig`` on ``alembic.ini``,
which has two failure modes when alembic is driven from inside a pytest
session:

1. ``fileConfig`` defaults to ``disable_existing_loggers=True``, which
   flips ``logger.disabled = True`` on every logger created before
   alembic runs (including PROTEA module loggers). caplog only tracks
   the global ``logging.disable()`` switch, not the per-logger
   ``.disabled`` attribute, so the logger stays muted for the rest of
   the session.
2. ``fileConfig`` REPLACES the root logger's handlers with the
   ``[handler_console]`` config from alembic.ini, removing pytest's
   ``LogCaptureHandler`` and any other handlers already attached.

Either symptom alone is enough to give downstream caplog tests an empty
``caplog.text``. Four caplog assertions in ``tests/test_storage.py`` and
``tests/test_telemetry.py`` failed silently for that reason in the
``integration`` CI job, even though PR #398 had pinned the level on the
named module loggers (level pinning does not clear ``.disabled`` and
cannot re-attach a removed handler).

``alembic/env.py`` now skips ``fileConfig`` when the root logger already
has handlers, which is the canonical pattern for env.py snippets that
need to coexist with a host application's logging (FastAPI, pytest,
structlog...). This test pins the guard so a future hand-edit to env.py
cannot silently regress caplog capture three test files over.
"""

from __future__ import annotations

import logging
from logging.config import fileConfig
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = REPO_ROOT / "alembic" / "env.py"
INI_PATH = REPO_ROOT / "alembic.ini"


def test_env_py_guards_fileconfig_against_existing_handlers() -> None:
    """The conditional must remain on env.py's fileConfig call.

    Catches the regression where a future contributor restores the
    unguarded ``fileConfig(config.config_file_name)`` boilerplate.
    """
    source = ENV_PATH.read_text()
    # The guard expression - any rewrite must keep both halves.
    assert "logging.getLogger().handlers" in source, (
        "alembic/env.py lost its 'logging.getLogger().handlers' guard; "
        "fileConfig will now clobber caplog the moment any test calls "
        "command.upgrade(). See tests/test_alembic_env_logging.py docstring."
    )
    assert "fileConfig(" in source
    # disable_existing_loggers=False is belt-and-braces in case the
    # guard ever leaks a fileConfig call (e.g., when env.py is imported
    # from a worker boot before logging is set up).
    assert "disable_existing_loggers=False" in source


def test_unguarded_fileconfig_would_break_caplog(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Sanity-check the failure mode the guard protects against.

    Calling ``fileConfig`` on PROTEA's ``alembic.ini`` directly (i.e.,
    the way the original boilerplate did) removes the caplog handler
    from the root logger. This test pins the failure shape so a future
    reader of the codebase can see exactly why the env.py guard
    exists.
    """
    # Pre-condition: caplog is wired to the root logger.
    pre_handlers = list(logging.getLogger().handlers)
    assert caplog.handler in pre_handlers

    try:
        fileConfig(str(INI_PATH), disable_existing_loggers=False)
        post_handlers = list(logging.getLogger().handlers)
        # This is the regression: fileConfig replaces root.handlers with
        # the [handler_console] from alembic.ini.
        assert caplog.handler not in post_handlers
    finally:
        # Restore root handlers so subsequent tests in this session can
        # use caplog normally.
        root = logging.getLogger()
        root.handlers.clear()
        for h in pre_handlers:
            root.addHandler(h)


def test_logger_disabled_flag_survives_set_level(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Document why pinning ``caplog.set_level(logger=...)`` was not enough.

    PR #398 pinned the level on the module logger but did not (could
    not) clear ``logger.disabled``. Once a logger is disabled,
    ``caplog.set_level`` is a no-op for it. This test captures that
    interaction so the env.py-level fix above is the only correct
    intervention.
    """
    name = "protea.test.alembic_env_logging.scratch"
    lg = logging.getLogger(name)
    try:
        lg.disabled = True
        caplog.set_level(logging.WARNING, logger=name)
        lg.warning("should-not-appear")
        assert lg.disabled is True  # caplog did not clear the flag
        assert "should-not-appear" not in caplog.text
    finally:
        lg.disabled = False
