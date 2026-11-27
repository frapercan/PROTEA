"""An instance can switch the API's prewarm off, and then nothing aggregates behind it.

The API's lifespan starts one task per cached aggregate, which computes it at
start and again before its time to live runs out, for as long as the process
runs. On the development store that kept Postgres busy for 16.5 CPU-hours in
16.9 hours while the API served two requests. ``PROTEA_API_PREWARM_ENABLED``
(``Settings.api_prewarm_enabled``, default true) is the switch: off, the
lifespan starts no task at all, neither the first pass nor the refresh, and
logs one line saying so.

These pin the setting's parsing, both states of the lifespan, and the wiring
from the setting to the lifespan through ``create_app``. Time is not waited
for: ``asyncio.sleep`` is replaced by one that returns at once and records what
it was asked, so a time to live elapses in a tick and a refresh loop shows
itself by calling its aggregate again.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from pydantic import ValidationError

from protea.api import app as app_module
from protea.infrastructure.settings import load_settings

_ENV = "PROTEA_API_PREWARM_ENABLED"
_TTL = 600.0
_REAL_SLEEP = asyncio.sleep


class _Aggregate:
    """A cached aggregate that only counts how often, and with what, it was computed.

    Called from the executor thread ``asyncio.to_thread`` uses, hence the lock.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.calls: list[Any] = []

    def __call__(self, factory: Any) -> None:
        with self._lock:
            self.calls.append(factory)

    @property
    def count(self) -> int:
        with self._lock:
            return len(self.calls)


class _Clock:
    """``asyncio.sleep`` with no waiting: each call is recorded and returns after one tick."""

    def __init__(self) -> None:
        self.asked: list[float] = []

    async def sleep(self, delay: float, result: Any = None) -> Any:
        self.asked.append(delay)
        await _REAL_SLEEP(0)
        return result


def _refresh_tasks() -> set[str]:
    """The names of this loop's refresh tasks, as ``_build_lifespan`` names them."""
    return {task.get_name() for task in asyncio.all_tasks() if task.get_name().endswith("-refresh")}


async def _wait_for(fact: Callable[[], bool], seconds: float = 10.0) -> None:
    """Yield to the loop until ``fact`` holds; the prewarms run on executor threads."""
    deadline = time.monotonic() + seconds
    while not fact() and time.monotonic() < deadline:
        await _REAL_SLEEP(0.005)


def _run_lifespan(
    *, prewarm_enabled: bool, body: Callable[[], Awaitable[None]], factory: Any
) -> None:
    lifespan = app_module._build_lifespan(factory, prewarm_enabled=prewarm_enabled)

    async def main() -> None:
        async with lifespan(FastAPI()):
            await body()

    asyncio.run(main())


@pytest.fixture()
def clock(monkeypatch: pytest.MonkeyPatch) -> _Clock:
    """The app module's ``asyncio.sleep``, replaced for the test only."""
    fake = _Clock()
    monkeypatch.setattr(app_module.asyncio, "sleep", fake.sleep)
    return fake


@pytest.fixture()
def one_aggregate(monkeypatch: pytest.MonkeyPatch) -> _Aggregate:
    """One cached aggregate with a ten-minute time to live in place of the real six."""
    aggregate = _Aggregate()
    monkeypatch.setattr(
        app_module, "_prewarm_targets", lambda: (("test:aggregate", aggregate, _TTL),)
    )
    return aggregate


class TestTheSettingIsRead:
    def test_on_when_unset(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Unset keeps every instance as it was before the switch existed."""
        monkeypatch.delenv(_ENV, raising=False)
        assert load_settings(tmp_path).api_prewarm_enabled is True

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("true", True),
            ("false", False),
            ("True", True),
            ("FALSE", False),
            ("1", True),
            ("0", False),
            ("yes", True),
            ("no", False),
            ("on", True),
            ("off", False),
        ],
    )
    def test_true_and_false_parse(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, raw: str, expected: bool
    ) -> None:
        monkeypatch.setenv(_ENV, raw)
        assert load_settings(tmp_path).api_prewarm_enabled is expected

    def test_a_value_that_is_not_a_boolean_stops_the_start(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A misspelt "false" must not leave the prewarm running in silence."""
        monkeypatch.setenv(_ENV, "flase")
        with pytest.raises(ValidationError, match="api_prewarm_enabled"):
            load_settings(tmp_path)


class TestOff:
    def test_no_task_at_start(self, clock: _Clock, one_aggregate: _Aggregate) -> None:
        seen: dict[str, Any] = {}

        async def body() -> None:
            await _REAL_SLEEP(0)
            seen["tasks"] = _refresh_tasks()

        _run_lifespan(prewarm_enabled=False, body=body, factory=MagicMock())
        assert seen["tasks"] == set()
        assert one_aggregate.count == 0, "not even the first pass"

    def test_no_task_after_a_time_to_live_elapses(
        self, clock: _Clock, one_aggregate: _Aggregate
    ) -> None:
        seen: dict[str, Any] = {}

        async def body() -> None:
            await app_module.asyncio.sleep(_TTL)
            for _tick in range(20):
                await _REAL_SLEEP(0)
            seen["tasks"] = _refresh_tasks()

        _run_lifespan(prewarm_enabled=False, body=body, factory=MagicMock())
        assert seen["tasks"] == set()
        assert one_aggregate.count == 0
        assert clock.asked == [_TTL], "the lifespan asked for no wait of its own"

    def test_it_says_so_in_one_line(
        self, clock: _Clock, one_aggregate: _Aggregate, caplog: pytest.LogCaptureFixture
    ) -> None:
        """At warning level: uvicorn gives this module's logger no handler, so an
        info line would never reach the journal an operator reads after a deploy."""

        async def body() -> None:
            await _REAL_SLEEP(0)

        with caplog.at_level(logging.WARNING, logger=app_module.__name__):
            _run_lifespan(prewarm_enabled=False, body=body, factory=MagicMock())
        lines = [r for r in caplog.records if r.name == app_module.__name__]
        assert [r.getMessage() for r in lines] == [app_module.PREWARM_OFF_MESSAGE]
        assert lines[0].levelno == logging.WARNING
        assert _ENV in lines[0].getMessage()


class TestOn:
    def test_todays_tasks_one_per_aggregate(
        self, clock: _Clock, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """The six aggregates of ``_prewarm_targets``, each computed once at start."""
        real = app_module._prewarm_targets()
        aggregates = {label: _Aggregate() for label, _fn, _ttl in real}
        monkeypatch.setattr(
            app_module,
            "_prewarm_targets",
            lambda: tuple((label, aggregates[label], ttl) for label, _fn, ttl in real),
        )
        factory = MagicMock()
        seen: dict[str, Any] = {}

        async def body() -> None:
            seen["tasks"] = _refresh_tasks()
            await _wait_for(lambda: all(a.count for a in aggregates.values()))

        with caplog.at_level(logging.WARNING, logger=app_module.__name__):
            _run_lifespan(prewarm_enabled=True, body=body, factory=factory)
        assert len(real) == 6
        assert seen["tasks"] == {f"{label}-refresh" for label in aggregates}
        for label, aggregate in aggregates.items():
            assert aggregate.calls[0] is factory, label
        assert app_module.PREWARM_OFF_MESSAGE not in caplog.text

    def test_it_refreshes_after_a_time_to_live_elapses(
        self, clock: _Clock, one_aggregate: _Aggregate
    ) -> None:
        """The same harness that sees nothing when off sees the refresh when on."""

        async def body() -> None:
            await _wait_for(lambda: one_aggregate.count >= 2)

        _run_lifespan(prewarm_enabled=True, body=body, factory=MagicMock())
        assert one_aggregate.count >= 2, "the first pass, then a refresh"
        assert app_module._refresh_interval(_TTL) in clock.asked


class TestCreateAppPassesTheSetting:
    def _client_calls(self, api_prewarm_enabled: bool) -> int:
        from fastapi.testclient import TestClient

        settings = MagicMock()
        settings.db_url = "sqlite:///:memory:"
        settings.amqp_url = "amqp://guest:guest@localhost/"
        settings.allowed_origins = ()
        settings.api_prewarm_enabled = api_prewarm_enabled
        aggregate = _Aggregate()
        with (
            patch.object(app_module, "load_settings", return_value=settings),
            patch.object(app_module, "build_session_factory", return_value=MagicMock()),
            patch.object(
                app_module, "_prewarm_targets", lambda: (("test:aggregate", aggregate, _TTL),)
            ),
        ):
            app = app_module.create_app(Path("/fake/root"))
            with TestClient(app):
                deadline = time.monotonic() + 10.0
                while api_prewarm_enabled and not aggregate.count and time.monotonic() < deadline:
                    time.sleep(0.005)
        return aggregate.count

    def test_off_reaches_the_lifespan(self) -> None:
        assert self._client_calls(api_prewarm_enabled=False) == 0

    def test_on_reaches_the_lifespan(self) -> None:
        assert self._client_calls(api_prewarm_enabled=True) >= 1
