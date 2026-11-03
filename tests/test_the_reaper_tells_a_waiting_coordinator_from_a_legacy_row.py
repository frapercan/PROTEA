"""``leased_until IS NULL`` named two things, and the reaper judged both by one window.

A row has no lease either because it predates the leasing column or because a
coordinator deferred and cleared it on purpose: ``base_worker`` does that once
``result.deferred``, with a comment saying the event path is what should keep the
job alive. The two want different windows. A job doing its own work and silent
for thirty minutes is stalled; a coordinator whose children run on another
machine is silent for as long as that machine pauses, and the compute node cedes
its card for two and three hours at a time.

MEASURED, which is why this file exists: ``b185a6d0`` started 2026-10-30 at 11:26
and the reaper failed it at 17:26, six hours to the minute, with
``error_code = lease_expired`` and progress ``0/7``. On 10-31 at 09:19:35 seven of
its batches were discarded as ``parent_not_running``. Nothing had gone wrong with
the coordinator; it was waiting.

The tests drive ``_reap_one_candidate`` with a session that answers only the two
questions it asks, so the decision table is pinned without a database. The
discriminator itself is pinned too: the fake fails the test if the reaper asks
for any event name other than ``job.dispatched``.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import pytest

from protea.workers.stale_job_reaper import (
    DISPATCHED_EVENT,
    StaleJobReaper,
    StaleJobReaperConfig,
)

NOW = datetime(2026, 11, 3, 12, 0, tzinfo=timezone.utc)


class _OneAnswer:
    """What ``session.query(...)`` returns: a chain that ends in one value."""

    def __init__(self, scalar: Any = None, first: Any = None) -> None:
        self._scalar = scalar
        self._first = first

    def filter(self, *_criteria: Any) -> _OneAnswer:
        return self

    def scalar(self) -> Any:
        return self._scalar

    def first(self) -> Any:
        return self._first


class _Session:
    """Answers the two queries ``_reap_one_candidate`` makes, and nothing else.

    The reaper asks for ``max(JobEvent.ts)`` and then, for a lease-less row, for
    the existence of one event by name. Telling them apart by the rendered
    entity is deliberate: it makes the test fail if the reaper stops asking for
    ``job.dispatched``, which is the whole discriminator.
    """

    def __init__(self, *, last_event_ts: datetime | None, deferred: bool) -> None:
        self._last_event_ts = last_event_ts
        self._deferred = deferred
        self.asked_for_events: list[str] = []

    def query(self, *entities: Any) -> _OneAnswer:
        rendered = str(entities[0])
        if "max" in rendered:
            return _OneAnswer(scalar=self._last_event_ts)
        self.asked_for_events.append(rendered)
        return _OneAnswer(first=object() if self._deferred else None)


def _reaper() -> StaleJobReaper:
    cfg = StaleJobReaperConfig(
        timeout_seconds=21_600,
        stall_seconds=1_800,
        deferred_stall_seconds=21_600,
    )
    return StaleJobReaper(session_factory=None, amqp_url=None, config=cfg)  # type: ignore[arg-type]


def _job() -> SimpleNamespace:
    """A RUNNING row with no lease, which is the only shape this branch sees."""
    return SimpleNamespace(
        id=uuid.uuid4(),
        operation="compute_embeddings",
        leased_until=None,
        status="RUNNING",
    )


def _decide(*, silent_for: timedelta, deferred: bool) -> tuple[int, Any, Any, _Session]:
    """Run one candidate through the reaper and report what it decided."""
    reaper = _reaper()
    session = _Session(last_event_ts=NOW - silent_for, deferred=deferred)
    stall_cutoff = NOW - timedelta(seconds=1_800)
    with (
        patch.object(StaleJobReaper, "_lease_requeue_attempts", return_value=0),
        patch.object(StaleJobReaper, "_requeue_lease_expired") as requeued,
        patch.object(StaleJobReaper, "_fail_lease_expired") as failed,
    ):
        recovered = reaper._reap_one_candidate(session, _job(), NOW, stall_cutoff)  # type: ignore[arg-type]
    return recovered, requeued, failed, session


@pytest.mark.parametrize("silent_minutes", [45, 120, 180, 355])
def test_a_waiting_coordinator_is_left_alone_for_its_own_window(silent_minutes: int) -> None:
    """Up to six hours of silence, a deferred coordinator is alive.

    Forty-five minutes is past the 1800s a legacy row gets, and two and three
    hours are the card cessions actually observed on the compute node. All four
    were reaped before this branch existed.
    """
    recovered, requeued, failed, _ = _decide(
        silent_for=timedelta(minutes=silent_minutes), deferred=True
    )
    assert recovered == 0
    assert requeued.call_count == 0
    assert failed.call_count == 0


def test_past_its_own_window_a_waiting_coordinator_is_still_recovered() -> None:
    """The longer window is a window and not an exemption.

    Seven hours of silence from every child is a genuine stall, and the job must
    not sit RUNNING for ever: it falls through to the same ladder as any other
    candidate.
    """
    recovered, requeued, failed, _ = _decide(silent_for=timedelta(hours=7), deferred=True)
    assert recovered == 1
    assert requeued.call_count + failed.call_count == 1


def test_a_legacy_row_keeps_the_short_window() -> None:
    """Nothing changes for a row that never deferred.

    This is the half of ``leased_until IS NULL`` the old window was written for,
    and forty-five minutes of silence from a job doing its own work is still
    stalled.
    """
    recovered, requeued, failed, _ = _decide(silent_for=timedelta(minutes=45), deferred=False)
    assert recovered == 1
    assert requeued.call_count + failed.call_count == 1


def test_a_legacy_row_inside_the_short_window_is_still_alive() -> None:
    """And the old grace keeps working, so this is strictly a widening."""
    recovered, requeued, failed, _ = _decide(silent_for=timedelta(minutes=20), deferred=False)
    assert recovered == 0
    assert requeued.call_count == 0
    assert failed.call_count == 0


def test_the_discriminator_is_the_dispatched_event_and_not_a_guess() -> None:
    """What the reaper asks for, by name.

    ``job.dispatched`` is written in exactly one place -- ``base_worker``'s
    deferred branch -- so it is the only record that a coordinator deferred. A
    reaper that inferred deferral from anything else (the operation's name, a
    null lease alone, a progress ratio) would be guessing, and this asserts it
    does not.
    """
    _, _, _, session = _decide(silent_for=timedelta(minutes=45), deferred=True)
    assert session.asked_for_events, "the reaper never asked whether the job had deferred"
    assert DISPATCHED_EVENT == "job.dispatched"


def test_the_default_window_is_six_hours_and_reaches_production() -> None:
    """The value a worker actually runs with, which no test pinned.

    The first attempt to kill these tests by mutation changed
    ``StaleJobReaperConfig``'s default and nothing went red, because every test
    above builds its config explicitly. Production does not: ``scripts/worker.py``
    reads ``WorkerTuning`` and the tuning default is what a worker gets, so the
    default and the wiring are the part a mutation could silently take away.
    """
    from protea.config.tuning import WorkerTuning
    from protea.workers.stale_job_reaper import StaleJobReaperConfig
    from scripts.worker import _build_reaper_config

    assert WorkerTuning().reaper_deferred_stall_seconds == 21_600
    assert StaleJobReaperConfig().deferred_stall_seconds == 21_600

    wt = WorkerTuning(reaper_deferred_stall_seconds=12_345)
    assert _build_reaper_config(wt).deferred_stall_seconds == 12_345, (
        "the tuning value never reaches the reaper, so the window is whatever the "
        "dataclass default happens to be"
    )
