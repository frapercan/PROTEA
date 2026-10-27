"""Tiny in-process TTL cache for aggregate API endpoints.

Built for stats/listing endpoints that run DISTINCT-over-JOIN queries on 10M+
row tables: queries that are structurally slow (tens of seconds) and whose
results change slowly enough that a 5-minute TTL is not user-visible.

Process-local by design: resets on uvicorn restart, does not need Redis, does
not leak across workers. Good enough for a single-instance deployment.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from typing import Any

from protea.config.tuning import get_tuning


def _default_ttl() -> float:
    """Resolved each call so env/yaml overrides apply at runtime."""
    return get_tuning().worker.api_cache_default_ttl_seconds


_lock = threading.Lock()
_store: dict[str, tuple[float, Any]] = {}

#: One lock per key, so only ONE producer runs per key at a time.
#:
#: Without this, ``cached`` released ``_lock`` before calling the producer, so
#: every concurrent caller on a cold or expired key ran its own copy. MEASURED
#: on the live API over 30 minutes of passive sampling of ``pg_stat_activity``:
#: the GO annotation matrix ran 8 times, median 137 s, max 171 s, for 1.167 s
#: of 1.800 -- the machine spent 65 per cent of wall time recomputing one
#: aggregate, with two parallel workers each time. The refresh loop alone would
#: have run it about 2,6 times; the rest were public page views arriving while
#: it was already in flight, each starting another copy of the same query.
#:
#: Never cleared. The key set is the fixed list of cached aggregates, so the
#: dict is bounded by it, and dropping a lock that a producer still holds would
#: reintroduce exactly the race this closes.
_key_locks: dict[str, threading.Lock] = {}


def _lock_for(key: str) -> threading.Lock:
    """The producer lock for ``key``, created once."""
    with _lock:
        existing = _key_locks.get(key)
        if existing is None:
            existing = _key_locks[key] = threading.Lock()
        return existing


def _fresh(key: str, now: float) -> tuple[bool, Any]:
    """``(hit, value)`` for a live entry, plus the stale one for the caller."""
    with _lock:
        entry = _store.get(key)
    if entry is None:
        return False, None
    return entry[0] > now, entry[1]


def cached(
    key: str,
    ttl: float,
    producer: Callable[[], Any],
    *,
    serve_stale_on_error: bool = False,
) -> Any:
    """Return ``producer()`` result, cached under ``key`` for ``ttl`` seconds.

    When ``serve_stale_on_error`` is true and the producer raises while a
    prior value is still in the store (even if expired), return the stale
    value instead of propagating; lets cold-cache hangs degrade to a slightly
    out-of-date payload rather than a 500.

    ONE PRODUCER PER KEY. A second caller arriving while a producer is in
    flight does not start its own: it takes the stale value if there is one,
    and otherwise waits for the one that is running. Serving stale rather than
    blocking is the right trade here and not a shortcut -- the producers this
    cache exists for take tens of seconds, and ``app._refresh_interval``'s own
    comment names the 30 s ngrok upstream deadline, so a caller made to wait
    for a 137 s aggregate gets a timeout, not a fresher answer.

    The expiry is dated from BEFORE the producer ran, deliberately. The value
    describes the database as of the query's start, so a slow producer should
    not buy itself extra lifetime; a 137 s producer under a 600 s ttl yields an
    entry good for the remaining 463 s.
    """
    now = time.monotonic()
    live, value = _fresh(key, now)
    if live:
        return value

    mine = _lock_for(key)
    if not mine.acquire(blocking=False):
        # Someone is already producing this key.
        if value is not None:
            return value
        # Nothing to serve: wait for them rather than start a second copy.
        with mine:
            _, value = _fresh(key, time.monotonic())
            return value
    try:
        # Re-check under the producer lock: the holder we may have queued
        # behind could have just stored a fresh value.
        live, fresh_value = _fresh(key, time.monotonic())
        if live:
            return fresh_value
        try:
            produced = producer()
        except Exception:
            if serve_stale_on_error and value is not None:
                return value
            raise
        with _lock:
            _store[key] = (now + ttl, produced)
        return produced
    finally:
        mine.release()


def refresh(key: str, ttl: float, producer: Callable[[], Any]) -> Any:
    """Recompute ``key`` on schedule WITHOUT emptying the store meanwhile.

    This is what a prewarm loop wants, and it is not ``invalidate`` followed by
    ``cached``. That pair was the shape every ``prewarm_*`` used, and it has a
    window: the drop happens FIRST, so for however long the producer runs there
    is nothing in the store at all. The producers this cache exists for take
    tens of seconds -- the annotation-set listing was measured at a median of
    145 s, once every 385 s -- so the public endpoint had no value to serve for
    roughly 38 per cent of wall time, on a key that was never actually missing.

    Before one producer per key existed that window was a stampede. Since
    :func:`cached` serves the stale value to a caller that arrives while a
    producer runs, the window became a wait instead -- and a wait of 145 s is a
    timeout, because ``app._refresh_interval``'s own comment names the 30 s
    ngrok upstream deadline. Either way the cause is the same: dropping a good
    value before having a better one.

    So: produce first, swap after. The previous value stays live and servable
    for the whole recompute, and callers never see the key absent.

    The producer lock is held for the duration, so a caller whose entry expires
    mid-refresh takes the stale value from :func:`cached` rather than starting a
    second copy of the same query. The expiry is dated from before the producer
    ran, like ``cached``, for the same reason: the value describes the database
    as of the query's start.

    Unlike ``cached`` this ALWAYS produces. A refresh loop that skipped because
    the entry was still live would let it expire before the next tick and hand
    the cold path to whoever arrived next, which is the thing prewarming exists
    to prevent.
    """
    now = time.monotonic()
    with _lock_for(key):
        value = producer()
        with _lock:
            _store[key] = (now + ttl, value)
    return value


def get_last_known(key: str) -> Any | None:
    """Return the last cached value for ``key``, ignoring TTL; ``None`` if absent."""
    with _lock:
        hit = _store.get(key)
        return hit[1] if hit is not None else None


def invalidate(key: str | None = None) -> None:
    """Drop a single key, or the whole cache when ``key`` is ``None``."""
    with _lock:
        if key is None:
            _store.clear()
        else:
            _store.pop(key, None)


__all__ = ["cached", "get_last_known", "invalidate", "refresh", "_default_ttl"]
