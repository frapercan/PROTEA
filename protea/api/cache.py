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
    """
    now = time.monotonic()
    with _lock:
        hit = _store.get(key)
        if hit is not None and hit[0] > now:
            return hit[1]
    try:
        value = producer()
    except Exception:
        if serve_stale_on_error and hit is not None:
            return hit[1]
        raise
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


__all__ = ["cached", "get_last_known", "invalidate", "_default_ttl"]
