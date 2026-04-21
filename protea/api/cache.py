"""Tiny in-process TTL cache for aggregate API endpoints.

Built for stats/listing endpoints that run DISTINCT-over-JOIN queries on 10M+
row tables — queries that are structurally slow (tens of seconds) and whose
results change slowly enough that a 5-minute TTL is not user-visible.

Process-local by design: resets on uvicorn restart, does not need Redis, does
not leak across workers. Good enough for a single-instance deployment.
"""

from __future__ import annotations

import threading
import time
from typing import Any, Callable

_DEFAULT_TTL = 300.0  # 5 minutes

_lock = threading.Lock()
_store: dict[str, tuple[float, Any]] = {}


def cached(key: str, ttl: float, producer: Callable[[], Any]) -> Any:
    """Return ``producer()`` result, cached under ``key`` for ``ttl`` seconds."""
    now = time.monotonic()
    with _lock:
        hit = _store.get(key)
        if hit is not None and hit[0] > now:
            return hit[1]
    value = producer()
    with _lock:
        _store[key] = (now + ttl, value)
    return value


def invalidate(key: str | None = None) -> None:
    """Drop a single key, or the whole cache when ``key`` is ``None``."""
    with _lock:
        if key is None:
            _store.clear()
        else:
            _store.pop(key, None)


__all__ = ["cached", "invalidate", "_DEFAULT_TTL"]
