"""Process-local string interning for annotation hot loops.

The predict / train pipelines build millions of per-row dicts of the
shape ``{"go_term_id": int, "qualifier": str|None, "evidence_code": str|None}``
when they materialise annotations from PostgreSQL. SQLAlchemy returns
each ``qualifier`` and ``evidence_code`` as a *fresh* Python string —
even though across a million rows there are only ~5-10 distinct values
(``"IEA"``, ``"IDA"``, ``"EXP"``, ``"TAS"``, …) and most rows have
``qualifier = None``.

Without interning, each duplicate string allocates ~50 B in CPython,
so a 5 M-row batch can carry ~500 MB of redundant string objects.
Interning collapses every duplicate to a single shared instance, a
Flyweight-style intrinsic-state share. Python already does this implicitly for short
identifier-like literals; this module forces the same dedup for the
strings that come back from the DB driver.

Process-local by design (one cache per worker), trivially small (the
domain has fewer than 50 distinct codes/qualifiers in practice), and
thread-safe via the GIL — ``setdefault`` is atomic for built-in dicts.
"""

from __future__ import annotations

# Bounded only by the number of distinct strings ever seen in this
# process — in practice that is the count of GO evidence codes and
# qualifier strings, both small finite sets.
_INTERN_POOL: dict[str, str] = {}


def intern_string(value: str | None) -> str | None:
    """Return a shared instance of ``value`` if it has been seen before.

    Pass ``None`` through unchanged so callers don't need a guard. The
    pool grows monotonically; reset only when the worker restarts.
    """
    if value is None:
        return None
    cached = _INTERN_POOL.setdefault(value, value)
    return cached


def pool_size() -> int:
    """Diagnostic — how many distinct strings the pool has retained."""
    return len(_INTERN_POOL)


__all__ = ["intern_string", "pool_size"]
