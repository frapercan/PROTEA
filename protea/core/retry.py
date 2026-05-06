"""Generic retry middleware for transient infrastructure errors.

Used by BaseWorker to survive Postgres deadlocks, serialization
failures and brief connection interruptions without marking the
job as failed. Application errors (validation, missing data) are
NOT retried; only transient infrastructure conditions are.

Example::

    from protea.core.retry import with_retry

    def do_work():
        ...

    with_retry(do_work, max_attempts=3, base_delay=1.0)
"""

from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable
from typing import Any, ParamSpec, TypeVar

from sqlalchemy.exc import OperationalError

logger = logging.getLogger("protea.retry")

# Postgres SQLSTATE codes that signal a retryable transient condition.
# - 40P01: deadlock_detected
# - 40001: serialization_failure
RETRYABLE_PG_CODES: frozenset[str] = frozenset({"40P01", "40001"})


def is_retryable_db_error(exc: BaseException) -> bool:
    """Return True if the exception represents a transient DB condition."""
    if not isinstance(exc, OperationalError):
        return False
    pgcode = getattr(getattr(exc, "orig", None), "pgcode", None)
    return pgcode in RETRYABLE_PG_CODES


def is_retryable_connection_error(exc: BaseException) -> bool:
    """Return True if the exception represents a transient network condition.

    These are typically raised by pika or low-level socket layers when the
    broker is briefly unreachable. The publisher retry loop handles these
    on the publish side; this predicate exists for the consume side.
    """
    return isinstance(exc, ConnectionResetError | ConnectionAbortedError | TimeoutError)


def is_retryable(exc: BaseException) -> bool:
    """Default predicate: retryable DB errors plus transient network errors."""
    return is_retryable_db_error(exc) or is_retryable_connection_error(exc)


P = ParamSpec("P")
R = TypeVar("R")


def with_retry(  # noqa: UP047 — keep ParamSpec/TypeVar form; PEP 695 syntax churn deferred.
    fn: Callable[P, R],
    *args: P.args,
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    jitter_ratio: float = 0.5,
    predicate: Callable[[BaseException], bool] = is_retryable,
    on_retry: Callable[[int, BaseException, float], None] | None = None,
    **kwargs: P.kwargs,
) -> R:
    """Run ``fn(*args, **kwargs)`` with exponential backoff and jitter.

    The callable is invoked up to ``max_attempts`` times. After each
    retryable failure (per ``predicate``), the loop sleeps for
    ``min(base_delay * 2**(attempt-1), max_delay)`` seconds, jittered
    by a random factor in ``[1-jitter_ratio, 1+jitter_ratio]``.

    Exceptions that do not match ``predicate`` propagate immediately.
    Once ``max_attempts`` retryable failures accumulate, the last
    exception propagates to the caller.

    ``on_retry(attempt, exc, sleep_seconds)`` is called before each
    sleep. Defaults to a structured WARNING log under
    ``protea.retry``.
    """
    if max_attempts < 1:
        raise ValueError("max_attempts must be >= 1")

    attempt = 0
    while True:
        attempt += 1
        try:
            return fn(*args, **kwargs)
        except BaseException as exc:
            if not predicate(exc) or attempt >= max_attempts:
                raise
            sleep_for = min(base_delay * (2 ** (attempt - 1)), max_delay)
            jitter_low = max(0.0, 1.0 - jitter_ratio)
            jitter_high = 1.0 + jitter_ratio
            sleep_for *= random.uniform(jitter_low, jitter_high)

            if on_retry is not None:
                on_retry(attempt, exc, sleep_for)
            else:
                _default_on_retry(attempt, exc, sleep_for, max_attempts)
            time.sleep(sleep_for)


def _default_on_retry(
    attempt: int, exc: BaseException, sleep_for: float, max_attempts: int
) -> None:
    pgcode = getattr(getattr(exc, "orig", None), "pgcode", None)
    extra: dict[str, Any] = {
        "attempt": attempt,
        "max_attempts": max_attempts,
        "sleep_seconds": round(sleep_for, 3),
        "error_class": type(exc).__name__,
    }
    if pgcode is not None:
        extra["pgcode"] = pgcode
    logger.warning(
        "retryable failure; sleeping %ss then retrying. attempt=%d/%d error=%s",
        round(sleep_for, 3),
        attempt,
        max_attempts,
        type(exc).__name__,
        extra=extra,
    )
