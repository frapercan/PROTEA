"""Generic retry middleware for transient infrastructure errors.

Used by BaseWorker to survive Postgres deadlocks, serialization
failures and brief connection interruptions without marking the
job as failed. Application errors (validation, missing data) are
NOT retried; only transient infrastructure conditions are.

Example::

    from protea.core.retry import RetryPolicy, with_retry

    def do_work():
        ...

    with_retry(do_work, policy=RetryPolicy(max_attempts=3, base_delay=1.0))
"""

from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable
from dataclasses import dataclass, field
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


@dataclass(frozen=True)
class RetryPolicy:
    """Bundle of tunable retry knobs consumed by :func:`with_retry`.

    Frozen so a default-constructed policy can sit in a function
    default safely; callers override individual fields by passing a
    new ``RetryPolicy(...)`` keyword. Fields:

    * ``max_attempts``: total attempts including the first; ``< 1`` is
      rejected at call time so ``RetryPolicy(max_attempts=0)`` does not
      silently no-op.
    * ``base_delay`` / ``max_delay`` / ``jitter_ratio``: exponential
      backoff schedule used between retryable failures.
    * ``predicate``: classifier returning ``True`` for retryable
      exceptions; defaults to :func:`is_retryable` (DB + transient
      network errors).
    * ``on_retry``: optional callback fired before each sleep with
      ``(attempt, exc, sleep_seconds)``. Defaults to a structured
      WARNING log under ``protea.retry``.
    """

    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 30.0
    jitter_ratio: float = 0.5
    predicate: Callable[[BaseException], bool] = field(default=is_retryable)
    on_retry: Callable[[int, BaseException, float], None] | None = None


_DEFAULT_RETRY_POLICY = RetryPolicy()


# PEP 612 strict says no params allowed between *args: P.args and
# **kwargs: P.kwargs; ``policy`` binds correctly at runtime because
# Python resolves explicit names before falling through to **kwargs.
def with_retry(  # type: ignore[valid-type]  # noqa: UP047
    fn: Callable[P, R],
    *args: P.args,
    policy: RetryPolicy = _DEFAULT_RETRY_POLICY,
    **kwargs: P.kwargs,
) -> R:
    """Run ``fn(*args, **kwargs)`` with exponential backoff and jitter.

    The callable is invoked up to ``policy.max_attempts`` times. After
    each retryable failure (per ``policy.predicate``), the loop sleeps
    for ``min(policy.base_delay * 2**(attempt-1), policy.max_delay)``
    seconds, jittered by a random factor in ``[1 - jitter_ratio,
    1 + jitter_ratio]``.

    Exceptions that do not match ``policy.predicate`` propagate
    immediately. Once ``policy.max_attempts`` retryable failures
    accumulate, the last exception propagates to the caller.
    """
    if policy.max_attempts < 1:
        raise ValueError("max_attempts must be >= 1")

    attempt = 0
    while True:
        attempt += 1
        try:
            return fn(*args, **kwargs)
        except BaseException as exc:
            if not policy.predicate(exc) or attempt >= policy.max_attempts:
                raise
            sleep_for = min(policy.base_delay * (2 ** (attempt - 1)), policy.max_delay)
            jitter_low = max(0.0, 1.0 - policy.jitter_ratio)
            jitter_high = 1.0 + policy.jitter_ratio
            sleep_for *= random.uniform(jitter_low, jitter_high)

            if policy.on_retry is not None:
                policy.on_retry(attempt, exc, sleep_for)
            else:
                _default_on_retry(attempt, exc, sleep_for, policy.max_attempts)
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
