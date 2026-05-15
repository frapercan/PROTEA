"""Tests for protea.core.retry (T0.3 of master plan v3)."""

from __future__ import annotations

import logging
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.exc import OperationalError

from protea.core.retry import (
    RETRYABLE_PG_CODES,
    RetryPolicy,
    is_retryable,
    is_retryable_connection_error,
    is_retryable_db_error,
    with_retry,
)


def _make_op_error(pgcode: str | None) -> OperationalError:
    """Build an OperationalError whose .orig has the given pgcode."""
    orig = MagicMock()
    orig.pgcode = pgcode
    err = OperationalError("stmt", {}, orig)
    err.orig = orig
    return err


class TestIsRetryableDbError:
    def test_deadlock_detected_is_retryable(self) -> None:
        assert is_retryable_db_error(_make_op_error("40P01")) is True

    def test_serialization_failure_is_retryable(self) -> None:
        assert is_retryable_db_error(_make_op_error("40001")) is True

    def test_other_pgcode_is_not_retryable(self) -> None:
        assert is_retryable_db_error(_make_op_error("23505")) is False

    def test_no_pgcode_is_not_retryable(self) -> None:
        assert is_retryable_db_error(_make_op_error(None)) is False

    def test_non_operational_error_is_not_retryable(self) -> None:
        assert is_retryable_db_error(ValueError("bad")) is False

    def test_known_codes_match_constant(self) -> None:
        assert "40P01" in RETRYABLE_PG_CODES
        assert "40001" in RETRYABLE_PG_CODES


class TestIsRetryableConnectionError:
    def test_connection_reset(self) -> None:
        assert is_retryable_connection_error(ConnectionResetError("kaboom")) is True

    def test_connection_aborted(self) -> None:
        assert is_retryable_connection_error(ConnectionAbortedError("bye")) is True

    def test_timeout(self) -> None:
        assert is_retryable_connection_error(TimeoutError("slow")) is True

    def test_value_error_not(self) -> None:
        assert is_retryable_connection_error(ValueError("nope")) is False


class TestIsRetryableCombined:
    def test_db_match(self) -> None:
        assert is_retryable(_make_op_error("40P01")) is True

    def test_connection_match(self) -> None:
        assert is_retryable(ConnectionResetError("x")) is True

    def test_unrelated_does_not_match(self) -> None:
        assert is_retryable(RuntimeError("x")) is False


class TestWithRetry:
    def test_returns_value_when_no_failure(self) -> None:
        fn = MagicMock(return_value=42)

        result = with_retry(fn, "a", k="b")

        assert result == 42
        fn.assert_called_once_with("a", k="b")

    def test_retries_on_retryable_then_succeeds(self) -> None:
        attempts = {"n": 0}

        def fn() -> str:
            attempts["n"] += 1
            if attempts["n"] < 3:
                raise _make_op_error("40P01")
            return "ok"

        with patch("protea.core.retry.time.sleep") as mock_sleep:
            result = with_retry(
                fn,
                policy=RetryPolicy(max_attempts=5, base_delay=0.01, jitter_ratio=0),
            )

        assert result == "ok"
        assert attempts["n"] == 3
        # Two sleeps: between attempt 1 and 2, between 2 and 3.
        assert mock_sleep.call_count == 2

    def test_propagates_non_retryable(self) -> None:
        fn = MagicMock(side_effect=ValueError("bad"))

        with patch("protea.core.retry.time.sleep") as mock_sleep:
            with pytest.raises(ValueError, match="bad"):
                with_retry(fn, policy=RetryPolicy(max_attempts=5))

        # Non-retryable exception means no sleep, no retry.
        assert fn.call_count == 1
        assert mock_sleep.call_count == 0

    def test_propagates_after_max_attempts(self) -> None:
        fn = MagicMock(side_effect=_make_op_error("40P01"))

        with patch("protea.core.retry.time.sleep") as mock_sleep:
            with pytest.raises(OperationalError):
                with_retry(
                    fn,
                    policy=RetryPolicy(max_attempts=3, base_delay=0.01, jitter_ratio=0),
                )

        assert fn.call_count == 3
        # Sleep happens only between attempts (so 2 sleeps for 3 attempts).
        assert mock_sleep.call_count == 2

    def test_exponential_backoff_no_jitter(self) -> None:
        fn = MagicMock(side_effect=_make_op_error("40P01"))

        with patch("protea.core.retry.time.sleep") as mock_sleep:
            with pytest.raises(OperationalError):
                with_retry(
                    fn,
                    policy=RetryPolicy(
                        max_attempts=4, base_delay=1.0, max_delay=30.0, jitter_ratio=0
                    ),
                )

        sleeps = [c.args[0] for c in mock_sleep.call_args_list]
        # attempt 1 -> sleep 1.0, attempt 2 -> 2.0, attempt 3 -> 4.0
        assert sleeps == [1.0, 2.0, 4.0]

    def test_max_delay_caps_backoff(self) -> None:
        fn = MagicMock(side_effect=_make_op_error("40P01"))

        with patch("protea.core.retry.time.sleep") as mock_sleep:
            with pytest.raises(OperationalError):
                with_retry(
                    fn,
                    policy=RetryPolicy(
                        max_attempts=8, base_delay=1.0, max_delay=5.0, jitter_ratio=0
                    ),
                )

        sleeps = [c.args[0] for c in mock_sleep.call_args_list]
        # 1, 2, 4, 5 (capped), 5, 5, 5
        assert sleeps == [1.0, 2.0, 4.0, 5.0, 5.0, 5.0, 5.0]

    def test_jitter_keeps_backoff_within_band(self) -> None:
        fn = MagicMock(side_effect=_make_op_error("40P01"))
        captured: list[float] = []

        with patch(
            "protea.core.retry.time.sleep", side_effect=lambda d: captured.append(d)
        ):
            with pytest.raises(OperationalError):
                with_retry(
                    fn,
                    policy=RetryPolicy(
                        max_attempts=4, base_delay=1.0, max_delay=30.0, jitter_ratio=0.5
                    ),
                )

        # First sleep base = 1.0; with jitter 0.5, range is [0.5, 1.5].
        assert 0.5 <= captured[0] <= 1.5
        # Second sleep base = 2.0; range [1.0, 3.0].
        assert 1.0 <= captured[1] <= 3.0

    def test_calls_on_retry_callback(self) -> None:
        events: list[tuple[int, str, float]] = []

        def cb(attempt: int, exc: BaseException, sleep_for: float) -> None:
            events.append((attempt, type(exc).__name__, sleep_for))

        fn = MagicMock(side_effect=[_make_op_error("40P01"), "ok"])

        with patch("protea.core.retry.time.sleep"):
            result = with_retry(
                fn,
                policy=RetryPolicy(
                    max_attempts=2, base_delay=0.01, jitter_ratio=0, on_retry=cb
                ),
            )

        assert result == "ok"
        assert len(events) == 1
        assert events[0][0] == 1
        assert events[0][1] == "OperationalError"

    def test_logs_warning_on_default_callback(self, caplog: pytest.LogCaptureFixture) -> None:
        fn = MagicMock(side_effect=[_make_op_error("40001"), "ok"])

        with caplog.at_level(logging.WARNING, logger="protea.retry"):
            with patch("protea.core.retry.time.sleep"):
                with_retry(
                    fn,
                    policy=RetryPolicy(max_attempts=2, base_delay=0.01, jitter_ratio=0),
                )

        relevant = [r for r in caplog.records if r.name == "protea.retry"]
        assert relevant, "expected at least one log under protea.retry"
        assert relevant[0].levelno == logging.WARNING
        assert relevant[0].pgcode == "40001"

    def test_max_attempts_zero_raises(self) -> None:
        fn = MagicMock()
        with pytest.raises(ValueError, match="max_attempts must be"):
            with_retry(fn, policy=RetryPolicy(max_attempts=0))

    def test_does_not_swallow_keyboard_interrupt(self) -> None:
        def fn() -> Any:
            raise KeyboardInterrupt

        with pytest.raises(KeyboardInterrupt):
            with_retry(fn, policy=RetryPolicy(max_attempts=3, base_delay=0.01))

    def test_default_policy_used_when_omitted(self) -> None:
        """Calling ``with_retry(fn)`` with no policy uses ``RetryPolicy()`` defaults."""
        fn = MagicMock(return_value="ok")
        # Default RetryPolicy has max_attempts=3, so a one-shot success
        # should still go through (no retries needed for non-failing fn).
        assert with_retry(fn) == "ok"
        fn.assert_called_once_with()
