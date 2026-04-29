"""Tests for protea.core.annotation_intern."""

from __future__ import annotations

import pytest

from protea.core import annotation_intern
from protea.core.annotation_intern import intern_string, pool_size


@pytest.fixture(autouse=True)
def _clean_pool():
    """The intern pool is process-global; clear before every test so
    one test's allocations don't leak into another's pool-size assertion.
    """
    annotation_intern._INTERN_POOL.clear()
    yield
    annotation_intern._INTERN_POOL.clear()


class TestInternString:
    def test_first_call_caches_value(self):
        result = intern_string("IEA")
        assert result == "IEA"
        assert pool_size() == 1

    def test_second_call_returns_same_instance(self):
        # Build two distinct string objects with the same content.
        a = "I" + "E" + "A"
        b = "IEA" + ""
        # Use sys.intern? No — we want to verify our pool actually dedups.
        first = intern_string(a)
        second = intern_string(b)
        assert first is second  # SAME object, not just equal

    def test_none_passes_through(self):
        assert intern_string(None) is None
        assert pool_size() == 0

    def test_pool_grows_with_distinct_values(self):
        intern_string("IEA")
        intern_string("IDA")
        intern_string("EXP")
        assert pool_size() == 3

    def test_empty_string_is_interned(self):
        # Empty string is a legitimate qualifier value; not the same as None.
        result = intern_string("")
        assert result == ""
        assert pool_size() == 1

    def test_many_duplicates_dedup_to_one(self):
        for _ in range(100):
            intern_string("IEA")
        assert pool_size() == 1


class TestPoolSize:
    def test_starts_empty(self):
        assert pool_size() == 0

    def test_counts_distinct_only(self):
        intern_string("IEA")
        intern_string("IEA")
        intern_string("IDA")
        assert pool_size() == 2
