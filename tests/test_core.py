"""
Unit tests for core contracts and simple operations.
No DB or network required.
"""

from __future__ import annotations

import pytest

from protea.core.contracts.operation import OperationResult, RetryLaterError
from protea.core.contracts.registry import OperationRegistry
from protea.core.evidence_codes import ECO_TO_CODE, EXPERIMENTAL, is_experimental, normalize
from protea.core.operations.ping import PingOperation
from protea.core.utils import chunks

# ---------------------------------------------------------------------------
# OperationRegistry
# ---------------------------------------------------------------------------


class TestOperationRegistry:
    def test_register_and_get(self):
        reg = OperationRegistry()
        op = PingOperation()
        reg.register(op)
        assert reg.get("ping") is op

    def test_duplicate_register_raises(self):
        reg = OperationRegistry()
        reg.register(PingOperation())
        with pytest.raises(ValueError, match="already registered"):
            reg.register(PingOperation())

    def test_get_unknown_raises(self):
        reg = OperationRegistry()
        with pytest.raises(KeyError, match="Unknown operation"):
            reg.get("does_not_exist")


# ---------------------------------------------------------------------------
# PingOperation
# ---------------------------------------------------------------------------


class TestPingOperation:
    def setup_method(self):
        self.op = PingOperation()

    def test_name(self):
        assert self.op.name == "ping"

    def test_execute_returns_operation_result(self):
        events = []

        def emit(event, message, fields, level):
            events.append(event)

        result = self.op.execute(session=None, payload={}, emit=emit)
        assert isinstance(result, OperationResult)
        assert result.result == {"ok": True}
        assert "ping.start" in events
        assert "ping.done" in events

    def test_execute_emits_info_level(self):
        levels = []

        def emit(event, message, fields, level):
            levels.append(level)

        self.op.execute(session=None, payload={}, emit=emit)
        assert all(lv == "info" for lv in levels)


# ---------------------------------------------------------------------------
# chunks()
# ---------------------------------------------------------------------------


class TestChunks:
    def test_even_split(self) -> None:
        result = list(chunks([1, 2, 3, 4], 2))
        assert result == [[1, 2], [3, 4]]

    def test_remainder(self) -> None:
        result = list(chunks([1, 2, 3, 4, 5], 2))
        assert result == [[1, 2], [3, 4], [5]]

    def test_chunk_larger_than_seq(self) -> None:
        result = list(chunks([1, 2], 10))
        assert result == [[1, 2]]

    def test_empty_seq(self) -> None:
        assert list(chunks([], 5)) == []


# ---------------------------------------------------------------------------
# evidence_codes — normalize and is_experimental
# ---------------------------------------------------------------------------


class TestNormalize:
    def test_go_code_passthrough(self):
        assert normalize("IDA") == "IDA"

    def test_eco_id_to_go_code(self):
        assert normalize("ECO:0000314") == "IDA"
        assert normalize("ECO:0000501") == "IEA"

    def test_unknown_code_passthrough(self):
        assert normalize("UNKNOWN_CODE") == "UNKNOWN_CODE"

    def test_all_eco_ids_resolve(self):
        for eco, expected in ECO_TO_CODE.items():
            assert normalize(eco) == expected


class TestIsExperimental:
    def test_experimental_go_code(self):
        for code in EXPERIMENTAL:
            assert is_experimental(code) is True

    def test_non_experimental_code(self):
        assert is_experimental("IEA") is False
        assert is_experimental("ISS") is False
        assert is_experimental("ND") is False

    def test_eco_experimental(self):
        # ECO:0000314 → IDA → experimental
        assert is_experimental("ECO:0000314") is True

    def test_eco_non_experimental(self):
        # ECO:0000501 → IEA → not experimental
        assert is_experimental("ECO:0000501") is False

    def test_unknown_code_not_experimental(self):
        assert is_experimental("BADCODE") is False


# ---------------------------------------------------------------------------
# RetryLaterError
# ---------------------------------------------------------------------------


class TestRetryLaterError:
    def test_default_delay(self):
        err = RetryLaterError("GPU busy")
        assert err.delay_seconds == 60
        assert str(err) == "GPU busy"

    def test_custom_delay(self):
        err = RetryLaterError("busy", delay_seconds=120)
        assert err.delay_seconds == 120

    def test_is_exception(self):
        with pytest.raises(RetryLaterError):
            raise RetryLaterError("test")


