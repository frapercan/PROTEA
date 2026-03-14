"""
Unit tests for core contracts and simple operations.
No DB or network required.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests

from protea.core.contracts.operation import OperationResult
from protea.core.contracts.registry import OperationRegistry
from protea.core.operations.ping import PingOperation
from protea.core.utils import UniProtHttpMixin, chunks

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
# UniProtHttpMixin
# ---------------------------------------------------------------------------

def _make_payload(max_retries=3, backoff_base=0.01, backoff_max=0.1, jitter=0.0):
    p = MagicMock()
    p.user_agent = "PROTEA/test"
    p.timeout_seconds = 5
    p.max_retries = max_retries
    p.backoff_base_seconds = backoff_base
    p.backoff_max_seconds = backoff_max
    p.jitter_seconds = jitter
    return p


class _ConcreteHttp(UniProtHttpMixin):
    def __init__(self):
        self._http_requests = 0
        self._http_retries = 0
        self._http = MagicMock()


_noop_emit = lambda *_: None


class TestUniProtHttpMixin:
    def _obj(self) -> _ConcreteHttp:
        return _ConcreteHttp()

    def test_returns_response_on_200(self) -> None:
        obj = self._obj()
        resp = MagicMock()
        resp.status_code = 200
        obj._http.get.return_value = resp
        result = obj._get_with_retries("http://x", _make_payload(), _noop_emit)
        assert result is resp

    def test_retries_on_429(self) -> None:
        obj = self._obj()
        bad = MagicMock(); bad.status_code = 429
        bad.headers = {}
        good = MagicMock(); good.status_code = 200
        obj._http.get.side_effect = [bad, good]
        with patch("protea.core.utils.time.sleep"):
            result = obj._get_with_retries("http://x", _make_payload(), _noop_emit)
        assert result is good
        assert obj._http_retries == 1

    def test_uses_retry_after_header(self) -> None:
        obj = self._obj()
        bad = MagicMock(); bad.status_code = 429
        bad.headers = {"Retry-After": "5"}
        good = MagicMock(); good.status_code = 200
        obj._http.get.side_effect = [bad, good]
        sleep_calls = []
        with patch("protea.core.utils.time.sleep", side_effect=sleep_calls.append):
            obj._get_with_retries("http://x", _make_payload(backoff_max=30.0), _noop_emit)
        assert len(sleep_calls) == 1
        assert sleep_calls[0] == pytest.approx(5.0)

    def test_raises_after_max_retries(self) -> None:
        obj = self._obj()
        bad = MagicMock(); bad.status_code = 503
        bad.headers = {}
        bad.raise_for_status.side_effect = requests.HTTPError("503")
        obj._http.get.return_value = bad
        with patch("protea.core.utils.time.sleep"):
            with pytest.raises(requests.HTTPError):
                obj._get_with_retries("http://x", _make_payload(max_retries=2), _noop_emit)

    def test_retries_on_network_exception(self) -> None:
        obj = self._obj()
        good = MagicMock(); good.status_code = 200
        obj._http.get.side_effect = [requests.ConnectionError("down"), good]
        with patch("protea.core.utils.time.sleep"):
            result = obj._get_with_retries("http://x", _make_payload(), _noop_emit)
        assert result is good

    def test_extract_next_cursor_present(self) -> None:
        obj = self._obj()
        header = '<https://rest.uniprot.org/uniprotkb/search?cursor=ABCD1234>; rel="next"'
        assert obj._extract_next_cursor(header) == "ABCD1234"

    def test_extract_next_cursor_absent(self) -> None:
        obj = self._obj()
        assert obj._extract_next_cursor("") is None
        assert obj._extract_next_cursor('<http://x>; rel="prev"') is None

    def test_extract_next_cursor_no_cursor_param(self) -> None:
        obj = self._obj()
        assert obj._extract_next_cursor('<http://x?page=2>; rel="next"') is None
