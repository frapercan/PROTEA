from __future__ import annotations

import random
import time
from collections.abc import Iterable
from collections.abc import Sequence as Seq
from datetime import UTC, datetime
from typing import Any, Protocol

import requests
from requests import Response

from protea.core.contracts.operation import EmitFn


def utcnow() -> datetime:
    """Return the current UTC datetime (timezone-aware)."""
    return datetime.now(UTC)


def chunks(seq: Seq[Any], n: int) -> Iterable[Seq[Any]]:
    """Yield successive n-sized chunks from seq."""
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


class _HttpPayload(Protocol):
    """Structural type for payloads that carry HTTP retry parameters."""

    user_agent: str
    timeout_seconds: int
    max_retries: int
    backoff_base_seconds: float
    backoff_max_seconds: float
    jitter_seconds: float


class UniProtHttpClient:
    """Composable HTTP client with retries, used by UniProt REST operations.

    Replaces the historical ``UniProtHttpMixin`` (favours composition
    over inheritance). Operations instantiate one client per execution
    and read counters from ``client.requests`` / ``client.retries``.

    Example::

        class InsertProteinsOperation:
            def __init__(self) -> None:
                self._http_client = UniProtHttpClient()

            def execute(self, session, payload, *, emit):
                self._http_client.reset()
                ...
                resp = self._http_client.get_with_retries(url, p, emit)
                cursor = self._http_client.extract_next_cursor(link)
    """

    def __init__(self) -> None:
        self.session: requests.Session = requests.Session()
        self.requests: int = 0
        self.retries: int = 0

    def reset(self) -> None:
        """Reset request/retry counters before a new execution.

        The underlying ``requests.Session`` is reused across executions
        so connection pooling stays effective.
        """
        self.requests = 0
        self.retries = 0

    def get_with_retries(self, url: str, p: _HttpPayload, emit: EmitFn) -> Response:
        headers = {"User-Agent": p.user_agent}
        attempt = 0
        while True:
            attempt += 1
            self.requests += 1
            try:
                resp = self.session.get(url, timeout=p.timeout_seconds, headers=headers)
            except requests.RequestException as e:
                if attempt > p.max_retries:
                    raise
                self.retries += 1
                self._sleep_backoff(
                    p, attempt, emit, reason=f"request_exception:{e.__class__.__name__}"
                )
                continue

            if 200 <= resp.status_code < 300:
                return resp

            if resp.status_code in (429, 500, 502, 503, 504):
                if attempt > p.max_retries:
                    resp.raise_for_status()
                self.retries += 1
                retry_after = resp.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    wait_s = min(float(retry_after), p.backoff_max_seconds)
                    emit(
                        "http.retry",
                        None,
                        {"attempt": attempt, "wait_seconds": wait_s, "reason": "retry_after"},
                        "warning",
                    )
                    time.sleep(wait_s)
                else:
                    self._sleep_backoff(p, attempt, emit, reason=f"status_{resp.status_code}")
                continue

            resp.raise_for_status()

    def _sleep_backoff(self, p: _HttpPayload, attempt: int, emit: EmitFn, reason: str) -> None:
        base = p.backoff_base_seconds * (2 ** (attempt - 1))
        wait_s = min(base, p.backoff_max_seconds) + random.uniform(0.0, p.jitter_seconds)
        emit(
            "http.retry",
            None,
            {"attempt": attempt, "wait_seconds": wait_s, "reason": reason},
            "warning",
        )
        time.sleep(wait_s)

    @staticmethod
    def extract_next_cursor(link_header: str) -> str | None:
        if not link_header or 'rel="next"' not in link_header or "cursor=" not in link_header:
            return None
        try:
            return link_header.split("cursor=")[-1].split(">")[0]
        except Exception:
            return None
