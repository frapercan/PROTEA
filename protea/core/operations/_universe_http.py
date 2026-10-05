"""HTTP transport for :mod:`ensure_goa_universe`, with the retries UniProt needs.

Separated from the operation because it is transport, not a decision about the
universe: the operation chooses WHICH accessions to ask for, this module only
gets them over the wire and decides which failures are worth waiting out.

The distinction that matters here is between a failure that says something about
the request and one that says nothing. A 400 means the query is malformed and no
amount of waiting will fix it. A 503 from UniProt's cache means its backend
hiccuped, and the same request a few seconds later succeeds.

MEASURED 2026-10-06: release 231's universe pass died on a 503 from the
``sec_acc`` search AFTER its batch phase had already finished, throwing away a
quarter of an hour. The driver counted the release as failed and moved on,
leaving a hole in a union that phase 1 exists to make complete. Over 75 passes,
each making hundreds of requests, one transient failure is not a possibility to
design around but a certainty.
"""

from __future__ import annotations

import random
import time
from typing import Any
from urllib import error, request

from protea.core.contracts.operation import EmitFn

#: Statuses UniProt fails transiently with. It sits behind Varnish, which
#: answers 503 "Backend fetch failed" when its own backend hiccups, 502 and 504
#: in the same family, and 429 when a client goes too fast. None of them says
#: anything about the request itself.
RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})
#: Six attempts over roughly two minutes of backoff. Long enough to ride out a
#: cache restart, short enough that a genuinely dead endpoint still fails the
#: job instead of hanging the campaign behind it.
ATTEMPTS = 6
BACKOFF_BASE_S = 2.0
BACKOFF_CAP_S = 60.0

_UA = "PROTEA/ensure_goa_universe"


def backoff(attempt: int) -> float:
    """Exponential, capped. ``attempt`` is 1-based."""
    return min(BACKOFF_BASE_S * (2 ** (attempt - 1)), BACKOFF_CAP_S)


def retry_after(exc: Any) -> float | None:
    """UniProt's own ``Retry-After``, in seconds, when it sends one."""
    cabeceras = getattr(exc, "headers", None)
    raw = cabeceras.get("Retry-After") if cabeceras is not None else None
    if not raw:
        return None
    try:
        return min(float(raw), BACKOFF_CAP_S)
    except (TypeError, ValueError):
        return None


def get(
    url: str,
    *,
    label: str,
    timeout: int,
    emit: EmitFn,
    accept: str | None = None,
) -> str:
    """GET ``url``, retrying only the failures that say nothing about it.

    Retrying is NOT a licence to carry on. When the attempts run out this still
    raises, because a batch that failed must never be counted as a batch that
    found nothing -- an earlier measurement reported 0% recoverable merges
    because ten batches had 400'd and the failures were tallied as zeroes. The
    retry shortens the odds of a transient failure; it never converts one into
    an empty answer.

    :raises RuntimeError: on a non-transient status, on exhausted attempts, and
        on an unreachable host, carrying UniProt's own message where there is one.
    """
    headers = {"User-Agent": _UA}
    if accept:
        headers["Accept"] = accept
    req = request.Request(url, headers=headers)

    for attempt in range(1, ATTEMPTS + 1):
        try:
            with request.urlopen(req, timeout=timeout) as resp:
                return resp.read().decode("utf-8")  # type: ignore[no-any-return]
        except error.HTTPError as exc:
            if exc.code not in RETRYABLE_STATUS or attempt == ATTEMPTS:
                body = exc.read().decode("utf-8", "replace")[:300]
                raise RuntimeError(f"UniProt {label} {exc.code}: {body}") from exc
            motivo = f"http_{exc.code}"
            espera = retry_after(exc) or backoff(attempt)
        except error.URLError as exc:
            if attempt == ATTEMPTS:
                raise RuntimeError(f"UniProt {label} unreachable: {exc.reason}") from exc
            motivo = f"urlerror:{exc.reason}"
            espera = backoff(attempt)
        espera += random.uniform(0, 0.5)  # noqa: S311 - spreading load, not crypto
        emit(
            "ensure_goa_universe.http_retry",
            None,
            {
                "label": label,
                "attempt": attempt,
                "of": ATTEMPTS,
                "reason": motivo,
                "sleep_seconds": round(espera, 2),
            },
            "warning",
        )
        time.sleep(espera)
    raise AssertionError("unreachable: the loop either returns or raises")
