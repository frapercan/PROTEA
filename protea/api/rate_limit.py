"""Per-endpoint rate limiting via :mod:`slowapi` (T5.6b).

Three POSTs are throttled out of the box:

* ``POST /jobs``                — 10/min, env ``PROTEA_RATELIMIT_JOBS``
* ``POST /auth/api-keys``       — 5/hour, env
  ``PROTEA_RATELIMIT_API_KEYS``
* ``POST /datasets``            — 5/min, env
  ``PROTEA_RATELIMIT_DATASETS``

Environment aware rate limiting
--------------------------------

In test/dev environments (``PROTEA_ENVIRONMENT=test|dev``), rate limits are
effectively disabled (set to 9999/hour) to allow integration tests and local
iteration without hitting quota walls. Production deployments should leave
``PROTEA_ENVIRONMENT`` unset or explicitly set it to "production".

Key function
------------

Every request is bucketed by:

1. The ``ApiKey.prefix`` if the caller authenticated with an API key
   (slowapi runs after the dep would store it on ``request.state``).
2. The ``sub`` of the Bearer JWT if authenticated that way.
3. The remote IP otherwise (unauthenticated requests still get a
   bucket so a flood of 401s does not amplify into an unbounded
   workload).

This keeps the buckets attributable: one misbehaving CI job does not
collide with another team's quota.

On 429
------

slowapi raises ``RateLimitExceeded`` which we map to a 429 problem
response carrying ``Retry-After``. The body inherits the same
``application/problem+json`` shape as the rest of the API.
"""

from __future__ import annotations

import logging
import os
from typing import Final

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from slowapi import Limiter
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from slowapi.util import get_remote_address

from protea.api.problem_details import PROBLEM_JSON_MEDIA_TYPE, ProblemDetail

logger = logging.getLogger(__name__)

# Env knob names + defaults. Keep them next to the limiter so an
# operator can grep for one term.
ENV_JOBS: Final = "PROTEA_RATELIMIT_JOBS"
ENV_API_KEYS: Final = "PROTEA_RATELIMIT_API_KEYS"
ENV_DATASETS: Final = "PROTEA_RATELIMIT_DATASETS"
ENV_ENVIRONMENT: Final = "PROTEA_ENVIRONMENT"

DEFAULT_JOBS: Final = "10/minute"
DEFAULT_API_KEYS: Final = "5/hour"
DEFAULT_DATASETS: Final = "5/minute"
# Test/dev disabled limit: high enough not to interfere with tests.
TEST_DISABLED_LIMIT: Final = "9999/hour"


def _is_test_env() -> bool:
    """Return True if running in test or dev environment."""
    env = os.getenv(ENV_ENVIRONMENT, "").strip().lower()
    return env in ("test", "dev")


def _resolve(env_var: str, default: str) -> str:
    """Return the operator override if set, else the slice default.

    If PROTEA_ENVIRONMENT is test or dev, return the disabled limit
    instead of the operator override or default.
    """
    if _is_test_env():
        return TEST_DISABLED_LIMIT
    value = os.getenv(env_var)
    return value.strip() if value and value.strip() else default


def jobs_limit() -> str:
    return _resolve(ENV_JOBS, DEFAULT_JOBS)


def api_keys_limit() -> str:
    return _resolve(ENV_API_KEYS, DEFAULT_API_KEYS)


def datasets_limit() -> str:
    return _resolve(ENV_DATASETS, DEFAULT_DATASETS)


def _principal_key(request: Request) -> str:
    """Return a stable key for the calling principal.

    We look at the headers ourselves rather than relying on the
    auth dep having run first; slowapi's middleware runs before the
    route's dependencies, so ``request.state`` is empty when the
    limiter consults this function. The prefix of an ``ApiKey``
    header (or the ``sub`` of the bearer payload) is plenty for
    bucketing; we are not authenticating here, only attributing.
    """
    auth = request.headers.get("authorization", "")
    x_api_key = request.headers.get("x-api-key")
    if x_api_key:
        return f"apikey:{x_api_key[:8]}"
    if auth.lower().startswith("apikey "):
        token = auth.split(None, 1)[1].strip()
        if token:
            return f"apikey:{token[:8]}"
    if auth.lower().startswith("bearer "):
        token = auth.split(None, 1)[1].strip()
        # Bucket on a stable prefix of the JWT so a single client's
        # rotated tokens still share a bucket within the limiter
        # window without us having to decode the payload here.
        if token:
            return f"bearer:{token[:16]}"
    return f"ip:{get_remote_address(request)}"


limiter = Limiter(key_func=_principal_key, headers_enabled=True)


def _rate_limit_handler(request: Request, exc: Exception) -> Response:
    """Format 429s as RFC 7807 problem responses with ``Retry-After``.

    Signature uses the broad ``Exception`` annotation because Starlette
    types exception handlers that way; the cast back to
    :class:`RateLimitExceeded` happens implicitly via slowapi's
    dispatch (this handler is only registered for that exception).
    """
    del request  # only needed by slowapi to inspect the route
    assert isinstance(exc, RateLimitExceeded)  # narrowing for the type checker
    detail = ProblemDetail(
        type="/problems/rate-limited",
        title="Too Many Requests",
        status=429,
        detail=f"Rate limit exceeded: {exc.detail}",
    )
    response = JSONResponse(
        status_code=429,
        content=detail.model_dump(exclude_none=True),
        media_type=PROBLEM_JSON_MEDIA_TYPE,
    )
    # slowapi attaches Retry-After via the middleware's response hook
    # for the normal flow; on our custom handler we mirror it from
    # the exception so curl users get a usable hint.
    retry_after = getattr(exc, "retry_after", None)
    if retry_after is not None:
        response.headers["Retry-After"] = str(retry_after)
    return response


def install_rate_limiter(app: FastAPI) -> None:
    """Wire the limiter + middleware + custom handler onto ``app``."""
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_handler)
    app.add_middleware(SlowAPIMiddleware)


__all__ = [
    "DEFAULT_API_KEYS",
    "DEFAULT_DATASETS",
    "DEFAULT_JOBS",
    "ENV_API_KEYS",
    "ENV_DATASETS",
    "ENV_ENVIRONMENT",
    "ENV_JOBS",
    "TEST_DISABLED_LIMIT",
    "api_keys_limit",
    "datasets_limit",
    "install_rate_limiter",
    "jobs_limit",
    "limiter",
]
