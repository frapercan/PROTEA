"""Anonymous visitor counting middleware.

Records one row per user-visible request into the ``visitor_event`` table so
that Grafana (or any SQL client) can compute "unique visitors per day" and
similar aggregate traffic metrics without storing IP addresses or using
cookies.

Privacy design
--------------
The client IP is never persisted. Instead, we compute a short hash:

    visitor_hash = sha256(daily_salt || client_ip)[:16]

where ``daily_salt`` is a 32-byte random value held only in process memory
and rotated on every calendar day (UTC). When the day rolls over the old
salt is discarded, so cross-day correlation becomes cryptographically
infeasible — the same "rotating salt" approach used by Plausible and Fathom.

Noise filters
-------------
The middleware is deliberately narrow in scope: it only counts requests that
represent actual user navigation. It skips assets, polling endpoints, health
probes and metrics scrapes. See ``_should_record``.
"""

from __future__ import annotations

import hashlib
import logging
import secrets
import threading
from datetime import date, datetime, timezone

from sqlalchemy.exc import SQLAlchemyError
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.types import ASGIApp

from protea.infrastructure.orm.models import VisitorEvent
from protea.infrastructure.session import session_scope

logger = logging.getLogger(__name__)

# Paths that are polling/asset/infra and should never count toward visits.
# Prefix match, compared with the request path *after* stripping the ASGI
# root_path so "/api-proxy/health" and "/health" both normalise to "/health".
_IGNORED_PREFIXES: tuple[str, ...] = (
    "/_next/",
    "/static/",
    "/favicon",
    "/metrics",
    "/health",
    "/sphinx/",
    # Job polling (the biggest source of request volume).
    "/jobs",  # GET /jobs, /jobs/{id}, /jobs/{id}/events
)

# Methods that represent passive reads we want to count. POST/PUT/DELETE are
# mutations that also matter, but counting them doesn't change the "unique
# visitors" signal meaningfully, so we keep the table small by only logging
# GET. Flip this set if you want full request auditing instead.
_COUNTED_METHODS: frozenset[str] = frozenset({"GET"})


class _DailySalt:
    """Thread-safe rotating salt keyed by UTC date."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._day: date | None = None
        self._salt: bytes = b""

    def get(self) -> bytes:
        today = datetime.now(timezone.utc).date()
        with self._lock:
            if today != self._day:
                self._day = today
                self._salt = secrets.token_bytes(32)
            return self._salt


_salt = _DailySalt()


def _client_ip(request: Request) -> str:
    """Extract the real client IP, honouring proxy headers.

    Order of precedence matches what ngrok / Cloudflare / Traefik set:
    1. ``X-Forwarded-For`` (first entry, which is the original client)
    2. ``X-Real-IP``
    3. Starlette's ``request.client.host`` (falls back to the direct peer)
    """
    xff = request.headers.get("x-forwarded-for")
    if xff:
        # Comma-separated list: client, proxy1, proxy2, ...
        return xff.split(",")[0].strip()
    xri = request.headers.get("x-real-ip")
    if xri:
        return xri.strip()
    if request.client:
        return request.client.host
    return "unknown"


def _normalised_path(request: Request) -> str:
    """Return the request path with any proxy/root prefix stripped.

    uvicorn is launched with ``--root-path /api-proxy`` but when Next.js
    rewrites ``/api-proxy/foo`` to the upstream it forwards the full path as
    ``/api-proxy/foo``, and ASGI ``scope["root_path"]`` is not always
    populated for those requests. We therefore consult three possible
    sources, in order: ``scope["root_path"]``, the FastAPI app's own
    ``root_path`` attribute, and as a last resort a literal ``/api-proxy``
    prefix.
    """
    path = request.url.path
    candidates = [
        request.scope.get("root_path", "") or "",
        getattr(request.app, "root_path", "") or "",
        "/api-proxy",
    ]
    for root in candidates:
        if root and path.startswith(root):
            stripped = path[len(root):]
            return stripped or "/"
    return path


def _should_record(request: Request) -> bool:
    if request.method not in _COUNTED_METHODS:
        return False
    path = _normalised_path(request)
    for prefix in _IGNORED_PREFIXES:
        if path.startswith(prefix):
            return False
    return True


def _visitor_hash(ip: str) -> str:
    return hashlib.sha256(_salt.get() + ip.encode("utf-8")).hexdigest()[:16]


def _truncate_path(path: str, max_len: int = 255) -> str:
    if len(path) <= max_len:
        return path
    return path[: max_len - 3] + "..."


class VisitorCounterMiddleware(BaseHTTPMiddleware):
    """Writes one ``VisitorEvent`` row per recorded request.

    The session factory is read from ``app.state.session_factory`` — set by
    ``create_app()`` at startup. If the factory isn't present (e.g. during
    tests that instantiate a bare FastAPI app), the middleware degrades to
    a no-op so it never breaks the request.
    """

    def __init__(self, app: ASGIApp) -> None:
        super().__init__(app)

    async def dispatch(self, request, call_next):
        response = await call_next(request)

        if not _should_record(request):
            return response

        factory = getattr(request.app.state, "session_factory", None)
        if factory is None:
            return response

        try:
            ip = _client_ip(request)
            event = VisitorEvent(
                day=datetime.now(timezone.utc).date(),
                visitor_hash=_visitor_hash(ip),
                path=_truncate_path(_normalised_path(request)),
                method=request.method,
                status=response.status_code,
            )
            with session_scope(factory) as session:
                session.add(event)
        except SQLAlchemyError as exc:
            # Never let analytics break the real response.
            logger.warning("visitor_counter insert failed: %s", exc)
        except Exception as exc:  # pragma: no cover — defensive
            logger.warning("visitor_counter unexpected error: %s", exc)

        return response
