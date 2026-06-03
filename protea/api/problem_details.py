"""RFC 7807 ``application/problem+json`` error responses (T4.4).

Installs FastAPI exception handlers that convert the framework's default
JSON error bodies (``{"detail": ...}``) into the canonical RFC 7807 shape
used by every modern HTTP API. Existing route code keeps raising
``HTTPException`` exactly as before — this module only changes how the
responses look on the wire.

RFC 7807 fields
---------------

* ``type``    — URI reference identifying the problem class. We use
                relative paths under ``/problems/{slug}`` so the docs
                site can host human-readable descriptions per slug.
* ``title``   — short, human-readable summary, stable across responses
                of the same type.
* ``status``  — HTTP status code (mirrors the response status).
* ``detail``  — long-form explanation specific to this occurrence.
* ``instance`` — relative URI of the request that produced the problem.
"""
from __future__ import annotations

from http import HTTPStatus
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from starlette.exceptions import HTTPException as StarletteHTTPException

PROBLEM_JSON_MEDIA_TYPE = "application/problem+json"

_PROBLEM_TYPE_PREFIX = "/problems"


class ProblemDetail(BaseModel):
    """Pydantic model for an RFC 7807 problem-details payload."""

    type: str = Field(
        ...,
        description=(
            "URI reference identifying the problem class. Relative URIs "
            "resolve under the API host; use ``about:blank`` for generic."
        ),
    )
    title: str = Field(..., description="Short, stable summary of the problem.")
    status: int = Field(
        ...,
        ge=100,
        le=599,
        description=(
            "HTTP status code repeated in the body so the payload is "
            "self-contained for clients that drop the status line."
        ),
    )
    detail: str | None = Field(
        default=None,
        description=(
            "Human-readable explanation of this specific occurrence, "
            "distinct from the generic ``title``."
        ),
    )
    instance: str | None = Field(
        default=None,
        description=(
            "URI of the specific occurrence (typically the request "
            "path); helps correlate with logs."
        ),
    )


def _slugify_status(status: int) -> str:
    """Map a status code to a URL slug. Falls back to ``error-<n>`` for
    unknown codes so the type URI is always derivable."""
    try:
        phrase = HTTPStatus(status).phrase
    except ValueError:
        return f"error-{status}"
    return phrase.lower().replace(" ", "-").replace("'", "")


def _problem_response(
    request: Request,
    *,
    status: int,
    detail: Any,
    title: str | None = None,
    type_uri: str | None = None,
    extra: dict[str, Any] | None = None,
) -> JSONResponse:
    """Build a single ``application/problem+json`` response.

    ``extra`` lets callers attach problem-specific fields (e.g. the
    ``errors`` list from ``RequestValidationError``) without hard-coding
    them in every handler.
    """
    if title is None:
        try:
            title = HTTPStatus(status).phrase
        except ValueError:
            title = "Error"
    if type_uri is None:
        type_uri = f"{_PROBLEM_TYPE_PREFIX}/{_slugify_status(status)}"
    body: dict[str, Any] = {
        "type": type_uri,
        "title": title,
        "status": status,
        "detail": detail if isinstance(detail, str) else str(detail),
        "instance": str(request.url.path),
    }
    if extra:
        body.update(extra)
    return JSONResponse(
        status_code=status,
        content=body,
        media_type=PROBLEM_JSON_MEDIA_TYPE,
    )


def register_problem_handlers(app: FastAPI) -> None:
    """Install RFC 7807 handlers for the standard FastAPI error paths.

    Three handlers cover every framework-emitted error:

    1. ``StarletteHTTPException`` — covers FastAPI's ``HTTPException``
       (which subclasses Starlette's). Handles 4xx/5xx raised from
       route bodies + dependency callables.
    2. ``RequestValidationError`` — Pydantic body / query / path
       validation failures (HTTP 422). The original FastAPI body
       lives under ``errors`` so clients can still drill into the
       per-field details.
    3. ``Exception`` — catch-all so unhandled crashes still produce a
       structured 500 instead of an HTML traceback. ``detail`` is
       intentionally generic: the full traceback is logged via the
       framework's normal error path; the wire surface stays opaque.
    """

    @app.exception_handler(StarletteHTTPException)
    async def _http_exception_handler(
        request: Request, exc: StarletteHTTPException
    ) -> JSONResponse:
        return _problem_response(request, status=exc.status_code, detail=exc.detail)

    @app.exception_handler(RequestValidationError)
    async def _validation_exception_handler(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        return _problem_response(
            request,
            status=422,
            title="Unprocessable Entity",
            detail="Request payload failed validation",
            extra={"errors": exc.errors()},
        )

    @app.exception_handler(Exception)
    async def _unhandled_exception_handler(
        request: Request, exc: Exception
    ) -> JSONResponse:
        return _problem_response(
            request,
            status=500,
            title="Internal Server Error",
            detail="An unexpected error occurred while handling the request.",
        )


def install_problem_openapi_schema(app: FastAPI) -> None:
    """Document RFC 7807 as the default 4xx/5xx schema in the OpenAPI spec.

    FastAPI's stock OpenAPI generator points 4xx/5xx responses at
    ``application/json`` with ``HTTPValidationError`` (or nothing). This
    hook patches every operation so every 4xx/5xx response advertises
    ``application/problem+json`` referencing :class:`ProblemDetail`,
    matching what the runtime handlers (installed by
    :func:`register_problem_handlers`) actually emit.

    The hook lives behind ``app.openapi`` (FastAPI's documented override
    point) so re-runs return the cached schema. Idempotent: an existing
    ``application/problem+json`` content entry on a given operation is
    left in place so endpoints can still document a more specific error
    shape later.
    """
    schema_ref = "#/components/schemas/ProblemDetail"
    problem_content = {"schema": {"$ref": schema_ref}}

    original_openapi = app.openapi

    def _patched_openapi() -> dict[str, Any]:
        if app.openapi_schema:
            return app.openapi_schema

        spec = original_openapi()
        components = spec.setdefault("components", {})
        schemas = components.setdefault("schemas", {})
        if "ProblemDetail" not in schemas:
            schemas["ProblemDetail"] = ProblemDetail.model_json_schema()

        for path_item in spec.get("paths", {}).values():
            if not isinstance(path_item, dict):
                continue
            for method, operation in path_item.items():
                if method == "parameters" or not isinstance(operation, dict):
                    continue
                responses = operation.get("responses", {})
                for status_code, response in list(responses.items()):
                    if not status_code.startswith(("4", "5")):
                        continue
                    if not isinstance(response, dict):
                        continue
                    content = response.setdefault("content", {})
                    content.setdefault(
                        "application/problem+json", dict(problem_content)
                    )
        app.openapi_schema = spec
        return spec

    app.openapi = _patched_openapi  # type: ignore[method-assign]


__all__ = [
    "PROBLEM_JSON_MEDIA_TYPE",
    "ProblemDetail",
    "install_problem_openapi_schema",
    "register_problem_handlers",
]
