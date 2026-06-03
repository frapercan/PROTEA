"""Tests for the RFC 7807 problem-details handlers (T4.4)."""
from __future__ import annotations

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from pydantic import BaseModel

from protea.api.problem_details import (
    PROBLEM_JSON_MEDIA_TYPE,
    install_problem_openapi_schema,
    register_problem_handlers,
)


class _ValidatedBody(BaseModel):
    """Module-level Pydantic model so FastAPI auto-detects it as Body."""

    name: str


def _make_app() -> FastAPI:
    app = FastAPI()
    register_problem_handlers(app)

    @app.get("/raise/{status}")
    def _raise(status: int) -> dict[str, str]:
        raise HTTPException(status_code=status, detail=f"manual {status}")

    @app.post("/validated")
    def _validated(body: _ValidatedBody) -> dict[str, str]:
        return {"name": body.name}

    @app.get("/boom")
    def _boom() -> dict[str, str]:
        raise RuntimeError("kaboom")

    return app


class TestProblemDetailsHTTPException:
    def test_404_returns_problem_json(self) -> None:
        app = _make_app()
        client = TestClient(app, raise_server_exceptions=False)
        resp = client.get("/raise/404")
        assert resp.status_code == 404
        assert resp.headers["content-type"].startswith(PROBLEM_JSON_MEDIA_TYPE)
        body = resp.json()
        assert body["status"] == 404
        assert body["title"] == "Not Found"
        assert body["detail"] == "manual 404"
        assert body["type"] == "/problems/not-found"
        assert body["instance"] == "/raise/404"

    def test_409_uses_status_phrase(self) -> None:
        app = _make_app()
        client = TestClient(app, raise_server_exceptions=False)
        resp = client.get("/raise/409")
        assert resp.status_code == 409
        body = resp.json()
        assert body["title"] == "Conflict"
        assert body["type"] == "/problems/conflict"

    def test_unknown_status_falls_back_to_error_slug(self) -> None:
        app = _make_app()
        client = TestClient(app, raise_server_exceptions=False)
        resp = client.get("/raise/418")
        body = resp.json()
        # 418 is "I'm a Teapot" since RFC 2324; HTTPStatus knows it on
        # 3.12. Either way the slug is derived from the phrase, so the
        # exact value depends on stdlib. Just assert the prefix.
        assert body["type"].startswith("/problems/")


class TestProblemDetailsValidation:
    def test_422_includes_errors_list(self) -> None:
        app = _make_app()
        client = TestClient(app, raise_server_exceptions=False)
        resp = client.post("/validated", json={})
        assert resp.status_code == 422
        assert resp.headers["content-type"].startswith(PROBLEM_JSON_MEDIA_TYPE)
        body = resp.json()
        assert body["status"] == 422
        assert body["title"] == "Unprocessable Entity"
        assert body["type"] == "/problems/unprocessable-entity"
        assert "errors" in body
        assert isinstance(body["errors"], list)
        assert len(body["errors"]) >= 1
        # Each error keeps Pydantic's structured shape so the UI can
        # still drill into ``loc`` / ``msg`` / ``type`` per-field.
        first = body["errors"][0]
        assert "loc" in first and "msg" in first and "type" in first
        # The ``body`` location is always present at the head of the loc
        # path for body validation errors; nested keys (``["body",
        # "name"]``) appear when Pydantic resolves the inner schema.
        assert first["loc"][0] == "body"


class TestProblemDetailsUnhandled:
    def test_500_response_is_generic(self) -> None:
        app = _make_app()
        client = TestClient(app, raise_server_exceptions=False)
        resp = client.get("/boom")
        assert resp.status_code == 500
        body = resp.json()
        assert body["status"] == 500
        assert body["title"] == "Internal Server Error"
        # Generic detail — no traceback bleed-through to the wire.
        assert "unexpected error" in body["detail"].lower()
        assert body["type"] == "/problems/internal-server-error"


class TestInstallProblemOpenapiSchema:
    """T4.3 — OpenAPI advertises ``application/problem+json`` for 4xx/5xx."""

    def _patched_app(self) -> FastAPI:
        app = _make_app()
        install_problem_openapi_schema(app)
        return app

    def test_problemdetail_schema_in_components(self) -> None:
        spec = self._patched_app().openapi()
        assert "ProblemDetail" in spec["components"]["schemas"]
        # Pydantic-generated schema preserves the canonical fields.
        problem_schema = spec["components"]["schemas"]["ProblemDetail"]
        assert {"type", "title", "status"} <= set(problem_schema["properties"])

    def test_4xx_5xx_responses_advertise_problem_json(self) -> None:
        spec = self._patched_app().openapi()
        # The /raise/{status} route documents 200; 4xx come from the
        # framework's auto-generated 422 (path validation) so we expect
        # at least one 4xx response per route to carry the schema ref.
        any_problem = False
        for path_item in spec["paths"].values():
            for method, op in path_item.items():
                if method == "parameters":
                    continue
                for code, resp in op.get("responses", {}).items():
                    if not code.startswith(("4", "5")):
                        continue
                    content = resp.get("content", {})
                    if "application/problem+json" in content:
                        any_problem = True
                        ref = content["application/problem+json"]["schema"]["$ref"]
                        assert ref == "#/components/schemas/ProblemDetail"
        assert any_problem, "No 4xx/5xx response carried application/problem+json"

    def test_idempotent_on_repeat_call(self) -> None:
        # Calling ``app.openapi()`` twice yields the same dict (cached).
        app = self._patched_app()
        first = app.openapi()
        second = app.openapi()
        assert first is second

    def test_existing_problem_content_left_alone(self) -> None:
        """If a future endpoint declares its own ``application/problem+json``
        schema, our hook must not overwrite it."""
        app = FastAPI()

        @app.get(
            "/specific",
            responses={
                "404": {
                    "content": {
                        "application/problem+json": {
                            "schema": {"$ref": "#/components/schemas/CustomProblem"}
                        }
                    }
                }
            },
        )
        def _ep() -> dict[str, str]:
            return {"ok": "1"}

        install_problem_openapi_schema(app)
        spec = app.openapi()
        ref = (
            spec["paths"]["/specific"]["get"]["responses"]["404"]
            ["content"]["application/problem+json"]["schema"]["$ref"]
        )
        assert ref == "#/components/schemas/CustomProblem"
