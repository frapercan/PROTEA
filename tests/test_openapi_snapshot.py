"""Structural validation of the committed ``docs/openapi.json`` snapshot.

T4.5 of master plan §24 Fase 4 specifies "Schemathesis CI" — full
property-based fuzzing requires a running app + DB and lands when the
deployment story is wired up. As a partial close we run a zero-dep
structural validator here so the test suite catches malformed specs
the moment they get committed.

Invariants asserted on every commit:

* The snapshot is well-formed JSON with the required top-level OpenAPI
  fields (``openapi``, ``info``, ``paths``).
* Every public route lives under the canonical ``/v1/`` prefix (or is
  one of the explicit root-level health endpoints we register
  separately).
* Every ``$ref`` inside ``paths`` and ``components`` resolves to a
  ``components/schemas`` entry that actually exists in the same
  document — a cheap proxy for "schemathesis would not crash on load".
* Every operation declares at least one response (FastAPI generates
  these automatically from the route signature; if the snapshot is
  missing them, something has gone seriously sideways upstream).
* The ``ProblemDetail`` schema (T4.3 / T4.4) is present.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SNAPSHOT_PATH = _REPO_ROOT / "docs" / "openapi.json"

_ROOT_LEVEL_PATHS = {"/health", "/health/ready"}
_REF_RE = re.compile(r"^#/components/schemas/(.+)$")
_OPERATION_KEYS = {"get", "post", "put", "patch", "delete", "options", "head"}


@pytest.fixture(scope="module")
def spec() -> dict[str, Any]:
    """Load the committed snapshot once per module."""
    if not _SNAPSHOT_PATH.exists():
        pytest.skip(f"OpenAPI snapshot missing at {_SNAPSHOT_PATH}")
    return json.loads(_SNAPSHOT_PATH.read_text(encoding="utf-8"))


class TestSnapshotShape:
    def test_top_level_fields_present(self, spec: dict[str, Any]) -> None:
        assert "openapi" in spec, "missing top-level 'openapi' field"
        assert spec["openapi"].startswith("3."), "expected OpenAPI 3.x"
        assert "info" in spec
        assert {"title", "version"} <= set(spec["info"])
        assert "paths" in spec and isinstance(spec["paths"], dict)
        assert spec["paths"], "no paths in the snapshot"

    def test_components_schemas_present(self, spec: dict[str, Any]) -> None:
        components = spec.get("components", {})
        schemas = components.get("schemas", {})
        assert isinstance(schemas, dict)
        assert "ProblemDetail" in schemas, (
            "ProblemDetail schema missing — T4.3 install_problem_openapi_schema "
            "should have injected it"
        )


class TestPathPrefixes:
    """Every endpoint should be under /v1/ or in the explicit root list."""

    def test_only_v1_or_root(self, spec: dict[str, Any]) -> None:
        offenders: list[str] = []
        for path in spec["paths"]:
            if path in _ROOT_LEVEL_PATHS:
                continue
            if path.startswith("/v1/"):
                continue
            offenders.append(path)
        assert offenders == [], (
            f"non-/v1/ public paths found in OpenAPI snapshot: {offenders!r}"
        )


def _walk(node: Any, path: tuple[str, ...] = ()) -> list[tuple[tuple[str, ...], str]]:
    """Yield ``(json_path, ref_target)`` for every ``$ref`` string."""
    refs: list[tuple[tuple[str, ...], str]] = []
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "$ref" and isinstance(value, str):
                refs.append((path, value))
            else:
                refs.extend(_walk(value, path + (str(key),)))
    elif isinstance(node, list):
        for idx, value in enumerate(node):
            refs.extend(_walk(value, path + (str(idx),)))
    return refs


class TestRefResolution:
    def test_all_refs_resolve(self, spec: dict[str, Any]) -> None:
        defined = set(spec.get("components", {}).get("schemas", {}).keys())
        offenders: list[tuple[tuple[str, ...], str]] = []
        for path, ref in _walk(spec):
            match = _REF_RE.match(ref)
            if match is None:
                # External refs aren't expected in this snapshot.
                offenders.append((path, ref))
                continue
            if match.group(1) not in defined:
                offenders.append((path, ref))
        assert offenders == [], f"unresolved $refs in snapshot: {offenders!r}"


class TestSchemas:
    """Schema-level invariants over ``components.schemas``."""

    # FastAPI generates these automatically from multipart bodies / Pydantic
    # validation; their descriptions are owned by the framework, not us.
    _AUTO_GENERATED = {
        "HTTPValidationError",
        "ValidationError",
    }
    _AUTO_PREFIX = "Body_"

    def test_every_user_schema_has_description(self, spec: dict[str, Any]) -> None:
        """T4.3 partial: user-defined Pydantic models must carry a docstring.

        Pydantic lifts the class docstring into ``components.schemas[X].
        description``, so a missing docstring means the snapshot ships
        with an opaque schema. Skips FastAPI's auto-generated ``Body_*``
        + ``ValidationError`` entries, which are framework-owned.
        """
        offenders: list[str] = []
        for name, schema in spec.get("components", {}).get("schemas", {}).items():
            if name in self._AUTO_GENERATED:
                continue
            if name.startswith(self._AUTO_PREFIX):
                continue
            if not schema.get("description"):
                offenders.append(name)
        assert offenders == [], (
            "user-defined schemas missing description: " + ", ".join(offenders)
        )

    def test_every_user_schema_field_has_description(self, spec: dict[str, Any]) -> None:
        """T4.3 close (field-level): every property in user schemas has a description.

        Catches future regressions where a new ``Field(...)`` lands without
        a ``description=`` kwarg or a bare type annotation slips in (e.g.
        ``foo: int`` instead of ``foo: int = Field(..., description=...)``).
        Same scope rules as the schema-level test: framework-owned
        ``Body_*`` + ``ValidationError`` entries are skipped.
        """
        offenders: list[str] = []
        for name, schema in spec.get("components", {}).get("schemas", {}).items():
            if name in self._AUTO_GENERATED:
                continue
            if name.startswith(self._AUTO_PREFIX):
                continue
            for prop_name, prop_def in schema.get("properties", {}).items():
                if not isinstance(prop_def, dict):
                    continue
                if not prop_def.get("description"):
                    offenders.append(f"{name}.{prop_name}")
        assert offenders == [], (
            "user-schema properties missing description: " + ", ".join(offenders)
        )


class TestOperations:
    def test_every_v1_operation_has_description(self, spec: dict[str, Any]) -> None:
        """T4.3 partial: every public op carries human-readable prose.

        FastAPI lifts the route handler's docstring into the OpenAPI
        ``description`` field. Missing prose means the snapshot ships
        without the rationale a downstream consumer needs to understand
        the endpoint. Pinned here so future additions cannot regress.
        """
        offenders: list[str] = []
        for path, item in spec["paths"].items():
            if not isinstance(item, dict):
                continue
            if not path.startswith("/v1/"):
                continue
            for method, operation in item.items():
                if method not in _OPERATION_KEYS or not isinstance(operation, dict):
                    continue
                if not operation.get("description"):
                    offenders.append(f"{method.upper()} {path}")
        assert offenders == [], (
            "v1 operations missing description: " + ", ".join(offenders)
        )

    def test_every_v1_query_or_header_param_has_description(
        self, spec: dict[str, Any]
    ) -> None:
        """T4.3 close (query-param level): every v1 query/header parameter is documented.

        Path parameters are deliberately excluded: they are
        self-documenting from the URL template (``{eval_id}``,
        ``{job_id}``, ...). Catches future regressions where a new
        ``Query(default=...)`` or ``Header(default=...)`` lands without
        a ``description=`` kwarg.
        """
        offenders: list[str] = []
        for path, item in spec["paths"].items():
            if not isinstance(item, dict):
                continue
            if not path.startswith("/v1/"):
                continue
            for method, operation in item.items():
                if method not in _OPERATION_KEYS or not isinstance(operation, dict):
                    continue
                for param in operation.get("parameters", []):
                    if param.get("in") not in ("query", "header"):
                        continue
                    if not param.get("description"):
                        offenders.append(
                            f"{method.upper()} {path} :: {param.get('name')} "
                            f"({param.get('in')})"
                        )
        assert offenders == [], (
            "v1 query/header parameters missing description: " + ", ".join(offenders)
        )

    def test_every_operation_has_responses(self, spec: dict[str, Any]) -> None:
        offenders: list[str] = []
        for path, item in spec["paths"].items():
            if not isinstance(item, dict):
                continue
            for method, operation in item.items():
                if method not in _OPERATION_KEYS:
                    continue
                if not isinstance(operation, dict):
                    continue
                responses = operation.get("responses", {})
                if not responses:
                    offenders.append(f"{method.upper()} {path}")
        assert offenders == [], (
            "operations missing responses block: " + ", ".join(offenders)
        )

    def test_problem_json_present_on_4xx_5xx(self, spec: dict[str, Any]) -> None:
        """T4.3 sanity: every 4xx/5xx response advertises problem+json."""
        # Counter-style — at least 90% of error responses carry the
        # canonical content type. A handful of FastAPI-generated 422 paths
        # for path-only routes legitimately have no body declared, so we
        # don't assert strict 100% coverage.
        seen = 0
        with_problem = 0
        for path_item in spec["paths"].values():
            if not isinstance(path_item, dict):
                continue
            for method, operation in path_item.items():
                if method not in _OPERATION_KEYS or not isinstance(operation, dict):
                    continue
                for code, response in operation.get("responses", {}).items():
                    if not code.startswith(("4", "5")):
                        continue
                    if not isinstance(response, dict):
                        continue
                    seen += 1
                    if "application/problem+json" in response.get("content", {}):
                        with_problem += 1
        assert seen > 0, "no 4xx/5xx responses in snapshot"
        coverage = with_problem / seen
        assert coverage >= 0.9, (
            f"problem+json coverage on error responses too low: "
            f"{with_problem}/{seen} = {coverage:.2%}"
        )
