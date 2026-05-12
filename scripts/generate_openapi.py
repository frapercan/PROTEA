#!/usr/bin/env python
"""Regenerate ``docs/openapi.json`` (T4.6 spec drift gate).

Instantiates the FastAPI app with mocked infrastructure deps so the
script never needs a running Postgres / RabbitMQ / MinIO. The CI
workflow ``openapi-drift.yml`` runs this and compares the output to
the committed snapshot — any unintended API surface change shows up
as a git diff and fails the PR.

Output is deterministic: keys sorted, indent 2, trailing newline.
Re-running on the same code produces a byte-identical file.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch


def _generate_spec() -> dict[str, Any]:
    """Build the live ``app.openapi()`` blob with mocked side-effecty deps."""
    fake_settings = MagicMock(
        db_url="postgresql://fake/none",
        amqp_url="amqp://fake/none",
        allowed_origins=(),
        storage_backend="local",
        artifacts_dir=Path("/tmp/protea-openapi-fake"),
        admin_token="",
    )
    with (
        patch("protea.api.app.load_settings", return_value=fake_settings),
        patch("protea.api.app.build_session_factory", return_value=MagicMock()),
        patch("protea.api.app.load_benchmark_config", return_value=MagicMock()),
        patch(
            "protea.api.app.build_operation_registry",
            return_value=MagicMock(),
        ),
        patch("protea.api.app.assert_bearer_config"),
    ):
        from protea.api.app import create_app

        app = create_app(Path("/tmp/protea-openapi-fake"))
        spec = app.openapi()
    return spec


def main() -> int:
    spec = _generate_spec()
    repo_root = Path(__file__).resolve().parent.parent
    out_path = repo_root / "docs" / "openapi.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(spec, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    n_paths = len(spec.get("paths", {}))
    n_schemas = len(spec.get("components", {}).get("schemas", {}))
    size_kb = out_path.stat().st_size / 1024
    print(
        f"Wrote {out_path.relative_to(repo_root)} "
        f"({size_kb:,.1f} KB, {n_paths} paths, {n_schemas} schemas)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
