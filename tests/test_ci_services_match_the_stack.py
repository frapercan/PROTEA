"""Integration CI and the suite run the images the stack pins, by the same reference.

docker-compose.yml pins its services by tag and digest. The integration workflow
went on running ``pgvector/pgvector:pg16`` for two months after the stack moved
to pg18 on 2026-09-14, and the suite's own default named the pg18 tag with no
digest. A suite on another engine than the stack stays green through the
failures only the stack's engine has (the comment in ``tests/conftest.py`` tells
the case that crash-looped the stack). These compare the references as written,
so a bump that forgets one of the places fails here instead of drifting.
"""

from __future__ import annotations

from pathlib import Path

import yaml

from tests.conftest import _DEFAULT_PG_IMAGE

_REPO_ROOT = Path(__file__).resolve().parents[1]


def _stack_image(service: str) -> str:
    compose = yaml.safe_load((_REPO_ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
    return compose["services"][service]["image"]


def _ci_image(service: str) -> str:
    workflow = yaml.safe_load(
        (_REPO_ROOT / ".github" / "workflows" / "integration.yml").read_text(encoding="utf-8")
    )
    return workflow["jobs"]["integration"]["services"][service]["image"]


def test_the_stack_pins_postgres_by_digest() -> None:
    assert "@sha256:" in _stack_image("postgres")


def test_integration_ci_runs_the_stack_postgres() -> None:
    assert _ci_image("postgres") == _stack_image("postgres")


def test_the_suite_defaults_to_the_stack_postgres() -> None:
    assert _DEFAULT_PG_IMAGE == _stack_image("postgres")
