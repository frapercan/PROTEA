"""Tests for ``scripts/check_cutoff_guard.py``.

Covers the registry self-consistency / red-team layers (no args) and the
emitted-bundle verification layer (``--bundle``) added by F-EVAL-PROTOCOL.c.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]


def _load_guard() -> object:
    spec = importlib.util.spec_from_file_location(
        "check_cutoff_guard", _REPO_ROOT / "scripts" / "check_cutoff_guard.py",
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["check_cutoff_guard"] = module
    spec.loader.exec_module(module)
    return module


def _write_manifest(bundle: Path, payload: dict) -> Path:
    bundle.mkdir(parents=True, exist_ok=True)
    (bundle / "manifest.json").write_text(json.dumps(payload))
    return bundle


def test_registry_only_passes() -> None:
    guard = _load_guard()
    assert guard.main([]) == 0  # type: ignore[attr-defined]


def test_clean_bundle_passes(tmp_path: Path) -> None:
    guard = _load_guard()
    bundle = _write_manifest(
        tmp_path / "frozen-v227",
        {"cutoff": "v227", "obo_version": "releases/2025-07-22"},
    )
    assert guard.main(["--bundle", str(bundle)]) == 0  # type: ignore[attr-defined]


def test_future_obo_bundle_fails(tmp_path: Path) -> None:
    guard = _load_guard()
    bundle = _write_manifest(
        tmp_path / "frozen-v227-bad",
        {"cutoff": "v227", "obo_version": "releases/2026-01-23"},
    )
    assert guard.main(["--bundle", str(bundle)]) == 1  # type: ignore[attr-defined]


def test_missing_manifest_fails(tmp_path: Path) -> None:
    guard = _load_guard()
    assert guard.main(["--bundle", str(tmp_path / "nope")]) == 1  # type: ignore[attr-defined]


def test_manifest_path_accepted_directly(tmp_path: Path) -> None:
    guard = _load_guard()
    bundle = _write_manifest(
        tmp_path / "frozen-v226",
        {"cutoff": "v226", "obo_version": "releases/2025-03-16"},
    )
    manifest = bundle / "manifest.json"
    assert guard.main(["--bundle", str(manifest)]) == 0  # type: ignore[attr-defined]
