"""The desktop development stack's disk gate (deploy/dev/protea-dev-disk-gate.sh).

Development shares the system disk with production (the author's decision of
2026-11-15: no new disk, no copies), so every dev unit runs this gate first and
must refuse, with exit status 78, when the free space falls below the floor.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

GATE = Path(__file__).resolve().parents[1] / "deploy" / "dev" / "protea-dev-disk-gate.sh"


def _run(root: Path, floor: str) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, "PROTEA_DEV_MIN_FREE_GB": floor}
    return subprocess.run(
        ["bash", str(GATE), str(root), "protea-dev-test"],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )


def test_starts_when_the_free_space_is_above_the_floor(tmp_path: Path) -> None:
    result = _run(tmp_path, "0")
    assert result.returncode == 0, result.stderr
    assert "disk=ok" in result.stdout


def test_refuses_with_78_below_the_floor(tmp_path: Path) -> None:
    result = _run(tmp_path, str(10**9))
    assert result.returncode == 78
    assert "below the floor" in result.stderr


def test_refuses_with_78_when_the_root_is_missing(tmp_path: Path) -> None:
    result = _run(tmp_path / "absent", "0")
    assert result.returncode == 78
    assert "does not exist" in result.stderr


def test_refuses_with_78_on_a_floor_that_is_not_a_number(tmp_path: Path) -> None:
    result = _run(tmp_path, "lots")
    assert result.returncode == 78
    assert "whole number" in result.stderr


def test_the_default_floor_is_100_gb() -> None:
    assert 'floor="${PROTEA_DEV_MIN_FREE_GB:-100}"' in GATE.read_text(encoding="utf-8")
