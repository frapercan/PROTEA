"""The dev units' clock gate waits for a synchronised clock and never refuses.

``deploy/dev/protea-dev-clock-gate.sh`` runs as ExecStartPre of every dev
worker and of the dev API. This machine dual-boots and comes up with the
clock hours off until chronyd steps it, while job leases last 120 s, so the
gate holds a unit back for a bounded time. A non-zero exit would keep the
unit down, which is the failure the gate must never cause, so every path is
checked for exit 0.

``timedatectl`` is replaced by a fake on PATH that answers from a script, so
the tests control what "synchronised" means without touching the clock.
"""

from __future__ import annotations

import shutil
import subprocess
import time
from pathlib import Path

import pytest

GATE = Path(__file__).resolve().parents[1] / "deploy" / "dev" / "protea-dev-clock-gate.sh"
UNIT = "protea-dev-worker@protea.jobs.service"


def _fake_timedatectl(bin_dir: Path, answers: list[str]) -> None:
    """A timedatectl that prints answers[i] on its i-th call, then the last one."""
    state = bin_dir / "calls"
    state.write_text("0")
    cases = "\n".join(f"  {i}) echo {a} ;;" for i, a in enumerate(answers))
    script = bin_dir / "timedatectl"
    script.write_text(
        "#!/bin/bash\n"
        f'n=$(cat "{state}"); echo $((n + 1)) > "{state}"\n'
        'case "$n" in\n'
        f"{cases}\n"
        f"  *) echo {answers[-1]} ;;\n"
        "esac\n"
    )
    script.chmod(0o755)


def _run(tmp_path: Path, path: str, **env: str) -> subprocess.CompletedProcess[str]:
    runtime = tmp_path / "run"
    runtime.mkdir(exist_ok=True)
    return subprocess.run(
        ["/bin/bash", str(GATE), UNIT],
        env={"PATH": path, "XDG_RUNTIME_DIR": str(runtime), **env},
        capture_output=True,
        text=True,
        timeout=60,
    )


def _marker(tmp_path: Path) -> Path:
    return tmp_path / "run" / f"protea-dev-clock-gate.{UNIT}.unsynced"


@pytest.fixture()
def fake_bin(tmp_path: Path) -> Path:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    return bin_dir


def test_a_synchronised_clock_passes_at_once(tmp_path, fake_bin):
    _fake_timedatectl(fake_bin, ["yes"])
    started = time.monotonic()
    result = _run(tmp_path, f"{fake_bin}:/usr/bin:/bin")
    assert result.returncode == 0
    assert "clock=ok" in result.stderr
    assert time.monotonic() - started < 5


def test_it_holds_the_unit_until_the_clock_synchronises(tmp_path, fake_bin):
    _fake_timedatectl(fake_bin, ["no", "no", "yes"])
    result = _run(tmp_path, f"{fake_bin}:/usr/bin:/bin", PROTEA_DEV_CLOCK_WAIT="10")
    assert result.returncode == 0
    assert "holding the unit" in result.stderr
    assert "clock synchronised after" in result.stderr
    assert "clock=ok" in result.stderr


def test_a_good_clock_clears_an_earlier_give_up(tmp_path, fake_bin):
    _fake_timedatectl(fake_bin, ["yes"])
    _marker(tmp_path).parent.mkdir(exist_ok=True)
    _marker(tmp_path).touch()
    result = _run(tmp_path, f"{fake_bin}:/usr/bin:/bin")
    assert result.returncode == 0
    assert "clock=ok" in result.stderr
    assert not _marker(tmp_path).exists()


def test_a_clock_that_never_synchronises_still_starts_the_unit(tmp_path, fake_bin):
    _fake_timedatectl(fake_bin, ["no"])
    result = _run(tmp_path, f"{fake_bin}:/usr/bin:/bin", PROTEA_DEV_CLOCK_WAIT="1")
    assert result.returncode == 0
    assert "starting anyway" in result.stderr
    assert "clock=unsynced" in result.stderr
    assert _marker(tmp_path).exists()

    # Once per boot: the next start of the same unit does not wait again.
    started = time.monotonic()
    result = _run(tmp_path, f"{fake_bin}:/usr/bin:/bin", PROTEA_DEV_CLOCK_WAIT="30")
    assert result.returncode == 0
    assert "already waited the full budget" in result.stderr
    assert time.monotonic() - started < 5


@pytest.mark.parametrize("budget", ["abc", "09", "-5", "99999"])
def test_a_bad_budget_never_breaks_the_gate(tmp_path, fake_bin, budget):
    _fake_timedatectl(fake_bin, ["yes"])
    result = _run(tmp_path, f"{fake_bin}:/usr/bin:/bin", PROTEA_DEV_CLOCK_WAIT=budget)
    assert result.returncode == 0
    assert "clock=ok" in result.stderr


def test_without_timedatectl_the_clock_is_unchecked_and_the_unit_starts(tmp_path, fake_bin):
    # A PATH holding only what the gate needs besides timedatectl.
    for tool in ("timeout", "date", "sleep", "rm", "touch", "cat"):
        found = shutil.which(tool)
        assert found is not None
        (fake_bin / tool).symlink_to(found)
    result = _run(tmp_path, str(fake_bin))
    assert result.returncode == 0
    assert "clock=unchecked" in result.stderr
